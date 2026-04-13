import logging
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags.pipeline.monitoring.bronze.common.material_montrg_temperature_common import (
    get_company_start_date,
    extract_data,
    load_data,
    update_variable,
    get_postgres_helper,
    SkipTemperatureCollection,
    INDO_TZ,
    TARGET_POSTGRES_CONN_ID,
    COMPANIES,
    INCREMENTAL_WINDOW_MINUTES,
    INCREMENTAL_SAFE_LAG_MINUTES,
    floor_time_to_window,
    format_dt_no_tz,
    jakarta_now,
    get_variable_datetime,
)

# ────────────────────────────────────────────────────────────────
# 1️⃣ Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1),
}

INCREMENTAL_MAX_WINDOWS_PER_RUN = int(
    os.environ.get("TEMPERATURE_INCREMENTAL_MAX_WINDOWS", "72")
)
AUTO_CLAMP_INCREMENTAL_VARIABLE = (
    os.environ.get("TEMPERATURE_INCREMENTAL_AUTO_CLAMP_VARIABLE", "true").lower()
    in ("1", "true", "yes")
)


def variable_ahead_of_safe_window(
    increment_key: str, latest_end: datetime
) -> bool:
    last = get_variable_datetime(increment_key)
    if last is None:
        return False
    return last > latest_end


def process_one_window(
    company_cd: str,
    increment_key: str,
    source_conn_id: str,
    start_time: datetime,
    end_time: datetime,
) -> dict:
    start_str = format_dt_no_tz(start_time)
    end_str = format_dt_no_tz(end_time)
    current_time_indo = datetime.now(INDO_TZ)

    logging.info(f"📅 {company_cd} 데이터 수집:")
    logging.info(
        f"  - 현재 인도네시아 시간: {current_time_indo.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logging.info(f"  - 수집 범위: {start_str} ~ {end_str}")

    try:
        source_pg = get_postgres_helper(source_conn_id, label=company_cd)
        target_pg = get_postgres_helper(TARGET_POSTGRES_CONN_ID, label="target_dw")
    except SkipTemperatureCollection as e:
        logging.warning(
            f"⏭ {company_cd} - 연결 불가로 스킵(Variable을 {end_str}까지 진행): {e}"
        )
        update_variable(increment_key, end_str)
        return {
            "status": "skipped_connection",
            "company_cd": company_cd,
            "error": str(e),
            "message": f"{company_cd} DB 연결 불가 — 구간 데이터 미수집, 워터마크만 진행",
            "variable_updated": True,
            "end_time": end_str,
        }

    try:
        data, row_count = extract_data(source_pg, start_str, end_str, company_cd)
    except SkipTemperatureCollection as e:
        logging.warning(
            f"⏭ {company_cd} - 추출 단계 연결 불가로 스킵(Variable을 {end_str}까지 진행): {e}"
        )
        update_variable(increment_key, end_str)
        return {
            "status": "skipped_connection",
            "company_cd": company_cd,
            "error": str(e),
            "message": f"{company_cd} 소스 연결 불가 — 구간 데이터 미수집, 워터마크만 진행",
            "variable_updated": True,
            "end_time": end_str,
        }
    except Exception as e:
        logging.error(f"❌ {company_cd} - 데이터 추출 실패: {str(e)}")
        update_variable(increment_key, end_str)
        return {
            "status": "extraction_failed",
            "company_cd": company_cd,
            "error": str(e),
            "message": f"{company_cd} 데이터 추출 실패",
            "variable_updated": True,
        }

    if row_count > 0:
        try:
            extract_time = jakarta_now()
            loaded_rows = load_data(target_pg, data, extract_time, company_cd)
            logging.info(f"✅ {company_cd} 데이터 수집 완료: {loaded_rows} rows")
            update_variable(increment_key, end_str)
        except SkipTemperatureCollection as e:
            logging.warning(
                f"⏭ {company_cd} - 적재 단계 연결 불가로 스킵(Variable을 {end_str}까지 진행): {e}"
            )
            update_variable(increment_key, end_str)
            return {
                "status": "skipped_connection",
                "company_cd": company_cd,
                "error": str(e),
                "message": f"{company_cd} 타겟 연결 불가 — 구간 데이터 미수집, 워터마크만 진행",
                "variable_updated": True,
                "end_time": end_str,
            }
        except Exception as e:
            logging.error(f"❌ {company_cd} - 데이터 로딩 실패: {str(e)}")
            return {
                "status": "loading_failed",
                "company_cd": company_cd,
                "error": str(e),
                "message": f"{company_cd} 데이터 로딩 실패",
            }
        return {
            "status": "incremental_completed",
            "company_cd": company_cd,
            "start_time": start_str,
            "end_time": end_str,
            "rows_processed": loaded_rows,
            "extract_time": extract_time.isoformat(),
        }

    logging.info(
        f"⚠️ {company_cd} 수집할 데이터가 없습니다: {start_str} ~ {end_str}"
    )
    update_variable(increment_key, end_str)
    return {
        "status": "incremental_completed_no_data",
        "company_cd": company_cd,
        "start_time": start_str,
        "end_time": end_str,
        "rows_processed": 0,
        "message": "수집할 데이터가 없음",
    }


def process_company_incremental(company_cd: str, increment_key: str, source_conn_id: str) -> dict:
    """10분 윈도우 증분. Variable은 윈도우 끝(end)까지 완료됨을 기록."""

    wm = INCREMENTAL_WINDOW_MINUTES
    now_indo = datetime.now(INDO_TZ)
    safe_now = now_indo - timedelta(minutes=INCREMENTAL_SAFE_LAG_MINUTES)
    latest_end = floor_time_to_window(safe_now, wm) - timedelta(seconds=1)

    if variable_ahead_of_safe_window(increment_key, latest_end):
        if AUTO_CLAMP_INCREMENTAL_VARIABLE:
            logging.warning(
                f"⚠️ {company_cd} - Variable이 안전 윈도우보다 앞섬 → "
                f"{format_dt_no_tz(latest_end)} 로 클램프"
            )
            update_variable(increment_key, format_dt_no_tz(latest_end))
        else:
            logging.warning(
                f"⚠️ {company_cd} - Variable이 안전 윈도우보다 앞섬, 스킵 "
                f"(AUTO_CLAMP_INCREMENTAL_VARIABLE=false)"
            )
            return {
                "status": "skipped_variable_ahead",
                "company_cd": company_cd,
                "latest_end": format_dt_no_tz(latest_end),
            }

    raw_start = get_company_start_date(increment_key, company_cd, initial_date=True)
    if raw_start.tzinfo is None:
        raw_start = raw_start.replace(tzinfo=INDO_TZ)
    start_time = floor_time_to_window(raw_start, wm)

    if start_time > latest_end:
        logging.info(
            f"⚠️ {company_cd} - 처리할 데이터 없음: "
            f"start({start_time}) > latest_end({latest_end})"
        )
        update_variable(increment_key, format_dt_no_tz(latest_end))
        return {
            "status": "skipped_no_data",
            "company_cd": company_cd,
            "start_time": format_dt_no_tz(start_time),
            "end_time": format_dt_no_tz(latest_end),
            "extracted_count": 0,
            "loaded_count": 0,
        }

    results = []
    windows_done = 0
    cur_start = start_time

    while cur_start <= latest_end and windows_done < INCREMENTAL_MAX_WINDOWS_PER_RUN:
        cur_end = cur_start + timedelta(minutes=wm) - timedelta(seconds=1)
        if cur_end > latest_end:
            cur_end = latest_end
        if cur_start > cur_end:
            break

        r = process_one_window(
            company_cd, increment_key, source_conn_id, cur_start, cur_end
        )
        results.append(r)
        windows_done += 1

        if r.get("status") == "skipped_connection":
            # 연결이 끊긴 상태에서 backlog를 계속 돌면 타임아웃만 누적됨.
            # 사용자가 원한대로 "그 시간은 못 모았으니" 워터마크는 최신까지 진행.
            update_variable(increment_key, format_dt_no_tz(latest_end))
            r["fast_forwarded_to"] = format_dt_no_tz(latest_end)
            return {
                "status": "incremental_skipped_connection",
                "company_cd": company_cd,
                "windows_processed": windows_done,
                "last_result": r,
                "results": results,
            }

        if r.get("status") in ("loading_failed",):
            return {
                "status": "incremental_partial_failure",
                "company_cd": company_cd,
                "windows_processed": windows_done,
                "last_result": r,
                "results": results,
            }

        cur_start = cur_end + timedelta(seconds=1)
        cur_start = floor_time_to_window(cur_start, wm)

    return {
        "status": "incremental_batch_done",
        "company_cd": company_cd,
        "windows_processed": windows_done,
        "results": results,
    }


# ────────────────────────────────────────────────────────────────
# 6️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="material_warehouse_temperature_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/10 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "Material", "Warehouse", "Temperature", "Incremental"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("🚀 Temperature Incremental 시작 (10분 윈도우)"),
    )

    incremental_tasks = []
    for company in COMPANIES:
        task = PythonOperator(
            task_id=f"incremental_{company['code'].lower()}",
            python_callable=lambda comp=company: process_company_incremental(
                comp["code"], comp["increment_key"], comp["source_conn_id"]
            ),
        )
        incremental_tasks.append(task)

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("🎉 Temperature Incremental 완료"),
    )

    start >> incremental_tasks >> end
