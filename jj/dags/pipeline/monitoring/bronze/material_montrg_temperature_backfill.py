import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.monitoring.bronze.common.material_montrg_temperature_common import (
    get_company_start_date,
    get_day_end_date,
    calculate_expected_daily_loops,
    extract_data,
    load_data,
    update_variable,
    get_postgres_helper,
    SkipTemperatureCollection,
    INDO_TZ,
    TARGET_POSTGRES_CONN_ID,
    HOURS_OFFSET_FOR_INCREMENTAL,
    COMPANIES,
    jakarta_now,
)

# ────────────────────────────────────────────────────────────────
# 1️⃣ Configuration Constants
# ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)
}

# ────────────────────────────────────────────────────────────────
# 2️⃣ Main Backfill Logic
# ────────────────────────────────────────────────────────────────
def process_daily_batch_for_company(
    source_pg: PostgresHelper, 
    target_pg: PostgresHelper, 
    start_date: datetime, 
    end_date: datetime,
    company_cd: str,
    loop_count: int,
    expected_loops: int
) -> dict:
    """Process a single daily batch for specific company"""
    logging.info(f"🔄 루프 {loop_count}/{expected_loops} 시작 (Company: {company_cd})")
    
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"배치 처리 중: {start_str} ~ {end_str} (Company: {company_cd})")
    
    data, row_count = extract_data(source_pg, start_str, end_str, company_cd)
    
    if row_count > 0:
        extract_time = jakarta_now()
        loaded_rows = load_data(target_pg, data, extract_time, company_cd)
        logging.info(f"✅ 배치 완료: {start_str} ~ {end_str} ({loaded_rows} rows) for {company_cd}")
    else:
        logging.info(f"배치에 데이터 없음: {start_str} ~ {end_str} (Company: {company_cd})")
    
    return {
        "loop": loop_count,
        "company_cd": company_cd,
        "start": start_str,
        "end": end_str,
        "row_count": row_count,
        "date": start_date.strftime("%Y-%m-%d")
    }

def backfill_company_task(company_config: dict) -> dict:
    """Backfill task for a company"""
    return process_company_backfill(
        company_config['code'],
        company_config['increment_key'],
        company_config['source_conn_id']
    )

def process_company_backfill(company_cd: str, increment_key: str, source_conn_id: str) -> dict:
    """Process backfill for a single company"""
    try:
        source_pg = get_postgres_helper(source_conn_id, label=company_cd)
        target_pg = get_postgres_helper(TARGET_POSTGRES_CONN_ID, label="target_dw")
    except SkipTemperatureCollection as e:
        logging.warning(
            f"⏭ {company_cd} Backfill — DB 연결 불가로 전체 스킵(Variable 유지): {e}"
        )
        return {
            "status": "skipped_connection",
            "company_cd": company_cd,
            "error": str(e),
            "results": [],
        }
    
    # Calculate end date (인도네시아 시간 기준, 2시간 전까지)
    # 예: KST 16:11 -> INDO 14:11 -> 2시간 전 = 12:11 -> 11:59:59까지 수집
    current_time_indo = datetime.now(INDO_TZ)
    safe_time = current_time_indo - timedelta(hours=HOURS_OFFSET_FOR_INCREMENTAL)
    # 이전 시간의 마지막 초로 설정 (예: 12:11 -> 11:59:59)
    end_date = safe_time.replace(minute=0, second=0, microsecond=0) - timedelta(seconds=1)
    
    logging.info(f"🔄 {company_cd} 처리 시작")
    logging.info(f"⏰ 현재 인도네시아 시간: {current_time_indo.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"⏰ 안전 마진 적용 후: {safe_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"⏰ 수집 종료 시점: {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get company-specific start date (backfill mode - no initial_date)
    start_date = get_company_start_date(increment_key, company_cd)
    
    # Set timezone if not set
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=INDO_TZ)
    
    # Calculate expected loops for this company
    expected_loops = calculate_expected_daily_loops(start_date, end_date)
    
    logging.info(f"{company_cd} 시작: {start_date} ~ {end_date}")
    logging.info(f"{company_cd} 예상 루프: {expected_loops}회 (일별)")
    logging.info(f"⚠️ 인도네시아 시간 기준 {HOURS_OFFSET_FOR_INCREMENTAL}시간 전까지 수집")
    
    # Process daily batches for this company
    company_results = []
    loop_count = 0
    current_date = start_date
    
    while current_date <= end_date:
        loop_count += 1
        
        # Calculate day end date
        day_end = get_day_end_date(current_date)
        if day_end > end_date:
            day_end = end_date
        
        try:
            batch_result = process_daily_batch_for_company(
                source_pg, target_pg, current_date, day_end, company_cd, loop_count, expected_loops
            )
            company_results.append(batch_result)
            logging.info(f"✅ {company_cd} 루프 {loop_count} 완료: {batch_result['row_count']:,} rows")
            
            # Update variable after each successful batch
            update_variable(increment_key, batch_result['end'])
            
        except SkipTemperatureCollection as e:
            logging.warning(
                f"⏭ {company_cd} 루프 {loop_count} 연결 불가로 중단(Variable 유지): {e}"
            )
            break
        except Exception as e:
            logging.error(f"❌ {company_cd} 루프 {loop_count} 실패: {str(e)}")
        
        # Move to next day
        current_date = current_date + timedelta(days=1)
    
    total_rows = sum([r['row_count'] for r in company_results])
    logging.info(f"🎉 {company_cd} 완료! {len(company_results)}회 루프, {total_rows:,}개 rows")
    
    return {
        "status": "backfill_completed",
        "company_cd": company_cd,
        "total_batches": len(company_results),
        "total_rows": total_rows,
        "results": company_results
    }

# ────────────────────────────────────────────────────────────────
# 3️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="material_warehouse_temperature_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "Material", "Warehouse" "Temperature", "Telemetry", "Backfill"]
) as dag:
    
    # Start task
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("🚀 Temperature 데이터 Backfill 시작"),
    )
    
    # Company-specific tasks (parallel execution) - 동적 생성
    backfill_tasks = []
    for company in COMPANIES:
        task = PythonOperator(
            task_id=f"backfill_{company['code'].lower()}",
            python_callable=lambda comp=company, **kwargs: process_company_backfill(
                comp['code'], comp['increment_key'], comp['source_conn_id']
            ),
            provide_context=True,
        )
        backfill_tasks.append(task)
    
    # End task
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("🎉 Temperature 데이터 Backfill 완료"),
    )
    
    # Task dependencies - All companies run in parallel
    start >> backfill_tasks >> end
