import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from plugins.hooks.oracle_hook import OracleHelper
from plugins.hooks.postgres_hook import PostgresHelper
from dags.pipeline.production.bronze.common.smp_ss_php_rst_raw_common import (
    parse_datetime,
    extract_data,
    load_data,
    update_variable,
    INDO_TZ,
    ORACLE_CONN_ID,
    POSTGRES_CONN_ID
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

# Database Configuration
INCREMENT_KEY = "last_extract_time_smp_ss_php_rst"

# ────────────────────────────────────────────────────────────────
# 2️⃣ Daily Incremental Collection
# ────────────────────────────────────────────────────────────────
def daily_incremental_collection_task(**kwargs) -> dict:
    """수집: orchestration에서 hourly로 트리거 시 '실행시점-1시간'까지, 그 외(수동 등)는 오늘 06:30:00까지"""
    oracle = OracleHelper(conn_id=ORACLE_CONN_ID)
    pg = PostgresHelper(conn_id=POSTGRES_CONN_ID)

    dag_run = kwargs.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    is_hourly = conf.get("hourly", False)

    if is_hourly:
        # hourly: 항상 현재 인도네시아 시간 기준으로 직전 1시간 끝을 사용
        now_indo = datetime.now(INDO_TZ)
        one_hour_ago = now_indo - timedelta(hours=1)
        target_end_date = one_hour_ago.replace(minute=0, second=0, microsecond=0)
        logging.info(f"📅 목표 종료 (hourly, now 기준): {target_end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        execution_date = kwargs.get('data_interval_end') or kwargs.get('execution_date')
        if execution_date:
            if execution_date.tzinfo is None:
                execution_date_utc = execution_date.replace(tzinfo=timezone.utc)
            else:
                execution_date_utc = execution_date.astimezone(timezone.utc)
            execution_date_indo = execution_date_utc.astimezone(INDO_TZ)
            logging.info(f"📅 실행 시간 (UTC): {execution_date_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            logging.info(f"📅 실행 시간 (인도네시아): {execution_date_indo.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            target_end_date = execution_date_indo.replace(hour=6, minute=30, second=0, microsecond=0)
        else:
            now_indo = datetime.now(INDO_TZ)
            target_end_date = now_indo.replace(hour=6, minute=30, second=0, microsecond=0)
            logging.info(f"⚠️ execution_date가 없어서 현재 시간 기준으로 계산")
    
    # 마지막 수집 시점 확인 (Variable 있으면 항상 (Variable+1초)~target 수집 → 전환 시 누락 없음)
    last_extract_time_str = Variable.get(INCREMENT_KEY, default_var=None)

    if last_extract_time_str:
        last_extract_time = parse_datetime(last_extract_time_str)

        # 안전하게 둘 다 인도네시아 시간대로 통일 (naive/aware 혼합 비교 방지)
        if last_extract_time.tzinfo is None:
            last_extract_time = last_extract_time.replace(tzinfo=INDO_TZ)
        else:
            last_extract_time = last_extract_time.astimezone(INDO_TZ)

        if target_end_date.tzinfo is None:
            target_end_date = target_end_date.replace(tzinfo=INDO_TZ)

        logging.info(f"📌 마지막 수집 시점: {last_extract_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # 마지막 수집 시점이 이미 목표 종료 시점 이후면 스킵
        if last_extract_time >= target_end_date:
            logging.info(f"⏭️ 이미 수집 완료: 마지막 수집 시점({last_extract_time.strftime('%Y-%m-%d %H:%M:%S')}) >= 목표 종료 시점({target_end_date.strftime('%Y-%m-%d %H:%M:%S')})")
            return {
                "status": "already_collected",
                "last_extract_time": last_extract_time.strftime("%Y-%m-%d %H:%M:%S"),
                "target_end_time": target_end_date.strftime("%Y-%m-%d %H:%M:%S"),
                "rows_processed": 0,
                "message": "이미 수집 완료된 구간"
            }
        
        # 마지막 수집 시점 + 1초부터 시작
        start_date = last_extract_time + timedelta(seconds=1)
        end_date = target_end_date
        logging.info(f"📅 수집 구간: {start_date.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_date.strftime('%Y-%m-%d %H:%M:%S')} (마지막 수집 시점부터)")
    else:
        if is_hourly:
            start_date = target_end_date - timedelta(hours=1)
            end_date = target_end_date
            logging.info(f"📅 수집 구간: {start_date.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_date.strftime('%Y-%m-%d %H:%M:%S')} (Variable 없음, 직전 1시간)")
        else:
            yesterday_indo = target_end_date - timedelta(days=1)
            start_date = yesterday_indo.replace(hour=6, minute=30, second=0, microsecond=0)
            end_date = target_end_date
            logging.info(f"📅 수집 구간: {start_date.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_date.strftime('%Y-%m-%d %H:%M:%S')} (Variable 없음, 전날 06:30부터)")
    
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"📅 데이터 수집 시작: {start_str} ~ {end_str}")
    logging.info(f"📊 처리 날짜: {start_date.strftime('%Y-%m-%d')}")
    
    # 데이터 추출 및 적재
    data, row_count = extract_data(oracle, start_str, end_str)
    
    if row_count > 0:
        extract_time = datetime.utcnow()
        load_data(pg, data, extract_time)
        logging.info(f"✅ 데이터 수집 완료: {row_count} rows")
        
        # Variable 업데이트 (종료 시간을 06:30:00으로 설정)
        update_variable(INCREMENT_KEY, end_str)
        
        return {
            "status": "daily_incremental_completed",
            "date": start_date.strftime("%Y-%m-%d"),
            "rows_processed": row_count,
            "start_time": start_str,
            "end_time": end_str,
            "extract_time": extract_time.isoformat()
        }
    else:
        logging.info(f"⚠️ 수집할 데이터가 없습니다: {start_str} ~ {end_str}")
        
        # Variable 업데이트 (데이터가 없어도 시간은 업데이트, 종료 시간을 06:30:00으로 설정)
        update_variable(INCREMENT_KEY, end_str)
        
        return {
            "status": "daily_incremental_completed_no_data",
            "date": start_date.strftime("%Y-%m-%d"),
            "rows_processed": 0,
            "start_time": start_str,
            "end_time": end_str,
            "message": "수집할 데이터가 없음"
        }

# ────────────────────────────────────────────────────────────────
# 3️⃣ DAG Definition
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="smp_ss_php_rst_raw_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # orchestration DAG에서 관리
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ","raw", "PHP", "bronze layer", "incremental", "production"]
) as dag:
    
    daily_collection = PythonOperator(
        task_id="daily_incremental_collection",
        python_callable=daily_incremental_collection_task,
        provide_context=True,
    )
    
    daily_collection

