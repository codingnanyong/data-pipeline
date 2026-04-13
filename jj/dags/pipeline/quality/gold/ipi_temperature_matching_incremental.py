"""IPI Temperature Matching Model Incremental DAG (Silver → Gold)
양품 및 불량 데이터와 온도 데이터를 매칭하여 Gold 레이어에 적재하는 DAG
Model: Temperature_matching_model
"""
import logging
from datetime import datetime, timedelta
from typing import Tuple
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from dags.pipeline.quality.gold.common.ipi_temperature_matching_common import (
    process_single_date,
    submit_spark_temperature_matching,
)


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=3),
}

INCREMENT_KEY = "ipi_temperature_matching_last_date"


# ════════════════════════════════════════════════════════════════
# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════

def prepare_incremental_target_date(**context):
    """DAG run 시작 시 Variable 기준으로 처리일 1회 고정 → pandas/Spark 동일 날짜."""
    start_date, end_date = get_processing_time_range(**context)
    if start_date is None or end_date is None:
        return None
    return {"date": start_date}


def get_processing_time_range(**context) -> Tuple[str, str]:
    """Airflow Variable에서 처리 시간 범위 가져오기 (Incremental: 1일치)"""
    last_date_str = None
    try:
        last_date_str = Variable.get(INCREMENT_KEY, default_var=None)
    except Exception:
        pass
    
    now_utc = datetime.utcnow()
    today_minus_1 = (now_utc - timedelta(days=1)).date()
    
    if last_date_str:
        try:
            last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
            target_date = last_date + timedelta(days=1)
        except Exception:
            target_date = today_minus_1
    else:
        target_date = today_minus_1
    
    if target_date > today_minus_1:
        logging.info(f"✅ 최신 상태입니다. 처리할 날짜가 없습니다. (target: {target_date}, max: {today_minus_1})")
        return None, None
    
    date_str = target_date.strftime('%Y-%m-%d')
    logging.info(f"📋 처리 날짜 범위 (Incremental): {date_str} (1일치, 최대: {today_minus_1})")
    return date_str, date_str


# ════════════════════════════════════════════════════════════════
# 3️⃣ Main ETL Logic
# ════════════════════════════════════════════════════════════════

def run_temperature_matching(**context) -> dict:
    """메인 ETL 함수 (증분 처리) - 기존 pandas 구현 (원본 테이블)"""
    info = context["ti"].xcom_pull(task_ids="prepare_incremental_target_date")
    if not info:
        logging.info("✅ 처리할 날짜가 없습니다. (이미 최신 상태)")
        return {"status": "success", "rows_processed": 0, "message": "Already up to date", "processed_date": None}

    start_date = info["date"]

    try:
        result = process_single_date(start_date)

        if result.get('status') == 'success':
            Variable.set(INCREMENT_KEY, start_date)
            logging.info(f"✅ Variable `{INCREMENT_KEY}` 업데이트: {start_date}")

        return result

    except Exception as e:
        logging.error(f"❌ Temperature Matching 실패: {str(e)}", exc_info=True)
        return {"status": "failed", "error": str(e)}


def run_spark_temperature_matching(**context) -> dict:
    """Spark 버전 ETL (병렬 검증) — pandas 증분과 동일 처리일, _spark 테이블만 적재."""
    info = context["ti"].xcom_pull(task_ids="prepare_incremental_target_date")
    if not info:
        logging.info(
            "✅ Spark 증분 스킵: 처리할 날짜 없음 (pandas와 동일 — 이미 최신). "
            "Livy 잡 미제출."
        )
        return {
            "status": "skipped",
            "message": "No incremental date; Livy batch not submitted",
        }

    target_date = info["date"]
    logging.info("🔥 Spark 증분 처리일 (pandas와 동일): %s", target_date)
    return submit_spark_temperature_matching(target_date=target_date, suffix="_spark")


# ════════════════════════════════════════════════════════════════
# 4️⃣ DAG Definition
# ════════════════════════════════════════════════════════════════

with DAG(
    dag_id="ipi_temperature_matching_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["JJ", "IP", "Quality", "Gold layer", "Incremental", "Temperature_matching_model"],
) as dag:

    prepare_incremental_task = PythonOperator(
        task_id="prepare_incremental_target_date",
        python_callable=prepare_incremental_target_date,
    )

    # 기존 pandas 구현 → gold.ipi_temperature_matching (원본, 변경 없음)
    temperature_matching_task = PythonOperator(
        task_id="run_temperature_matching",
        python_callable=run_temperature_matching,
    )

    # Spark 구현 → gold.ipi_temperature_matching_spark (병렬 검증용)
    spark_temperature_matching_task = PythonOperator(
        task_id="run_spark_temperature_matching",
        python_callable=run_spark_temperature_matching,
    )

    prepare_incremental_task >> [temperature_matching_task, spark_temperature_matching_task]

