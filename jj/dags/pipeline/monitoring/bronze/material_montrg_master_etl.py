"""DAG: Material Warehouse master data (machine, device, sensor) — JJ + JJ2 → bronze."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from dags.pipeline.monitoring.bronze.common.material_montrg_master import (
    check_jj_source,
    check_jj2_source,
    check_target_dw,
    extract_jj_devices,
    extract_jj_machines,
    extract_jj_sensors,
    extract_jj2_devices,
    extract_jj2_machines,
    extract_jj2_sensors,
    load_devices,
    load_machines,
    load_sensors,
    validate_data_quality,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "material_warehouse_master_etl",
    default_args=default_args,
    description="Daily ETL for Material Warehouse master data (machine, device, sensor)",
    schedule_interval="@daily",
    catchup=False,
    tags=["JJ", "Material", "Warehouse", "Master"],
)

start_task = DummyOperator(task_id="start", dag=dag)
end_task = DummyOperator(task_id="end", dag=dag)

check_jj_source_task = PythonOperator(
    task_id="check_jj_source",
    python_callable=check_jj_source,
    dag=dag,
)
check_jj2_source_task = PythonOperator(
    task_id="check_jj2_source",
    python_callable=check_jj2_source,
    dag=dag,
)
check_target_dw_task = PythonOperator(
    task_id="check_target_dw",
    python_callable=check_target_dw,
    dag=dag,
)

extract_jj_machines_task = PythonOperator(
    task_id="extract_jj_machines",
    python_callable=extract_jj_machines,
    dag=dag,
)
extract_jj_devices_task = PythonOperator(
    task_id="extract_jj_devices",
    python_callable=extract_jj_devices,
    dag=dag,
)
extract_jj_sensors_task = PythonOperator(
    task_id="extract_jj_sensors",
    python_callable=extract_jj_sensors,
    dag=dag,
)

extract_jj2_machines_task = PythonOperator(
    task_id="extract_jj2_machines",
    python_callable=extract_jj2_machines,
    dag=dag,
)
extract_jj2_devices_task = PythonOperator(
    task_id="extract_jj2_devices",
    python_callable=extract_jj2_devices,
    dag=dag,
)
extract_jj2_sensors_task = PythonOperator(
    task_id="extract_jj2_sensors",
    python_callable=extract_jj2_sensors,
    dag=dag,
)

load_machines_task = PythonOperator(
    task_id="load_machines",
    python_callable=load_machines,
    dag=dag,
)
load_devices_task = PythonOperator(
    task_id="load_devices",
    python_callable=load_devices,
    dag=dag,
)
load_sensors_task = PythonOperator(
    task_id="load_sensors",
    python_callable=load_sensors,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_data_quality",
    python_callable=validate_data_quality,
    dag=dag,
)

# 시작 → 연결 검사 3개 병렬
start_task >> [check_jj_source_task, check_jj2_source_task, check_target_dw_task]

check_jj_source_task >> [
    extract_jj_machines_task,
    extract_jj_devices_task,
    extract_jj_sensors_task,
]
check_jj2_source_task >> [
    extract_jj2_machines_task,
    extract_jj2_devices_task,
    extract_jj2_sensors_task,
]

[
    extract_jj_machines_task,
    extract_jj2_machines_task,
    check_target_dw_task,
] >> load_machines_task
[
    extract_jj_devices_task,
    extract_jj2_devices_task,
    check_target_dw_task,
] >> load_devices_task
[
    extract_jj_sensors_task,
    extract_jj2_sensors_task,
    check_target_dw_task,
] >> load_sensors_task

load_machines_task >> load_devices_task >> load_sensors_task
load_sensors_task >> validate_task >> end_task
