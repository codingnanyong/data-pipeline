"""
RDB Extract DAGs (config-driven)
================================

패턴:
- config에서 SOURCE_DBS, TABLES만 추가하면 자동 확장
- source(DB) 단위로 TaskGroup을 만들고, 그 안에 table 단위 태스크를 생성

결과:
- 로컬 CSV 아카이브: ${RDB_EXTRACT_BASE_PATH:-/media/btx/rdb_extract}/{dataset}/{source_id}/{schema}/{table}/{run_ts}.csv
- 증분 상태: Airflow Variable `last_extract_time__{source_id}__{dataset}__{schema__table}`
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from pipeline.rdb_extract.rdb_extract_config import (
    DEFAULT_ARGS,
    DAYS_OFFSET_FOR_BACKFILL,
    DAYS_OFFSET_FOR_INCREMENTAL,
    INITIAL_START_DATE,
    SOURCE_DBS,
    TABLES,
)
from pipeline.rdb_extract.rdb_extract_tasks import copy_table_to_target, test_db_connection


def incremental_end_dt() -> datetime:
    now = datetime.now(timezone.utc)
    end = (now - timedelta(days=DAYS_OFFSET_FOR_INCREMENTAL)).replace(hour=23, minute=59, second=59, microsecond=0)
    logging.info(f"📅 Incremental end_dt(UTC): {end.isoformat()}")
    return end


def backfill_range() -> dict:
    now = datetime.now(timezone.utc)
    end = (now - timedelta(days=DAYS_OFFSET_FOR_BACKFILL)).replace(hour=23, minute=59, second=59, microsecond=0)
    start = INITIAL_START_DATE
    logging.info(f"📅 Backfill range(UTC): {start.isoformat()} ~ {end.isoformat()}")
    return {"start": start, "end": end}


def build_source_group(dag: DAG, *, source_id: str, conn_id: str, end_dt_task_id: str, enabled: bool) -> TaskGroup:
    tg = TaskGroup(group_id=f"src__{source_id}", dag=dag)

    if not enabled:
        DummyOperator(task_id="disabled", task_group=tg)
        return tg

    t_test = PythonOperator(
        task_id="test_connection",
        python_callable=test_db_connection,
        op_kwargs={"conn_id": conn_id, "source_id": source_id},
        task_group=tg,
    )

    # 테이블별 extract
    table_tasks = []
    for t in TABLES:
        task_id = f"copy__{t.table.replace('.', '__')}"
        task = PythonOperator(
            task_id=task_id,
            python_callable=_make_copy_callable(conn_id, source_id, t, end_dt_task_id),
            task_group=tg,
        )
        t_test >> task
        table_tasks.append(task)

    return tg


def _make_copy_callable(conn_id: str, source_id: str, table_cfg, end_dt_task_id: str):
    def _run(**kwargs):
        ti = kwargs["ti"]
        end_dt = ti.xcom_pull(task_ids=end_dt_task_id)
        if isinstance(end_dt, str):
            end_dt = datetime.fromisoformat(end_dt)
        return copy_table_to_target(source_conn_id=conn_id, source_id=source_id, table_cfg=table_cfg, end_dt=end_dt)

    return _run


# ────────────────────────────────────────────────────────────────
# Incremental
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="rdb_extract_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    tags=["pipeline", "extract", "rdb", "incremental"],
    max_active_runs=1,
) as dag_inc:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    t_end = PythonOperator(
        task_id="compute_end_dt",
        python_callable=lambda: incremental_end_dt().isoformat(),
    )

    start >> t_end

    groups = []
    for src in SOURCE_DBS:
        g = build_source_group(
            dag_inc,
            source_id=src.source_id,
            conn_id=src.conn_id,
            end_dt_task_id="compute_end_dt",
            enabled=src.incremental_enabled,
        )
        t_end >> g >> end
        groups.append(g)


# ────────────────────────────────────────────────────────────────
# Backfill (manual)
# ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="rdb_extract_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["pipeline", "extract", "rdb", "backfill"],
    max_active_runs=1,
) as dag_bf:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    t_range = PythonOperator(task_id="compute_range", python_callable=backfill_range)

    def _end_only(**kwargs):
        r = kwargs["ti"].xcom_pull(task_ids="compute_range") or {}
        end_dt = r.get("end")
        if isinstance(end_dt, str):
            return end_dt
        return end_dt.isoformat() if end_dt else None

    t_end = PythonOperator(task_id="end_dt", python_callable=_end_only)

    start >> t_range >> t_end

    for src in SOURCE_DBS:
        g = build_source_group(
            dag_bf,
            source_id=src.source_id,
            conn_id=src.conn_id,
            end_dt_task_id="end_dt",
            enabled=src.backfill_enabled,
        )
        t_end >> g >> end

