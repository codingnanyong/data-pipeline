from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.hooks.postgres_hook import PostgresHelper
from pipeline.rdb_extract.rdb_extract_config import TARGET_CONN_ID, TableExtract
from pipeline.rdb_extract.rdb_extract_utils import compact, get_last_extract, set_last_extract


def test_db_connection(conn_id: str, source_id: str, **_) -> dict:
    conn = BaseHook.get_connection(conn_id)
    logging.info(compact("🔌 DB 연결 확인:", source_id=source_id, host=conn.host, port=conn.port, schema=conn.schema))
    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.get_first("SELECT 1;")
    logging.info(compact("✅ DB 연결 OK:", source_id=source_id))
    return {"status": "ok", "source_id": source_id}


def copy_table_to_target(
    *,
    source_conn_id: str,
    source_id: str,
    table_cfg: TableExtract,
    end_dt: datetime,
    start_dt: Optional[datetime] = None,
    use_variable_start: bool = True,
    **_,
) -> dict:
    """Source(Postgres/Timescale) → Target(Postgres) 테이블 복사.

    - incremental_column이 있으면 범위로 읽어서 Target에 upsert/append
    - conflict_columns 지정 시 `PostgresHelper.insert_data(..., conflict_columns=...)`로 upsert
    """
    src_table = table_cfg.table
    inc_col = table_cfg.incremental_column
    tgt_table = table_cfg.target_table or src_table
    conflict_cols = table_cfg.conflict_columns

    if inc_col and use_variable_start and start_dt is None:
        start_dt = get_last_extract(source_id, src_table)

    if inc_col and start_dt and start_dt >= end_dt:
        raise AirflowSkipException(
            compact("✅ 최신 상태(스킵):", source_id=source_id, table=src_table, start=start_dt, end=end_dt)
        )

    hook = PostgresHook(postgres_conn_id=source_conn_id)
    where = ""
    params = {}
    if inc_col:
        if start_dt:
            where = f"WHERE {inc_col} >= %(start)s AND {inc_col} <= %(end)s"
            params = {"start": start_dt, "end": end_dt}
        else:
            where = f"WHERE {inc_col} <= %(end)s"
            params = {"end": end_dt}

    sql = f"SELECT * FROM {src_table} {where};"
    logging.info(
        compact(
            "📥 Copy:",
            source_id=source_id,
            source_table=src_table,
            target_table=tgt_table,
            incremental_column=inc_col,
            start=start_dt.isoformat() if start_dt else None,
            end=end_dt.isoformat(),
        )
    )

    # target helper (write)
    tgt_schema, tgt_name = tgt_table.split(".", 1) if "." in tgt_table else ("public", tgt_table)
    tgt = PostgresHelper(conn_id=TARGET_CONN_ID)

    row_count = 0
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        colnames = [d[0] for d in cur.description]
        while True:
            rows = cur.fetchmany(10_000)
            if not rows:
                break
            # target insert (column-order safe)
            tgt.insert_data(tgt_schema, tgt_name, list(rows), columns=colnames, conflict_columns=conflict_cols, chunk_size=10_000)
            row_count += len(rows)

    logging.info(compact("✅ Copy 완료:", source_id=source_id, table=src_table, rows=row_count))

    if inc_col:
        set_last_extract(source_id, src_table, end_dt)
        logging.info(compact("✅ Variable 업데이트:", source_id=source_id, table=src_table, last_extract=end_dt.isoformat()))

    return {
        "status": "success",
        "source_id": source_id,
        "source_table": src_table,
        "target_table": tgt_table,
        "rows": row_count,
        "start_dt": start_dt.isoformat() if start_dt else None,
        "end_dt": end_dt.isoformat(),
    }

