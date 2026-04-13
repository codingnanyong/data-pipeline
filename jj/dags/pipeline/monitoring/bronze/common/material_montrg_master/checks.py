"""소스·타겟 DW 연결 검사."""
from __future__ import annotations

import logging
from typing import Any

from .config import (
    PG_TARGET_DW,
    SOURCE_JJ,
    SOURCE_JJ2,
    SOURCE_TABLE_TRIPLES,
    TARGET_TABLE_TRIPLES,
    MasterSourceConfig,
    load_postgres_helper,
)


def _check_source(cfg: MasterSourceConfig, **context: Any) -> None:
    """단일 소스 연결 + public 필수 테이블."""
    logging.info("🔍 [%s] PostgreSQL source (%s)...", cfg.label, cfg.conn_id)

    def _run_strict() -> None:
        result = cfg.helper.execute_query(
            sql="SELECT 1 AS test",
            task_id=cfg.connection_test_task_id,
            xcom_key=None,
            **context,
        )
        if not result:
            raise RuntimeError(f"[{cfg.label}] connection test returned no rows")
        for sch, tbl in SOURCE_TABLE_TRIPLES:
            if not cfg.helper.check_table(sch, tbl):
                raise RuntimeError(f"[{cfg.label}] source table '{sch}.{tbl}' missing")
        logging.info("✅ [%s] source connection and tables OK", cfg.label)

    if cfg.strict:
        _run_strict()
        return

    ti = context["task_instance"]
    try:
        _run_strict()
        for key, val in cfg.xcom_on_ok:
            ti.xcom_push(key=key, value=val)
    except Exception as e:
        logging.warning(
            "⚠️ [%s] unavailable (downstream extract will use empty data): %s",
            cfg.label,
            e,
        )
        for key, val in cfg.xcom_on_fail:
            ti.xcom_push(key=key, value=val)


def check_jj_source(**context: Any) -> None:
    _check_source(SOURCE_JJ, **context)


def check_jj2_source(**context: Any) -> None:
    _check_source(SOURCE_JJ2, **context)


def check_target_dw(**context: Any) -> None:
    logging.info("🔍 [Target] PostgreSQL DW (%s)...", PG_TARGET_DW)
    target_result = load_postgres_helper.execute_query(
        sql="SELECT 1 AS test",
        task_id="postgres_target_connection_test",
        xcom_key=None,
        **context,
    )
    if not target_result:
        raise RuntimeError("PostgreSQL target connection test failed")

    for sch, tbl in TARGET_TABLE_TRIPLES:
        if not load_postgres_helper.check_table(sch, tbl):
            raise RuntimeError(f"Target table '{sch}.{tbl}' does not exist")

    logging.info("✅ [Target] DW connection and bronze tables OK")
