"""소스별 추출 (JJ / JJ2)."""
from __future__ import annotations

import logging
from typing import Any

from .config import SOURCE_JJ, SOURCE_JJ2, MasterSourceConfig
from .entities import ENTITY_EXTRACT, EntityExtractSpec


def _resolve_sql(spec: EntityExtractSpec, cfg: MasterSourceConfig) -> str:
    if callable(spec.sql):
        return spec.sql(cfg.label)
    return spec.sql


def _execute_extract(
    cfg: MasterSourceConfig,
    sql: str,
    task_id: str,
    xcom_key: str,
    **context: Any,
):
    if cfg.strict:
        return cfg.helper.execute_query(
            sql=sql, task_id=task_id, xcom_key=xcom_key, **context
        )
    try:
        return cfg.helper.execute_query(
            sql=sql, task_id=task_id, xcom_key=xcom_key, **context
        )
    except Exception as e:
        logging.warning(
            "⚠️ [%s] %s failed: %s — source may be offline", cfg.label, task_id, e
        )
        return []


def _extract_for_source(cfg: MasterSourceConfig, entity_key: str, **context: Any):
    spec = ENTITY_EXTRACT[entity_key]
    sql = _resolve_sql(spec, cfg)
    task_id, xcom_key = spec.task_and_xcom[cfg.label]
    return _execute_extract(cfg, sql, task_id, xcom_key, **context)


def extract_jj_machines(**context: Any):
    return _extract_for_source(SOURCE_JJ, "machine", **context)


def extract_jj2_machines(**context: Any):
    return _extract_for_source(SOURCE_JJ2, "machine", **context)


def extract_jj_devices(**context: Any):
    return _extract_for_source(SOURCE_JJ, "device", **context)


def extract_jj2_devices(**context: Any):
    return _extract_for_source(SOURCE_JJ2, "device", **context)


def extract_jj_sensors(**context: Any):
    return _extract_for_source(SOURCE_JJ, "sensor", **context)


def extract_jj2_sensors(**context: Any):
    return _extract_for_source(SOURCE_JJ2, "sensor", **context)
