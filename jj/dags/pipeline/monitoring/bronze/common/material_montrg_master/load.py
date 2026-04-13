"""bronze 적재 (JJ + JJ2 XCom 병합)."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, List, Tuple

from .config import SCHEMA_BRONZE, SOURCES, load_postgres_helper
from .entities import ENTITY_EXTRACT


def _append_etl_time(row: Any, extract_time: datetime) -> Tuple[Any, ...]:
    if isinstance(row, (list, tuple)):
        return tuple(list(row) + [extract_time])
    return (*row, extract_time)


def _merge_pulls(
    ti: Any,
    entity_key: str,
    extract_time: datetime,
) -> List[Tuple[Any, ...]]:
    spec = ENTITY_EXTRACT[entity_key]
    out: List[Tuple[Any, ...]] = []
    for cfg in SOURCES:
        task_id = spec.task_and_xcom[cfg.label][0]
        rows = ti.xcom_pull(task_ids=task_id)
        if rows:
            logging.info(
                "📦 %s %s: %s records", cfg.label, entity_key, len(rows)
            )
        for row in rows or []:
            out.append(_append_etl_time(row, extract_time))
    return out


def load_machines(**context: Any) -> None:
    ti = context["task_instance"]
    merged = _merge_pulls(ti, "machine", datetime.utcnow())
    if not merged:
        logging.warning("No machine data to load")
        return
    columns = [
        "company_cd",
        "mach_id",
        "mach_kind",
        "mach_name",
        "descn",
        "orgn_cd",
        "loc_cd",
        "line_cd",
        "mline_cd",
        "op_cd",
        "upd_dt",
        "etl_extract_time",
    ]
    load_postgres_helper.insert_data(
        schema_name=SCHEMA_BRONZE,
        table_name="machine",
        data=merged,
        columns=columns,
        conflict_columns=["company_cd", "mach_id"],
    )
    logging.info("✅ %s total machine records loaded", len(merged))


def load_devices(**context: Any) -> None:
    ti = context["task_instance"]
    merged = _merge_pulls(ti, "device", datetime.utcnow())
    if not merged:
        logging.warning("No device data to load")
        return
    columns = [
        "company_cd",
        "mach_id",
        "device_id",
        "name",
        "descn",
        "upd_dt",
        "etl_extract_time",
    ]
    load_postgres_helper.insert_data(
        schema_name=SCHEMA_BRONZE,
        table_name="device",
        data=merged,
        columns=columns,
        conflict_columns=["company_cd", "device_id"],
    )
    logging.info("✅ %s total device records loaded", len(merged))


def load_sensors(**context: Any) -> None:
    ti = context["task_instance"]
    merged = _merge_pulls(ti, "sensor", datetime.utcnow())
    if not merged:
        logging.warning("No sensor data to load")
        return
    columns = [
        "company_cd",
        "mach_id",
        "device_id",
        "sensor_id",
        "name",
        "addr",
        "topic",
        "descn",
        "upd_dt",
        "etl_extract_time",
    ]
    load_postgres_helper.insert_data(
        schema_name=SCHEMA_BRONZE,
        table_name="sensor",
        data=merged,
        columns=columns,
        conflict_columns=["company_cd", "sensor_id"],
    )
    logging.info("✅ %s total sensor records loaded", len(merged))
