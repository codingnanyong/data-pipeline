"""추출 SQL·엔티티별 Airflow task_id / XCom 키 매핑."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Tuple, Union

SqlBuilder = Union[str, Callable[[str], str]]


@dataclass(frozen=True)
class EntityExtractSpec:
    """엔티티 하나에 대해 소스별 Airflow task_id / xcom_key."""

    sql: SqlBuilder
    task_and_xcom: Dict[str, Tuple[str, str]]


def _sql_machine(company_cd: str) -> str:
    return f"""
        SELECT
            '{company_cd}' AS company_cd,
            mach_id, mach_kind, mach_name, descn, orgn_cd, loc_cd,
            line_cd, mline_cd, op_cd, upd_dt
        FROM public.machine
    """


_SQL_DEVICE = """
    SELECT company_cd, mach_id, device_id, name, descn, upd_dt
    FROM public.device
"""

_SQL_SENSOR = """
    SELECT
        company_cd, mach_id, device_id, sensor_id, name, addr, topic,
        descn, upd_dt
    FROM public.sensor
"""

ENTITY_EXTRACT: Dict[str, EntityExtractSpec] = {
    "machine": EntityExtractSpec(
        sql=_sql_machine,
        task_and_xcom={
            "JJ": ("extract_jj_machines", "jj_machine_data"),
            "JJ2": ("extract_jj2_machines", "jj2_machine_data"),
        },
    ),
    "device": EntityExtractSpec(
        sql=_SQL_DEVICE,
        task_and_xcom={
            "JJ": ("extract_jj_devices", "jj_device_data"),
            "JJ2": ("extract_jj2_devices", "jj2_device_data"),
        },
    ),
    "sensor": EntityExtractSpec(
        sql=_SQL_SENSOR,
        task_and_xcom={
            "JJ": ("extract_jj_sensors", "jj_sensor_data"),
            "JJ2": ("extract_jj2_sensors", "jj2_sensor_data"),
        },
    ),
}
