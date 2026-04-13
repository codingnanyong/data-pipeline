"""연결 ID, PostgresHelper, 소스 설정 (JJ / JJ2)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Tuple

from plugins.hooks.postgres_hook import PostgresHelper

PG_MATERIAL_JJ = "pg_material_jj"
PG_MATERIAL_JJ2 = "pg_material_jj2"
PG_TARGET_DW = "pg_jj_monitoring_dw"

SCHEMA_BRONZE = "bronze"
SOURCE_TABLE_TRIPLES: Tuple[Tuple[str, str], ...] = (
    ("public", "device"),
    ("public", "machine"),
    ("public", "sensor"),
)
TARGET_TABLE_TRIPLES: Tuple[Tuple[str, str], ...] = (
    ("bronze", "device"),
    ("bronze", "machine"),
    ("bronze", "sensor"),
)

jj_extract_postgres_helper = PostgresHelper(conn_id=PG_MATERIAL_JJ)
jj2_extract_postgres_helper = PostgresHelper(conn_id=PG_MATERIAL_JJ2)
load_postgres_helper = PostgresHelper(conn_id=PG_TARGET_DW)


@dataclass(frozen=True)
class MasterSourceConfig:
    """JJ / JJ2 공통 설정. strict=False 이면 연결·추출 실패 시 태스크는 성공·빈 결과."""

    label: str
    conn_id: str
    helper: PostgresHelper
    strict: bool
    connection_test_task_id: str
    xcom_on_ok: Tuple[Tuple[str, Any], ...] = ()
    xcom_on_fail: Tuple[Tuple[str, Any], ...] = ()


SOURCE_JJ = MasterSourceConfig(
    label="JJ",
    conn_id=PG_MATERIAL_JJ,
    helper=jj_extract_postgres_helper,
    strict=False,
    connection_test_task_id="postgres_jj_source_connection_test",
    xcom_on_ok=(("jj_ok", True),),
    xcom_on_fail=(
        ("jj_ok", False),
        ("jj_connection_status", "failed"),
        ("jj_table_status", "failed"),
    ),
)

SOURCE_JJ2 = MasterSourceConfig(
    label="JJ2",
    conn_id=PG_MATERIAL_JJ2,
    helper=jj2_extract_postgres_helper,
    strict=False,
    connection_test_task_id="postgres_jj2_source_connection_test",
    xcom_on_ok=(("jj2_ok", True),),
    xcom_on_fail=(
        ("jj2_ok", False),
        ("jj2_connection_status", "failed"),
        ("jj2_table_status", "failed"),
    ),
)

SOURCES: Tuple[MasterSourceConfig, ...] = (SOURCE_JJ, SOURCE_JJ2)
