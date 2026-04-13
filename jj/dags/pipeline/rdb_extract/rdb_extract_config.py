"""
RDB Extract Configuration (config-driven)
========================================
다수의 RDB 소스에서 테이블들을 추출하는 설정만 모아둔 모듈.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DB→DB 적재만 담당하므로 로컬 아카이브 경로는 사용하지 않습니다.

# 날짜 범위 정책 (UTC 기준)
UTC = timezone.utc
INITIAL_START_DATE = datetime(2026, 1, 1, tzinfo=UTC)  # Backfill 시작 날짜
DAYS_OFFSET_FOR_INCREMENTAL = 1  # Incremental: 오늘 - 1일까지
DAYS_OFFSET_FOR_BACKFILL = 2  # Backfill: 오늘 - 2일까지

# 타겟(적재) DB Connection ID (Airflow Connections)
TARGET_CONN_ID = "pg_jj_quality_dw"


@dataclass(frozen=True)
class SourceDB:
    """수집 대상 DB(소스) 정의."""

    source_id: str
    conn_id: str
    incremental_enabled: bool = True
    backfill_enabled: bool = True


@dataclass(frozen=True)
class TableExtract:
    """테이블(또는 뷰) 추출 정의.

    incremental_column:
    - 설정 시: WHERE {col} >= start AND {col} <= end 로 범위 추출
    - 미설정 시: 전체 추출(주의: 큰 테이블은 위험)
    """

    table: str  # 스키마 포함 (예: public.equip_status_hist)
    incremental_column: Optional[str] = None
    target_table: Optional[str] = None  # 미지정 시 source table과 동일
    conflict_columns: Optional[list[str]] = None  # upsert 키(없으면 append)


# ────────────────────────────────────────────────────────────────
# 예시: 다수 DB 소스들
# ────────────────────────────────────────────────────────────────
SOURCE_DBS: list[SourceDB] = [
    # 운영 중인 timescaledb 컨테이너를 가리키는 Connection ID로 맞추면 됨
    SourceDB(source_id="timescale_main", conn_id="pg_timescale_main", incremental_enabled=True, backfill_enabled=True),
]


# ────────────────────────────────────────────────────────────────
# 예시: ERD의 일부 테이블을 "증분 컬럼" 기준으로 추출
# 실제 테이블/컬럼명은 운영 DB 기준으로 수정하세요.
# ────────────────────────────────────────────────────────────────
TABLES: list[TableExtract] = [
    TableExtract(
        table="public.equip_status_hist",
        incremental_column="capture_time",
        target_table="bronze.equip_status_hist",
        conflict_columns=["id"],
    ),
    TableExtract(
        table="public.service_hist",
        incremental_column="create_time",
        target_table="bronze.service_hist",
        conflict_columns=["id"],
    ),
    TableExtract(
        table="public.alarm_hist",
        incremental_column="alarm_time",
        target_table="bronze.alarm_hist",
        conflict_columns=["id"],
    ),
]

