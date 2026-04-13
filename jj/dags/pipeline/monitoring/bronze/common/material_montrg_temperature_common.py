"""공통 함수 모듈 - Material Monitoring Temperature Raw"""
import errno
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from plugins.hooks.postgres_hook import PostgresHelper
from airflow.models import Variable


class SkipTemperatureCollection(Exception):
    """소스/타겟 DB에 연결할 수 없을 때. 워터마크(Variable)를 진행시키지 말 것."""


# ════════════════════════════════════════════════════════════════
# 1️⃣ Configuration Constants
# ════════════════════════════════════════════════════════════════

# Default Configuration
TABLE_NAME = "temperature_raw"
SCHEMA_NAME = "bronze"
TARGET_POSTGRES_CONN_ID = "pg_jj_monitoring_dw"
INDO_TZ = timezone(timedelta(hours=7))  # 인도네시아 시간 (UTC+7)
INITIAL_START_DATE = datetime(2025, 8, 1, 0, 0, 0)
HOURS_OFFSET_FOR_INCREMENTAL = 2  # (backfill에서 사용) 2시간 전 데이터까지 수집
INCREMENTAL_WINDOW_MINUTES = 10  # 증분 수집 윈도우 (분 단위)
INCREMENTAL_SAFE_LAG_MINUTES = 0  # (incremental에서 사용) 현재 시각 대비 안전 마진(분)

# Company Configuration - 배열로 관리
COMPANIES = [
    {
        "code": "JJ",
        "increment_key": "last_extract_time_temperature_raw_jj",
        "source_conn_id": "pg_material_jj"
    },
    {
        "code": "JJ2",
        "increment_key": "last_extract_time_temperature_raw_jj2",
        "source_conn_id": "pg_material_jj2"
    }
]


# ════════════════════════════════════════════════════════════════
# 2️⃣ Utility Functions
# ════════════════════════════════════════════════════════════════

def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string with microsecond support"""
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


def get_day_end_date(start_date: datetime) -> datetime:
    """Get the end of the day for a given date"""
    return start_date.replace(hour=23, minute=59, second=59, microsecond=999999)


def calculate_expected_daily_loops(start_date: datetime, end_date: datetime) -> int:
    """Calculate expected number of daily loops"""
    return (end_date.date() - start_date.date()).days + 1


def _is_db_connection_error(exc: BaseException) -> bool:
    """연결/네트워크 계열 오류인지 판별 (쿼리 문법 오류 등과 구분)."""
    t = type(exc)
    name = t.__name__
    mod = getattr(t, "__module__", "") or ""
    if "psycopg2" in mod or "psycopg" in mod:
        if name in ("OperationalError", "InterfaceError"):
            return True
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) is not None:
        if exc.errno in (
            errno.ECONNREFUSED,
            errno.ETIMEDOUT,
            errno.ENETUNREACH,
            errno.EHOSTUNREACH,
            errno.ECONNRESET,
        ):
            return True
    msg = str(exc).lower()
    needles = (
        "could not connect",
        "connection refused",
        "connection timed out",
        "timeout expired",
        "server closed the connection",
        "connection reset",
        "connection lost",
        "connection to server",
        "no route to host",
        "name or service not known",
        "could not translate host name",
        "broken pipe",
        "ssl syscall error",
        "network is unreachable",
    )
    return any(n in msg for n in needles)


def get_postgres_helper(conn_id: str, label: str = "") -> PostgresHelper:
    """PostgresHelper 생성 시 훅이 즉시 연결 검증하는 경우 연결 실패면 SkipTemperatureCollection."""
    try:
        return PostgresHelper(conn_id=conn_id)
    except Exception as e:
        if _is_db_connection_error(e):
            tag = label or conn_id
            logging.warning(f"⏭ {tag} DB 연결 불가 — 수집 스킵: {e}")
            raise SkipTemperatureCollection(str(e)) from e
        raise


def floor_time_to_window(dt: datetime, window_minutes: int) -> datetime:
    """내림: dt를 window_minutes 단위로 내림(초/마이크로초 0)."""
    if window_minutes <= 0 or 60 % window_minutes != 0:
        raise ValueError(f"window_minutes must divide 60: {window_minutes}")
    floored_minute = (dt.minute // window_minutes) * window_minutes
    return dt.replace(minute=floored_minute, second=0, microsecond=0)


def format_dt_no_tz(dt: datetime) -> str:
    """Airflow Variable 저장 포맷(타임존 제거, 초 단위)."""
    return dt.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def jakarta_now() -> datetime:
    """적재 etl_extract_time: Asia/Jakarta timezone-aware."""
    return datetime.now(INDO_TZ)


def localize_source_ts_for_insert(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=INDO_TZ)
        return value
    return value


def get_variable_datetime(key: str) -> Optional[datetime]:
    """Airflow Variable을 datetime으로 읽기 (없으면 None)."""
    raw = Variable.get(key, default_var=None)
    if not raw:
        return None
    dt = parse_datetime(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=INDO_TZ)
    return dt


def get_company_start_date(increment_key: str, company_name: str, initial_date: datetime = None) -> datetime:
    """Get start date for specific company
    
    Args:
        increment_key: Variable key for last extract time
        company_name: Company code (e.g., 'JJ', 'JJ2')
        initial_date: Initial start date if variable doesn't exist (for incremental mode)
    """
    last_time_str = Variable.get(increment_key, default_var=None)
    if last_time_str:
        last_time = parse_datetime(last_time_str)
        # Set timezone if not set
        if last_time.tzinfo is None:
            last_time = last_time.replace(tzinfo=INDO_TZ)
        # 마지막 수집 시간의 다음 초부터 시작
        start_time = last_time + timedelta(seconds=1)
        logging.info(f"{company_name} - 마지막 추출 시간: {last_time}")
        logging.info(f"{company_name} - 다음 수집 시작 시간: {start_time}")
        return start_time
    else:
        if initial_date:
            # Incremental mode: use the previous full window if no variable
            now_indo = datetime.now(INDO_TZ)
            safe_now = now_indo - timedelta(minutes=INCREMENTAL_SAFE_LAG_MINUTES)
            start_time = floor_time_to_window(
                safe_now - timedelta(minutes=INCREMENTAL_WINDOW_MINUTES),
                INCREMENTAL_WINDOW_MINUTES,
            )
            logging.info(
                f"{company_name} - Variable이 없어 직전 {INCREMENTAL_WINDOW_MINUTES}분 구간("
                f"{start_time.strftime('%Y-%m-%d %H:%M:00')})부터 수집"
            )
            return start_time
        else:
            # Backfill mode: use INITIAL_START_DATE
            logging.info(f"{company_name} - 초기 시작 날짜 사용: {INITIAL_START_DATE}")
            return INITIAL_START_DATE


# ════════════════════════════════════════════════════════════════
# 3️⃣ Data Extraction
# ════════════════════════════════════════════════════════════════

def build_extract_sql(start_date: str, end_date: str) -> str:
    """Build SQL query for data extraction"""
    return f'''
        SELECT 
            ymd,
            hmsf,
            sensor_id,
            device_id,
            capture_dt,
            t1,
            t2,
            t3,
            t4,
            t5,
            t6,
            upload_yn,
            upload_dt
        FROM public.temperature
        WHERE capture_dt >= '{start_date}'::timestamp 
          AND capture_dt <= '{end_date}'::timestamp
        ORDER BY capture_dt
    '''


def extract_data(pg: PostgresHelper, start_date: str, end_date: str, source_name: str) -> tuple:
    """Extract data from PostgreSQL database"""
    sql = build_extract_sql(start_date, end_date)
    logging.info(f"{source_name} - 실행 쿼리: {sql}")

    try:
        data = pg.execute_query(sql, task_id=f"extract_data_{source_name}", xcom_key=None)
    except Exception as e:
        if _is_db_connection_error(e):
            logging.warning(
                f"⏭ {source_name} 소스 DB 연결/네트워크 문제로 추출 스킵: {e}"
            )
            raise SkipTemperatureCollection(str(e)) from e
        raise

    # Calculate row count
    if data and isinstance(data, list):
        row_count = len(data)
    else:
        row_count = 0

    logging.info(f"{source_name} - {start_date} ~ {end_date} 추출 row 수: {row_count}")
    return data, row_count


# ════════════════════════════════════════════════════════════════
# 4️⃣ Data Loading
# ════════════════════════════════════════════════════════════════

def prepare_insert_data(data: list, company_cd: str, extract_time: datetime) -> list:
    """Prepare data for PostgreSQL insertion"""
    if not data:
        return []
    
    # 딕셔너리 형태인지 확인하고 처리
    if isinstance(data[0], dict):
        return [
            (
                company_cd,
                row['sensor_id'], row['device_id'], row['ymd'], row['hmsf'],
                localize_source_ts_for_insert(row['capture_dt']),
                row['t1'], row['t2'], row['t3'], row['t4'], row['t5'], row['t6'],
                row['upload_yn'], localize_source_ts_for_insert(row['upload_dt']),
                extract_time
            ) for row in data
        ]
    else:
        # 리스트/튜플 형태인 경우
        # 원본: ymd, hmsf, sensor_id, device_id, capture_dt, t1, t2, t3, t4, t5, t6, upload_yn, upload_dt
        # 타겟: company_cd, sensor_id, device_id, ymd, hmsf, capture_dt, temperature, humidity, t3, find_dust_1.0, finde_dust2.5, fine_dust10, upload_yn, upload_dt, etl_extract_time
        return [
            (
                company_cd,
                row[2], row[3], row[0], row[1],
                localize_source_ts_for_insert(row[4]),
                row[5], row[6], row[7], row[8], row[9], row[10],
                row[11], localize_source_ts_for_insert(row[12]),
                extract_time
            ) for row in data
        ]


def get_column_names() -> list:
    """Get column names for PostgreSQL table"""
    return [
        "company_cd", "sensor_id", "device_id", "ymd", "hmsf", "capture_dt",
        "temperature", "humidity", "t3", '"find_dust_1.0"', '"finde_dust2.5"', "fine_dust10",
        "upload_yn", "upload_dt", "etl_extract_time"
    ]


def load_data(
    pg: PostgresHelper, 
    data: list, 
    extract_time: datetime, 
    company_cd: str,
    schema_name: str = SCHEMA_NAME,
    table_name: str = TABLE_NAME
) -> int:
    """Load data into PostgreSQL database"""
    if not data:
        logging.info(f"{company_cd} - No data to load")
        return 0
    
    prepared_data = prepare_insert_data(data, company_cd, extract_time)
    columns = get_column_names()
    conflict_columns = ["company_cd", "sensor_id", "capture_dt"]

    try:
        pg.insert_data(schema_name, table_name, prepared_data, columns, conflict_columns)
    except Exception as e:
        if _is_db_connection_error(e):
            logging.warning(
                f"⏭ {company_cd} 타겟 DB 연결/네트워크 문제로 적재 스킵: {e}"
            )
            raise SkipTemperatureCollection(str(e)) from e
        raise

    logging.info(f"✅ {company_cd} - {len(prepared_data)} rows inserted (duplicates ignored).")

    return len(prepared_data)


# ════════════════════════════════════════════════════════════════
# 5️⃣ Variable Management
# ════════════════════════════════════════════════════════════════

def update_variable(key: str, end_extract_time: str) -> None:
    """Update Airflow variable with last extract time"""
    Variable.set(key, end_extract_time)
    logging.info(f"📌 Variable `{key}` Update: {end_extract_time}")

