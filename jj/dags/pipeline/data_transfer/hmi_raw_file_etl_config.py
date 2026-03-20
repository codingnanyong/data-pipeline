"""
HMI Raw Data ETL Configuration
===============================
Configuration constants and HMI transfer settings.
"""

import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

# ════════════════════════════════════════════════════════════════
# DAG 기본 설정
# ════════════════════════════════════════════════════════════════

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ════════════════════════════════════════════════════════════════
# File processing settings
# ════════════════════════════════════════════════════════════════

# File pattern (HMI raw data)
REMOTE_FILE_PATTERNS = ["*.csv"]

# Local base directory for downloaded/archived CSV files.
# Env precedence:
# - ASE_DIR (preferred)
# - HMI_RAW_DATA_BASE_PATH (legacy)
# - default: placeholder (must be overridden via env)
HMI_RAW_ROOT_DEFAULT = "/path/to/your/local/hmi_raw"
HMI_RAW_ROOT = (
    os.getenv("ASE_DIR")
    or os.getenv("HMI_RAW_DATA_BASE_PATH")
    or HMI_RAW_ROOT_DEFAULT
)

HMI_RAW_ROOT_PATH = Path(HMI_RAW_ROOT).expanduser().absolute()
if not (os.getenv("ASE_DIR") or os.getenv("HMI_RAW_DATA_BASE_PATH")):
    raise ValueError("Set ASE_DIR (preferred) or HMI_RAW_DATA_BASE_PATH. Using the placeholder default is not allowed.")

# Basic safety checks to avoid accidentally targeting root.
# (The safe_local_path() function below also guarantees writes stay under HMI_RAW_ROOT_PATH.)
if str(HMI_RAW_ROOT_PATH) == str(Path("/").absolute()):
    raise ValueError("Unsafe HMI raw root directory: '/' is not allowed.")


def safe_local_path(root: Path, *parts: str) -> str:
    """
    Join path parts under `root` and ensure the result stays within `root`.
    This prevents accidental path traversal due to misconfiguration.
    """
    candidate = root.joinpath(*parts).absolute()
    root_abs = root.absolute()
    if candidate != root_abs and not str(candidate).startswith(str(root_abs) + os.sep):
        raise ValueError("Refusing to create a path outside the configured HMI raw root directory.")
    return str(candidate)


# Remote base directory on the SFTP server for HMI raw files.
# This must not be hard-coded to a real path in-repo.
# Override via env var:
# - HMI_REMOTE_BASE_PATH (preferred)
# - HMI_SFTP_REMOTE_BASE_PATH (legacy)
REMOTE_BASE_DEFAULT = "/path/to/your/remote/raw/data"
REMOTE_BASE = (
    os.getenv("HMI_REMOTE_BASE_PATH")
    or os.getenv("HMI_SFTP_REMOTE_BASE_PATH")
    or REMOTE_BASE_DEFAULT
)


def safe_remote_base_path(remote_base: str) -> str:
    """
    Basic safety checks for remote paths used in SFTP transfers.
    We prevent trivial traversal patterns to reduce risk of misconfiguration.
    """
    if not remote_base or remote_base.strip() == "":
        raise ValueError("Unsafe remote_base_path: empty.")
    if ".." in remote_base:
        raise ValueError("Unsafe remote_base_path: contains '..'. Please provide a safe absolute path.")
    # Allow either unix absolute (/...) or windows absolute (C:\...) style.
    if not (remote_base.startswith("/") or (len(remote_base) >= 2 and remote_base[1] == ":")):
        raise ValueError("Unsafe remote_base_path: expected an absolute path (starts with '/' or 'C:').")
    return remote_base


REMOTE_BASE = safe_remote_base_path(REMOTE_BASE)

# File validation options
VERIFY_FILE_SIZE = True
VERIFY_FILE_HASH = False  # Optional: enable checksum verification (MD5/SHA256/etc.)

MIN_FILE_SIZE = 0  # bytes (0 means no limit)
MAX_FILE_SIZE = None  # bytes (None means no limit)

LARGE_FILE_THRESHOLD_MB = 100  # MB threshold for detailed logging
DOWNLOAD_TIMEOUT_SECONDS = 3600  # seconds

# Time zone (UTC+7)
INDO_TZ = timezone(timedelta(hours=7))

INITIAL_START_DATE = datetime(2025, 12, 27)  # Backfill start date
DAYS_OFFSET_FOR_INCREMENTAL = 1  # Incremental: until today - 1 day
DAYS_OFFSET_FOR_BACKFILL = 2  # Backfill: until today - 2 days
HOURS_OFFSET_FOR_HOURLY = 1  # Hourly: until current time - 1 hour

# ════════════════════════════════════════════════════════════════
# HMI settings list
# ════════════════════════════════════════════════════════════════

# Per-HMI transfer settings
# process_code: process code (e.g. "os", "extrusion", "mixing")
# equipment_code: equipment code (e.g. "banb", "extr", "mix")
# incremental_enabled: include this HMI in Incremental/Hourly runs
# backfill_enabled: include this HMI in Backfill runs
HMI_CONFIGS = [
    {
        "hmi_id": "Machine03",
        "process_code": "os",
        "equipment_code": "banb",
        "sftp_conn_id": "conn_id_of_sftp_server",
        "remote_base_path": REMOTE_BASE,
        "local_save_path": safe_local_path(HMI_RAW_ROOT_PATH, "os", "osr", "Machine03"),
        "remote_retention_days": 30,
        "remote_cleanup_enabled": True,
        "incremental_enabled": True,
        "backfill_enabled": False,
    },
    # TODO: add more HMI entries here
    {
        "hmi_id": "Machine04",
        "process_code": "ip",
        "equipment_code": "ip",
        "sftp_conn_id": "conn_id_of_sftp_server",
        "remote_base_path": REMOTE_BASE,
        "local_save_path": safe_local_path(HMI_RAW_ROOT_PATH, "ip", "ipi", "Machine04"),
        "remote_retention_days": 30,
        "remote_cleanup_enabled": True,
        "incremental_enabled": True,
        "backfill_enabled": False,
    },
    {
        "hmi_id": "Machine12",
        "process_code": "ip",
        "equipment_code": "ip",
        "sftp_conn_id": "conn_id_of_sftp_server",
        "remote_base_path": REMOTE_BASE,
        "local_save_path": safe_local_path(HMI_RAW_ROOT_PATH, "ip", "ipi", "Machine12"),
        "remote_retention_days": 30,
        "remote_cleanup_enabled": True,
        "incremental_enabled": True,
        "backfill_enabled": False,
    },
    {
        "hmi_id": "Machine37",
        "process_code": "ip",
        "equipment_code": "ip",
        "sftp_conn_id": "conn_id_of_sftp_server",
        "remote_base_path": REMOTE_BASE,
        "local_save_path": safe_local_path(HMI_RAW_ROOT_PATH, "ip", "ipi", "Machine37"),
        "remote_retention_days": 30,
        "remote_cleanup_enabled": True,
        "incremental_enabled": True,
        "backfill_enabled": False,
    },
]

