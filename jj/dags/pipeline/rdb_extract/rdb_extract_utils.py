from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from airflow.models import Variable


def variable_key(source_id: str, table: str) -> str:
    safe_table = table.replace(".", "__")
    return f"last_extract_time__{source_id}__{safe_table}"


def parse_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str)
    except ValueError:
        try:
            d = datetime.strptime(dt_str, "%Y-%m-%d")
            return d.replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def get_last_extract(source_id: str, table: str) -> Optional[datetime]:
    k = variable_key(source_id, table)
    return parse_dt(Variable.get(k, default_var=None))


def set_last_extract(source_id: str, table: str, dt: datetime) -> None:
    k = variable_key(source_id, table)
    Variable.set(k, dt.astimezone(timezone.utc).isoformat())


def compact(prefix: str, **kwargs) -> str:
    items = " ".join(f"{k}={v}" for k, v in kwargs.items())
    return f"{prefix} {items}".strip()

