#!/usr/bin/env bash
set -euo pipefail

# Airflow log root (host path). Provide via env var to avoid hard-coding.
# Example: AIRFLOW_LOG_ROOT=/path/your/local/airflow/logs
LOG_ROOT="${AIRFLOW_LOG_ROOT:-}"

if [ -z "${LOG_ROOT}" ] || [ "${LOG_ROOT}" = "/" ]; then
  echo "Refusing to run: AIRFLOW_LOG_ROOT is not set (or is unsafe)." >&2
  exit 1
fi

if [ ! -d "${LOG_ROOT}" ]; then
  echo "Skip: LOG_ROOT does not exist or is not a directory." >&2
  exit 0
fi

echo "Cleaning Airflow logs..."

# 1) Compress *.log older than 7 days (skip files that are already gz)
echo "[1/2] Compressing *.log older than 7 days..."
to_compress_count=$(find "${LOG_ROOT}" -type f -name '*.log' -mtime +7 | wc -l || echo 0)
echo "  -> ${to_compress_count} files to compress"

if [ "${to_compress_count}" -gt 0 ]; then
  find "${LOG_ROOT}" \
    -xdev \
    -type f \
    -name '*.log' \
    -mtime +7 \
    -print0 | xargs -0 -r gzip -9 --
  echo "  Compression step completed."
else
  echo "  No *.log files older than 7 days. Skipping compression."
fi

# 2) Delete *.log / *.log.gz older than 30 days
echo "[2/2] Deleting *.log / *.log.gz older than 30 days..."
to_delete_count=$(find "${LOG_ROOT}" -type f \( -name '*.log' -o -name '*.log.gz' \) -mtime +30 | wc -l || echo 0)
echo "  -> ${to_delete_count} files to delete"

if [ "${to_delete_count}" -gt 0 ]; then
  find "${LOG_ROOT}" \
    -xdev \
    -type f \
    \( -name '*.log' -o -name '*.log.gz' \) \
    -mtime +30 \
    -print0 | xargs -0 -r rm -f --
  echo "  Deletion step completed."
else
  echo "  No *.log / *.log.gz files older than 30 days. Skipping deletion."
fi

echo "Done."

