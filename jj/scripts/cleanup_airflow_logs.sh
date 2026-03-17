#!/usr/bin/env bash
set -euo pipefail

# Airflow log root on the host (mounted to /opt/airflow/logs in docker-compose)
# TODO: Change this path to the appropriate log directory on your system.
LOG_ROOT="/path/to/airflow/logs"

echo "Cleaning Airflow logs under ${LOG_ROOT}"

# 1) Compress .log files older than 7 days with gzip (excluding existing .gz files)
echo "[1/2] Compressing *.log files older than 7 days..."
to_compress_count=$(find "${LOG_ROOT}" -type f -name '*.log' -mtime +7 | wc -l || echo 0)
echo "  -> ${to_compress_count} files to compress"

if [ "${to_compress_count}" -gt 0 ]; then
  find "${LOG_ROOT}" \
    -type f \
    -name '*.log' \
    -mtime +7 \
    -print0 | xargs -0 -r gzip -9
  echo "  Compression step completed."
else
  echo "  No *.log files older than 7 days. Skipping compression."
fi

# 2) Delete .log / .log.gz files older than 30 days
echo "[2/2] Deleting *.log / *.log.gz files older than 30 days..."
to_delete_count=$(find "${LOG_ROOT}" -type f \( -name '*.log' -o -name '*.log.gz' \) -mtime +30 | wc -l || echo 0)
echo "  -> ${to_delete_count} files to delete"

if [ "${to_delete_count}" -gt 0 ]; then
  find "${LOG_ROOT}" \
    -type f \
    \( -name '*.log' -o -name '*.log.gz' \) \
    -mtime +30 \
    -print0 | xargs -0 -r rm -f
  echo "  Deletion step completed."
else
  echo "  No *.log / *.log.gz files older than 30 days. Skipping deletion."
fi

echo "Done."

