#!/usr/bin/env bash
set -euo pipefail

# Root directory for HMI RAW data
# TODO: Change this path to the appropriate HMI raw data directory on your system.
BASE_DIR="/path/to/hmi_raw"

# Today's date (e.g., 20260317). Only data before today (up to yesterday) will be archived.
TODAY=$(date +"%Y%m%d")

echo "Archiving CSV files under ${BASE_DIR} by directory, up to yesterday (before ${TODAY})."

# Iterate over all directories that contain CSV files
find "${BASE_DIR}" -type f -name "*.csv" -printf '%h\n' | sort -u | while read -r dir; do
  echo "Checking directory: ${dir}"

  # Extract date (YYYYMMDD) from CSV filenames in this directory
  dates=$(find "${dir}" -maxdepth 1 -type f -name "*.csv" -printf "%f\n" \
    | sed -n 's/.*_\([0-9]\{8\}\)[0-9]\{2\}_.*/\1/p' \
    | sort -u)

  for d in ${dates}; do
    # Skip today and future dates (keep today and future data)
    if [ "${d}" -ge "${TODAY}" ]; then
      continue
    fi

    TAR_PATH="${dir}/${d}.tar.gz"

    # Skip if an archive already exists for this date in this directory
    if [ -f "${TAR_PATH}" ]; then
      echo "  Archive already exists for ${d} in ${dir} (skipping)"
      continue
    fi

    echo "  Processing date ${d} in ${dir} ..."

    tmp_list=$(mktemp)
    find "${dir}" -maxdepth 1 -type f -name "*_${d}*.csv" -print0 > "${tmp_list}"

    if [ -s "${tmp_list}" ]; then
      # Create YYYYMMDD.tar.gz in this directory
      tar --null --files-from="${tmp_list}" -czf "${TAR_PATH}"
      # Remove original CSV files
      xargs -0 -a "${tmp_list}" rm -f
      echo "    Created archive: ${TAR_PATH} and removed source CSV files for ${d}."
    else
      echo "    No CSV files found for date ${d} in ${dir}. Skipping."
    fi

    rm -f "${tmp_list}"
  done
done

echo "All done."

