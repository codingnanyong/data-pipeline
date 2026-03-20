#!/usr/bin/env bash
set -euo pipefail

umask 077

# HMI raw data root (host path). Provide via env var (ASE_DIR preferred).
# Example: ASE_DIR=/path/your/local/hmi_raw
BASE_DIR="${ASE_DIR:-${BASE_DIR:-}}"

# Optional: process only a specific subdirectory
TARGET_DIR="${TARGET_DIR:-}"

# Basic safety check: require a configured base directory.
if [ -z "${BASE_DIR}" ] || [ "${BASE_DIR}" = "/" ]; then
  echo "Refusing to run: ASE_DIR (or BASE_DIR) is not set (or is unsafe)." >&2
  exit 1
fi

# Today (example: 20260317). Compress only files older than today (up to yesterday).
TODAY=$(date +"%Y%m%d")

if [ -n "${TARGET_DIR}" ]; then
  SEARCH_ROOT="${TARGET_DIR}"
  echo "Archiving CSV files in the selected target (up to yesterday, before ${TODAY})."
else
  SEARCH_ROOT="${BASE_DIR}"
  echo "Archiving CSV files under the configured base directory (up to yesterday, before ${TODAY})."
fi

# If TARGET_DIR is provided, ensure it stays under BASE_DIR (to prevent accidental large deletion).
if [ -n "${TARGET_DIR}" ]; then
  if [[ "${SEARCH_ROOT}" != "${BASE_DIR}"* ]]; then
    echo "Refusing to run: TARGET_DIR must be within BASE_DIR." >&2
    exit 1
  fi
fi

if [ ! -d "${SEARCH_ROOT}" ]; then
  echo "Refusing to run: SEARCH_ROOT does not exist." >&2
  exit 1
fi

# Walk through every directory that contains CSV files
find "${SEARCH_ROOT}" -type f -name "*.csv" -printf '%h\n' | sort -u | while read -r dir; do
  echo "Checking directory..."

  # Extract dates (YYYYMMDD) from CSV filenames in this directory
  dates=$(find "${dir}" -maxdepth 1 -type f -name "*.csv" -printf "%f\n" \
    | sed -n 's/.*_\([0-9]\{8\}\)[0-9]\{2\}_.*/\1/p' \
    | sort -u)

  for d in ${dates}; do
    # Skip today and future dates (keep today/future data)
    if [ "${d}" -ge "${TODAY}" ]; then
      continue
    fi

    TAR_PATH="${dir}/${d}.tar.gz"
    echo "  Processing date ${d} ..."

    tmp_list=$(mktemp)
    # Store only CSV basenames whose embedded date matches d exactly
    find "${dir}" -maxdepth 1 -type f -name "*.csv" -printf "%f\n" \
      | sed -n "s/.*_\\(${d}\\)[0-9]\\{2\\}_.*/&/p" \
      | while IFS= read -r csv_name; do
          printf "%s\0" "${csv_name}"
        done > "${tmp_list}"

    if [ -s "${tmp_list}" ]; then
      if [ -f "${TAR_PATH}" ]; then
        # If an archive already exists: merge existing + new CSVs and regenerate
        tmp_merge_dir=$(mktemp -d)
        tmp_merge_list=$(mktemp)
        tmp_new_tar=$(mktemp "${dir}/${d}.tar.gz.new.XXXXXX")

        # Even if the tar contains path traversal entries, we flatten to basenames
        # and extract only into tmp_merge_dir.
        tar --no-same-owner --no-same-permissions \
          --transform='s#^.*\/##' \
          --wildcards '*.csv' \
          -xzf "${TAR_PATH}" -C "${tmp_merge_dir}"

        # Flatten existing archive CSVs and keep only entries matching date d
        while IFS= read -r -d '' existing_csv; do
          base_name=$(basename "${existing_csv}")
          existing_date=$(printf "%s\n" "${base_name}" | sed -n 's/.*_\([0-9]\{8\}\)[0-9]\{2\}_.*/\1/p')
          if [ "${existing_date}" = "${d}" ]; then
            # Avoid cp failure when src/dst are the same
            if [ "${existing_csv}" != "${tmp_merge_dir}/${base_name}" ]; then
              cp -f -- "${existing_csv}" "${tmp_merge_dir}/${base_name}"
            fi
          fi
        done < <(find "${tmp_merge_dir}" -type f -name "*.csv" -print0)

        while IFS= read -r -d '' csv_name; do
          cp -f -- "${dir}/${csv_name}" "${tmp_merge_dir}/${csv_name}"
        done < "${tmp_list}"

        # Rebuilt tar includes only top-level basenames (no directory structure)
        find "${tmp_merge_dir}" -maxdepth 1 -type f -name "*.csv" -printf "%f\0" > "${tmp_merge_list}"
        tar -C "${tmp_merge_dir}" --null --files-from="${tmp_merge_list}" -czf "${tmp_new_tar}"
        mv -f "${tmp_new_tar}" "${TAR_PATH}"

        rm -f "${tmp_merge_list}"
        rm -rf "${tmp_merge_dir}"
        echo "    Updated archive: $(basename "${TAR_PATH}") (merged additional CSV files)."
      else
        # Create a new archive
        tar -C "${dir}" --null --files-from="${tmp_list}" -czf "${TAR_PATH}"
        echo "    Created archive: $(basename "${TAR_PATH}")."
      fi

      # Remove original source CSV files
      while IFS= read -r -d '' csv_name; do
        rm -f -- "${dir}/${csv_name}"
      done < "${tmp_list}"
      echo "    Removed source CSV files for ${d}."
    else
      echo "    No CSV files found for date ${d}. Skipping."
    fi

    rm -f "${tmp_list}"
  done
done

echo "All done."

