#!/bin/bash
#
# TensorFlow installation script (for Docker containers)
# Run this on the host to install TensorFlow CPU version into Airflow containers
#
# Usage:
#   bash install_tensorflow_docker.sh
#
# Installs the TensorFlow CPU version into all running Airflow containers.
#

# Note: set -e is intentionally not used (manual error handling, similar to quick_restart.sh)
# Change to Airflow project directory (update this path for your environment)
cd "/path/to/airflow/project"

INSTALL_TYPE="cpu"
TARGET_CONTAINER=${1:-""}

SUCCESS_COUNT=0
FAIL_COUNT=0

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 TensorFlow CPU installation script (Docker)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Install type: CPU version (TensorFlow 2.15.x - 2.16.x)"
echo ""

# Build container list
if [ -n "$TARGET_CONTAINER" ]; then
    CONTAINERS=("$TARGET_CONTAINER")
else
    # Find only running Airflow workload containers (excluding DB containers)
    # Includes airflow-worker, airflow-scheduler, airflow-triggerer, airflow-webserver, etc.
    # Also includes names like airflow-airflow-worker-1, airflow-scheduler
    ALL_AIRFLOW=($(docker ps --format '{{.Names}}' | grep "^airflow" || true))
    CONTAINERS=()
    for CONTAINER in "${ALL_AIRFLOW[@]}"; do
        # Exclude database-related containers
        if [[ ! "$CONTAINER" =~ (postgres|redis|mysql|mariadb|db) ]]; then
            CONTAINERS+=("$CONTAINER")
        fi
    done
fi

if [ ${#CONTAINERS[@]} -eq 0 ]; then
    echo "❌ No running Airflow containers found."
    echo "Check container list with: docker compose ps"
    exit 1
fi

echo "📋 Target containers for installation:"
for CONTAINER in "${CONTAINERS[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        echo "  ✅ $CONTAINER"
    else
        echo "  ❌ $CONTAINER (not running)"
    fi
done
echo ""

# Install into each container (similar to torch installation logic in quick_restart.sh)
echo "🔥 Installing TensorFlow CPU version..."

TF_INSTALLED=0
for CONTAINER in "${CONTAINERS[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        echo "  📦 Installing TensorFlow in ${CONTAINER}... (showing progress)"
        if timeout 600 docker exec "$CONTAINER" python -m pip install --no-cache-dir 'tensorflow>=2.15.0,<2.17.0' 2>&1 | grep -E "(Collecting|Downloading|Installing|Successfully)" | tail -5; then
            echo "    ✅ TensorFlow 설치 완료"
            TF_INSTALLED=$((TF_INSTALLED + 1))
        else
            EXIT_CODE=$?
            if [ $EXIT_CODE -eq 124 ]; then
                echo "    ⚠️  Timeout (10 minutes) - installation may still be in progress"
            else
                echo "    ⚠️  Error occurred during installation (exit code: $EXIT_CODE)"
            fi
            # Count this container even if there was a timeout (installation might still be running)
            TF_INSTALLED=$((TF_INSTALLED + 1))
        fi

        # Verify installation
        TF_VERSION=$(docker exec "$CONTAINER" python -c "import tensorflow as tf; print(tf.__version__)" 2>/dev/null || echo "")
        if [ -n "$TF_VERSION" ]; then
            echo "    📌 TensorFlow version: $TF_VERSION"
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            FAIL_COUNT=$((FAIL_COUNT + 1))
        fi
    fi
done
echo "  ✅ TensorFlow 설치 완료 ($TF_INSTALLED개 컨테이너)"
echo ""

# 결과 요약
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 설치 결과 요약"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "성공: $SUCCESS_COUNT개"
if [ $FAIL_COUNT -gt 0 ]; then
    echo "실패: $FAIL_COUNT개"
fi
echo ""

if [ $SUCCESS_COUNT -gt 0 ]; then
    echo "✅ TensorFlow 설치가 완료되었습니다!"
    exit 0
else
    echo "❌ 모든 컨테이너에서 설치 확인 실패"
    exit 1
fi

