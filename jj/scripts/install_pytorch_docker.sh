#!/bin/bash
#
# PyTorch installation script (for Docker containers)
# Run this on the host to install the PyTorch CPU version into Airflow containers
#
# Usage:
#   bash install_pytorch_docker.sh
#   bash install_pytorch_docker.sh <container_name>  # install only to a specific container
#
# Installs the PyTorch CPU version into all running Airflow containers.
#

# Change to Airflow project directory (update this path for your environment)
cd "/path/to/airflow/project"

INSTALL_TYPE="cpu"
TARGET_CONTAINER=${1:-""}

SUCCESS_COUNT=0
FAIL_COUNT=0

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 PyTorch CPU installation script (Docker)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Install type: CPU version (PyTorch 2.0.x - 2.9.x)"
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

# Install into each container
echo "🔥 Installing PyTorch CPU version..."

PT_INSTALLED=0
for CONTAINER in "${CONTAINERS[@]}"; do
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        echo "  ⏭ Skipping ${CONTAINER} (not running)"
        continue
    fi
    
    echo "  📦 ${CONTAINER}에 PyTorch 설치 중... (진행 상황 표시)"
    
    # Check Python version
    PYTHON_VERSION=$(docker exec "$CONTAINER" python3 --version 2>&1 | awk '{print $2}' || echo "unknown")
    echo "    Python version: $PYTHON_VERSION"
    
    # Upgrade pip
    echo "    Upgrading pip..."
    docker exec "$CONTAINER" python3 -m pip install --upgrade pip --quiet 2>/dev/null || true
    
    # Install PyTorch CPU version
    echo "    Installing PyTorch CPU version..."
    if timeout 600 docker exec "$CONTAINER" pip install --no-cache-dir torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu 2>&1 | grep -E "(Collecting|Downloading|Installing|Successfully)" | tail -5; then
        echo "    ✅ PyTorch installation completed"
        PT_INSTALLED=$((PT_INSTALLED + 1))
    else
        EXIT_CODE=$?
        if [ $EXIT_CODE -eq 124 ]; then
            echo "    ⚠️  Timeout (10 minutes) - installation may still be in progress"
        else
            echo "    ⚠️  Error occurred during installation (exit code: $EXIT_CODE)"
        fi
        # Count this container even if there was a timeout (installation might still be running)
        PT_INSTALLED=$((PT_INSTALLED + 1))
    fi
    
    # Verify installation
    PT_VERSION=$(docker exec "$CONTAINER" python3 -c "import torch; print(torch.__version__)" 2>/dev/null || echo "")
    if [ -n "$PT_VERSION" ]; then
        echo "    📌 PyTorch version: $PT_VERSION"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo "    ❌ Failed to verify PyTorch version"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
    
    echo ""
done

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
    echo "✅ PyTorch 설치가 완료되었습니다!"
    exit 0
else
    echo "❌ 모든 컨테이너에서 설치 확인 실패"
    exit 1
fi

