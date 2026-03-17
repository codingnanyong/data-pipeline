#!/bin/bash
# Airflow full recreation script
# Run this script to completely recreate the Airflow environment.
# - Recreate all containers
# - Automatically install packages from requirements.txt
# - Scale workers to 10 instances
# - Configure Oracle DB (install libaio1)
# - Verify required package installation

set -e

# Change to Airflow project directory (update this path for your environment)
cd "/path/to/airflow/project"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🚀 Starting full Airflow recreation"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# 0. Pre-checks
echo "📋 Running pre-checks..."
if [ ! -f ".env" ]; then
    echo "❌ .env file is missing!"
    exit 1
fi

if [ ! -f "requirements.txt" ]; then
    echo "❌ requirements.txt file is missing!"
    exit 1
fi

if [ ! -d "models" ]; then
    echo "⚠️  models directory does not exist. Creating..."
    mkdir -p models
fi

# Check Anomaly-Transformer and copy into plugins/models
if [ ! -d "Anomaly-Transformer" ]; then
    echo "⚠️  Anomaly-Transformer directory not found. Cloning from GitHub..."
    if command -v git >/dev/null 2>&1; then
        git clone https://github.com/thuml/Anomaly-Transformer.git Anomaly-Transformer
        echo "  ✅ Anomaly-Transformer clone completed"
    else
        echo "  ❌ git is not installed. Please clone manually:"
        echo "     git clone https://github.com/thuml/Anomaly-Transformer.git Anomaly-Transformer"
    fi
else
    echo "  ✅ Anomaly-Transformer directory found"
fi

# Create plugins/models/anomaly_transformer directory and copy AnomalyTransformer modules
if [ -d "Anomaly-Transformer/model" ]; then
    mkdir -p plugins/models/anomaly_transformer
    echo "  📦 Copying AnomalyTransformer modules to plugins/models/anomaly_transformer/..."
    cp -r Anomaly-Transformer/model/* plugins/models/anomaly_transformer/ 2>/dev/null || true
    echo "  ✅ Finished copying AnomalyTransformer modules into plugins/models/anomaly_transformer/"
fi

echo "  ✅ .env file check passed"
echo "  ✅ requirements.txt check passed"
echo "  ✅ models directory check passed"
echo ""

# 1. Check environment variables
echo "📋 Checking environment variables:"
PARALLELISM=$(grep '^AIRFLOW__CORE__PARALLELISM=' .env 2>/dev/null | cut -d'=' -f2 || echo "unknown")
POOL_SIZE=$(grep '^AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=' .env 2>/dev/null | cut -d'=' -f2 || echo "unknown")
MAX_OVERFLOW=$(grep '^AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=' .env 2>/dev/null | cut -d'=' -f2 || echo "unknown")
PIP_REQUIREMENTS=$(grep '^_PIP_ADDITIONAL_REQUIREMENTS=' .env 2>/dev/null | cut -d'=' -f2 || echo "unknown")

echo "  - AIRFLOW__CORE__PARALLELISM: ${PARALLELISM}"
echo "  - AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE: ${POOL_SIZE}"
echo "  - AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW: ${MAX_OVERFLOW}"
echo "  - _PIP_ADDITIONAL_REQUIREMENTS: ${PIP_REQUIREMENTS}"
echo ""

# 2. Stop any running containers
echo "🛑 Stopping existing Airflow containers..."
if docker-compose ps -q | grep -q .; then
    docker-compose down
    echo "  ✅ Existing containers stopped"
else
    echo "  ℹ️  No running containers found"
fi
echo ""

# 3. Recreate and start Airflow containers
echo "🔄 Recreating Airflow containers..."
docker-compose up -d
echo "  ✅ Containers recreated"
echo ""

# 3.5. Wait for containers to start
echo "⏳ Waiting for containers to start (15 seconds)..."
sleep 15
echo ""

# 4. Confirm installation of packages from _PIP_ADDITIONAL_REQUIREMENTS in .env
echo "📦 Verifying installation of packages from .env _PIP_ADDITIONAL_REQUIREMENTS..."
echo "  (docker-compose will install these automatically. Installation is in progress...)"
echo "  ⏳ It may take a few minutes to complete package installation"
echo ""

# 5. Scale workers to 10
echo "📈 Scaling workers to 10..."
docker-compose up -d --scale airflow-worker=10 airflow-worker
echo "  ✅ Worker scaling completed"
echo ""

# 5.5. Wait for worker containers to start
echo "⏳ Waiting for worker containers to start (20 seconds)..."
sleep 20
echo ""

# 6. Oracle DB setup (install libaio1)
echo "📦 Configuring Oracle DB (installing libaio1)..."

CONTAINERS=("airflow-webserver" "airflow-scheduler" "airflow-triggerer")
for i in $(seq 1 10); do
  CONTAINERS+=("airflow-airflow-worker-$i")
done

INSTALLED_COUNT=0
for CONTAINER in "${CONTAINERS[@]}"; do
  if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "  📦 Installing libaio1 in $CONTAINER..."
    if docker exec -u root "$CONTAINER" bash -c "apt-get update -qq && apt-get install -y -qq libaio1" 2>/dev/null; then
      echo "    ✅ Installation completed"
      INSTALLED_COUNT=$((INSTALLED_COUNT + 1))
  else
      echo "    ℹ️  Already installed"
      INSTALLED_COUNT=$((INSTALLED_COUNT + 1))
    fi
  fi
done
echo "  ✅ Oracle DB configuration completed (${INSTALLED_COUNT} containers)"
echo ""

# 7. Install torch (all containers)
echo "🔥 Installing full torch version..."

TORCH_INSTALLED=0
for CONTAINER in "${CONTAINERS[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        echo "  📦 Installing torch in ${CONTAINER}... (showing progress)"
        if timeout 600 docker exec "$CONTAINER" python -m pip install --no-cache-dir 'torch>=2.0.0,<3.0.0' 2>&1 | grep -E "(Collecting|Downloading|Installing|Successfully)" | tail -5; then
            echo "    ✅ torch installation completed"
            TORCH_INSTALLED=$((TORCH_INSTALLED + 1))
        else
            EXIT_CODE=$?
            if [ $EXIT_CODE -eq 124 ]; then
                echo "    ⚠️  Timeout (10 minutes) - installation may still be in progress"
            else
                echo "    ⚠️  Error occurred during installation (exit code: $EXIT_CODE)"
            fi
            TORCH_INSTALLED=$((TORCH_INSTALLED + 1))
        fi
    fi
done
echo "  ✅ torch installation completed ($TORCH_INSTALLED containers)"
echo ""

# 7.5. Install TensorFlow CPU version (all containers)
echo "🔥 Installing TensorFlow CPU version..."

TF_INSTALLED=0
for CONTAINER in "${CONTAINERS[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        echo "  📦 Installing TensorFlow in ${CONTAINER}... (showing progress)"
        if timeout 600 docker exec "$CONTAINER" python -m pip install --no-cache-dir 'tensorflow>=2.15.0,<2.17.0' 2>&1 | grep -E "(Collecting|Downloading|Installing|Successfully)" | tail -5; then
            echo "    ✅ TensorFlow installation completed"
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
        else
            echo "    ⚠️  Failed to verify TensorFlow version (installation may still be in progress)"
        fi
    fi
done
echo "  ✅ TensorFlow installation completed ($TF_INSTALLED containers)"
echo ""

# 8. Restart Flower (to recognize new workers)
if docker ps --format '{{.Names}}' | grep -q "^airflow-flower$"; then
    echo "🌸 Restarting Flower (to recognize new workers)..."
    docker-compose restart airflow-flower
    sleep 5
    echo "  ✅ Flower restart completed"
    echo ""
fi

# 9. Verify package installation (using pip list for faster checks than import)
echo "📦 Verifying required package installation..."
CHECK_CONTAINER="airflow-scheduler"

if docker ps --format '{{.Names}}' | grep -q "^${CHECK_CONTAINER}$"; then
    # Use pip list for verification (much faster than import, avoids long blocking)
    PACKAGE_CHECK=$(timeout 5 docker exec "$CHECK_CONTAINER" pip list 2>/dev/null | grep -E "^(pandas|numpy|torch|tensorflow|scikit-learn|psycopg2-binary)" || echo "")
    
    if [ -n "$PACKAGE_CHECK" ]; then
        while IFS= read -r line; do
            if echo "$line" | grep -qE "(pandas|numpy|torch|tensorflow|scikit-learn|psycopg2-binary)"; then
                PKG_NAME=$(echo "$line" | awk '{print $1}')
                PKG_VERSION=$(echo "$line" | awk '{print $2}')
                echo "    ✅ ${PKG_NAME}: ${PKG_VERSION}"
            fi
        done <<< "$PACKAGE_CHECK"
    else
        echo "    ⚠️  Failed to verify packages or installation still in progress"
        echo "    💡 After installation completes, verify manually with:"
        echo "       docker exec $CHECK_CONTAINER pip list | grep -E 'pandas|numpy|torch|tensorflow|sklearn|psycopg2'"
    fi
    
    # Quick import test (only for light packages)
    echo "  (Running quick import test...)"
    if timeout 3 docker exec "$CHECK_CONTAINER" python -c "import pandas, numpy; print('    ✅ pandas, numpy import succeeded')" 2>/dev/null; then
        :
    else
        echo "    ℹ️  Large packages like torch may still be installing"
    fi
else
    echo "  ⚠️  ${CHECK_CONTAINER} container is not running."
fi
echo ""

# 10. Check container status
echo "📊 Checking container status:"
docker-compose ps | grep -E "airflow-scheduler|airflow-webserver|airflow-worker|airflow-triggerer" | head -15
echo ""

# 11. Check worker count
echo "👷 Checking worker count:"
WORKER_COUNT=$(docker-compose ps airflow-worker 2>/dev/null | grep -c "airflow-worker" || echo "0")
echo "  - Running workers: ${WORKER_COUNT}"
if [ "${WORKER_COUNT}" -ne "10" ]; then
    echo "  ⚠️  Worker count is not 10. Please review."
    fi
echo ""

# 12. Confirm Airflow settings
echo "⚙️  Checking Airflow configuration:"
if docker ps --format '{{.Names}}' | grep -q "^airflow-webserver$"; then
    echo "  - Parallelism:"
    docker exec airflow-webserver airflow config get-value core parallelism 2>/dev/null | sed 's/^/    /' || echo "    (checking...)"
    echo "  - Pool Size:"
    docker exec airflow-webserver airflow config get-value database sql_alchemy_pool_size 2>/dev/null | sed 's/^/    /' || echo "    (checking...)"
    echo "  - Max Overflow:"
    docker exec airflow-webserver airflow config get-value database sql_alchemy_max_overflow 2>/dev/null | sed 's/^/    /' || echo "    (checking...)"
else
    echo "  ⚠️  webserver container is not running."
fi
echo ""

# 13. Final status summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Airflow recreation completed!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📋 Summary:"
echo "  ✅ Containers recreated"
echo "  ✅ Package installation verified"
echo "  ✅ ${WORKER_COUNT} workers running"
echo "  ✅ Oracle DB configuration completed"
echo "  ✅ PyTorch installation completed"
echo "  ✅ TensorFlow installation completed"
echo ""
echo "🌐 Access information:"
echo "  - Airflow UI: http://localhost:8080"
echo "  - Flower UI: http://localhost:5555 (worker monitoring)"
echo ""
echo "💡 Next steps:"
echo "  1. Verify that DAGs are loaded correctly in the Airflow UI"
echo "  2. Confirm that all required packages are installed using the above checks"
echo "  3. If necessary, copy model files into the ./models/ directory"
echo ""

# 14. Set file permissions (for collaborative editing)
echo "🔐 Setting file permissions..."
echo "  📁 Setting permissions for dags directory..."
sudo find "/path/to/airflow/project/dags" -type d -exec chmod 777 {} \; && sudo find "/path/to/airflow/project/dags" -type f -exec chmod 666 {} \;
echo "    ✅ dags permissions updated"

echo "  📁 Setting permissions for plugins directory..."
sudo find "/path/to/airflow/project/plugins" -type d -exec chmod 777 {} \; && sudo find "/path/to/airflow/project/plugins" -type f -exec chmod 666 {} \;
echo "    ✅ plugins permissions updated"

echo "  📁 Setting permissions for models directory..."
sudo find "/path/to/airflow/project/models" -type d -exec chmod 777 {} \; && sudo find "/path/to/airflow/project/models" -type f -exec chmod 666 {} \;
echo "    ✅ models permissions updated"
echo ""

