#!/bin/bash
# Airflow performance monitoring script

# Change to Airflow project directory (update this path for your environment)
cd "/path/to/airflow/project"

echo "=========================================="
echo "📊 Airflow Performance Monitoring"
echo "=========================================="
echo ""

# 1. Container resource usage
echo "🖥️  Container resource usage:"
echo "----------------------------------------"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" \
  $(docker-compose ps -q airflow-scheduler airflow-webserver) \
  $(docker-compose ps -q airflow-worker | head -5) 2>/dev/null | head -10
echo ""

# 2. Worker status and count
echo "👷 Worker status:"
echo "----------------------------------------"
WORKER_COUNT=$(docker-compose ps airflow-worker | grep -c "airflow-worker" || echo "0")
HEALTHY_WORKERS=$(docker-compose ps airflow-worker | grep -c "healthy" || echo "0")
UNHEALTHY_WORKERS=$((WORKER_COUNT - HEALTHY_WORKERS))
echo "  - Total workers: ${WORKER_COUNT}"
echo "  - Healthy: ${HEALTHY_WORKERS}"
echo "  - Unhealthy: ${UNHEALTHY_WORKERS}"
echo ""

# 3. Check worker status from Flower
if docker ps --format '{{.Names}}' | grep -q "^airflow-flower$"; then
    echo "🌸 Flower worker monitoring:"
    echo "----------------------------------------"
    FLOWER_WORKER_COUNT=$(curl -s http://localhost:5555/api/workers 2>/dev/null | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "0")
    echo "  - Workers recognized in Flower: ${FLOWER_WORKER_COUNT}"

    # Check number of active tasks
    ACTIVE_TASKS=$(curl -s http://localhost:5555/api/tasks?state=STARTED 2>/dev/null | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "0")
    echo "  - Currently running tasks: ${ACTIVE_TASKS}"
    echo ""
fi

# 4. Database connection status
echo "🗄️  Database connection status:"
echo "----------------------------------------"
PG_CONNECTIONS=$(docker exec airflow-postgres psql -U airflow -d airflow -t -c "SELECT count(*) FROM pg_stat_activity WHERE datname='airflow';" 2>/dev/null | tr -d ' ' || echo "N/A")
PG_ACTIVE=$(docker exec airflow-postgres psql -U airflow -d airflow -t -c "SELECT count(*) FROM pg_stat_activity WHERE datname='airflow' AND state='active';" 2>/dev/null | tr -d ' ' || echo "N/A")
PG_IDLE=$(docker exec airflow-postgres psql -U airflow -d airflow -t -c "SELECT count(*) FROM pg_stat_activity WHERE datname='airflow' AND state='idle';" 2>/dev/null | tr -d ' ' || echo "N/A")
echo "  - Total PostgreSQL connections: ${PG_CONNECTIONS}"
echo "  - Active connections: ${PG_ACTIVE}"
echo "  - Idle connections: ${PG_IDLE}"
echo ""

# 5. Redis memory usage
echo "📦 Redis memory usage:"
echo "----------------------------------------"
REDIS_MEMORY=$(docker exec redis redis-cli INFO memory 2>/dev/null | grep "used_memory_human" | cut -d: -f2 | tr -d '\r' || echo "N/A")
REDIS_CONNECTIONS=$(docker exec redis redis-cli INFO stats 2>/dev/null | grep "total_connections_received" | cut -d: -f2 | tr -d '\r' || echo "N/A")
echo "  - Used memory: ${REDIS_MEMORY}"
echo "  - Total connections: ${REDIS_CONNECTIONS}"
echo ""

# 6. Airflow configuration
echo "⚙️  Airflow configuration:"
echo "----------------------------------------"
PARALLELISM=$(docker exec airflow-webserver airflow config get-value core parallelism 2>/dev/null | grep -v "FutureWarning\|RemovedInAirflow3Warning" | tail -1)
POOL_SIZE=$(docker exec airflow-webserver airflow config get-value database sql_alchemy_pool_size 2>/dev/null | grep -v "FutureWarning\|RemovedInAirflow3Warning" | tail -1)
MAX_ACTIVE_TASKS=$(docker exec airflow-webserver airflow config get-value core max_active_tasks_per_dag 2>/dev/null | grep -v "FutureWarning\|RemovedInAirflow3Warning" | tail -1)
echo "  - Parallelism: ${PARALLELISM}"
echo "  - DB pool size: ${POOL_SIZE}"
echo "  - Max active tasks per DAG: ${MAX_ACTIVE_TASKS}"
echo ""

# 7. Recent DAG run status
echo "📈 Recent DAG run status (latest 10):"
echo "----------------------------------------"
docker exec airflow-webserver airflow dags list-runs --state running --limit 10 2>/dev/null | head -12 || echo "  No running DAGs"
echo ""

# 8. System resources (host)
echo "💻 Host system resources:"
echo "----------------------------------------"
echo "  - CPU usage:"
if command -v bc >/dev/null 2>&1; then
    CPU_IDLE=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/")
    CPU_USAGE=$(echo "100 - $CPU_IDLE" | bc)
    echo "    In use: ${CPU_USAGE}%"
else
    top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{printf "    In use: %.1f%%\n", 100 - $1}'
fi
echo "  - Memory usage:"
MEM_INFO=$(free -h | grep "Mem:")
MEM_TOTAL=$(echo "$MEM_INFO" | awk '{print $2}')
MEM_USED=$(echo "$MEM_INFO" | awk '{print $3}')
MEM_FREE=$(echo "$MEM_INFO" | awk '{print $4}')
MEM_AVAILABLE=$(echo "$MEM_INFO" | awk '{print $7}')
MEM_BUFF_CACHE=$(echo "$MEM_INFO" | awk '{print $6}')
echo "    Total: ${MEM_TOTAL}"
echo "    Used: ${MEM_USED}"
echo "    Free: ${MEM_FREE}"
echo "    buff/cache: ${MEM_BUFF_CACHE}"
echo "    Available: ${MEM_AVAILABLE}"
echo "  - Disk usage:"
df -h / | tail -1 | awk '{print "    Used: " $3 " / " $2 " (" $5 ")"}'
echo ""

echo "=========================================="
echo "💡 Additional monitoring tools:"
echo "  - Airflow UI: http://localhost:8080"
echo "  - Flower UI: http://localhost:5555"
echo "  - Live monitoring: docker stats"
echo "=========================================="

