# 🌊 Enterprise Data Pipeline with Apache Airflow

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.3-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Multi--Source-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![dbt](https://img.shields.io/badge/dbt-Core-FF694B?logo=dbt&logoColor=white)](https://docs.getdbt.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Production-ready Apache Airflow data pipeline for enterprise multi-source data integration, transformation, and analytics with automated ETL workflows and comprehensive monitoring.

## 🏗️ **Architecture Overview**

```text
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                             │
├─────────────────┬─────────────────┬─────────────────────────┤
│     Oracle      │    PostgreSQL   │      SQL Server         │
│                 │                 │                         │
│   Manufacturing │   IoT Sensors   │   Business Systems      │
└─────────────────┴─────────────────┴─────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                Apache Airflow ETL                           │
├─────────────────────────────────────────────────────────────┤
│  ◆ HQ Pipeline       │  ◆ JJ Pipeline                       │
│    - FDW Integration │    - IoT Data Processing             │
│    - Multi-DB Sync   │    - ML Anomaly Detection            │
│    - Data Quality    │    - Real-time Monitoring            │
│                      │    - dbt Transformations             │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              Target Data Warehouse                          │
├─────────────────────────────────────────────────────────────┤
│  PostgreSQL + TimescaleDB                                   │
│  - Bronze Layer (Raw Data)                                  │
│  - Silver Layer (Cleaned Data)                              │
│  - Gold Layer (Analytics-Ready)                             │
└─────────────────────────────────────────────────────────────┘
```

## 📁 **Project Structure**

```text
data_pipeline(airflow)/
├── 🏢 hq/                           # Headquarters Data Pipeline
│   ├── dags/                        # HQ-specific DAGs
│   ├── db/flet_montrg/             # Database schemas
│   └── README.md                    # HQ pipeline documentation
│
├── 🏭 jj/                           # Manufacturing Plant Pipeline
│   ├── dags/                        # Production DAGs
│   │   ├── pipeline/                # Core data pipelines
│   │   │   ├── production/          # Manufacturing system integration (1 DAG)
│   │   │   ├── telemetry/          # IoT sensor data (29 DAGs)
│   │   │   ├── maintenance/        # Equipment maintenance (19 DAGs)
│   │   │   ├── orchestration/      # Pipeline coordination (5 DAGs)
│   │   │   └── ml/                 # Machine learning workflows (1 DAG)
│   │   └── dbt/                    # dbt transformations
│   │       ├── banbury_anomaly_detection/  # Anomaly detection ML
│   │       └── unified_montrg/     # Unified monitoring models
│   ├── plugins/                    # Custom Airflow plugins
│   │   ├── hooks/                  # Database connectors
│   │   └── models/                 # ML models (Anomaly Transformer)
│   ├── scripts/                    # Utility scripts
│   └── README.md                   # Detailed setup guide
│
└── 📋 docker-compose.yml           # Multi-environment deployment
```

## ⚡ **Core Features**

### 🔄 **Multi-Pipeline Architecture**

- **HQ Pipeline**: Enterprise FDW integration and cross-database synchronization
- **JJ Pipeline**: Manufacturing IoT data processing with ML-powered anomaly detection
- **Unified Monitoring**: Real-time system health and performance tracking

### 📊 **Data Processing Capabilities**

- **55+ Production DAGs**: Comprehensive manufacturing data processing
- **Real-time IoT Ingestion**: Temperature, pressure, vibration sensor data
- **ML Anomaly Detection**: Automated anomaly detection using Transformer models
- **Data Quality Monitoring**: Automated validation and alerting
- **Multi-Source Integration**: Oracle, PostgreSQL, SQL Server, IoT devices

### 🛠️ **Technology Stack**

- **Orchestration**: Apache Airflow 2.10.3 with CeleryExecutor
- **Processing**: Python, Pandas, NumPy, TensorFlow, Scikit-learn
- **Transformation**: dbt Core with Elementary data observability
- **Storage**: PostgreSQL, TimescaleDB for time-series data
- **Containerization**: Docker Compose with auto-scaling
- **Monitoring**: Flower UI, Prometheus integration

## 🚀 **Quick Start**

### **1. HQ Pipeline Setup**

```bash
cd hq/
# Follow HQ-specific setup instructions
```

### **2. JJ Manufacturing Pipeline Setup**

```bash
cd jj/

# 1. Environment Setup
cp .env.example .env
# Edit .env with your database connections

# 2. Start Airflow Services
docker compose up -d

# 3. Access Web UI
# Airflow: http://localhost:8080
# Flower: http://localhost:5555
```

### **3. Deploy DAGs**

```bash
# Manufacturing pipelines are auto-loaded from:
# - dags/pipeline/telemetry/ (29 IoT processing DAGs)
# - dags/pipeline/maintenance/ (19 maintenance DAGs)
# - dags/pipeline/orchestration/ (5 coordination DAGs)
```

## 📊 **Pipeline Categories**

| Category | Location | Purpose | DAG Count |
| ------ | ------ | ------ | ------ |
| **Telemetry** | `jj/dags/pipeline/telemetry/` | IoT sensor data (Chiller, HMI, RTF) | **29** |
| **Maintenance** | `jj/dags/pipeline/maintenance/` | Equipment maintenance, work orders | **19** |
| **Orchestration** | `jj/dags/pipeline/orchestration/` | Pipeline coordination and scheduling | **5** |
| **Production** | `jj/dags/pipeline/production/` | Manufacturing system integration | **1** |
| **ML/Analytics** | `jj/dags/pipeline/ml/` | Machine learning model workflows | **1** |
| **dbt Models** | `jj/dags/dbt/` | Data transformations, unified monitoring | **Multiple** |

## 🔍 **Key Pipelines**

### **🏭 Manufacturing Data Processing**

- **Equipment Maintenance**: Work order processing, machine lifecycle management (19 DAGs)
- **Production Integration**: Manufacturing system coordination and orchestration (5 DAGs)
- **Quality Control**: Defect detection, statistical process control

### **📡 IoT Telemetry Processing** (29 DAGs)

- **Chiller Monitoring**: Temperature, vibration, electrical current tracking
- **HMI Data Processing**: Operator interface data, alarms, maintenance logs (Banbury systems)
- **Multi-Site Integration**: CTM, IP, and OS facility sensor data aggregation
- **Real-time Pipelines**: Incremental and backfill data processing workflows

### **🧠 ML-Powered Analytics**

- **Anomaly Detection**: Transformer-based anomaly detection for manufacturing
- **Quality Prediction**: ML models for product quality forecasting
- **Performance Optimization**: Data-driven process improvement

## 📚 **Documentation**

| Document | Description |
| ------ | ------ |
| **[jj/README.md](./jj/README.md)** | Complete Airflow setup and usage guide |
| **[jj/RESTART_GUIDE.md](./jj/RESTART_GUIDE.md)** | Safe restart procedures |
| **[jj/PERFORMANCE_MONITORING.md](./jj/PERFORMANCE_MONITORING.md)** | Performance optimization guide |
| **[jj/dags/dbt/README.md](./jj/dags/dbt/README.md)** | dbt project management |
| **[hq/README.md](./hq/README.md)** | HQ pipeline documentation |

## 🛡️ **Security & Monitoring**

- **Environment Isolation**: Separate `.env` configurations per environment
- **Database Security**: Connection pooling, encrypted connections
- **Performance Monitoring**: Flower UI, custom performance scripts
- **Data Quality**: Automated validation with Elementary integration
- **Logging**: Comprehensive logging with structured log formats

## 💡 **Use Cases**

✅ **Enterprise Data Integration** - Multi-source database synchronization  
✅ **Manufacturing Analytics** - Real-time production monitoring and KPI calculation  
✅ **IoT Data Processing** - Sensor data ingestion, validation, and storage  
✅ **Anomaly Detection** - ML-powered detection of production anomalies  
✅ **Data Quality Assurance** - Automated data validation and alerting  
✅ **Performance Optimization** - Data-driven manufacturing process improvement  

## 🏆 **Production Stats**

- **55+ DAGs** deployed across HQ and manufacturing environments
- **Multi-TB** data processing capability  
- **Real-time** IoT sensor data ingestion (29 telemetry pipelines)
- **24/7** production monitoring and alerting
- **Enterprise-grade** security and compliance

## 📄 **License**

This project is licensed under the MIT License. See [LICENSE](./jj/LICENSE) for details.

---

**🏭 Enterprise Data Engineering at Scale**
Built with ❤️ for manufacturing excellence and data-driven insights.