# 📊 Flet Monitoring Application Database Schema

Database schema and configuration files for the Flet-based monitoring application, which is part of the Airflow data pipeline infrastructure.

## 🌐 Overview

This directory contains the database schema definitions for a Python-based monitoring application built with the Flet framework. The application provides real-time data monitoring and alerting capabilities for temperature sensors and other IoT devices.

## ✨ Features

- **Real-time monitoring** of incoming sensor data
- **Temperature sensor management** for multiple devices
- **Alert system** with customizable thresholds and history
- **Location-based tracking** of sensors and devices
- **Relational database integration** for reliable persistence
- **Tight integration** with the Airflow data pipeline

## 📁 Project Structure

```text
flet_montrg/
├── schema/                     # Database schema files
│   ├── alert_subscriptions.sql # Alert subscription management
│   ├── alerts.sql              # Alert history and notifications
│   ├── location.sql            # Sensor location information
│   ├── sensor.sql              # Device information and configuration
│   ├── temperature.sql         # Temperature readings from sensors
│   └── thresholds.sql          # Customizable alert thresholds
├── hq_flet_montrg_ERD.pdf      # Entity Relationship Diagram
└── README.md                   # This file
```

## 🗄️ Database Schema

The application uses a relational database with the following main entities:

### Core Tables

- **sensor**: Device information and configuration
- **temperature**: Temperature readings from sensors
- **location**: Sensor location information

### Alert System

- **alerts**: Alert history and notifications
- **thresholds**: Customizable alert thresholds
- **alert_subscriptions**: User notification preferences

## 🚀 Database Setup

1. **Schema creation**: Execute the SQL files in the `schema/` directory in the following order:

   ```sql
   -- 1. Create base tables
   sensor.sql;
   location.sql;
   temperature.sql;

   -- 2. Create alert system tables
   thresholds.sql;
   alerts.sql;
   alert_subscriptions.sql;
   ```

2. **ERD reference**: Review `hq_flet_montrg_ERD.pdf` for detailed entity relationships.
3. **Airflow integration**: Ensure your Airflow DAGs use the same schema and connection configuration.

## 🌀 Integration with Airflow

This database schema is part of the larger Airflow data pipeline infrastructure:

- **Data ingestion**: Airflow DAGs collect data from sensors
- **Data processing**: Transform and validate sensor data
- **Alert generation**: Trigger alerts based on thresholds
- **Data storage**: Persist processed data in the `sensor`, `temperature`, and related tables

## 🧭 Usage Guide

1. Set up the database using the SQL files in the `schema/` directory.
2. Configure Airflow DAGs to read and write to these tables.
3. Deploy the Flet monitoring application (frontend).
4. Monitor data flow and alerts via the UI and Airflow UI.

## 🔗 Related Components

- **Flet application**: Frontend monitoring interface
- **Airflow DAGs**: Data pipeline orchestration
- **Sensor integration**: IoT device connectivity
- **Alert system**: Notification and subscription management
