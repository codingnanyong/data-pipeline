# 📚 HQ Database Schemas

This directory contains database schemas and related assets used by the HQ services in the data pipeline.

## Structure

- `flet_montrg/` – schema for the Flet-based monitoring application (real-time monitoring, alerts, sensor data)
- Other subdirectories (if present) – additional service-specific database schemas

### How to Use

1. Open the target subdirectory (for example, `flet_montrg/`).
2. Follow the instructions in that directory’s `README.md` to create and initialize the database.
3. Configure your Airflow DAGs and applications to use the corresponding database and schema.

For details on the Flet monitoring schema, see `flet_montrg/README.md`.
