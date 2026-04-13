"""적재 후 품질 검증."""
from __future__ import annotations

import logging
from typing import Any

from .config import load_postgres_helper


def validate_data_quality(**context: Any) -> None:
    machine_count = load_postgres_helper.execute_query(
        sql="SELECT COUNT(*) FROM bronze.machine",
        task_id="validate_machine_count",
        xcom_key=None,
        **context,
    )
    device_count = load_postgres_helper.execute_query(
        sql="SELECT COUNT(*) FROM bronze.device",
        task_id="validate_device_count",
        xcom_key=None,
        **context,
    )
    sensor_count = load_postgres_helper.execute_query(
        sql="SELECT COUNT(*) FROM bronze.sensor",
        task_id="validate_sensor_count",
        xcom_key=None,
        **context,
    )

    orphaned_devices = load_postgres_helper.execute_query(
        sql="""
        SELECT COUNT(*) FROM bronze.device d
        LEFT JOIN bronze.machine m
          ON d.company_cd = m.company_cd AND d.mach_id = m.mach_id
        WHERE m.mach_id IS NULL
        """,
        task_id="validate_orphaned_devices",
        xcom_key=None,
        **context,
    )
    orphaned_sensors = load_postgres_helper.execute_query(
        sql="""
        SELECT COUNT(*) FROM bronze.sensor s
        LEFT JOIN bronze.device d
          ON s.company_cd = d.company_cd AND s.device_id = d.device_id
        WHERE d.device_id IS NULL
        """,
        task_id="validate_orphaned_sensors",
        xcom_key=None,
        **context,
    )

    logging.info("Data quality check results:")
    logging.info("- Machines: %s", machine_count[0][0] if machine_count else 0)
    logging.info("- Devices: %s", device_count[0][0] if device_count else 0)
    logging.info("- Sensors: %s", sensor_count[0][0] if sensor_count else 0)
    logging.info(
        "- Orphaned devices: %s",
        orphaned_devices[0][0] if orphaned_devices else 0,
    )
    logging.info(
        "- Orphaned sensors: %s",
        orphaned_sensors[0][0] if orphaned_sensors else 0,
    )

    if orphaned_devices and orphaned_devices[0][0] > 0:
        logging.warning(
            "Found %s devices without valid machine", orphaned_devices[0][0]
        )
    if orphaned_sensors and orphaned_sensors[0][0] > 0:
        logging.warning(
            "Found %s sensors without valid device", orphaned_sensors[0][0]
        )
