"""
Bronze Layer - Raw Telemetry Ingestion
======================================
Auto Loader streams raw JSON files from the UC Volume into the
bronze_telemetry streaming table, preserving all original fields
and adding ingestion metadata for downstream lineage.
"""

import dlt
from pyspark.sql import functions as F

VOLUME_PATH = spark.conf.get("volume_path")


@dlt.table(
    name="bronze_telemetry",
    comment="Raw satellite telemetry ingested via Auto Loader from UC Volume.",
    table_properties={"quality": "bronze"},
)
def bronze_telemetry():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option(
            "cloudFiles.schemaHints",
            "satellite_id STRING, event_timestamp STRING, "
            "subsystem STRING, ground_station_id STRING, "
            "metrics STRUCT<battery_voltage: DOUBLE, signal_dbm: DOUBLE>",
        )
        .load(VOLUME_PATH)
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
