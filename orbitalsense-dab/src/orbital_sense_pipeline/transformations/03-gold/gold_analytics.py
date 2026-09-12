"""
Gold Layer - Business Analytics Materialized Views
===================================================
Four materialized views answering key operational questions:
  1. Telemetry volume by satellite and ground station
  2. Voltage degradation monitor with CRITICAL flag
  3. Signal strength report (weakest satellites)
  4. Subsystem alert & quarantine correlation
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# 1. Telemetry Volume by Satellite x Ground Station
# ---------------------------------------------------------------------------
@dlt.table(
    name="telemetry_volume_by_station",
    comment="Telemetry record counts grouped by satellite and ground station.",
    table_properties={"quality": "gold"},
)
def telemetry_volume_by_station():
    curated = dlt.read("telemetry_curated")
    return (
        curated.groupBy("satellite_id", "ground_station_id")
        .agg(
            F.count("*").alias("record_count"),
            F.min("event_ts").alias("first_seen"),
            F.max("event_ts").alias("last_seen"),
        )
        .orderBy(F.desc("record_count"))
    )


# ---------------------------------------------------------------------------
# 2. Voltage Degradation Monitor - Rolling Avg + CRITICAL Flag
# ---------------------------------------------------------------------------
@dlt.table(
    name="voltage_degradation_monitor",
    comment=(
        "Rolling average battery voltage per satellite. "
        "Flags satellites trending toward failure (rolling avg < 25.0V as CRITICAL)."
    ),
    table_properties={"quality": "gold"},
)
def voltage_degradation_monitor():
    curated = dlt.read("telemetry_curated")

    # 50-record rolling window per satellite, ordered by event time
    window_spec = (
        Window.partitionBy("satellite_id")
        .orderBy("event_ts")
        .rowsBetween(-49, 0)
    )

    return (
        curated.withColumn(
            "rolling_avg_voltage", F.round(F.avg("battery_voltage").over(window_spec), 3)
        )
        .withColumn(
            "health_status",
            F.when(F.col("rolling_avg_voltage") < 25.0, "CRITICAL")
            .when(F.col("rolling_avg_voltage") < 30.0, "WARNING")
            .otherwise("NOMINAL"),
        )
        .select(
            "satellite_id",
            "event_ts",
            "battery_voltage",
            "rolling_avg_voltage",
            "health_status",
            "ground_station_id",
        )
    )


# ---------------------------------------------------------------------------
# 3. Signal Strength Report - Weakest Signals by Satellite & Window
# ---------------------------------------------------------------------------
@dlt.table(
    name="signal_strength_report",
    comment="Weakest communication signals by satellite with time-window context.",
    table_properties={"quality": "gold"},
)
def signal_strength_report():
    curated = dlt.read("telemetry_curated")

    return (
        curated.groupBy("satellite_id")
        .agg(
            F.min("signal_dbm").alias("min_signal_dbm"),
            F.round(F.avg("signal_dbm"), 2).alias("avg_signal_dbm"),
            F.max("signal_dbm").alias("max_signal_dbm"),
            F.count("*").alias("reading_count"),
            F.min("event_ts").alias("window_start"),
            F.max("event_ts").alias("window_end"),
            F.round(F.stddev("signal_dbm"), 2).alias("signal_stddev"),
        )
        .withColumn(
            "signal_quality",
            F.when(F.col("avg_signal_dbm") < -100.0, "POOR")
            .when(F.col("avg_signal_dbm") < -80.0, "FAIR")
            .when(F.col("avg_signal_dbm") < -60.0, "GOOD")
            .otherwise("EXCELLENT"),
        )
        .orderBy("min_signal_dbm")
    )


# ---------------------------------------------------------------------------
# 4. Subsystem Alerts & Quarantine Correlation
# ---------------------------------------------------------------------------
@dlt.table(
    name="subsystem_alert_correlation",
    comment=(
        "Subsystem quarantine frequency and most common error codes. "
        "Identifies which subsystem generates the highest number of alerts."
    ),
    table_properties={"quality": "gold"},
)
def subsystem_alert_correlation():
    quarantine = dlt.read("telemetry_quarantine")

    return (
        quarantine.groupBy("subsystem", "error_code")
        .agg(
            F.count("*").alias("alert_count"),
            F.min("quarantine_timestamp").alias("first_alert"),
            F.max("quarantine_timestamp").alias("last_alert"),
            F.countDistinct("satellite_id").alias("affected_satellites"),
        )
        .orderBy(F.desc("alert_count"))
    )
