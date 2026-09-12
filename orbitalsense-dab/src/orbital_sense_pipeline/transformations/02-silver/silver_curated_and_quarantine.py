"""
Silver Layer - Curated Telemetry & Quarantine (Dead-Letter)
============================================================
Splits the Bronze stream into:
  * telemetry_quarantine  - invalid records (missing fields, OOB voltage)
  * telemetry_curated     - valid, deduplicated records with parsed types

Deduplication uses a deterministic SHA-256 hash computed on the raw payload
(before adding ingestion metadata) and a watermark on event_timestamp to
bound state for late-arriving data.
"""

import dlt
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Validation thresholds
# ---------------------------------------------------------------------------
_VOLTAGE_LOWER = 0.0
_VOLTAGE_UPPER = 50.0


# ---------------------------------------------------------------------------
# Validation predicates
# ---------------------------------------------------------------------------
def _is_missing_satellite():
    return F.col("satellite_id").isNull() | (F.trim(F.col("satellite_id")) == "")


def _is_missing_timestamp():
    return F.col("event_timestamp").isNull() | (
        F.trim(F.col("event_timestamp")) == ""
    )


def _is_oob_voltage():
    voltage = F.col("metrics.battery_voltage")
    return (
        voltage.isNull()
        | (voltage < F.lit(_VOLTAGE_LOWER))
        | (voltage > F.lit(_VOLTAGE_UPPER))
    )


def _is_invalid():
    """Composite filter: True when any validation rule fails."""
    return _is_missing_satellite() | _is_missing_timestamp() | _is_oob_voltage()


def _build_error_code():
    """Return a column expression encoding *all* failures (pipe-delimited)."""
    return F.concat_ws(
        "|",
        F.when(_is_missing_satellite(), F.lit("MISSING_SATELLITE_ID")),
        F.when(_is_missing_timestamp(), F.lit("MISSING_TIMESTAMP")),
        F.when(_is_oob_voltage(), F.lit("OOB_BATTERY_VOLTAGE")),
    )


# ---------------------------------------------------------------------------
# SHA-256 deduplication key (payload columns only)
# ---------------------------------------------------------------------------
_PAYLOAD_COLS = [
    "satellite_id",
    "event_timestamp",
    "subsystem",
    "ground_station_id",
    "metrics",
]


def _add_record_hash(df):
    """Add a record_hash column: SHA-256 over the canonical payload."""
    concat_expr = F.concat_ws(
        "||",
        *[
            F.coalesce(F.col(c).cast("string"), F.lit("__null__"))
            for c in _PAYLOAD_COLS
        ],
    )
    return df.withColumn("record_hash", F.sha2(concat_expr, 256))


# ---------------------------------------------------------------------------
# Quarantine (dead-letter) streaming table
# ---------------------------------------------------------------------------
@dlt.table(
    name="telemetry_quarantine",
    comment="Dead-letter table for invalid telemetry records.",
    table_properties={"quality": "silver_quarantine"},
)
def telemetry_quarantine():
    bronze = dlt.read_stream("bronze_telemetry")
    invalid = bronze.filter(_is_invalid())

    return (
        invalid.withColumn("error_code", _build_error_code())
        .withColumn(
            "raw_payload",
            F.to_json(F.struct(*[F.col(c) for c in bronze.columns])),
        )
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Curated (valid, deduplicated) streaming table
# ---------------------------------------------------------------------------
@dlt.table(
    name="telemetry_curated",
    comment="Validated and deduplicated satellite telemetry.",
    table_properties={"quality": "silver"},
)
def telemetry_curated():
    bronze = dlt.read_stream("bronze_telemetry")
    valid = bronze.filter(~_is_invalid())

    # Parse event_timestamp and extract metric scalars
    parsed = (
        valid.withColumn("event_ts", F.to_timestamp("event_timestamp"))
        .withColumn("battery_voltage", F.col("metrics.battery_voltage"))
        .withColumn("signal_dbm", F.col("metrics.signal_dbm"))
    )

    # SHA-256 hash for deduplication (on payload columns, not ingestion metadata)
    hashed = _add_record_hash(parsed)

    # Stateful dedup with 10-minute watermark for late-arriving data
    deduped = hashed.withWatermark("event_ts", "10 minutes").dropDuplicatesWithinWatermark(
        ["record_hash"]
    )

    return deduped
