# OrbitalSense — Satellite Telemetry Streaming Platform

> A production-grade streaming data pipeline for satellite telemetry, built on
> Databricks Lakehouse with Lakeflow Spark Declarative Pipelines, Unity Catalog,
> and Delta Lake.

## Architecture Overview

OrbitalSense simulates a fleet of **12 LEO (Low Earth Orbit) satellites**
transmitting telemetry through a network of **8 ground stations**. The platform
implements a **Medallion Architecture** (Bronze → Silver → Gold) to progressively
refine raw telemetry into analytics-ready datasets.

```
UC Volume (raw JSON) ─► Bronze (Auto Loader) ─► Silver (Curated + Quarantine) ─► Gold (Analytics MVs)
```

### Key Capabilities

- **Streaming ingestion** via Auto Loader with schema evolution
- **Dead-letter quarantine** for malformed / OOB telemetry records
- **Stateful deduplication** using SHA-256 payload hashing with watermarks
- **Fault-tolerant simulation** with configurable chaos injection
- **Unity Catalog governance** with parameterized catalog/schema per environment
- **Continuous pipeline schedule** with performance optimization and failure email alerts
- **Analytics dashboard** (4 pages, 17 widgets) with custom dark theme for real-time fleet monitoring

## Dataset Schemas

### Bronze: `bronze_telemetry`

| Column | Type | Description |
|---|---|---|
| `satellite_id` | STRING | Satellite identifier (e.g., SAT-0001) |
| `event_timestamp` | STRING | ISO-8601 UTC timestamp of the telemetry event |
| `subsystem` | STRING | Source subsystem (POWER, COMMS, THERMAL, ATTITUDE, PROPULSION, PAYLOAD) |
| `ground_station_id` | STRING | Receiving ground station |
| `metrics` | STRUCT\<battery_voltage: DOUBLE, signal_dbm: DOUBLE\> | Telemetry measurements |
| `ingestion_timestamp` | TIMESTAMP | Pipeline ingestion time |
| `_source_file` | STRING | Source file path for lineage |

### Silver: `telemetry_curated`

All Bronze columns plus:

| Column | Type | Description |
|---|---|---|
| `battery_voltage` | DOUBLE | Extracted and validated voltage (0.0 – 50.0 V) |
| `signal_dbm` | DOUBLE | Extracted signal strength |
| `event_ts` | TIMESTAMP | Parsed event timestamp |
| `record_hash` | STRING | SHA-256 deduplication key |

### Silver: `telemetry_quarantine`

| Column | Type | Description |
|---|---|---|
| `error_code` | STRING | Pipe-delimited validation failure codes |
| `raw_payload` | STRING | Full JSON record for forensic analysis |
| `quarantine_timestamp` | TIMESTAMP | Time of quarantine routing |

### Gold Materialized Views

| View | Business Question |
|---|---|
| `telemetry_volume_by_station` | Which satellites generated the most telemetry per ground station? |
| `voltage_degradation_monitor` | Rolling avg battery voltage per satellite; CRITICAL flag (< 25.0 V) |
| `signal_strength_report` | Weakest communication signals by satellite and time window |
| `subsystem_alert_correlation` | Subsystem quarantine frequency and top error codes |

## Quickstart

### Prerequisites

- Databricks CLI >= 0.230.0
- Unity Catalog enabled workspace with CREATE CATALOG / CREATE SCHEMA permissions
- Python 3.10+

### Deploy & Run

```bash
# Clone and navigate
git clone <repo-url> && cd orbitalsense-dab

# Deploy the bundle (dev target)
databricks bundle deploy --target dev

# Run the telemetry simulator (serverless)
databricks bundle run telemetry_simulator --target dev

# The pipeline runs continuously via its backing schedule job.
# To trigger manually instead:
databricks bundle run orbital_sense_pipeline --target dev
```

The simulator and pipeline are **decoupled by design** — run them
independently or simultaneously. Auto Loader picks up files as they land.

### Teardown

```bash
databricks bundle destroy --target dev
```

## Project Structure

```
orbitalsense-dab/
├── databricks.yml                          # Bundle configuration
├── README.md
├── docs/
│   ├── architecture_diagram.md             # Mermaid.js system diagram
│   ├── deployment_guide.md                 # Step-by-step deployment
│   └── design_rationale.md                 # Technical design decisions
├── resources/
│   ├── raw_telemetry_vol.volume.yml              # UC Volume definition
│   ├── telemetry_simulator.job.yml               # Simulator job (serverless)
│   ├── orbital_sense_pipeline.pipeline.yml       # SDP pipeline definition
│   ├── orbital_sense_pipeline_schedule.job.yml   # Continuous pipeline schedule
│   └── orbital_sense_dashboard.dashboard.yml     # Analytics dashboard
├── utilities/
│   └── simulator.py                              # Telemetry data generator
└── src/
    ├── dashboards/
    │   └── orbital_sense_analytics.lvdash.json    # Dashboard config (4 pages)
    └── orbital_sense_pipeline/
        └── transformations/
            ├── 01-bronze/
            │   └── bronze_ingestion.py            # Auto Loader ingestion
            ├── 02-silver/
            │   └── silver_curated_and_quarantine.py
            └── 03-gold/
                └── gold_analytics.py              # Business analytics views
```

## Documentation

- [Architecture Diagram](docs/architecture_diagram.md)
- [Deployment Guide](docs/deployment_guide.md)
- [Design Rationale](docs/design_rationale.md)
- [DABs Configuration Reference](https://docs.databricks.com/dev-tools/bundles/reference)

## License

MIT License — see [LICENSE](LICENSE) for details.
