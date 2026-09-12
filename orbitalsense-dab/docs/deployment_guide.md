# OrbitalSense — Deployment Guide

## 1. Prerequisites

### Tooling

| Tool | Minimum Version | Install |
|---|---|---|
| Databricks CLI | >= 0.230.0 | `brew install databricks` or `pip install databricks-cli` |
| Python | >= 3.10 | System or pyenv |
| Git | >= 2.30 | System package manager |

### Workspace Permissions

- **Unity Catalog**: CREATE CATALOG, CREATE SCHEMA, CREATE VOLUME
  on the target metastore (or pre-created catalog/schema).
- **Compute**: Permission to create job clusters or access to serverless
  compute for the Lakeflow pipeline.
- **Workspace**: Write access to the deployment directory
  (auto-managed by DABs under the bundle root path).

### Authentication

Configure the Databricks CLI with a profile or environment variables:

```bash
# OAuth (recommended)
databricks auth login --host https://adb-7990025474660843.3.azuredatabricks.net
```

Alternatively, set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment
variables with a PAT token.

## 2. Deployment Steps

### 2.1 Clone the Repository

```bash
git clone <repo-url>
cd orbitalsense-dab
```

### 2.2 Review Variables

Open `databricks.yml` and verify the variable defaults match your
environment:

```yaml
variables:
  catalog:
    default: "orbitalsense"
  schema:
    default: "telemetry"
  warehouse_id:
    default: "1cf90f29dc0ea17d"  # quick-demos warehouse
```

The `dev` target overrides `catalog` to `dev_orbitalsense`. The
`warehouse_id` is used by the analytics dashboard. To use a different
SQL warehouse, update this value with your warehouse ID.

### 2.3 Validate the Bundle

```bash
databricks bundle validate --target dev
```

Expected output: resource summary with no errors.

### 2.4 Deploy

```bash
databricks bundle deploy --target dev
```

This creates:
- Unity Catalog Volume: `dev_orbitalsense.telemetry.raw_telemetry_vol`
- Databricks Job: `[dev <user>] OrbitalSense Telemetry Simulator` (serverless)
- Databricks Job: `[dev <user>] OrbitalSense Pipeline Schedule` (continuous)
- Lakeflow Pipeline: `[dev <user>] OrbitalSense Streaming Pipeline`
- AI/BI Dashboard: `[dev <user>] OrbitalSense Telemetry Analytics`

### 2.5 Verify Deployment

```bash
databricks bundle summary --target dev
```

## 3. Execution

### 3.1 Start the Telemetry Simulator

```bash
databricks bundle run telemetry_simulator --target dev
```

The simulator writes 100 batches of JSON-lines files (12 satellites per
batch, approximately 5-second intervals) to the UC Volume. Monitor
progress in the job run UI.

For quick testing with a smaller dataset, override parameters:

```bash
databricks bundle run telemetry_simulator --target dev \
  -- --num-batches 10 --batch-interval 2
```

### 3.2 Streaming Pipeline

The pipeline runs **continuously** via its backing schedule job
(`OrbitalSense Pipeline Schedule`), which is deployed in UNPAUSED state.
It will automatically trigger pipeline updates as new data lands.

To trigger a manual one-off update instead:

```bash
databricks bundle run orbital_sense_pipeline --target dev
```

The simulator and pipeline are **decoupled** — run them independently
or simultaneously. Auto Loader picks up files from the Volume checkpoint.

### 3.3 Monitor

- **Simulator job**: Workspace > Lakeflow Jobs > OrbitalSense Telemetry Simulator
- **Pipeline schedule**: Workspace > Lakeflow Jobs > OrbitalSense Pipeline Schedule
- **Pipeline**: Workspace > Lakeflow Pipelines > OrbitalSense Streaming Pipeline
- **Dashboard**: Workspace > SQL > Dashboards > OrbitalSense Telemetry Analytics
- **Tables**: Unity Catalog > dev_orbitalsense.telemetry (all tables)
- **Failure alerts**: Email notifications to richard.ogoma@outlook.com on pipeline failures

## 4. Production Deployment

```bash
databricks bundle validate --target prod
databricks bundle deploy --target prod
databricks bundle run telemetry_simulator --target prod
databricks bundle run orbital_sense_pipeline --target prod
```

Production uses `orbitalsense.telemetry` catalog/schema and runs as
`richard.ogoma-a@nlng.onmicrosoft.com`.

## 5. Selective Re-deployment

To re-deploy after code changes without affecting existing data:

```bash
databricks bundle deploy --target dev
```

To force a full refresh of the pipeline (reprocesses all data from
the Volume checkpoint):

```bash
databricks bundle run orbital_sense_pipeline --target dev --full-refresh
```

## 6. Teardown

Use `databricks bundle` with the appropriate subcommand to remove
deployed resources when they are no longer needed. See the
[DABs documentation](https://docs.databricks.com/dev-tools/bundles/settings.html)
for the full lifecycle management reference.

For manual cleanup, use the Databricks SQL Editor to manage schemas
and catalogs through standard Unity Catalog DDL.

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| VOLUME_NOT_FOUND | Volume not yet created | Run `bundle deploy` first |
| Pipeline stuck in STARTING | Serverless compute provisioning | Wait 2-3 min; check workspace quota |
| No data in Gold tables | Pipeline not triggered | Run `bundle run orbital_sense_pipeline` |
| Duplicate records in curated | Watermark expired | Expected for records > 10 min late |
| Dashboard shows no data | Gold tables empty | Run simulator first, then check pipeline |
