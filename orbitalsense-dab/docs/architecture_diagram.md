# OrbitalSense — Architecture Diagram

## System Overview

The diagram below shows the end-to-end data flow from satellite telemetry
generation through the Medallion Architecture (Bronze → Silver → Gold),
including the quarantine (dead-letter) routing for malformed records,
SHA-256 deduplication mechanics, and watermark handling for late-arriving data.

```mermaid
flowchart TB
    subgraph SIM["Telemetry Simulator"]
        direction TB
        SAT["12 LEO Satellites<br/>SAT-0001 … SAT-0012"]
        GS["8 Ground Stations<br/>Svalbard · McMurdo · Canberra<br/>Goldstone · Madrid · Kourou<br/>Bangalore · Hartebeesthoek"]
        FAULT["Fault Injector<br/>─────────────<br/>• Duplicates (5%)<br/>• Missing fields (4%)<br/>• OOB voltage (6%)<br/>• Dropout (2% start)"]
        SAT -->|orbital visibility<br/>4 stations/pass| GS
        GS --> FAULT
    end

    subgraph VOL["Unity Catalog Volume"]
        RAW["raw_telemetry_vol<br/>(JSON-lines files)"]
    end

    FAULT -->|write JSON-lines| RAW

    subgraph PIPELINE["Lakeflow Spark Declarative Pipeline"]
        direction TB

        subgraph BRONZE["01-Bronze"]
            AL["Auto Loader<br/>(cloudFiles)"]
            BT["bronze_telemetry<br/>───────────<br/>+ ingestion_timestamp<br/>+ _source_file"]
            AL -->|schema hints<br/>+ evolution| BT
        end

        subgraph SILVER["02-Silver"]
            direction TB
            VAL{"Validation<br/>Gate"}
            Q_PATH["INVALID"]
            V_PATH["VALID"]
            QT["telemetry_quarantine<br/>──────────────<br/>+ error_code<br/>+ raw_payload<br/>+ quarantine_timestamp"]
            HASH["SHA-256 Hash<br/>(payload cols only)"]
            WM["Watermark<br/>event_ts + 10 min"]
            DEDUP["dropDuplicatesWithinWatermark<br/>(record_hash)"]
            CT["telemetry_curated<br/>──────────────<br/>+ event_ts (parsed)<br/>+ battery_voltage<br/>+ signal_dbm<br/>+ record_hash"]

            VAL -->|missing fields<br/>OOB voltage| Q_PATH --> QT
            VAL -->|all rules pass| V_PATH --> HASH
            HASH --> WM --> DEDUP --> CT
        end

        subgraph GOLD["03-Gold (Materialized Views)"]
            MV1["telemetry_volume_by_station<br/>satellite × ground station counts"]
            MV2["voltage_degradation_monitor<br/>50-row rolling avg + CRITICAL flag"]
            MV3["signal_strength_report<br/>min/avg/max dBm + quality tier"]
            MV4["subsystem_alert_correlation<br/>error_code × subsystem frequency"]
        end

        RAW -->|Auto Loader<br/>streaming| AL
        BT --> VAL
        CT --> MV1
        CT --> MV2
        CT --> MV3
        QT --> MV4
    end

    style SIM fill:#1a1a2e,stroke:#e94560,color:#fff
    style VOL fill:#16213e,stroke:#0f3460,color:#fff
    style BRONZE fill:#0f3460,stroke:#533483,color:#fff
    style SILVER fill:#533483,stroke:#e94560,color:#fff
    style GOLD fill:#e94560,stroke:#fff,color:#fff
    style QT fill:#ff6b6b,stroke:#c0392b,color:#fff
```

## Data Flow Detail

### Happy Path (Nominal Telemetry)

```
Satellite → Ground Station → JSON-lines file → UC Volume
  → Auto Loader (Bronze) → Validation Gate (PASS)
  → SHA-256 Hash → Watermark (10 min) → Dedup → telemetry_curated
  → Gold MVs (volume, voltage, signal, subsystem)
```

### Quarantine Path (Malformed Data)

```
Satellite → Ground Station → JSON-lines file → UC Volume
  → Auto Loader (Bronze) → Validation Gate (FAIL)
  → error_code assigned → raw_payload preserved → telemetry_quarantine
  → subsystem_alert_correlation (Gold)
```

### Deduplication Mechanics

1. **Hash computation**: SHA-256 over `satellite_id || event_timestamp ||
   subsystem || ground_station_id || metrics` — ingestion metadata excluded
   to ensure identical payloads produce the same hash regardless of when
   they were ingested.
2. **Watermark**: 10-minute window on `event_ts` bounds Spark state.
   Duplicate records arriving within 10 minutes of the original are dropped.
3. **Late data**: Records arriving after the watermark closes are still
   processed if they pass validation, but they will not be deduped against
   records that have already left the state store.

### Late-Arriving Data & Watermark

```
Time ──────────────────────────────────────────────────►

  event_ts = T        event_ts = T + 8min    event_ts = T + 12min
  ┌───────┐           ┌───────┐              ┌───────┐
  │ SAT-1 │           │ SAT-1 │              │ SAT-1 │
  │ hash: │           │ hash: │              │ hash: │
  │  abc   │           │  abc   │              │  abc   │
  └───┬───┘           └───┬───┘              └───┬───┘
      │                   │                      │
      ▼                   ▼                      ▼
  ACCEPTED            DEDUP DROP             ACCEPTED
  (first seen)        (within 10m            (watermark expired;
                       watermark)             state evicted;
                                              treated as new)
```

### Satellite Dropout Scenario

```
Batch 1    Batch 2    Batch 3    Batch 4    Batch 5    Batch 6
  SAT-3      SAT-3    [DROPOUT]  [DROPOUT]  [DROPOUT]    SAT-3
 reports   reports     silent     silent     silent     resumes

  Bronze     Bronze    (nothing)  (nothing)  (nothing)   Bronze
  Silver     Silver                                      Silver
                       └─── No records generated ───┘
                            (gap in time series)
```

Dropout windows create gaps in the Gold time-series aggregations. The
`voltage_degradation_monitor` rolling window naturally shrinks during
dropout periods and recovers when the satellite resumes reporting.
