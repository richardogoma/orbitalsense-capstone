# OrbitalSense — Design Rationale

This document provides technical defense for the key architectural decisions
in the OrbitalSense satellite telemetry streaming platform.

---

## 1. Ground Station Coverage & Ordering

### Design

The simulator maps each satellite to a **rotating window of 4 visible
ground stations** (out of 8 total) that shifts every batch cycle. The
visibility function uses a deterministic offset based on satellite index
and batch number:

```python
offset = (satellite_idx * 3 + batch_num) % len(GROUND_STATIONS)
visible = [GROUND_STATIONS[(offset + i) % 8] for i in range(4)]
```

### Rationale

- **Orbital realism**: LEO satellites in polar/sun-synchronous orbits see
  different ground stations as they progress through their orbit. The
  rotating window approximates ~90-minute orbital periods where each pass
  exposes a different hemisphere's stations.
- **Non-uniform distribution**: The `* 3` multiplier ensures adjacent
  satellites don't share identical visibility windows, creating realistic
  inter-satellite diversity in ground station assignments.

### Downstream Impact on Message Ordering

Because a satellite's ground station changes between batches, the same
satellite's telemetry arrives through different stations over time. This
means:

- **No guaranteed ordering** by ground station within a satellite's
  time series — the Gold layer's `telemetry_volume_by_station` view
  correctly counts per (satellite, station) pair rather than assuming
  a fixed mapping.
- **Auto Loader file ordering** is based on file arrival, not event
  time. The watermark in Silver handles out-of-order events.

---

## 2. Data Integrity vs. Lateness

### The Distinction

| Scenario | Handling | Destination |
|---|---|---|
| **Malformed data** (missing `satellite_id`, OOB voltage) | Deterministic validation at Silver | `telemetry_quarantine` |
| **Late valid data** (correct but delayed > watermark) | Watermark expiry at Silver dedup | `telemetry_curated` (accepted as new) |
| **Late malformed data** | Validation first, then watermark | `telemetry_quarantine` (never reaches dedup) |

### How It Works

1. **Validation runs first**: Every record from Bronze hits the validation
   gate before any dedup logic. This ensures malformed records are quarantined
   regardless of timing.

2. **Watermark bounds dedup state**: The 10-minute watermark on `event_ts`
   means Spark retains dedup state for 10 minutes after the latest event
   timestamp seen. Records arriving within this window are deduplicated;
   records arriving after are treated as new.

3. **Post-watermark duplicates**: If a legitimate duplicate arrives after
   the watermark expires (e.g., a ground station retransmits after a 15-minute
   outage), it will appear as a new record in `telemetry_curated`. This is
   an **accepted trade-off** — the alternative (infinite state retention)
   is not scalable.

### Dropout Window Behavior

When a satellite enters dropout (3–8 batches silent):

- No records are generated during the dropout window.
- When the satellite resumes, its first post-dropout record has a fresh
  `event_timestamp` that is well beyond the watermark of its last
  pre-dropout record.
- The dedup state for the dropout satellite's old records has already
  been evicted, so there is no risk of false dedup matches.

---

## 3. Retention & Storage Policy

### Raw Data (UC Volume)

| Aspect | Policy | Rationale |
|---|---|---|
| **Format** | JSON-lines files | Human-readable, debuggable, compatible with Auto Loader |
| **Retention** | Indefinite (manual cleanup) | Raw files are the immutable audit trail |
| **Cleanup** | Manual or scheduled job | `dbutils.fs.rm("/Volumes/.../raw_telemetry_vol/", recurse=True)` |
| **Cost control** | Managed Volume (cloud-provider storage) | No external bucket management needed |

### Delta Tables (Bronze / Silver / Gold)

| Layer | Table Type | Retention | Rationale |
|---|---|---|---|
| Bronze | Streaming Table | 30 days (configurable via `delta.deletedFileRetentionDuration`) | Raw landing zone; original files in Volume serve as backup |
| Silver (Curated) | Streaming Table | 90 days | Primary analytical source; longer retention for trend analysis |
| Silver (Quarantine) | Streaming Table | 90 days | Forensic investigation of data quality issues |
| Gold | Materialized Views | N/A (recomputed) | Always current; no independent retention needed |

### Recommended VACUUM Schedule

```sql
-- Run weekly to reclaim storage from old Delta versions
VACUUM dev_orbitalsense.telemetry.bronze_telemetry RETAIN 720 HOURS;
VACUUM dev_orbitalsense.telemetry.telemetry_curated RETAIN 2160 HOURS;
VACUUM dev_orbitalsense.telemetry.telemetry_quarantine RETAIN 2160 HOURS;
```

---

## 4. Scalability Analysis

### Current Design Point

- **12 satellites**, 8 ground stations, ~12 records/batch, 5s interval
- ~144 records/min sustained, ~8,640 records/hour
- Single JSON-lines file per batch

### What Breaks First at 1,200 Satellites

| Component | 12 Satellites | 1,200 Satellites | Breaks? |
|---|---|---|---|
| **Simulator I/O** | 1 file/batch, ~12 records | 1 file/batch, ~1,200 records | No — file size scales linearly |
| **Auto Loader** | Trivial throughput | ~14,400 records/min | No — Auto Loader handles this |
| **Silver dedup state** | ~720 hashes in watermark | ~72,000 hashes in watermark | **Yes** — state size grows 100x |
| **Gold window functions** | 12 partitions | 1,200 partitions | Moderate — shuffle increases |
| **Single-file batches** | Fine | Bottleneck | **Yes** — single writer thread |

### The Bottleneck: Dedup State Size

At 1,200 satellites with a 10-minute watermark, Spark must maintain
~72,000 SHA-256 hashes in the state store per micro-batch. This is
manageable but approaches the point where state checkpointing adds
latency.

### The Single Cheapest Change: Partition the Simulator Output

**Change**: Modify the simulator to write **one file per ground station
per batch** instead of a single monolithic file.

```python
# Current: 1 file with all records
_write_payload(all_records, volume_path)

# Proposed: 1 file per ground station
for gs, records in group_by_ground_station(all_records).items():
    _write_payload(records, f"{volume_path}/{gs}/")
```

**Impact**:

- Auto Loader parallelizes ingestion across 8 subdirectories.
- The pipeline can leverage file-level parallelism for Bronze ingestion.
- No changes needed to Silver/Gold logic.
- Estimated throughput improvement: **4–8x** for the ingestion bottleneck.

**Cost**: ~10 lines of simulator code. Zero infrastructure changes.

### Additional Scaling Levers (if needed)

1. **Increase watermark** from 10 min to 30 min if duplicate rate rises
   with scale (trades memory for correctness).
2. **Switch to RocksDB state store** for Silver dedup if in-memory state
   exceeds driver memory (configuration change only).
3. **Add `satellite_id` partitioning** to Silver/Gold tables for
   partition pruning on satellite-specific queries.
4. **Move to continuous pipeline mode** via the backing job schedule
   pattern (already supported in the DAB configuration) for sub-second
   latency requirements.
