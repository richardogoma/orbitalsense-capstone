"""
OrbitalSense Telemetry Simulator
=================================
Generates synthetic satellite telemetry JSON payloads with configurable fault
injection to validate the downstream Lakeflow Declarative Pipeline.

Fault Modes
-----------
- Duplicate records   : same payload written twice in the same batch
- Missing fields      : satellite_id or event_timestamp omitted
- Out-of-bounds voltage: battery_voltage set to -99.0, -1.0, 55.0, or 999.0
- Satellite dropout   : a satellite stops reporting for 3-8 consecutive batches

Usage (standalone)::

    python simulator.py --volume-path /Volumes/catalog/schema/raw_telemetry_vol \
                        --num-satellites 12 --batch-interval 5 --num-batches 100
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SUBSYSTEMS = ["POWER", "COMMS", "THERMAL", "ATTITUDE", "PROPULSION", "PAYLOAD"]

GROUND_STATIONS = [
    "GS-SVALBARD",
    "GS-MCMURDO",
    "GS-CANBERRA",
    "GS-GOLDSTONE",
    "GS-MADRID",
    "GS-KOUROU",
    "GS-BANGALORE",
    "GS-HARTEBEESTHOEK",
]

# Orbital visibility: each satellite can see this many ground stations at once
ORBIT_SLOTS = 4

# Fault-injection probabilities (non-deterministic)
P_DUPLICATE = 0.05  # 5% chance a record is duplicated
P_MISSING_FIELD = 0.04  # 4% chance a required field is dropped
P_OOB_VOLTAGE = 0.06  # 6% chance battery_voltage is out of bounds
P_DROPOUT_START = 0.02  # 2% chance a satellite enters dropout per batch
DROPOUT_DURATION_BATCHES = (3, 8)  # min/max batches a dropout lasts


# ---------------------------------------------------------------------------
# Ground-station visibility simulation
# ---------------------------------------------------------------------------
def _visible_ground_stations(satellite_idx: int, batch_num: int) -> list:
    """Return a rotating subset of ground stations visible to a satellite.

    Simulates orbital mechanics: the visible set shifts each batch as the
    satellite progresses along its orbit, ensuring downstream message
    ordering varies realistically across ground stations.
    """
    offset = (satellite_idx * 3 + batch_num) % len(GROUND_STATIONS)
    return [
        GROUND_STATIONS[(offset + i) % len(GROUND_STATIONS)]
        for i in range(ORBIT_SLOTS)
    ]


# ---------------------------------------------------------------------------
# Record generators
# ---------------------------------------------------------------------------
def _nominal_record(satellite_id: str, ground_station: str) -> dict:
    """Generate a nominal (healthy) telemetry record."""
    return {
        "satellite_id": satellite_id,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "subsystem": random.choice(SUBSYSTEMS),
        "ground_station_id": ground_station,
        "metrics": {
            "battery_voltage": round(random.uniform(22.0, 42.0), 2),
            "signal_dbm": round(random.uniform(-120.0, -30.0), 2),
        },
    }


def _inject_faults(record: dict) -> tuple:
    """Apply zero or more fault injections to *record*.

    Returns (list_of_records, fault_tag_string).  The list may contain
    duplicates.  Faults are applied independently so a single record can
    exhibit multiple faults simultaneously.
    """
    faults = []
    records = [record]

    # --- Out-of-bounds voltage ------------------------------------------------
    if random.random() < P_OOB_VOLTAGE:
        bad_voltage = random.choice([-99.0, -1.0, 55.0, 999.0])
        record["metrics"]["battery_voltage"] = bad_voltage
        faults.append(f"oob_voltage({bad_voltage})")

    # --- Missing required field -----------------------------------------------
    if random.random() < P_MISSING_FIELD:
        field = random.choice(["satellite_id", "event_timestamp"])
        record.pop(field, None)
        faults.append(f"missing_{field}")

    # --- Duplicate record -----------------------------------------------------
    if random.random() < P_DUPLICATE:
        import copy

        records.append(copy.deepcopy(record))
        faults.append("duplicate")

    tag = "|".join(faults) if faults else "nominal"
    return records, tag


# ---------------------------------------------------------------------------
# File writer
# ---------------------------------------------------------------------------
def _write_payload(records: list, volume_path: str) -> str:
    """Write records as a JSON-lines file to the UC Volume."""
    file_id = uuid.uuid4().hex[:12]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    filename = f"telemetry_{ts}_{file_id}.json"
    filepath = os.path.join(volume_path, filename)

    with open(filepath, "w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")

    return filepath


# ---------------------------------------------------------------------------
# Main simulation loop
# ---------------------------------------------------------------------------
def run_simulator(
    volume_path: str,
    num_satellites: int = 12,
    batch_interval: int = 5,
    num_batches: int = 100,
):
    """Generate *num_batches* of telemetry for *num_satellites* satellites.

    Each batch writes one JSON-lines file to *volume_path*.  Fault injection
    fires at non-deterministic intervals to simulate real-world chaos.
    """
    satellite_ids = [f"SAT-{str(i).zfill(4)}" for i in range(1, num_satellites + 1)]

    # Track dropout state: {sat_id: remaining_dropout_batches}
    dropout_state: dict = {}

    print(
        f"[OrbitalSense Simulator] Starting — {num_satellites} satellites, "
        f"{num_batches} batches, interval {batch_interval}s"
    )
    print(f"[OrbitalSense Simulator] Volume path: {volume_path}")

    for batch in range(num_batches):
        batch_records = []
        fault_log = []

        for idx, sat_id in enumerate(satellite_ids):
            # ---- Dropout logic ------------------------------------------------
            if sat_id in dropout_state:
                dropout_state[sat_id] -= 1
                if dropout_state[sat_id] <= 0:
                    del dropout_state[sat_id]
                    print(f"  [DROPOUT END]   {sat_id} back online at batch {batch}")
                else:
                    fault_log.append(f"{sat_id}:dropout")
                    continue  # satellite is silent
            elif random.random() < P_DROPOUT_START:
                dur = random.randint(*DROPOUT_DURATION_BATCHES)
                dropout_state[sat_id] = dur
                print(
                    f"  [DROPOUT START] {sat_id} going silent for {dur} batches"
                )
                fault_log.append(f"{sat_id}:dropout_start({dur})")
                continue

            # ---- Generate telemetry -------------------------------------------
            visible = _visible_ground_stations(idx, batch)
            gs = random.choice(visible)
            record = _nominal_record(sat_id, gs)
            injected, tag = _inject_faults(record)
            batch_records.extend(injected)
            if tag != "nominal":
                fault_log.append(f"{sat_id}:{tag}")

        # ---- Write batch ------------------------------------------------------
        if batch_records:
            fp = _write_payload(batch_records, volume_path)
            print(
                f"  Batch {batch + 1}/{num_batches} — "
                f"{len(batch_records)} records -> {os.path.basename(fp)}"
            )
        else:
            print(
                f"  Batch {batch + 1}/{num_batches} — "
                f"0 records (all satellites in dropout)"
            )

        if fault_log:
            print(f"    Faults: {', '.join(fault_log)}")

        if batch < num_batches - 1:
            time.sleep(batch_interval)

    print("[OrbitalSense Simulator] Complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="OrbitalSense Telemetry Simulator"
    )
    parser.add_argument(
        "--volume-path",
        required=True,
        help="UC Volume path for output (e.g. /Volumes/cat/schema/vol)",
    )
    parser.add_argument(
        "--num-satellites", type=int, default=12, help="Number of satellites"
    )
    parser.add_argument(
        "--batch-interval",
        type=int,
        default=5,
        help="Seconds between batches",
    )
    parser.add_argument(
        "--num-batches", type=int, default=100, help="Total batches to generate"
    )
    args = parser.parse_args()

    run_simulator(
        volume_path=args.volume_path,
        num_satellites=args.num_satellites,
        batch_interval=args.batch_interval,
        num_batches=args.num_batches,
    )
