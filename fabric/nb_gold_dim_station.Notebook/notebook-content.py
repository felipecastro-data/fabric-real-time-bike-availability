# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Gold · `dim_station` SCD2 + DQ gate
# - Applies every `lh_bronze` `dbo.station_information` snapshot newer than `lh_gold` `dbo.dim_station`, oldest first:
#   hash-diff on name / short_name / capacity + relocation > 100 m → new version dated by the feed's `last_updated`;
#   small drift, region, address, charging → type 1; missing stations closed with `end_reason = 'removed'`
# - Re-points `fact_availability_15min` rows whose as-of station version changed (windows written before the load)
# - DQ gate: SCD2 invariants, Bronze = Silver + dead-letter, dead-letter rate, Silver freshness → fails the run
# - Reports the trips bridge coverage (`short_name` ↔ medallion `fact_trips`, not gating)
# - Results appended to `lh_gold` `dbo.dq_results`; run record in `lh_gold/Files/runs/nb_gold_dim_station/`

# PARAMETERS CELL ********************

freshness_max_minutes = 30  # prod streams 24/7; dev/test pass a large value (bounded runs)
reconcile_lag_minutes = 60  # only Bronze rows polled before now - lag must be in Silver / dead-letter
max_deadletter_rate = 0.01

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Install the divvy_rti wheel on the driver (same as nb_silver_availability).
import subprocess
import sys

import notebookutils

context = notebookutils.runtime.context
workspace_id = context["currentWorkspaceId"]


def lakehouse(name):
    # OneLake paths take both IDs or both names, never a mix
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{notebookutils.lakehouse.get(name)['id']}"


bronze, silver, gold = lakehouse("lh_bronze"), lakehouse("lh_silver"), lakehouse("lh_gold")
wheels = [f for f in notebookutils.fs.ls(f"{bronze}/Files/wheels") if f.name.startswith("divvy_rti-") and f.name.endswith(".whl")]
if len(wheels) != 1:
    raise RuntimeError(f"expected exactly one divvy_rti wheel in lh_bronze/Files/wheels, found {[w.name for w in wheels]}")
local_wheel = f"/tmp/wheels/{wheels[0].name}"
notebookutils.fs.cp(wheels[0].path, f"file:{local_wheel}")
subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "--no-deps", "--force-reinstall", local_wheel], check=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import traceback
import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from divvy_rti import dq
from divvy_rti.dim_station import apply_snapshots, rekey_facts


def table(root, name):
    return f"{root}/Tables/dbo/{name}"


def read(root, name):
    return spark.read.format("delta").load(table(root, name))


def run_checks(now):
    applied = apply_snapshots(spark, table(bronze, "station_information"), table(gold, "dim_station"))
    rekeyed = rekey_facts(spark, table(gold, "dim_station"), table(gold, "fact_availability_15min"))
    applied.append({"fact_rows_rekeyed": rekeyed})
    events, silver_rows, deadletter = read(bronze, "station_status_events"), read(silver, "station_availability"), read(silver, "availability_deadletter")
    checks = [
        *dq.scd2_checks(read(gold, "dim_station")),
        *dq.reconciliation_checks(events, silver_rows, deadletter, now - timedelta(minutes=float(reconcile_lag_minutes))),
        dq.deadletter_rate_check(events.count(), deadletter.count(), float(max_deadletter_rate)),
        dq.freshness_check(silver_rows, now, timedelta(minutes=float(freshness_max_minutes))),
        *dq.coverage_checks(read(gold, "dim_station"), read(gold, "fact_trips")),
    ]
    return applied, checks


# The job API only reports a generic failure, so every run writes its outcome (and traceback) to Files/runs.
run_id, started_at = str(uuid.uuid4()), datetime.now(UTC)
try:
    applied, checks = run_checks(started_at)
    results = [{"run_id": run_id, "checked_at": started_at, **asdict(c), "value": None if c.value is None else float(c.value)} for c in checks]
    spark.createDataFrame(results, "run_id string, checked_at timestamp, name string, passed boolean, value double, detail string, gate boolean") \
        .write.format("delta").mode("append").save(table(gold, "dq_results"))
    failed = [c.name for c in checks if c.gate and not c.passed]
    record = {"status": "failed" if failed else "succeeded", "applied_snapshots": applied, "failed_checks": failed,
              "checks": [{**asdict(c)} for c in checks]}
    if failed:
        record["error"] = f"DQ gate failed: {failed}"
except Exception:
    record = {"status": "failed", "error": traceback.format_exc()}
record |= {"run_id": run_id, "started_at": started_at.isoformat(), "ended_at": datetime.now(UTC).isoformat(), "user": context.get("userName")}
print(json.dumps(record, default=str))
notebookutils.fs.put(f"{gold}/Files/runs/nb_gold_dim_station/{started_at:%Y-%m-%d}/{run_id}.json", json.dumps(record, default=str), True)
if record["status"] == "failed":
    raise RuntimeError(record["error"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
