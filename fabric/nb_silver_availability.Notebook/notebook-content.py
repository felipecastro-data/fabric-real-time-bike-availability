# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Warm path · Bronze → Silver + dead-letter → Gold 15 min
# - Streams `lh_bronze` `dbo.station_status_events` (Delta) with two queries, each with its own checkpoint:
#   - `foreachBatch` insert-only MERGE into `lh_silver` `dbo.station_availability` keyed by `(station_id, last_reported)`,
#     rule violations to `dbo.availability_deadletter` with a reason code (replay-safe → exactly-once)
#   - 15-min windows per station into `lh_gold` `dbo.fact_availability_15min` (watermark 10 min, append mode)
# - `duration_minutes = 0` → `availableNow` (dev/test/CI); prod runs hourly with a 1-min trigger for `duration_minutes`
# - Logic lives in `divvy_rti.availability` (tested locally with Spark + Delta); run record in `lh_silver/Files/runs/`

# PARAMETERS CELL ********************

duration_minutes = 50  # prod: hourly schedule, ends before the next run starts (two runs must not share a checkpoint)
processing_time = "1 minute"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Install the divvy_rti wheel on the driver (DataFrame API only: executors don't need it). --no-deps: azure-eventhub
# is only for the poller and the Spark runtime already has pyspark + delta.
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
from datetime import UTC, datetime

from divvy_rti.availability import Paths, run

paths = Paths(
    bronze=f"{bronze}/Tables/dbo/station_status_events",
    silver=f"{silver}/Tables/dbo/station_availability",
    deadletter=f"{silver}/Tables/dbo/availability_deadletter",
    gold=f"{gold}/Tables/dbo/fact_availability_15min",
    silver_checkpoint=f"{silver}/Files/checkpoints/station_availability",
    gold_checkpoint=f"{gold}/Files/checkpoints/fact_availability_15min_v2",  # v2: foreachBatch + as-of key (Step 7)
    dim_station=f"{gold}/Tables/dbo/dim_station",
)

# The job API only reports a generic failure, so every run writes its outcome (and traceback) to Files/runs.
run_id, started_at = str(uuid.uuid4()), datetime.now(UTC)
try:
    last_batches = run(spark, paths, duration_s=float(duration_minutes) * 60, processing_time=processing_time)
    counts = {
        name: spark.read.format("delta").load(path).count()
        for name, path in (("bronze", paths.bronze), ("silver", paths.silver), ("deadletter", paths.deadletter), ("gold", paths.gold))
    }
    record = {"status": "succeeded", "last_batch_ids": last_batches, "rows": counts}
except Exception:
    record = {"status": "failed", "error": traceback.format_exc()}
record |= {"run_id": run_id, "started_at": started_at.isoformat(), "ended_at": datetime.now(UTC).isoformat(),
           "user": context.get("userName"), "duration_minutes": duration_minutes}
print(json.dumps(record))
notebookutils.fs.put(f"{silver}/Files/runs/nb_silver_availability/{started_at:%Y-%m-%d}/{run_id}.json", json.dumps(record), True)
if record["status"] == "failed":
    raise RuntimeError(record["error"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
