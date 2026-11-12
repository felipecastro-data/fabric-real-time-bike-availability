# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Bronze · Daily station_information snapshot
# - Lands GBFS `station_information` (names, `short_name`, capacity, location) in `lh_bronze` `dbo.station_information`
# - One snapshot per `snapshot_date` (the feed's Chicago date); a rerun on the same day replaces that day only (idempotent)
# - Keeps each station as raw JSON too, so Silver/SCD2 can be rebuilt from Bronze
# - Python notebook (delta-rs), no Spark session; each run writes its outcome to `lh_bronze/Files/runs/nb_bronze_station_information/`

# CELL ********************

# Install the divvy_rti wheel from lh_bronze/Files/wheels of this workspace (same cell as nb_gbfs_poller).
import subprocess
import sys

import notebookutils

context = notebookutils.runtime.context
workspace_id = context["currentWorkspaceId"]
# OneLake paths take both IDs or both names, never a mix
bronze = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{notebookutils.lakehouse.get('lh_bronze')['id']}"
wheels = [f for f in notebookutils.fs.ls(f"{bronze}/Files/wheels") if f.name.startswith("divvy_rti-") and f.name.endswith(".whl")]
if len(wheels) != 1:
    raise RuntimeError(f"expected exactly one divvy_rti wheel in lh_bronze/Files/wheels, found {[w.name for w in wheels]}")
local_wheel = f"/tmp/wheels/{wheels[0].name}"
notebookutils.fs.cp(wheels[0].path, f"file:{local_wheel}")
subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", local_wheel], check=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json
import traceback
import uuid
from datetime import UTC, date, datetime

from divvy_rti.gbfs import DISCOVERY_URL, discover_feeds, get_with_retry, station_information_rows


def land_snapshot():
    import pyarrow as pa  # imported here so an import failure is captured in the run record
    import pyarrow.dataset as ds
    from deltalake import DeltaTable, write_deltalake

    schema = pa.schema(
        [
            ("station_id", pa.string()),
            ("name", pa.string()),
            ("short_name", pa.string()),
            ("lat", pa.float64()),
            ("lon", pa.float64()),
            ("capacity", pa.int64()),
            ("region_id", pa.string()),
            ("address", pa.string()),
            ("is_charging", pa.bool_()),
            ("raw", pa.string()),
            ("feed_last_updated", pa.timestamp("us", tz="UTC")),
            ("fetched_at", pa.timestamp("us", tz="UTC")),
            ("snapshot_date", pa.date32()),
        ]
    )

    feeds = discover_feeds(get_with_retry(DISCOVERY_URL, {}).body)
    fetched_at = datetime.now(UTC)
    snapshot_date, rows = station_information_rows(get_with_retry(feeds["station_information"], {}).body, fetched_at)
    for row in rows:
        row["snapshot_date"] = date.fromisoformat(row["snapshot_date"])

    table_uri = f"{bronze}/Tables/dbo/station_information"
    storage_options = {"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}
    write_deltalake(
        table_uri,
        pa.Table.from_pylist(rows, schema=schema),
        mode="overwrite",
        predicate=f"snapshot_date = '{snapshot_date}'",  # replace this day only
        partition_by=["snapshot_date"],
        storage_options=storage_options,
    )
    landed = (
        DeltaTable(table_uri, storage_options=storage_options)
        .to_pyarrow_dataset()
        .count_rows(filter=ds.field("snapshot_date") == date.fromisoformat(snapshot_date))
    )
    if landed != len(rows):
        raise RuntimeError(f"snapshot {snapshot_date} has {landed} rows, expected {len(rows)}")
    return {"snapshot_date": snapshot_date, "stations": len(rows), "rows_in_snapshot": landed}


# The job API only reports a generic failure, so every run writes its outcome (and traceback) to Files/runs.
run_id, started_at = str(uuid.uuid4()), datetime.now(UTC)
try:
    record = {"status": "succeeded", **land_snapshot()}
except Exception:
    record = {"status": "failed", "error": traceback.format_exc()}
record |= {"run_id": run_id, "started_at": started_at.isoformat(), "ended_at": datetime.now(UTC).isoformat(),
           "user": context.get("userName")}
print(json.dumps(record))
notebookutils.fs.put(
    f"{bronze}/Files/runs/nb_bronze_station_information/{started_at:%Y-%m-%d}/{run_id}.json", json.dumps(record), True
)
if record["status"] == "failed":
    raise RuntimeError(record["error"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
