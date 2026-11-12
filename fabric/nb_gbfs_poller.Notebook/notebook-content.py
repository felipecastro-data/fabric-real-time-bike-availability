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

# ## GBFS poller → `es_station_status`
# - Polls Divvy GBFS `station_status` every `interval_seconds` for `duration_minutes`, then exits; prod reschedules it hourly (Terraform)
# - Conditional GET + unchanged `last_updated` skip, so each feed version is sent once per run (dedup downstream covers the hourly restart)
# - Sends one JSON event per station to the custom endpoint `src_gbfs_poller`, found by name in this workspace (no per-stage settings)
# - Auth: Entra token of the identity running the job (`notebookutils`), no keys
# - Logs one JSON line per poll; writes a run summary to `lh_bronze/Files/runs/nb_gbfs_poller/<date>/<run_id>.json`
# - Fails the job when nothing was sent

# PARAMETERS CELL ********************

duration_minutes = 55
interval_seconds = 60

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Install the divvy_rti wheel (and azure-eventhub, its dependency) from lh_bronze/Files/wheels of this workspace.
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
import uuid
from datetime import UTC, datetime

from azure.eventhub import EventHubProducerClient

from divvy_rti.gbfs import DISCOVERY_URL, StationStatusPoller, discover_feeds, get_with_retry
from divvy_rti.stream import EVENTHUBS_AUDIENCE, CallableTokenCredential, resolve_custom_endpoint, run_poll_loop, send_events

run_id = str(uuid.uuid4())
started_at = datetime.now(UTC)


def log(record):
    print(json.dumps({"ts": datetime.now(UTC).isoformat(), "run_id": run_id, **record}), flush=True)


namespace, hub = resolve_custom_endpoint(workspace_id, notebookutils.credentials.getToken("pbi"))
log({"event": "start", "namespace": namespace, "hub": hub, "user": context.get("userName"),
     "duration_minutes": duration_minutes, "interval_seconds": interval_seconds})

poller = StationStatusPoller(discover_feeds(get_with_retry(DISCOVERY_URL, {}).body)["station_status"])
credential = CallableTokenCredential(lambda: notebookutils.credentials.getToken(EVENTHUBS_AUDIENCE))
with EventHubProducerClient(namespace, hub, credential) as producer:
    stats = run_poll_loop(
        poller.poll,
        lambda events: send_events(producer, events),
        duration_s=float(duration_minutes) * 60,
        interval_s=float(interval_seconds),
        log=log,
    )

summary = {
    "event": "end",
    "started_at": started_at.isoformat(),
    "ended_at": datetime.now(UTC).isoformat(),
    "user": context.get("userName"),
    "stats": dict(stats),
}
log(summary)
notebookutils.fs.put(
    f"{bronze}/Files/runs/nb_gbfs_poller/{started_at:%Y-%m-%d}/{run_id}.json", json.dumps({"run_id": run_id, **summary}), True
)
if not stats["events_sent"]:
    raise RuntimeError(f"nb_gbfs_poller sent no events: {dict(stats)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
