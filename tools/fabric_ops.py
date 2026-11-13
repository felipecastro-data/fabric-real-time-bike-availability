"""Fabric operations via the REST API as the Azure CLI identity (user locally, service principal in CI).

python tools/fabric_ops.py push <workspace-id> fabric/<item>.<Type>      # create the item or update its definition
python tools/fabric_ops.py upload-wheel <workspace-id>                     # build + upload to lh_bronze/Files/wheels
python tools/fabric_ops.py run <workspace-id> <notebook> '{"duration_minutes": "3"}'  # RunNotebook job + poll
python tools/fabric_ops.py kql <workspace-id> kqldb_bikes kql/schema.kql   # `.execute database script`, stop on error
python tools/fabric_ops.py refresh-sql <workspace-id> lh_gold                 # SQL endpoint metadata sync
python tools/fabric_ops.py refresh-model <workspace-id> sm_bike_availability  # Direct Lake framing, wait for result
python tools/fabric_ops.py monitor <workspace-id>                             # prod run health; exit 1 + problems
python tools/fabric_ops.py schedule <workspace-id> infra/prod_schedules.json  # notebook schedules = spec (upsert)

`push` is the dev bootstrap until the dev workspace is Git-connected (Step 8); test/prod get items from the
deployment pipeline. `req`, `wait_lro` and `run` are copied from the medallion repo's tools/fabric_ops.py.
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from fabric_auth import FABRIC_SCOPE, access_token  # fresh GitHub OIDC exchange in CI, az CLI locally

API = "https://api.fabric.microsoft.com/v1"
ONELAKE = "https://onelake.dfs.fabric.microsoft.com"
STORAGE_SCOPE = "https://storage.azure.com/.default"
KUSTO_SCOPE = "https://kusto.kusto.windows.net/.default"
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
POWERBI = "https://api.powerbi.com/v1.0/myorg"
TERMINAL_JOB_STATES = ("Completed", "Failed", "Cancelled", "Deduped")
ROOT = Path(__file__).resolve().parents[1]


def req(method: str, url: str, body: dict | None = None, scope: str = FABRIC_SCOPE) -> tuple[int, dict, dict | None]:
    url = url if url.startswith("http") else API + url
    data = json.dumps(body).encode() if body is not None else None
    for attempt in range(8):
        headers = {"Authorization": f"Bearer {access_token(scope)}", "Content-Type": "application/json"}
        request = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                raw = response.read()
                return response.status, dict(response.headers), json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raise SystemExit(f"{method} {url} -> {exc.code} {exc.read().decode()}") from exc
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:  # transient network
            print(f"  network error ({exc}); retry {attempt + 1}", flush=True)
            time.sleep(10)
    raise SystemExit(f"{method} {url}: network retries exhausted")


def wait_lro(headers: dict, label: str) -> dict:
    while True:
        time.sleep(int(headers.get("Retry-After", 5)))
        _, _, state = req("GET", headers["Location"])
        status = (state or {}).get("status")
        print(f"  {label}: {status}", flush=True)
        if status in ("Succeeded", "Completed"):
            return state
        if status in ("Failed", "Cancelled"):
            raise SystemExit(json.dumps(state))


PLACEHOLDER = re.compile(r"\{\{(workspace|(\w+):([\w-]+))\}\}")


def fill_placeholders(text: str, ws: str, item_id: Callable[[str, str], str]) -> str:
    """Replace `{{workspace}}` and `{{<Type>:<name>}}` with IDs from the target workspace, so one definition in Git
    deploys to every stage (Eventstream destinations must name items of their own workspace by ID)."""
    return PLACEHOLDER.sub(lambda m: ws if m[1] == "workspace" else item_id(m[2], m[3]), text)


def resolve_placeholder(ws: str, item_type: str, name: str) -> str:
    """`{{<Type>:<name>}}` → item ID, plus stage-specific endpoints: `KQLDatabaseUri` (query URI), `SqlEndpoint`
    (lakehouse SQL endpoint ID) and `SqlConnection` (its server name), used by the dashboard and the semantic model;
    `Env` reads an environment variable for values kept out of the public repo (the Activator alert recipient)."""
    if item_type == "Env":
        if not os.environ.get(name):
            raise SystemExit(f"environment variable {name} is required by a {{{{Env:{name}}}}} placeholder")
        return os.environ[name]
    if item_type == "KQLDatabaseUri":
        _, _, items = req("GET", f"/workspaces/{ws}/kqlDatabases")
        return next(i["properties"]["queryServiceUri"] for i in items["value"] if i["displayName"] == name)
    if item_type in ("SqlEndpoint", "SqlConnection"):
        _, _, lakehouse = req("GET", f"/workspaces/{ws}/lakehouses/{workspace_item_id(ws, 'Lakehouse', name)}")
        sql = lakehouse["properties"]["sqlEndpointProperties"]
        return sql["id"] if item_type == "SqlEndpoint" else sql["connectionString"]
    return workspace_item_id(ws, item_type, name)


def workspace_item_id(ws: str, item_type: str, name: str) -> str:
    _, _, items = req("GET", f"/workspaces/{ws}/items?type={item_type}")
    matches = [i["id"] for i in items["value"] if i["displayName"] == name]
    if not matches:
        raise SystemExit(f"{item_type} {name!r} not found in workspace {ws}")
    return matches[0]


def item_definition(item_dir: Path, fill: Callable[[str], str] = lambda text: text) -> tuple[str, str, dict]:
    """(display name, type, definition) from a Git-format item folder; `.platform` only supplies name and type."""
    metadata = json.loads((item_dir / ".platform").read_text())["metadata"]
    parts = [
        {
            "path": path.relative_to(item_dir).as_posix(),
            "payload": base64.b64encode(fill(path.read_text()).encode()).decode(),
            "payloadType": "InlineBase64",
        }
        for path in sorted(item_dir.rglob("*"))
        if path.is_file() and path.name != ".platform"
    ]
    return metadata["displayName"], metadata["type"], {"parts": parts}


def push_item(ws: str, item_dir: Path) -> str:
    """Create the item, or update the definition of the existing item with the same name and type."""
    fill = lambda text: fill_placeholders(text, ws, lambda item_type, name: resolve_placeholder(ws, item_type, name))  # noqa: E731
    name, item_type, definition = item_definition(item_dir, fill)
    _, _, items = req("GET", f"/workspaces/{ws}/items?type={item_type}")
    existing = [i["id"] for i in items["value"] if i["displayName"] == name]
    if existing:
        status, headers, _ = req(
            "POST", f"/workspaces/{ws}/items/{existing[0]}/updateDefinition", {"definition": definition}
        )
        action = "updated"
    else:
        body = {"displayName": name, "type": item_type, "definition": definition}
        status, headers, _ = req("POST", f"/workspaces/{ws}/items", body)
        action = "created"
    if status == 202:
        wait_lro(headers, f"{action} {name}")
    print(f"  {action} {item_type} {name}")
    return action


def onelake_write(ws: str, path: str, data: bytes) -> None:
    """Create or overwrite a OneLake file: create (needs Content-Length: 0) → append → flush."""
    url = f"{ONELAKE}/{ws}/{path}"
    headers = {"Authorization": f"Bearer {access_token(STORAGE_SCOPE)}", "x-ms-version": "2023-11-03"}
    steps = [
        ("PUT", f"{url}?resource=file", b""),
        ("PATCH", f"{url}?action=append&position=0", data),
        ("PATCH", f"{url}?action=flush&position={len(data)}", b""),
    ]
    for method, step_url, body in steps:
        request = urllib.request.Request(step_url, data=body, method=method, headers=headers)
        request.add_header("Content-Length", str(len(body)))
        try:
            urllib.request.urlopen(request, timeout=120).close()  # noqa: S310 - fixed https host
        except urllib.error.HTTPError as exc:
            raise SystemExit(f"{method} {step_url} -> {exc.code} {exc.read().decode()}") from exc


def upload_wheel(ws: str) -> str:
    # OneLake paths take both IDs or both names, never a mix: resolve the lakehouse ID.
    _, _, items = req("GET", f"/workspaces/{ws}/items?type=Lakehouse")
    bronze = next(i["id"] for i in items["value"] if i["displayName"] == "lh_bronze")
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(["uv", "build", "--wheel", "--quiet", "--out-dir", tmp], cwd=ROOT, check=True)
        (wheel,) = Path(tmp).glob("divvy_rti-*.whl")
        onelake_write(ws, f"{bronze}/Files/wheels/{wheel.name}", wheel.read_bytes())
    print(f"  uploaded {wheel.name} to lh_bronze/Files/wheels")
    return wheel.name


def run_notebook(ws: str, name: str, params: dict[str, str]) -> dict:
    _, _, items = req("GET", f"/workspaces/{ws}/items?type=Notebook")
    item = next(i["id"] for i in items["value"] if i["displayName"] == name)
    parameters = {k: {"value": v, "type": "string"} for k, v in params.items()}
    _, headers, _ = req("POST", f"/workspaces/{ws}/items/{item}/jobs/instances?jobType=RunNotebook",
                        {"executionData": {"parameters": parameters}})  # fmt: skip
    started = time.time()
    while True:
        time.sleep(20)
        _, _, job = req("GET", headers["Location"])
        print(f"  {name}: {job['status']} ({int(time.time() - started)}s)", flush=True)
        if job["status"] in TERMINAL_JOB_STATES:
            if job["status"] != "Completed":
                raise SystemExit(json.dumps(job.get("failureReason")))
            return job


def kusto_mgmt(query_uri: str, database: str, command: str, endpoint: str = "mgmt") -> list[dict]:
    """Run a Kusto management command (or a query with `endpoint="query"`); return the primary result as row dicts."""
    body = json.dumps({"db": database, "csl": command}).encode()
    headers = {"Authorization": f"Bearer {access_token(KUSTO_SCOPE)}", "Content-Type": "application/json"}
    request = urllib.request.Request(f"{query_uri}/v1/rest/{endpoint}", data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=300) as response:  # noqa: S310 - Fabric Eventhouse URI
            table = json.load(response)["Tables"][0]
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"{command.splitlines()[0][:80]} -> {exc.code} {exc.read().decode()}") from exc
    names = [c["ColumnName"] for c in table["Columns"]]
    return [dict(zip(names, row, strict=True)) for row in table["Rows"]]


def script_failures(results: list[dict]) -> list[str]:
    """Commands of an `.execute database script` result that did not complete."""
    return [f"{r['CommandText'][:100]!r}: {r['Reason']}" for r in results if r["Result"] != "Completed"]


def apply_kql_script(ws: str, database: str, script: Path) -> int:
    _, _, items = req("GET", f"/workspaces/{ws}/kqlDatabases")
    db = next(i for i in items["value"] if i["displayName"] == database)
    text = fill_placeholders(script.read_text(), ws, lambda item_type, name: resolve_placeholder(ws, item_type, name))
    command = ".execute database script with (ContinueOnErrors = false) <|\n" + text
    results = kusto_mgmt(db["properties"]["queryServiceUri"], database, command)
    failures = script_failures(results)
    if failures:
        raise SystemExit("KQL script failed:\n" + "\n".join(failures))
    print(f"  {script.name}: {len(results)} commands completed on {database}")
    return len(results)


def refresh_sql_endpoint(ws: str, lakehouse: str) -> None:
    """Sync the lakehouse SQL endpoint with new Delta tables before the Direct Lake model is framed."""
    sql_id = resolve_placeholder(ws, "SqlEndpoint", lakehouse)
    status, headers, _ = req("POST", f"/workspaces/{ws}/sqlEndpoints/{sql_id}/refreshMetadata", {})
    if status == 202:
        wait_lro(headers, f"refresh {lakehouse} SQL endpoint")
    print(f"  refreshed {lakehouse} SQL endpoint metadata")


def refresh_model(ws: str, model: str) -> None:
    """Frame a Direct Lake model (Power BI refresh) and wait for the outcome."""
    url = f"{POWERBI}/groups/{ws}/datasets/{workspace_item_id(ws, 'SemanticModel', model)}/refreshes"
    _, headers, _ = req("POST", url, {"type": "full"}, POWERBI_SCOPE)  # enhanced refresh → Location of this refresh
    while True:
        time.sleep(10)
        _, _, refresh = req("GET", headers["Location"], scope=POWERBI_SCOPE)
        print(f"  {model}: {refresh['status']}", flush=True)
        if refresh["status"] == "Completed":
            return
        if refresh["status"] in ("Failed", "Cancelled", "Disabled", "TimedOut"):
            raise SystemExit(json.dumps(refresh.get("messages") or refresh["status"]))


def apply_schedules(ws: str, spec: Path) -> list[str]:
    """Make each notebook's RunNotebook schedule match the spec: update the first existing one (extras deleted) or
    create it. Not Terraform: the provider's `fabric_item_job_scheduler` rejects Notebook items."""
    actions = []
    for entry in json.loads(spec.read_text()):
        item = workspace_item_id(ws, "Notebook", entry["notebook"])
        base = f"/workspaces/{ws}/items/{item}/jobs/RunNotebook/schedules"
        body = {"enabled": entry["enabled"], "configuration": entry["configuration"]}
        existing = [s["id"] for s in req("GET", base)[2]["value"]]
        for extra in existing[1:]:
            req("DELETE", f"{base}/{extra}")
        if existing:
            req("PATCH", f"{base}/{existing[0]}", body)
            action = "updated"
        else:
            req("POST", base, body)
            action = "created"
        print(f"  {action} schedule {entry['notebook']} (enabled={entry['enabled']})")
        actions.append(f"{action} {entry['notebook']}")
    return actions


HOT_PATH_MAX_AGE_MIN = 15  # poller gap at the hourly reschedule is ~5 min
WARM_PATH_MAX_GAP_MIN = 90  # Silver stream restarts hourly


def monitor(ws: str) -> list[str]:
    """Problems with the prod run: no recent polls in KQL (poller or Eventstream stopped), or the hourly Silver/Gold
    stream failed or stopped being scheduled. Empty list = healthy."""
    problems = []
    uri = resolve_placeholder(ws, "KQLDatabaseUri", "kqldb_bikes")
    query = "station_status | summarize age_min = datetime_diff('minute', now(), max(polled_at))"
    age = kusto_mgmt(uri, "kqldb_bikes", query, endpoint="query")[0]["age_min"]
    if age is None or age > HOT_PATH_MAX_AGE_MIN:
        problems.append(f"hot path: newest poll in kqldb_bikes is {age} min old (poller or Eventstream stopped)")
    item = workspace_item_id(ws, "Notebook", "nb_silver_availability")
    _, _, jobs = req("GET", f"/workspaces/{ws}/items/{item}/jobs/instances")
    runs = sorted(jobs["value"], key=lambda j: j.get("startTimeUtc") or "", reverse=True)
    if not runs:
        return [*problems, "warm path: nb_silver_availability has never run"]
    latest = runs[0]
    started = datetime.fromisoformat(latest["startTimeUtc"][:19]).replace(tzinfo=UTC)
    gap_min = (datetime.now(UTC) - started).total_seconds() / 60
    if latest["status"] == "Failed":
        problems.append(f"warm path: nb_silver_availability run {latest['id']} failed: {latest.get('failureReason')}")
    elif latest["status"] != "InProgress" and gap_min > WARM_PATH_MAX_GAP_MIN:
        problems.append(f"warm path: nb_silver_availability last started {gap_min:.0f} min ago (schedule stopped?)")
    return problems


if __name__ == "__main__":
    command, workspace = sys.argv[1], sys.argv[2]
    if command == "push":
        push_item(workspace, Path(sys.argv[3]))
    elif command == "upload-wheel":
        upload_wheel(workspace)
    elif command == "run":
        run_notebook(workspace, sys.argv[3], json.loads(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] else {})
    elif command == "kql":
        apply_kql_script(workspace, sys.argv[3], Path(sys.argv[4]))
    elif command == "refresh-sql":
        refresh_sql_endpoint(workspace, sys.argv[3])
    elif command == "refresh-model":
        refresh_model(workspace, sys.argv[3])
    elif command == "schedule":
        apply_schedules(workspace, Path(sys.argv[3]))
    elif command == "monitor":
        found = monitor(workspace)
        print("\n".join(found) or "healthy")
        sys.exit(1 if found else 0)
    else:
        raise SystemExit(__doc__)
