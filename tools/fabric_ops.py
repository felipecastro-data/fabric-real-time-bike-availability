"""Fabric operations via the REST API as the Azure CLI identity (user locally, service principal in CI).

python tools/fabric_ops.py push <workspace-id> fabric/<item>.<Type>      # create the item or update its definition
python tools/fabric_ops.py upload-wheel <workspace-id>                     # build + upload to lh_bronze/Files/wheels
python tools/fabric_ops.py run <workspace-id> <notebook> '{"duration_minutes": "3"}'  # RunNotebook job + poll

`push` is the dev bootstrap until the dev workspace is Git-connected (Step 8); test/prod get items from the
deployment pipeline. `req`, `wait_lro` and `run` are copied from the medallion repo's tools/fabric_ops.py.
"""

from __future__ import annotations

import base64
import json
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from fabric_auth import access_token  # fresh GitHub OIDC exchange in CI, az CLI locally

API = "https://api.fabric.microsoft.com/v1"
ONELAKE = "https://onelake.dfs.fabric.microsoft.com"
STORAGE_SCOPE = "https://storage.azure.com/.default"
TERMINAL_JOB_STATES = ("Completed", "Failed", "Cancelled", "Deduped")
ROOT = Path(__file__).resolve().parents[1]


def req(method: str, url: str, body: dict | None = None) -> tuple[int, dict, dict | None]:
    url = url if url.startswith("http") else API + url
    data = json.dumps(body).encode() if body is not None else None
    for attempt in range(8):
        headers = {"Authorization": f"Bearer {access_token()}", "Content-Type": "application/json"}
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


def item_definition(item_dir: Path) -> tuple[str, str, dict]:
    """(display name, type, definition) from a Git-format item folder; `.platform` only supplies name and type."""
    metadata = json.loads((item_dir / ".platform").read_text())["metadata"]
    parts = [
        {
            "path": path.relative_to(item_dir).as_posix(),
            "payload": base64.b64encode(path.read_bytes()).decode(),
            "payloadType": "InlineBase64",
        }
        for path in sorted(item_dir.rglob("*"))
        if path.is_file() and path.name != ".platform"
    ]
    return metadata["displayName"], metadata["type"], {"parts": parts}


def push_item(ws: str, item_dir: Path) -> str:
    """Create the item, or update the definition of the existing item with the same name and type."""
    name, item_type, definition = item_definition(item_dir)
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


if __name__ == "__main__":
    command, workspace = sys.argv[1], sys.argv[2]
    if command == "push":
        push_item(workspace, Path(sys.argv[3]))
    elif command == "upload-wheel":
        upload_wheel(workspace)
    elif command == "run":
        run_notebook(workspace, sys.argv[3], json.loads(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] else {})
    else:
        raise SystemExit(__doc__)
