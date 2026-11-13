import base64
import json

import fabric_ops
import pytest


def make_item(tmp_path):
    item = tmp_path / "nb_x.Notebook"
    item.mkdir()
    (item / ".platform").write_text(json.dumps({"metadata": {"type": "Notebook", "displayName": "nb_x"}}))
    (item / "notebook-content.py").write_text("print(1)\n")
    return item


def test_item_definition_skips_platform(tmp_path):
    name, item_type, definition = fabric_ops.item_definition(make_item(tmp_path))
    assert (name, item_type) == ("nb_x", "Notebook")
    assert [p["path"] for p in definition["parts"]] == ["notebook-content.py"]
    assert base64.b64decode(definition["parts"][0]["payload"]) == b"print(1)\n"


def test_push_item_creates_then_updates(tmp_path, monkeypatch):
    item = make_item(tmp_path)
    calls = []
    existing = {"value": []}

    def fake_req(method, url, body=None):
        calls.append((method, url))
        return (200, {}, existing) if method == "GET" else (201, {}, None)

    monkeypatch.setattr(fabric_ops, "req", fake_req)
    assert fabric_ops.push_item("ws", item) == "created"
    assert calls[-1] == ("POST", "/workspaces/ws/items")

    existing["value"] = [{"id": "other", "displayName": "nb_y"}, {"id": "id1", "displayName": "nb_x"}]
    assert fabric_ops.push_item("ws", item) == "updated"
    assert calls[-1] == ("POST", "/workspaces/ws/items/id1/updateDefinition")


def test_script_failures_lists_only_incomplete_commands():
    results = [
        {"CommandText": ".create-merge table t (a: int)", "Result": "Completed", "Reason": ""},
        {"CommandText": ".alter table t policy update @'[...]'", "Result": "Failed", "Reason": "Schema mismatch"},
        {"CommandText": ".alter table t policy caching hot = 7d", "Result": "Skipped", "Reason": ""},
    ]
    failures = fabric_ops.script_failures(results)
    assert len(failures) == 2 and "Schema mismatch" in failures[0]


def test_fill_placeholders_resolves_by_type_and_name():
    ids = {("KQLDatabase", "kqldb_bikes"): "db-1", ("Lakehouse", "lh_bronze"): "lh-1"}
    text = (
        '{"workspaceId": "{{workspace}}", "a": "{{KQLDatabase:kqldb_bikes}}", '
        '"b": "{{Lakehouse:lh_bronze}}", "c": "{x}"}'
    )
    filled = fabric_ops.fill_placeholders(text, "ws-1", lambda t, n: ids[(t, n)])
    assert json.loads(filled) == {"workspaceId": "ws-1", "a": "db-1", "b": "lh-1", "c": "{x}"}


def test_resolve_placeholder_endpoints(monkeypatch):
    responses = {
        "/workspaces/ws/kqlDatabases": {
            "value": [{"displayName": "kqldb_bikes", "properties": {"queryServiceUri": "https://q"}}]
        },
        "/workspaces/ws/items?type=Lakehouse": {"value": [{"id": "lh-1", "displayName": "lh_gold"}]},
        "/workspaces/ws/lakehouses/lh-1": {
            "properties": {"sqlEndpointProperties": {"id": "sql-1", "connectionString": "srv"}}
        },
    }
    monkeypatch.setattr(fabric_ops, "req", lambda method, url, body=None: (200, {}, responses[url]))
    resolve = fabric_ops.resolve_placeholder
    assert resolve("ws", "KQLDatabaseUri", "kqldb_bikes") == "https://q"
    assert (resolve("ws", "SqlEndpoint", "lh_gold"), resolve("ws", "SqlConnection", "lh_gold")) == ("sql-1", "srv")
    assert resolve("ws", "Lakehouse", "lh_gold") == "lh-1"


def test_resolve_placeholder_env(monkeypatch):
    monkeypatch.setenv("ALERT_EMAIL", "ops@example.com")
    assert fabric_ops.resolve_placeholder("ws", "Env", "ALERT_EMAIL") == "ops@example.com"
    monkeypatch.delenv("ALERT_EMAIL")
    with pytest.raises(SystemExit, match="ALERT_EMAIL"):
        fabric_ops.resolve_placeholder("ws", "Env", "ALERT_EMAIL")


def test_refresh_model_polls_its_refresh_until_terminal(monkeypatch):
    statuses = iter(["NotStarted", "InProgress", "Failed"])
    calls = []

    def fake_req(method, url, body=None, scope=None):
        calls.append((method, url))
        if method == "POST":
            return 202, {"Location": "https://pbi/refreshes/r-1"}, None
        return 200, {}, {"status": next(statuses), "messages": [{"message": "table missing"}]}

    monkeypatch.setattr(fabric_ops, "req", fake_req)
    monkeypatch.setattr(fabric_ops, "workspace_item_id", lambda ws, item_type, name: "sm-1")
    monkeypatch.setattr(fabric_ops.time, "sleep", lambda s: None)
    with pytest.raises(SystemExit, match="table missing"):
        fabric_ops.refresh_model("ws", "sm_x")
    assert calls[0] == ("POST", f"{fabric_ops.POWERBI}/groups/ws/datasets/sm-1/refreshes")
    assert calls[1:] == [("GET", "https://pbi/refreshes/r-1")] * 3


def fake_monitor_env(monkeypatch, age_min, runs):
    monkeypatch.setattr(fabric_ops, "resolve_placeholder", lambda ws, t, n: "https://q")
    monkeypatch.setattr(fabric_ops, "kusto_mgmt", lambda uri, db, q, endpoint="mgmt": [{"age_min": age_min}])
    monkeypatch.setattr(fabric_ops, "workspace_item_id", lambda ws, t, n: "nb-1")
    monkeypatch.setattr(fabric_ops, "req", lambda method, url, body=None, scope=None: (200, {}, {"value": runs}))


def started(minutes_ago):
    from datetime import UTC, datetime, timedelta

    return (datetime.now(UTC) - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%dT%H:%M:%S.1234567")


def test_monitor_healthy(monkeypatch):
    fake_monitor_env(monkeypatch, 2, [{"id": "a", "status": "Completed", "startTimeUtc": started(30)}])
    assert fabric_ops.monitor("ws") == []


def test_monitor_flags_stale_polls_failed_and_unscheduled_stream(monkeypatch):
    failed = {"id": "a", "status": "Failed", "startTimeUtc": started(10), "failureReason": "x"}
    fake_monitor_env(monkeypatch, 40, [failed])
    assert [p.split(":")[0] for p in fabric_ops.monitor("ws")] == ["hot path", "warm path"]
    runs = [{"id": "old", "status": "Completed", "startTimeUtc": started(200)},
            {"id": "older", "status": "Completed", "startTimeUtc": started(300)}]  # fmt: skip
    fake_monitor_env(monkeypatch, None, runs)
    problems = fabric_ops.monitor("ws")
    assert "None min old" in problems[0] and "last started 200 min ago" in problems[1]
