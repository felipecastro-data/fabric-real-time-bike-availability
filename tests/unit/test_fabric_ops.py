import base64
import json

import fabric_ops


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
