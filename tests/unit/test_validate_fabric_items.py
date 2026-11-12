import json
from pathlib import Path

import pytest
from validate_fabric_items import validate


def _item(root: Path, folder: str, item_type: str, display_name: str, logical_id: str) -> Path:
    item = root / folder
    item.mkdir()
    platform = {
        "metadata": {"type": item_type, "displayName": display_name},
        "config": {"version": "2.0", "logicalId": logical_id},
    }
    (item / ".platform").write_text(json.dumps(platform))
    return item


def test_valid_items_pass(tmp_path):
    _item(tmp_path, "lh_bronze.Lakehouse", "Lakehouse", "lh_bronze", "id-1")
    _item(tmp_path, "nb_ingest.Notebook", "Notebook", "nb_ingest", "id-2")

    report = validate(tmp_path)

    assert report.ok
    assert report.items == 2


def test_naming_convention_is_enforced(tmp_path):
    _item(tmp_path, "bronze.Lakehouse", "Lakehouse", "bronze", "id-1")

    report = validate(tmp_path)

    assert any("must start with 'lh_'" in e for e in report.errors)


def test_duplicate_logical_ids_fail(tmp_path):
    _item(tmp_path, "lh_bronze.Lakehouse", "Lakehouse", "lh_bronze", "same")
    _item(tmp_path, "lh_silver.Lakehouse", "Lakehouse", "lh_silver", "same")

    report = validate(tmp_path)

    assert any("duplicate logicalId" in e for e in report.errors)


def test_folder_must_match_metadata(tmp_path):
    _item(tmp_path, "lh_gold.Lakehouse", "Lakehouse", "lh_platinum", "id-1")

    report = validate(tmp_path)

    assert any("!= metadata.displayName" in e for e in report.errors)


def test_invalid_json_is_reported(tmp_path):
    item = _item(tmp_path, "lh_bronze.Lakehouse", "Lakehouse", "lh_bronze", "id-1")
    (item / "lakehouse.metadata.json").write_text("{not json")

    report = validate(tmp_path)

    assert any("invalid JSON" in e for e in report.errors)


def test_items_in_nested_workspace_folders_are_validated(tmp_path):
    folder = tmp_path / "bronze" / "ingestion"
    folder.mkdir(parents=True)
    (folder / "lh_raw.Lakehouse").mkdir()  # no .platform

    report = validate(tmp_path)

    assert report.items == 1
    assert any("missing .platform" in e for e in report.errors)


def test_duplicate_logical_ids_across_nested_folders_fail(tmp_path):
    (tmp_path / "bronze").mkdir()
    (tmp_path / "silver").mkdir()
    _item(tmp_path / "bronze", "lh_bronze.Lakehouse", "Lakehouse", "lh_bronze", "same")
    _item(tmp_path / "silver", "lh_silver.Lakehouse", "Lakehouse", "lh_silver", "same")

    report = validate(tmp_path)

    assert report.items == 2
    assert any("duplicate logicalId" in e for e in report.errors)


@pytest.mark.parametrize("content", ["null", "[]", '"text"', '{"metadata": null, "config": {}}'])
def test_non_object_platform_is_rejected(tmp_path, content):
    item = tmp_path / "lh_bronze.Lakehouse"
    item.mkdir()
    (item / ".platform").write_text(content)

    report = validate(tmp_path)

    assert not report.ok
    assert any("must" in e and "JSON object" in e for e in report.errors)


def test_repository_fabric_folder_is_valid():
    repo_fabric = Path(__file__).resolve().parents[2] / "fabric"

    report = validate(repo_fabric)

    assert report.ok, report.errors
