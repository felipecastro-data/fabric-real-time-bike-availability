"""Validate Fabric Git-integration item folders before they reach a workspace.

Checks every ``<displayName>.<ItemType>`` folder under the Fabric root, including items nested in
workspace folders:
  * ``.platform`` exists and is a JSON object with ``metadata`` and ``config`` objects
  * folder name matches ``metadata.displayName`` and ``metadata.type``
  * ``config.logicalId`` is unique across items
  * display names follow the project naming convention per item type
  * every ``*.json`` file in the item parses
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

NAMING_PREFIXES: dict[str, str] = {
    "Lakehouse": "lh_",
    "Notebook": "nb_",
    "DataPipeline": "pl_",
    "Environment": "env_",
    "VariableLibrary": "vl_",
    "SparkJobDefinition": "sjd_",
    "Eventhouse": "eh_",
    "KQLDatabase": "kqldb_",
    "Eventstream": "es_",
    "KQLDashboard": "rtd_",
    "SemanticModel": "sm_",
}


@dataclass
class Report:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    items: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors


_INVALID = object()


def _load_json(path: Path, report: Report) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report.errors.append(f"{path}: invalid JSON ({exc})")
        return _INVALID


def _discover_items(folder: Path) -> Iterator[Path]:
    """Yield item folders, descending through workspace folders but stopping at item boundaries."""
    for child in sorted(p for p in folder.iterdir() if p.is_dir() and not p.name.startswith(".")):
        if (child / ".platform").exists() or "." in child.name:
            yield child
        else:
            yield from _discover_items(child)


def validate(root: Path) -> Report:
    report = Report()
    logical_ids: dict[str, Path] = {}

    for item_dir in _discover_items(root):
        report.items += 1
        name, _, item_type = item_dir.name.rpartition(".")
        platform_file = item_dir / ".platform"

        if not platform_file.is_file():
            report.errors.append(f"{item_dir}: missing .platform file")
            continue

        platform = _load_json(platform_file, report)
        if platform is _INVALID:
            continue
        if not isinstance(platform, dict):
            report.errors.append(f"{platform_file}: .platform must contain a JSON object")
            continue

        metadata = platform.get("metadata")
        config = platform.get("config")
        if not isinstance(metadata, dict) or not isinstance(config, dict):
            report.errors.append(f"{platform_file}: 'metadata' and 'config' must be JSON objects")
            continue

        if metadata.get("type") != item_type:
            report.errors.append(f"{item_dir}: folder type '{item_type}' != metadata.type '{metadata.get('type')}'")
        if metadata.get("displayName") != name:
            report.errors.append(
                f"{item_dir}: folder name '{name}' != metadata.displayName '{metadata.get('displayName')}'"
            )

        logical_id = config.get("logicalId")
        if not logical_id:
            report.errors.append(f"{item_dir}: missing config.logicalId")
        elif logical_id in logical_ids:
            report.errors.append(f"{item_dir}: duplicate logicalId also used by {logical_ids[logical_id]}")
        else:
            logical_ids[logical_id] = item_dir

        prefix = NAMING_PREFIXES.get(item_type)
        if prefix is None:
            report.warnings.append(f"{item_dir}: no naming convention defined for type '{item_type}'")
        elif not name.startswith(prefix):
            report.errors.append(f"{item_dir}: {item_type} names must start with '{prefix}'")

        for json_file in item_dir.rglob("*.json"):
            _load_json(json_file, report)

    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", nargs="?", default="fabric", type=Path, help="Fabric Git folder (default: fabric)")
    args = parser.parse_args(argv)

    if not args.root.is_dir():
        print(f"::error::Fabric folder not found: {args.root}")
        return 1

    report = validate(args.root)
    for warning in report.warnings:
        print(f"::warning::{warning}")
    for error in report.errors:
        print(f"::error::{error}")
    print(f"Validated {report.items} Fabric item(s): {len(report.errors)} error(s), {len(report.warnings)} warning(s)")
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
