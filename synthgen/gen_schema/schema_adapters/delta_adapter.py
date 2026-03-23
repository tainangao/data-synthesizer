from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .common import normalize_schema


def load_delta_schema(path: Path) -> dict[str, Any]:
    root = _resolve_delta_root(path)

    fields, table_name = _load_fields_with_deltalake(root)
    if not fields:
        fields, table_name = _load_fields_from_delta_log(root)

    if not fields:
        raise ValueError(
            "Could not read Delta schema metadata. Ensure the path points to a valid Delta table."
        )

    columns = [_column_from_spark_field(field) for field in fields]
    raw_schema = {
        "schema_name": table_name,
        "domain": "Imported Delta",
        "tables": [
            {
                "name": table_name,
                "description": f"Imported from Delta table '{root.name}'.",
                "columns": columns,
            }
        ],
    }

    return normalize_schema(
        raw_schema,
        fallback_schema_name=table_name,
        fallback_domain="Imported Delta",
    )


def _resolve_delta_root(path: Path) -> Path:
    if path.is_dir():
        root = path
    elif path.is_file() and path.parent.name == "_delta_log":
        root = path.parent.parent
    else:
        raise ValueError("Delta input must be a directory or a _delta_log JSON file.")

    if not (root / "_delta_log").is_dir():
        raise ValueError(f"Delta log folder not found under: {root}")
    return root


def _load_fields_with_deltalake(root: Path) -> tuple[list[dict[str, Any]], str]:
    try:
        from deltalake import DeltaTable
    except ImportError:
        return [], root.name

    try:
        table = DeltaTable(str(root))
        schema_json = json.loads(table.schema().json())
    except Exception:
        return [], root.name

    fields = schema_json.get("fields")
    if not isinstance(fields, list):
        return [], root.name

    return fields, root.name


def _load_fields_from_delta_log(root: Path) -> tuple[list[dict[str, Any]], str]:
    log_dir = root / "_delta_log"
    json_logs = sorted(log_dir.glob("*.json"))
    for log_file in reversed(json_logs):
        lines = log_file.read_text(encoding="utf-8").splitlines()
        for line in reversed(lines):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            metadata = event.get("metaData")
            if not isinstance(metadata, dict):
                continue

            schema_string = metadata.get("schemaString")
            if not isinstance(schema_string, str):
                continue

            try:
                parsed = json.loads(schema_string)
            except json.JSONDecodeError:
                continue

            fields = parsed.get("fields")
            if not isinstance(fields, list):
                continue

            table_name = str(metadata.get("name") or root.name)
            return fields, table_name

    return [], root.name


def _column_from_spark_field(field: dict[str, Any]) -> dict[str, Any]:
    name = str(field.get("name") or "column")
    raw_type = field.get("type", "string")

    if isinstance(raw_type, dict):
        type_name = str(raw_type.get("type") or "struct")
    else:
        type_name = str(raw_type)

    return {
        "name": name,
        "type": type_name,
        "nullable": bool(field.get("nullable", True)),
    }
