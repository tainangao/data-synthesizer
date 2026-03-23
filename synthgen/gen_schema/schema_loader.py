from __future__ import annotations

from pathlib import Path

from synthgen.gen_schema.schema_adapters import (
    load_delta_schema,
    load_json_schema,
    load_parquet_schema,
    load_sql_schema,
)


def load_schema(path: str | Path) -> dict:
    schema_path = Path(path).expanduser().resolve()
    if not schema_path.exists():
        raise SystemExit(f"Schema input does not exist: {schema_path}")

    format_name = detect_schema_format(schema_path)
    try:
        if format_name == "json":
            return load_json_schema(schema_path)
        if format_name == "sql":
            return load_sql_schema(schema_path)
        if format_name == "parquet":
            return load_parquet_schema(schema_path)
        if format_name == "delta":
            return load_delta_schema(schema_path)
    except SystemExit:
        raise
    except Exception as exc:
        raise SystemExit(
            f"Failed to load {format_name} schema from {schema_path}: {exc}"
        ) from exc

    raise SystemExit(f"Unsupported schema format: {schema_path}")


def detect_schema_format(path: Path) -> str:
    if path.is_dir():
        if (path / "_delta_log").is_dir():
            return "delta"
        if any(path.rglob("*.parquet")):
            return "parquet"
        raise SystemExit(
            "Directory schema input must be a Delta table (contains _delta_log) "
            "or a parquet directory."
        )

    suffix = path.suffix.lower()
    if suffix == ".sql":
        return "sql"
    if suffix == ".parquet":
        return "parquet"
    if suffix in {".delta", ".deltatable"}:
        return "delta"
    if suffix == ".json":
        if path.parent.name == "_delta_log":
            return "delta"
        return "json"

    preview = path.read_text(encoding="utf-8", errors="ignore")[:2000].lower()
    if "create table" in preview:
        return "sql"
    if preview.lstrip().startswith("{"):
        return "json"

    raise SystemExit(
        "Could not detect schema format. Supported inputs: JSON, SQL, parquet, delta."
    )
