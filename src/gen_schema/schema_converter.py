from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Callable

from src.common import safe_name
from src.gen_schema.schema_utils import table_order

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPPORTED_EXPORT_FORMATS = {"sqlite", "psql", "parquet", "delta"}
PSQL_ALIASES = {"psql", "postgres", "postgresql"}


def convert_schema(
    schema: dict,
    out_dir: str,
    export_formats: list[str],
):
    output_root = Path(out_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    normalized_formats = _normalize_export_formats(export_formats)
    logger.info("Schema conversion output directory: %s", output_root.resolve())
    logger.info("Requested export formats: %s", ", ".join(normalized_formats))

    if "sqlite" in normalized_formats:
        sch_sqlite = _write_sqlite_artifacts(schema, output_root / "sqlite")
        _log_artifact_paths("sqlite", sch_sqlite)

    if "psql" in normalized_formats:
        sch_psql = _write_postgres_artifacts(schema, output_root / "psql")
        _log_artifact_paths("psql", sch_psql)

    if "parquet" in normalized_formats:
        sch_parquet = _write_parquet_artifacts(schema, output_root / "parquet")
        _log_artifact_paths("parquet", sch_parquet)

    if "delta" in normalized_formats:
        sch_delta = _write_delta_artifacts(schema, output_root / "delta")
        _log_artifact_paths("delta", sch_delta)


def _log_artifact_paths(format_name: str, artifacts: dict[str, Any]) -> None:
    for key, value in artifacts.items():
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                logger.info(
                    "Saved %s artifact (%s.%s): %s",
                    format_name,
                    key,
                    nested_key,
                    nested_value,
                )
            continue

        logger.info("Saved %s artifact (%s): %s", format_name, key, value)


def _normalize_export_formats(export_formats: list[str]) -> list[str]:
    normalized: list[str] = []
    for raw in export_formats:
        name = raw.strip().lower()
        if name in PSQL_ALIASES:
            name = "psql"
        if name not in SUPPORTED_EXPORT_FORMATS:
            supported = ", ".join(sorted(SUPPORTED_EXPORT_FORMATS | PSQL_ALIASES))
            raise ValueError(
                f"Unsupported export format '{raw}'. Supported values: {supported}."
            )
        if name not in normalized:
            normalized.append(name)
    return normalized


def _ordered_tables(schema: dict[str, Any]) -> list[dict[str, Any]]:
    by_name = {table["name"]: table for table in schema["tables"]}
    return [by_name[name] for name in table_order(schema) if name in by_name]


def _quote_identifier(identifier: str) -> str:
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def _sqlite_type(column: dict[str, Any]) -> str:
    data_type = str(column.get("type", "TEXT")).upper()
    mapping = {
        "INTEGER": "INTEGER",
        "NUMERIC": "NUMERIC",
        "REAL": "REAL",
        "BOOLEAN": "INTEGER",
        "DATE": "TEXT",
        "TIMESTAMP": "TEXT",
        "JSON": "TEXT",
        "XML": "TEXT",
        "TEXT": "TEXT",
    }
    return mapping.get(data_type, "TEXT")


def _postgres_type(column: dict[str, Any]) -> str:
    data_type = str(column.get("type", "TEXT")).upper()
    mapping = {
        "INTEGER": "BIGINT",
        "NUMERIC": "NUMERIC",
        "REAL": "DOUBLE PRECISION",
        "BOOLEAN": "BOOLEAN",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "JSON": "JSONB",
        "XML": "XML",
        "TEXT": "TEXT",
    }
    return mapping.get(data_type, "TEXT")


def _build_create_table_sql(
    table: dict[str, Any],
    *,
    type_mapper: Callable[[dict[str, Any]], str],
) -> str:
    lines: list[str] = []
    for column in table["columns"]:
        line = f"{_quote_identifier(column['name'])} {type_mapper(column)}"
        if column.get("primary_key"):
            line += " PRIMARY KEY"
        if not column.get("nullable", True) and not column.get("primary_key"):
            line += " NOT NULL"
        lines.append(line)

    for column in table["columns"]:
        foreign_key = column.get("foreign_key")
        if not foreign_key:
            continue
        lines.append(
            "FOREIGN KEY "
            f"({_quote_identifier(column['name'])}) REFERENCES "
            f"{_quote_identifier(foreign_key['table'])}"
            f"({_quote_identifier(foreign_key['column'])})"
        )

    body = ",\n  ".join(lines)
    return (
        f"CREATE TABLE IF NOT EXISTS {_quote_identifier(table['name'])} (\n  {body}\n);"
    )


def _build_sqlite_ddl(schema: dict[str, Any]) -> str:
    statements = ["PRAGMA foreign_keys = ON;"]
    for table in _ordered_tables(schema):
        statements.append(_build_create_table_sql(table, type_mapper=_sqlite_type))
    return "\n\n".join(statements) + "\n"


def _build_postgres_ddl(schema: dict[str, Any]) -> str:
    statements = []
    for table in _ordered_tables(schema):
        statements.append(_build_create_table_sql(table, type_mapper=_postgres_type))
    return "\n\n".join(statements) + "\n"


def _write_sqlite_artifacts(schema: dict[str, Any], out_dir: Path) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ddl = _build_sqlite_ddl(schema)

    sql_path = out_dir / "schema_sqlite.sql"
    sql_path.write_text(ddl, encoding="utf-8")

    db_path = out_dir / "sqlite.db"
    if db_path.exists():
        db_path.unlink()
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(ddl)
        connection.commit()
    finally:
        connection.close()

    return {
        "sql": str(sql_path.resolve()),
        "db": str(db_path.resolve()),
    }


def _write_postgres_artifacts(schema: dict[str, Any], out_dir: Path) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ddl = _build_postgres_ddl(schema)
    sql_path = out_dir / "schema.sql"
    sql_path.write_text(ddl, encoding="utf-8")
    return {"sql": str(sql_path.resolve())}


def _arrow_type(pa: Any, column: dict[str, Any]) -> Any:
    data_type = str(column.get("type", "TEXT")).upper()
    mapping = {
        "INTEGER": pa.int64(),
        "NUMERIC": pa.float64(),
        "REAL": pa.float64(),
        "BOOLEAN": pa.bool_(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us"),
        "JSON": pa.string(),
        "XML": pa.string(),
        "TEXT": pa.string(),
    }
    return mapping.get(data_type, pa.string())


def _empty_arrow_table(pa: Any, table: dict[str, Any]) -> Any:
    fields = []
    for column in table["columns"]:
        nullable = bool(column.get("nullable", True) and not column.get("primary_key"))
        fields.append(
            pa.field(
                column["name"],
                _arrow_type(pa, column),
                nullable=nullable,
            )
        )

    schema = pa.schema(fields)
    arrays = [pa.array([], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _write_parquet_artifacts(schema: dict[str, Any], out_dir: Path) -> dict[str, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("Parquet export requires the 'pyarrow' package.") from exc

    out_dir.mkdir(parents=True, exist_ok=True)
    table_paths: dict[str, str] = {}
    for table in schema["tables"]:
        table_name = table["name"]
        filename = f"{safe_name(table_name) or table_name}.parquet"
        table_path = out_dir / filename
        arrow_table = _empty_arrow_table(pa, table)
        pq.write_table(arrow_table, table_path)
        table_paths[table_name] = str(table_path.resolve())

    return {"tables": table_paths, "root": str(out_dir.resolve())}


def _write_delta_artifacts(schema: dict[str, Any], out_dir: Path) -> dict[str, Any]:
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise RuntimeError("Delta export requires the 'pyarrow' package.") from exc

    try:
        from deltalake import write_deltalake
    except ImportError as exc:
        raise RuntimeError("Delta export requires the 'deltalake' package.") from exc

    out_dir.mkdir(parents=True, exist_ok=True)
    table_paths: dict[str, str] = {}
    for table in schema["tables"]:
        table_name = table["name"]
        table_dir = out_dir / (safe_name(table_name) or table_name)
        arrow_table = _empty_arrow_table(pa, table)
        write_deltalake(str(table_dir), arrow_table, mode="overwrite")
        table_paths[table_name] = str(table_dir.resolve())

    return {"tables": table_paths, "root": str(out_dir.resolve())}


if __name__ == "__main__":
    f_sch = "/Users/jacquelinewong/Documents/GitHub/data-synthesizer/output/synthetic/credit_risk_10_records/schema.json"
    # read f_sch into dict
    import json

    with open(f_sch, "r", encoding="utf-8") as f:
        schema_dict = json.load(f)

    out_dir = "/Users/jacquelinewong/Documents/GitHub/data-synthesizer/output/synthetic/credit_risk_10_records/schema"

    export_formats = ["sqlite", "psql", "parquet", "delta"]
    convert_schema(schema_dict, out_dir, export_formats)
