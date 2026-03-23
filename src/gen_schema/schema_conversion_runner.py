from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Callable

from src.common import safe_name
from src.gen_schema.schema_generator import gen_schema_with_request
from src.gen_schema.schema_utils import table_order

SUPPORTED_EXPORT_FORMATS = {"sqlite", "psql", "parquet", "delta"}
PSQL_ALIASES = {"psql", "postgres", "postgresql"}


def generate_and_convert_schema(
    scenario: str,
    *,
    out_dir: str,
    max_attempts: int,
    records: int,
    seed: int,
    data_formats: list[str] | None,
    export_formats: list[str],
) -> dict[str, Any]:
    output_root = Path(out_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    generated = gen_schema_with_request(
        scenario,
        max_attempts=max_attempts,
        records=records,
        seed=seed,
        out_dir=str(output_root),
        data_formats=data_formats,
    )
    schema = generated["schema"]

    artifacts: dict[str, Any] = {
        "json_schema": str(output_root / "json_schema.json"),
        "validation_report": str(output_root / "schema_validation_report.json"),
        "data_request": str(output_root / "data_generation_request.json"),
        "converters": {},
    }

    normalized_formats = _normalize_export_formats(export_formats)
    if "sqlite" in normalized_formats:
        artifacts["converters"]["sqlite"] = _write_sqlite_artifacts(
            schema, output_root / "sqlite"
        )
    if "psql" in normalized_formats:
        artifacts["converters"]["psql"] = _write_postgres_artifacts(
            schema, output_root / "psql"
        )
    if "parquet" in normalized_formats:
        artifacts["converters"]["parquet"] = _write_parquet_artifacts(
            schema, output_root / "parquet"
        )
    if "delta" in normalized_formats:
        artifacts["converters"]["delta"] = _write_delta_artifacts(
            schema, output_root / "delta"
        )

    return {
        "schema": schema,
        "validation_report": generated["validation_report"],
        "data_generation_request": generated["data_generation_request"],
        "artifacts": artifacts,
    }


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

    sql_path = out_dir / "schema.sql"
    sql_path.write_text(ddl, encoding="utf-8")

    db_path = out_dir / "schema.db"
    if db_path.exists():
        db_path.unlink()
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(ddl)
        connection.commit()
    finally:
        connection.close()

    return {
        "sql": str(sql_path),
        "db": str(db_path),
    }


def _write_postgres_artifacts(schema: dict[str, Any], out_dir: Path) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ddl = _build_postgres_ddl(schema)
    sql_path = out_dir / "schema.sql"
    sql_path.write_text(ddl, encoding="utf-8")
    return {"sql": str(sql_path)}


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
        table_paths[table_name] = str(table_path)

    return {"tables": table_paths, "root": str(out_dir)}


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
        table_paths[table_name] = str(table_dir)

    return {"tables": table_paths, "root": str(out_dir)}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate schema with Gemini and convert it to SQLite, PostgreSQL, "
            "Parquet, and Delta Lake artifacts."
        )
    )
    parser.add_argument(
        "scenario", help="Business scenario prompt for schema generation"
    )
    parser.add_argument(
        "--out-dir",
        default="output/schema_conversions",
        help="Output directory for generated schema and converted artifacts",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Maximum schema generation retries",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=500,
        help="Record count to include in data generation request",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed to include in data generation request",
    )
    parser.add_argument(
        "--data-formats",
        nargs="+",
        default=None,
        help="Data formats to include in data_generation_request.generation.data_formats",
    )
    parser.add_argument(
        "--export-formats",
        nargs="+",
        default=["sqlite", "psql", "parquet", "delta"],
        help="Schema conversion targets (sqlite, psql, parquet, delta)",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = generate_and_convert_schema(
        args.scenario,
        out_dir=args.out_dir,
        max_attempts=args.max_attempts,
        records=args.records,
        seed=args.seed,
        data_formats=args.data_formats,
        export_formats=args.export_formats,
    )

    artifacts = result["artifacts"]
    print(f"JSON schema: {artifacts['json_schema']}")
    print(f"Validation report: {artifacts['validation_report']}")
    print(f"Data generation request: {artifacts['data_request']}")

    for name, payload in artifacts["converters"].items():
        print(f"{name}: {payload}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
