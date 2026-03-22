from __future__ import annotations

from typing import Any

from ..common import tokens

CANONICAL_TYPES = {
    "TEXT",
    "INTEGER",
    "NUMERIC",
    "REAL",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "JSON",
    "XML",
}

CANONICAL_ROLES = {
    "identifier",
    "numerical",
    "categorical",
    "semi_structured",
    "temporal",
    "text",
    "boolean",
}


def normalize_identifier(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    if "." in text:
        text = text.split(".")[-1].strip()

    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'", "`"}:
        text = text[1:-1]
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]

    return text.strip()


def normalize_type(value: object) -> str:
    raw = str(value or "TEXT").strip().lower()
    if not raw:
        return "TEXT"

    base = raw.split("(", 1)[0].strip()

    if base in {
        "int",
        "integer",
        "bigint",
        "smallint",
        "tinyint",
        "serial",
        "bigserial",
        "long",
        "short",
        "byte",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
    }:
        return "INTEGER"

    if base in {
        "numeric",
        "decimal",
        "number",
        "money",
    } or base.startswith("decimal"):
        return "NUMERIC"

    if base in {
        "real",
        "float",
        "float4",
        "float8",
        "double",
        "double precision",
        "float16",
        "float32",
        "float64",
    }:
        return "REAL"

    if base in {"bool", "boolean"}:
        return "BOOLEAN"

    if base == "date" or base.startswith("date"):
        return "DATE"

    if "timestamp" in base or base in {"datetime", "time", "timestamptz"}:
        return "TIMESTAMP"

    if base in {"json", "jsonb", "variant", "struct", "array", "map"}:
        return "JSON"
    if raw.startswith("struct<") or raw.startswith("array<") or raw.startswith("map<"):
        return "JSON"

    if base == "xml":
        return "XML"

    if base in {"str", "string", "varchar", "char", "text", "binary"}:
        return "TEXT"

    return "TEXT"


def infer_field_role(column_name: str, dtype: str) -> str:
    name_tokens = tokens(column_name)
    lowered = column_name.lower()

    if dtype == "BOOLEAN":
        return "boolean"
    if dtype in {"INTEGER", "NUMERIC", "REAL"}:
        return "numerical"
    if dtype in {"DATE", "TIMESTAMP"}:
        return "temporal"
    if dtype in {"JSON", "XML"}:
        return "semi_structured"

    if lowered == "id" or lowered.endswith("_id") or "id" in name_tokens:
        return "identifier"

    if name_tokens & {
        "status",
        "type",
        "segment",
        "risk",
        "currency",
        "channel",
        "country",
        "side",
        "outcome",
        "rating",
        "category",
    }:
        return "categorical"

    return "text"


def normalize_schema(
    payload: dict[str, Any],
    *,
    fallback_schema_name: str,
    fallback_domain: str,
) -> dict[str, Any]:
    tables_raw = payload.get("tables") if isinstance(payload, dict) else None
    if not isinstance(tables_raw, list) or not tables_raw:
        raise ValueError("Schema must contain a non-empty 'tables' list.")

    tables: list[dict[str, Any]] = []
    for table_index, table_raw in enumerate(tables_raw, start=1):
        if not isinstance(table_raw, dict):
            continue

        table_name = (
            normalize_identifier(table_raw.get("name")) or f"table_{table_index}"
        )
        description = str(
            table_raw.get("description") or f"Imported table '{table_name}'."
        )

        columns_raw = table_raw.get("columns")
        if not isinstance(columns_raw, list):
            columns_raw = []

        columns: list[dict[str, Any]] = []
        for col_index, col_raw in enumerate(columns_raw, start=1):
            if not isinstance(col_raw, dict):
                continue

            col_name = (
                normalize_identifier(col_raw.get("name")) or f"column_{col_index}"
            )
            raw_type = str(col_raw.get("type") or "")
            dtype = normalize_type(raw_type)

            raw_role = str(col_raw.get("field_role") or "").strip().lower()
            if raw_role in CANONICAL_ROLES:
                role = raw_role
            else:
                type_hint = raw_type.strip().lower()
                if type_hint in {"categorical", "category", "enum"}:
                    role = "categorical"
                elif type_hint in {
                    "json",
                    "jsonb",
                    "xml",
                    "struct",
                    "array",
                    "map",
                }:
                    role = "semi_structured"
                elif type_hint in {"bool", "boolean"}:
                    role = "boolean"
                else:
                    role = infer_field_role(col_name, dtype)

            nullable = bool(col_raw.get("nullable", True))
            primary_key = bool(col_raw.get("primary_key", False))

            fk_value = col_raw.get("foreign_key")
            foreign_key = None
            if isinstance(fk_value, dict):
                fk_table = normalize_identifier(fk_value.get("table"))
                fk_column = normalize_identifier(fk_value.get("column"))
                if fk_table and fk_column:
                    foreign_key = {"table": fk_table, "column": fk_column}

            if primary_key:
                nullable = False
                role = "identifier"

            columns.append(
                {
                    "name": col_name,
                    "type": dtype if dtype in CANONICAL_TYPES else "TEXT",
                    "field_role": role,
                    "nullable": nullable,
                    "primary_key": primary_key,
                    "foreign_key": foreign_key,
                }
            )

        if not columns:
            columns = [
                {
                    "name": "id",
                    "type": "INTEGER",
                    "field_role": "identifier",
                    "nullable": False,
                    "primary_key": True,
                    "foreign_key": None,
                }
            ]

        tables.append(
            {"name": table_name, "description": description, "columns": columns}
        )

    if not tables:
        raise ValueError("Schema contains no usable tables.")

    _ensure_primary_keys(tables)
    _canonicalize_foreign_keys(tables)
    _infer_foreign_keys(tables)

    schema_name = str(
        payload.get("schema_name") or fallback_schema_name or "ImportedSchema"
    )
    domain = str(payload.get("domain") or fallback_domain or "Imported")
    return {
        "schema_name": schema_name,
        "domain": domain,
        "tables": tables,
    }


def _ensure_primary_keys(tables: list[dict[str, Any]]) -> None:
    for table in tables:
        columns = table["columns"]
        primary_keys = [column for column in columns if column["primary_key"]]

        if len(primary_keys) > 1:
            for extra in primary_keys[1:]:
                extra["primary_key"] = False
            primary_keys = primary_keys[:1]

        if not primary_keys:
            candidate = next(
                (
                    column
                    for column in columns
                    if column["name"].lower() == "id"
                    or column["name"].lower().endswith("_id")
                ),
                columns[0],
            )
            candidate["primary_key"] = True
            candidate["nullable"] = False
            candidate["field_role"] = "identifier"
        else:
            primary_keys[0]["nullable"] = False
            primary_keys[0]["field_role"] = "identifier"


def _canonicalize_foreign_keys(tables: list[dict[str, Any]]) -> None:
    table_name_lookup = {table["name"].lower(): table["name"] for table in tables}
    columns_lookup: dict[str, dict[str, str]] = {}
    for table in tables:
        columns_lookup[table["name"]] = {
            column["name"].lower(): column["name"] for column in table["columns"]
        }

    for table in tables:
        for column in table["columns"]:
            fk = column.get("foreign_key")
            if not isinstance(fk, dict):
                continue

            fk_table = normalize_identifier(fk.get("table"))
            fk_column = normalize_identifier(fk.get("column"))
            if not fk_table or not fk_column:
                column["foreign_key"] = None
                continue

            resolved_table = table_name_lookup.get(fk_table.lower())
            if not resolved_table:
                column["foreign_key"] = None
                continue

            resolved_column = columns_lookup[resolved_table].get(fk_column.lower())
            if not resolved_column:
                column["foreign_key"] = None
                continue

            column["foreign_key"] = {"table": resolved_table, "column": resolved_column}
            column["field_role"] = "identifier"


def _infer_foreign_keys(tables: list[dict[str, Any]]) -> None:
    pk_by_table: dict[str, str] = {}
    pk_name_to_tables: dict[str, list[str]] = {}

    for table in tables:
        pk_column = next(
            (column for column in table["columns"] if column["primary_key"]), None
        )
        if pk_column is None:
            continue
        pk_name = pk_column["name"]
        pk_by_table[table["name"]] = pk_name
        pk_name_to_tables.setdefault(pk_name.lower(), []).append(table["name"])

    for table in tables:
        table_name = table["name"]
        for column in table["columns"]:
            if column["primary_key"] or column.get("foreign_key") is not None:
                continue

            col_name = column["name"]
            col_lower = col_name.lower()
            if col_lower == "id":
                continue

            parent_candidates = [
                parent
                for parent in pk_name_to_tables.get(col_lower, [])
                if parent != table_name
            ]
            if not parent_candidates:
                continue

            parent_table = _pick_parent_table(col_name, parent_candidates)
            column["foreign_key"] = {
                "table": parent_table,
                "column": pk_by_table[parent_table],
            }
            column["field_role"] = "identifier"


def _pick_parent_table(column_name: str, candidates: list[str]) -> str:
    if len(candidates) == 1:
        return candidates[0]

    base = column_name.lower()
    if base.endswith("_id"):
        base = base[:-3]

    ranked = sorted(
        candidates, key=lambda name: (base not in tokens(name), len(name), name)
    )
    return ranked[0]
