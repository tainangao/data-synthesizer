from __future__ import annotations

from collections import Counter
from typing import Any

from pydantic import ValidationError

from src.gen_schema.schema_models import SchemaModel

MIN_TABLE_COUNT = 3
REQUIRED_FIELD_ROLES = ("numerical", "categorical", "semi_structured")


def validate_schema(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        issues = [
            _issue(
                "schema_type",
                "Schema must be a JSON object.",
                "schema",
            )
        ]
        return {
            "valid": False,
            "schema": None,
            "issues": issues,
            "summary": _empty_summary(),
        }

    try:
        schema_model = SchemaModel.model_validate(payload)
    except ValidationError as exc:
        issues = [_from_pydantic_error(error) for error in exc.errors()]
        return {
            "valid": False,
            "schema": None,
            "issues": issues,
            "summary": _raw_summary(payload),
        }

    schema = schema_model.model_dump(mode="python")
    issues = _logical_issues(schema)

    return {
        "valid": not issues,
        "schema": schema,
        "issues": issues,
        "summary": _schema_summary(schema),
    }


def format_validation_feedback(
    issues: list[dict[str, Any]], *, max_items: int = 20
) -> str:
    if not issues:
        return "No validation issues found."

    lines: list[str] = []
    for index, issue in enumerate(issues[:max_items], start=1):
        code = str(issue.get("code", "validation_error"))
        message = str(issue.get("message", "Unknown validation issue."))
        path = issue.get("path")
        if path:
            lines.append(f"{index}. [{code}] {message} (path: {path})")
        else:
            lines.append(f"{index}. [{code}] {message}")

    remaining = len(issues) - max_items
    if remaining > 0:
        lines.append(f"... {remaining} more issue(s)")

    return "\n".join(lines)


def _logical_issues(schema: dict[str, Any]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    tables = schema["tables"]

    if len(tables) < MIN_TABLE_COUNT:
        issues.append(
            _issue(
                "table_count",
                f"Schema must include at least {MIN_TABLE_COUNT} tables.",
                "tables",
            )
        )

    table_name_index: dict[str, int] = {}
    table_columns: dict[str, set[str]] = {}
    table_columns_lower: dict[str, dict[str, str]] = {}
    role_counts = Counter()
    fk_count = 0

    for table_index, table in enumerate(tables):
        table_name = table["name"]
        table_name_key = table_name.lower()
        if table_name_key in table_name_index:
            issues.append(
                _issue(
                    "duplicate_table",
                    f"Duplicate table name '{table_name}'.",
                    f"tables[{table_index}].name",
                )
            )
        else:
            table_name_index[table_name_key] = table_index

        column_name_index: dict[str, int] = {}
        pk_count = 0
        columns = table["columns"]
        table_columns[table_name] = {column["name"] for column in columns}
        table_columns_lower[table_name] = {
            column["name"].lower(): column["name"] for column in columns
        }

        for column_index, column in enumerate(columns):
            column_name = column["name"]
            column_name_key = column_name.lower()

            if column_name_key in column_name_index:
                issues.append(
                    _issue(
                        "duplicate_column",
                        (f"Duplicate column '{column_name}' in table '{table_name}'."),
                        f"tables[{table_index}].columns[{column_index}].name",
                    )
                )
            else:
                column_name_index[column_name_key] = column_index

            role_counts[column["field_role"]] += 1
            if column.get("primary_key"):
                pk_count += 1
            if column.get("foreign_key"):
                fk_count += 1

        if pk_count != 1:
            issues.append(
                _issue(
                    "primary_key_count",
                    (
                        f"Table '{table_name}' must have exactly one primary key "
                        f"column; found {pk_count}."
                    ),
                    f"tables[{table_index}].columns",
                )
            )

    if fk_count == 0:
        issues.append(
            _issue(
                "foreign_key_count",
                "Schema must include at least one foreign key relationship.",
                "tables",
            )
        )

    for required_role in REQUIRED_FIELD_ROLES:
        if role_counts[required_role] == 0:
            issues.append(
                _issue(
                    "required_field_role",
                    (
                        "Schema must include at least one column with field_role "
                        f"'{required_role}'."
                    ),
                    "tables",
                )
            )

    table_name_lookup = {name.lower(): name for name in table_columns}
    for table_index, table in enumerate(tables):
        table_name = table["name"]
        for column_index, column in enumerate(table["columns"]):
            foreign_key = column.get("foreign_key")
            if not foreign_key:
                continue

            target_table = foreign_key["table"]
            target_column = foreign_key["column"]
            path = f"tables[{table_index}].columns[{column_index}].foreign_key"

            resolved_table = target_table
            if target_table not in table_columns:
                resolved_table = table_name_lookup.get(target_table.lower(), "")
                if not resolved_table:
                    issues.append(
                        _issue(
                            "foreign_key_table_missing",
                            (
                                f"Foreign key '{table_name}.{column['name']}' references "
                                f"unknown table '{target_table}'."
                            ),
                            path,
                        )
                    )
                    continue
                issues.append(
                    _issue(
                        "foreign_key_table_name_mismatch",
                        (
                            f"Foreign key '{table_name}.{column['name']}' references "
                            f"table '{target_table}', but the canonical name is "
                            f"'{resolved_table}'."
                        ),
                        path,
                    )
                )

            if target_column not in table_columns[resolved_table]:
                resolved_column = table_columns_lower[resolved_table].get(
                    target_column.lower()
                )
                if resolved_column:
                    message = (
                        f"Foreign key '{table_name}.{column['name']}' references "
                        f"'{target_table}.{target_column}', but the canonical name is "
                        f"'{resolved_table}.{resolved_column}'."
                    )
                else:
                    message = (
                        f"Foreign key '{table_name}.{column['name']}' references "
                        f"unknown column '{target_column}' on table '{resolved_table}'."
                    )
                issues.append(_issue("foreign_key_column_missing", message, path))

    return issues


def _schema_summary(schema: dict[str, Any]) -> dict[str, Any]:
    role_counts = Counter()
    table_count = len(schema.get("tables", []))
    column_count = 0
    primary_key_count = 0
    foreign_key_count = 0

    for table in schema.get("tables", []):
        columns = table.get("columns", [])
        column_count += len(columns)
        for column in columns:
            role = str(column.get("field_role", ""))
            if role:
                role_counts[role] += 1
            if column.get("primary_key"):
                primary_key_count += 1
            if column.get("foreign_key"):
                foreign_key_count += 1

    return {
        "table_count": table_count,
        "column_count": column_count,
        "primary_key_count": primary_key_count,
        "foreign_key_count": foreign_key_count,
        "field_role_counts": dict(role_counts),
    }


def _raw_summary(payload: dict[str, Any]) -> dict[str, Any]:
    tables = payload.get("tables")
    if not isinstance(tables, list):
        return _empty_summary()

    table_count = len(tables)
    column_count = 0
    for table in tables:
        columns = table.get("columns") if isinstance(table, dict) else None
        if isinstance(columns, list):
            column_count += len(columns)

    summary = _empty_summary()
    summary["table_count"] = table_count
    summary["column_count"] = column_count
    return summary


def _empty_summary() -> dict[str, Any]:
    return {
        "table_count": 0,
        "column_count": 0,
        "primary_key_count": 0,
        "foreign_key_count": 0,
        "field_role_counts": {},
    }


def _from_pydantic_error(error: dict[str, Any]) -> dict[str, str]:
    path = _loc_to_path(error.get("loc", ()))
    return _issue(
        "schema_shape",
        str(error.get("msg", "Schema shape validation failed.")),
        path,
    )


def _loc_to_path(loc: Any) -> str:
    parts: list[str] = []
    for item in loc:
        if isinstance(item, int):
            if parts:
                parts[-1] = f"{parts[-1]}[{item}]"
            else:
                parts.append(f"[{item}]")
        else:
            parts.append(str(item))
    return ".".join(parts)


def _issue(code: str, message: str, path: str) -> dict[str, str]:
    return {
        "code": code,
        "message": message,
        "path": path,
    }


__all__ = ["format_validation_feedback", "validate_schema"]
