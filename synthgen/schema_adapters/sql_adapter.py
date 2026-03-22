from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .common import normalize_identifier, normalize_schema

CREATE_TABLE_RE = re.compile(
    r"create\s+table\s+(?:if\s+not\s+exists\s+)?(?P<name>[^\(\n]+?)\s*\((?P<body>.*?)\)\s*(?:;|$)",
    flags=re.IGNORECASE | re.DOTALL,
)


def load_sql_schema(path: Path) -> dict[str, Any]:
    sql_text = path.read_text(encoding="utf-8")
    tables = _parse_create_tables(sql_text)
    if not tables:
        raise ValueError("No CREATE TABLE statements found in SQL input.")

    raw_schema = {
        "schema_name": path.stem,
        "domain": "Imported SQL",
        "tables": tables,
    }
    return normalize_schema(
        raw_schema,
        fallback_schema_name=path.stem,
        fallback_domain="Imported SQL",
    )


def _parse_create_tables(sql_text: str) -> list[dict[str, Any]]:
    cleaned = _strip_comments(sql_text)
    tables: list[dict[str, Any]] = []

    for match in CREATE_TABLE_RE.finditer(cleaned):
        table_name = normalize_identifier(match.group("name"))
        body = match.group("body")
        if not table_name:
            continue

        columns: list[dict[str, Any]] = []
        table_pk_columns: list[str] = []
        table_fk_rows: list[tuple[str, str, str]] = []

        for part in _split_top_level(body):
            entry = part.strip()
            if not entry:
                continue

            normalized_entry = re.sub(
                r"^constraint\s+[^\s]+\s+",
                "",
                entry,
                flags=re.IGNORECASE,
            )
            lowered = normalized_entry.lower()

            if lowered.startswith("primary key"):
                table_pk_columns.extend(_parse_primary_key_columns(normalized_entry))
                continue

            if lowered.startswith("foreign key"):
                table_fk_rows.extend(_parse_foreign_keys(normalized_entry))
                continue

            if lowered.startswith("unique") or lowered.startswith("check"):
                continue

            column = _parse_column_definition(normalized_entry)
            if column is None:
                continue
            columns.append(column)

        _apply_table_constraints(columns, table_pk_columns, table_fk_rows)
        tables.append(
            {
                "name": table_name,
                "description": f"Imported from SQL table '{table_name}'.",
                "columns": columns,
            }
        )

    return tables


def _strip_comments(sql_text: str) -> str:
    no_block = re.sub(r"/\*.*?\*/", "", sql_text, flags=re.DOTALL)
    return re.sub(r"--.*?$", "", no_block, flags=re.MULTILINE)


def _split_top_level(text: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    depth = 0
    in_single = False
    in_double = False

    for char in text:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if char == "(":
                depth += 1
            elif char == ")" and depth > 0:
                depth -= 1
            elif char == "," and depth == 0:
                items.append("".join(current).strip())
                current = []
                continue
        current.append(char)

    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


def _parse_column_definition(entry: str) -> dict[str, Any] | None:
    match = re.match(
        r"^(?P<name>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[^\s]+)\s*(?P<rest>.*)$",
        entry,
        flags=re.DOTALL,
    )
    if not match:
        return None

    name = normalize_identifier(match.group("name"))
    rest = (match.group("rest") or "").strip()
    if not name:
        return None

    keyword_match = re.search(
        r"\b(primary\s+key|not\s+null|references|unique|check|default|constraint)\b",
        rest,
        flags=re.IGNORECASE,
    )
    if keyword_match:
        type_text = rest[: keyword_match.start()].strip()
        constraints = rest[keyword_match.start() :]
    else:
        type_text = rest
        constraints = ""

    primary_key = bool(
        re.search(r"\bprimary\s+key\b", constraints, flags=re.IGNORECASE)
    )
    nullable = not bool(re.search(r"\bnot\s+null\b", constraints, flags=re.IGNORECASE))
    if primary_key:
        nullable = False

    foreign_key = None
    fk_match = re.search(
        r"\breferences\s+([^\s\(]+)\s*\(([^\)]+)\)",
        constraints,
        flags=re.IGNORECASE,
    )
    if fk_match:
        foreign_key = {
            "table": normalize_identifier(fk_match.group(1)),
            "column": normalize_identifier(fk_match.group(2)),
        }

    return {
        "name": name,
        "type": type_text or "TEXT",
        "nullable": nullable,
        "primary_key": primary_key,
        "foreign_key": foreign_key,
    }


def _parse_primary_key_columns(entry: str) -> list[str]:
    match = re.search(r"primary\s+key\s*\(([^\)]+)\)", entry, flags=re.IGNORECASE)
    if not match:
        return []
    return [
        normalize_identifier(piece)
        for piece in _split_top_level(match.group(1))
        if normalize_identifier(piece)
    ]


def _parse_foreign_keys(entry: str) -> list[tuple[str, str, str]]:
    match = re.search(
        r"foreign\s+key\s*\(([^\)]+)\)\s*references\s+([^\s\(]+)\s*\(([^\)]+)\)",
        entry,
        flags=re.IGNORECASE,
    )
    if not match:
        return []

    source_columns = [
        normalize_identifier(piece)
        for piece in _split_top_level(match.group(1))
        if normalize_identifier(piece)
    ]
    target_columns = [
        normalize_identifier(piece)
        for piece in _split_top_level(match.group(3))
        if normalize_identifier(piece)
    ]
    target_table = normalize_identifier(match.group(2))

    rows: list[tuple[str, str, str]] = []
    for src, dst in zip(source_columns, target_columns):
        rows.append((src, target_table, dst))
    return rows


def _apply_table_constraints(
    columns: list[dict[str, Any]],
    pk_columns: list[str],
    fk_rows: list[tuple[str, str, str]],
) -> None:
    by_name = {column["name"].lower(): column for column in columns}

    for pk_name in pk_columns:
        column = by_name.get(pk_name.lower())
        if column is None:
            continue
        column["primary_key"] = True
        column["nullable"] = False

    for src_col, target_table, target_col in fk_rows:
        column = by_name.get(src_col.lower())
        if column is None:
            continue
        column["foreign_key"] = {
            "table": target_table,
            "column": target_col,
        }
