import csv
import json
from pathlib import Path
import sqlite3

from synthgen.common import safe_name


def serialize_cell(value: object) -> object:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, bool):
        return int(value)
    return value


class CSVWriter:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._file = None
        self._writer = None

    def start_table(self, table: dict) -> None:
        table_name = table["name"]
        fieldnames = [c["name"] for c in table["columns"]]
        path = self.out_dir / f"{safe_name(table_name)}.csv"
        self._file = path.open("w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=fieldnames)
        self._writer.writeheader()

    def write_row(self, row: dict) -> None:
        assert self._writer is not None
        self._writer.writerow({k: serialize_cell(v) for k, v in row.items()})

    def end_table(self) -> None:
        if self._file:
            self._file.close()
        self._file = None
        self._writer = None

    def close(self) -> None:
        self.end_table()


class SQLiteWriter:
    def __init__(self, sqlite_path: Path, schema: dict, order: list[str]):
        self.sqlite_path = sqlite_path
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        if self.sqlite_path.exists():
            self.sqlite_path.unlink()

        self.conn = sqlite3.connect(self.sqlite_path)
        self.conn.execute("PRAGMA foreign_keys = ON")

        tables_by_name = {t["name"]: t for t in schema["tables"]}
        for table_name in order:
            self._create_table(tables_by_name[table_name])

        self._insert_sql: str | None = None
        self._insert_columns: list[str] = []

    def _sqlite_type(self, col: dict) -> str:
        dtype = str(col.get("type", "TEXT")).upper()
        if dtype == "INTEGER":
            return "INTEGER"
        if dtype == "REAL":
            return "REAL"
        if dtype == "NUMERIC":
            return "NUMERIC"
        if dtype == "BOOLEAN":
            return "INTEGER"
        return "TEXT"

    def _create_table(self, table: dict) -> None:
        columns_sql: list[str] = []
        fk_sql: list[str] = []

        for col in table["columns"]:
            name = col["name"]
            sql_type = self._sqlite_type(col)
            clause = f'"{name}" {sql_type}'
            if col.get("primary_key"):
                clause += " PRIMARY KEY"
            if not col.get("nullable", True) and not col.get("primary_key"):
                clause += " NOT NULL"
            columns_sql.append(clause)

            fk = col.get("foreign_key")
            if fk:
                fk_sql.append(
                    f'FOREIGN KEY("{name}") REFERENCES "{fk["table"]}"("{fk["column"]}")'
                )

        ddl = f'CREATE TABLE "{table["name"]}" ({", ".join(columns_sql + fk_sql)})'
        self.conn.execute(ddl)

    def start_table(self, table: dict) -> None:
        table_name = table["name"]
        self._insert_columns = [c["name"] for c in table["columns"]]
        col_sql = ", ".join(f'"{c}"' for c in self._insert_columns)
        placeholders = ", ".join("?" for _ in self._insert_columns)
        self._insert_sql = (
            f'INSERT INTO "{table_name}" ({col_sql}) VALUES ({placeholders})'
        )

    def write_row(self, row: dict) -> None:
        assert self._insert_sql is not None
        values = [serialize_cell(row.get(c)) for c in self._insert_columns]
        self.conn.execute(self._insert_sql, values)

    def end_table(self) -> None:
        self.conn.commit()
        self._insert_sql = None
        self._insert_columns = []

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()
