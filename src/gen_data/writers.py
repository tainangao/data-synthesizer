import csv
import json
from pathlib import Path
import shutil
import sqlite3
from typing import Any

from src.common import parse_datetime, safe_name


def serialize_cell(value: object) -> object:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, bool):
        return int(value)
    return value


def _arrow_type(pa: Any, col: dict) -> Any:
    dtype = str(col.get("type", "TEXT")).upper()
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
    return mapping.get(dtype, pa.string())


def _coerce_arrow_value(col: dict, value: object) -> object:
    if value is None:
        return None

    dtype = str(col.get("type", "TEXT")).upper()

    if isinstance(value, (dict, list)):
        return json.dumps(value)

    if dtype == "BOOLEAN":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        lowered = str(value).strip().lower()
        if lowered in {"1", "true", "t", "yes", "y"}:
            return True
        if lowered in {"0", "false", "f", "no", "n"}:
            return False
        return bool(value)

    if dtype == "INTEGER":
        return int(value)

    if dtype in {"NUMERIC", "REAL"}:
        return float(value)

    if dtype == "DATE":
        parsed = parse_datetime(value)
        return parsed.date() if parsed else None

    if dtype == "TIMESTAMP":
        return parse_datetime(value)

    if dtype in {"JSON", "XML"}:
        return str(value)

    return value


def _build_arrow_table(pa: Any, table: dict, rows: list[dict[str, object]]) -> Any:
    fields = []
    arrays = []
    for col in table["columns"]:
        field = pa.field(
            col["name"],
            _arrow_type(pa, col),
            nullable=bool(col.get("nullable", True) and not col.get("primary_key")),
        )
        fields.append(field)
        values = [_coerce_arrow_value(col, row.get(col["name"])) for row in rows]
        arrays.append(pa.array(values, type=field.type))

    schema = pa.schema(fields)
    return pa.Table.from_arrays(arrays, schema=schema)


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
    def __init__(
        self, sqlite_path: Path, schema: dict, order: list[str], batch_size: int = 5000
    ):
        self.sqlite_path = sqlite_path
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        if self.sqlite_path.exists():
            self.sqlite_path.unlink()

        self.conn = sqlite3.connect(self.sqlite_path)
        self.conn.execute("PRAGMA foreign_keys = ON")

        # Performance tuning for large datasets
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA journal_mode = MEMORY")
        self.conn.execute("PRAGMA temp_store = MEMORY")

        tables_by_name = {t["name"]: t for t in schema["tables"]}
        for table_name in order:
            self._create_table(tables_by_name[table_name])

        self._insert_sql: str | None = None
        self._insert_columns: list[str] = []
        self._batch: list[tuple] = []
        self._batch_size = batch_size

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
        values = tuple(serialize_cell(row.get(c)) for c in self._insert_columns)
        self._batch.append(values)

        # Flush when batch is full
        if len(self._batch) >= self._batch_size:
            self._flush_batch()

    def _flush_batch(self) -> None:
        if self._batch:
            self.conn.executemany(self._insert_sql, self._batch)
            self._batch.clear()

    def end_table(self) -> None:
        self._flush_batch()  # Flush remaining rows
        self.conn.commit()
        self._insert_sql = None
        self._insert_columns = []
        self._batch.clear()

    def close(self) -> None:
        self._flush_batch()
        self.conn.commit()
        self.conn.close()


class ParquetWriter:
    def __init__(self, out_dir: Path, chunk_size: int = 50000):
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")

        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise RuntimeError("ParquetWriter requires the 'pyarrow' package.") from exc

        self._pa = pa
        self._pq = pq
        self._table: dict | None = None
        self._rows: list[dict[str, object]] = []
        self.table_paths: dict[str, Path] = {}
        # Cap in-memory buffering so very large tables stream out incrementally.
        self._chunk_size = chunk_size
        self._output_path: Path | None = None
        self._writer = None

    def start_table(self, table: dict) -> None:
        self._table = table
        self._rows = []
        table_name = table["name"]
        self._output_path = self.out_dir / f"{safe_name(table_name)}.parquet"
        # Ensure reruns do not append to stale files from previous runs.
        if self._output_path.exists():
            self._output_path.unlink()
        self._writer = None

    def write_row(self, row: dict) -> None:
        self._rows.append(dict(row))
        if len(self._rows) >= self._chunk_size:
            self._flush_chunk()

    def _flush_chunk(self) -> None:
        if self._table is None or not self._rows:
            return

        assert self._output_path is not None
        # Convert only the buffered chunk, then write as a Parquet row group.
        arrow_table = _build_arrow_table(self._pa, self._table, self._rows)
        if self._writer is None:
            # Keep one open writer per table so chunks are appended efficiently.
            self._writer = self._pq.ParquetWriter(self._output_path, arrow_table.schema)
        self._writer.write_table(arrow_table)
        self._rows = []

    def end_table(self) -> None:
        if self._table is None:
            return

        table_name = self._table["name"]
        # Flush any final partial chunk.
        self._flush_chunk()

        assert self._output_path is not None
        if self._writer is None:
            # Preserve table presence even when schema-derived count is zero.
            empty_table = _build_arrow_table(self._pa, self._table, [])
            self._pq.write_table(empty_table, self._output_path)
        else:
            self._writer.close()
            self._writer = None

        self.table_paths[table_name] = self._output_path

        self._table = None
        self._rows = []
        self._output_path = None

    def close(self) -> None:
        self.end_table()


class DeltaWriter:
    def __init__(self, out_dir: Path, chunk_size: int = 50000):
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")

        try:
            import pyarrow as pa
        except ImportError as exc:
            raise RuntimeError("DeltaWriter requires the 'pyarrow' package.") from exc

        try:
            from deltalake import write_deltalake
        except ImportError as exc:
            raise RuntimeError("DeltaWriter requires the 'deltalake' package.") from exc

        self._pa = pa
        self._write_deltalake = write_deltalake
        self._table: dict | None = None
        self._rows: list[dict[str, object]] = []
        self.table_paths: dict[str, Path] = {}
        # Cap in-memory buffering so very large tables stream out incrementally.
        self._chunk_size = chunk_size
        self._table_path: Path | None = None
        self._has_written = False

    def start_table(self, table: dict) -> None:
        self._table = table
        self._rows = []
        self._has_written = False
        table_name = table["name"]
        self._table_path = self.out_dir / safe_name(table_name)
        # Remove prior table data so each run produces a clean Delta table.
        if self._table_path.exists():
            if self._table_path.is_dir():
                shutil.rmtree(self._table_path)
            else:
                self._table_path.unlink()

    def write_row(self, row: dict) -> None:
        self._rows.append(dict(row))
        if len(self._rows) >= self._chunk_size:
            self._flush_chunk()

    def _flush_chunk(self) -> None:
        if self._table is None or not self._rows:
            return

        assert self._table_path is not None
        # Write buffered rows as one Delta commit per chunk.
        arrow_table = _build_arrow_table(self._pa, self._table, self._rows)
        # First chunk initializes the table; later chunks append.
        mode = "append" if self._has_written else "overwrite"
        self._write_deltalake(str(self._table_path), arrow_table, mode=mode)
        self._has_written = True
        self._rows = []

    def end_table(self) -> None:
        if self._table is None:
            return

        table_name = self._table["name"]
        # Flush any final partial chunk.
        self._flush_chunk()

        assert self._table_path is not None
        if not self._has_written:
            # Keep output shape consistent by materializing empty tables too.
            empty_table = _build_arrow_table(self._pa, self._table, [])
            self._write_deltalake(str(self._table_path), empty_table, mode="overwrite")
            self._has_written = True

        self.table_paths[table_name] = self._table_path

        self._table = None
        self._rows = []
        self._table_path = None
        self._has_written = False

    def close(self) -> None:
        self.end_table()
