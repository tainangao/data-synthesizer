from __future__ import annotations

from pathlib import Path
from typing import Any

from .common import normalize_schema


def load_parquet_schema(path: Path) -> dict[str, Any]:
    columns = _load_with_pyarrow(path)
    if not columns:
        columns = _load_with_fastparquet(path)
    if not columns:
        raise RuntimeError(
            "Parquet schema loading requires 'pyarrow' or 'fastparquet'."
        )

    table_name = path.stem if path.is_file() else path.name
    raw_schema = {
        "schema_name": table_name,
        "domain": "Imported Parquet",
        "tables": [
            {
                "name": table_name,
                "description": f"Imported from parquet source '{path.name}'.",
                "columns": columns,
            }
        ],
    }

    return normalize_schema(
        raw_schema,
        fallback_schema_name=table_name,
        fallback_domain="Imported Parquet",
    )


def _load_with_pyarrow(path: Path) -> list[dict[str, Any]]:
    try:
        import pyarrow.dataset as ds
    except ImportError:
        return []

    dataset = ds.dataset(str(path), format="parquet")
    columns = []
    for field in dataset.schema:
        columns.append(
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": bool(getattr(field, "nullable", True)),
            }
        )
    return columns


def _load_with_fastparquet(path: Path) -> list[dict[str, Any]]:
    try:
        from fastparquet import ParquetFile
    except ImportError:
        return []

    parquet = ParquetFile(str(path))
    dtypes = getattr(parquet, "dtypes", None)
    columns = []
    for name in parquet.columns:
        dtype_text = str(dtypes.get(name)) if dtypes is not None else "TEXT"
        columns.append(
            {
                "name": str(name),
                "type": dtype_text,
                "nullable": True,
            }
        )
    return columns
