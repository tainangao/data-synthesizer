"""Data quality reporting for gen_data output.

Computes FK integrity, categorical distributions, numerical summaries,
and null rates directly from Polars DataFrames.
"""

import json
from pathlib import Path

import polars as pl


def build_quality_report(
    schema: dict,
    row_counts: dict[str, int],
    seed: int,
    out_dir: Path,
    csv_dir: Path | None = None,
) -> dict:
    """Build a data quality report from generated CSV files.

    Args:
        schema: JSON schema (tables + columns)
        row_counts: {table_name: row_count} from generate_data()
        seed: Random seed used for generation
        out_dir: Directory to write data_quality_report.json
        csv_dir: Directory containing generated CSV files (defaults to out_dir/csv)

    Returns:
        Quality report dict (also written to out_dir/data_quality_report.json)
    """
    csv_dir = csv_dir or (out_dir / "csv")

    tables_by_name = {t["name"]: t for t in schema["tables"]}
    table_dfs: dict[str, pl.DataFrame] = {}

    # Load all CSVs into DataFrames for analysis
    for table_name in row_counts:
        safe = _safe_name(table_name)
        csv_path = csv_dir / f"{safe}.csv"
        if csv_path.exists():
            try:
                table_dfs[table_name] = pl.read_csv(csv_path, infer_schema_length=10000)
            except Exception:
                pass

    fk_integrity: dict = {}
    categorical_distributions: dict = {}
    numerical_summaries: dict = {}
    null_rates: dict = {}

    for table_name, df in table_dfs.items():
        table = tables_by_name.get(table_name)
        if table is None:
            continue

        # ── FK integrity ────────────────────────────────────────────
        for col in table["columns"]:
            fk = col.get("foreign_key")
            if not fk:
                continue
            col_name = col["name"]
            parent_table = fk["table"]
            parent_pk_col = fk["column"]
            if col_name not in df.columns:
                continue

            parent_df = table_dfs.get(parent_table)
            total = len(df)
            nulls = df[col_name].null_count()

            if parent_df is not None and parent_pk_col in parent_df.columns:
                parent_pks = set(parent_df[parent_pk_col].drop_nulls().to_list())
                fk_vals = df[col_name].drop_nulls().to_list()
                invalid = sum(1 for v in fk_vals if v not in parent_pks)
            else:
                invalid = 0

            key = f"{table_name}.{col_name} → {parent_table}.{parent_pk_col}"
            fk_integrity[key] = {
                "total_rows": total,
                "null_fks": nulls,
                "invalid_fks": invalid,
                "valid_rate": round((total - nulls - invalid) / total, 4) if total else 1.0,
            }

        # ── Categorical distributions ────────────────────────────────
        for col in table["columns"]:
            if col.get("field_role") != "categorical":
                continue
            col_name = col["name"]
            if col_name not in df.columns:
                continue
            counts = (
                df[col_name]
                .drop_nulls()
                .value_counts()
                .sort("count", descending=True)
                .head(10)
            )
            key = f"{table_name}.{col_name}"
            categorical_distributions[key] = [
                {"value": str(row[col_name]), "count": row["count"]}
                for row in counts.iter_rows(named=True)
            ]

        # ── Numerical summaries ──────────────────────────────────────
        for col in table["columns"]:
            if col.get("field_role") != "numerical":
                continue
            col_name = col["name"]
            if col_name not in df.columns:
                continue
            series = df[col_name].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
            if len(series) == 0:
                continue
            key = f"{table_name}.{col_name}"
            numerical_summaries[key] = {
                "count": len(series),
                "min": round(series.min(), 4),
                "max": round(series.max(), 4),
                "mean": round(series.mean(), 4),
                "std": round(series.std(), 4),
            }

        # ── Null rates ───────────────────────────────────────────────
        total = len(df)
        for col_name in df.columns:
            nulls = df[col_name].null_count()
            if nulls > 0:
                null_rates[f"{table_name}.{col_name}"] = {
                    "nulls": nulls,
                    "null_rate": round(nulls / total, 4) if total else 0.0,
                }

    report = {
        "schema_name": schema.get("schema_name"),
        "domain": schema.get("domain"),
        "seed": seed,
        "row_counts": row_counts,
        "fk_integrity": fk_integrity,
        "categorical_distributions": categorical_distributions,
        "numerical_summaries": numerical_summaries,
        "null_rates": null_rates,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "data_quality_report.json").write_text(
        json.dumps(report, indent=2, default=str), encoding="utf-8"
    )

    return report


def _safe_name(name: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
