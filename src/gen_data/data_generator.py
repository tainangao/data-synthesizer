from collections import Counter, defaultdict
import json
import random
import time
from pathlib import Path

from faker import Faker

from .relationship_rules import build_relationship_context, value_matches
from src.gen_schema.schema_utils import pk_column, table_counts, table_order
from .metrics_collector import update_metrics
from .value_generators import (
    apply_nullable,
    non_key_value,
    pk_value,
    profile_for_parent,
    sample_parent_key,
)


def _to_jsonable(value: object) -> object:
    if isinstance(value, Counter):
        return dict(value)
    if isinstance(value, defaultdict):
        return {k: _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, dict):
        return {k: _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(v) for v in value]
    return value


def generate_data(
    schema: dict,
    records: int,
    seed: int,
    writers: list[object],
    order: list[str] | None = None,
    out_dir: Path | None = None,
    stress_mode: bool = False,
) -> tuple[dict, dict]:
    """Generate synthetic data for every table in the schema.

    Args:
        stress_mode: If True, skips expensive metrics (fk_child_counts, relationship_checks)
                     for better performance on large datasets.
    Returns:
        A tuple of:
        - summary: row count generated per table
        - metrics: quality/distribution/performance diagnostics
    """
    # Seed both Python random and Faker for reproducible runs.
    random.seed(seed)
    fake = Faker()
    fake.seed_instance(seed)

    # Generate parent tables before children so FK sampling has parent keys available.
    generation_order = order or table_order(schema)
    tables_by_name = {t["name"]: t for t in schema["tables"]}
    counts = table_counts(schema, generation_order, records)

    # Shared generation state used across table loops.
    state = {
        "pk_values": defaultdict(list),
        "pk_profiles": defaultdict(dict),
        "categorical_options": {},
        "relationship_context": build_relationship_context(schema),
    }

    # Metrics used by reporting.py to build quality and performance reports.
    # In stress_mode, skip expensive per-parent-child counts and relationship tracking.
    metrics: dict = {
        "null_counts": Counter(),
        "categorical_counts": defaultdict(Counter),
        "numeric_stats": defaultdict(
            lambda: {"count": 0, "sum": 0.0, "min": None, "max": None}
        ),
        "fk_stats": defaultdict(lambda: {"rows": 0, "nulls": 0, "invalid": 0}),
        "table_performance": {},
    }
    if not stress_mode:
        metrics["fk_child_counts"] = defaultdict(Counter)
        metrics["relationship_checks"] = defaultdict(
            lambda: {"rule": "", "rows": 0, "aligned": 0, "nulls": 0}
        )
    summary: dict[str, int] = {}

    for table_name in generation_order:
        table = tables_by_name[table_name]
        columns = table["columns"]
        pk_col = pk_column(table)
        row_count = counts[table_name]
        table_start = time.perf_counter()

        # Tell each writer we are starting a new table.
        for writer in writers:
            writer.start_table(table)

        for i in range(1, row_count + 1):
            # Build each row in two passes:
            # 1) keys first (PK/FK), then
            # 2) remaining non-key fields with relationship-aware generation.
            row: dict[str, object] = {}
            parent_profiles: list[dict] = []

            # Handle PK
            if pk_col is not None:
                # PK generation is deterministic by table/type/index.
                # By default use i as the PK value, but this can be overridden by pk_value() for more complex strategies.
                row[pk_col["name"]] = pk_value(table_name, pk_col, i)

            # Handle FKs
            for col in columns:
                if col.get("primary_key"):
                    continue
                fk = col.get("foreign_key")
                if not fk:
                    continue

                # FK values are sampled from already-generated parent PK pools.
                parent_table = fk["table"]
                parent_pool = state["pk_values"].get(parent_table, [])
                chosen = sample_parent_key(parent_pool)
                row[col["name"]] = chosen

                # Track FK integrity and parent-child fanout diagnostics.
                metric_key = f"{table_name}.{col['name']}"
                metrics["fk_stats"][metric_key]["rows"] += 1
                if chosen is None:
                    metrics["fk_stats"][metric_key]["nulls"] += 1
                    if not col.get("nullable", True):
                        metrics["fk_stats"][metric_key]["invalid"] += 1
                else:
                    if "fk_child_counts" in metrics:
                        metrics["fk_child_counts"][metric_key][chosen] += 1
                    # Parent profiles enable child attribute alignment (status/type/etc).
                    parent_profile = (
                        state["pk_profiles"].get(parent_table, {}).get(chosen)
                    )
                    if parent_profile:
                        parent_profiles.append(parent_profile)

            for col in columns:
                col_name = col["name"]
                if col_name in row:
                    continue

                # Generate context-aware non-key field values.
                value, relationship_info = non_key_value(
                    fake=fake,
                    table_name=table_name,
                    col=col,
                    row=row,
                    parent_profiles=parent_profiles,
                    state=state,
                )

                # Nullability is applied after producing a candidate value.
                final_value = apply_nullable(col, value)
                row[col_name] = final_value

                if relationship_info is not None and "relationship_checks" in metrics:
                    # Measure whether relationship-conditioned values align as expected.
                    metric_key = f"{table_name}.{col_name}"
                    relation_stats = metrics["relationship_checks"][metric_key]
                    relation_stats["rule"] = str(
                        relationship_info.get("rule") or relation_stats["rule"]
                    )
                    relation_stats["rows"] += 1

                    if final_value is None:
                        relation_stats["nulls"] += 1
                    elif value_matches(
                        final_value,
                        relationship_info.get("expected_value"),
                    ):
                        relation_stats["aligned"] += 1

            for writer in writers:
                writer.write_row(row)

            # Update null/categorical/numeric summary stats for this row.
            update_metrics(metrics, table_name, columns, row)

            if pk_col is not None:
                # Persist generated PKs and compact parent profiles for child tables.
                pk_name = pk_col["name"]
                pk_val = row[pk_name]
                state["pk_values"][table_name].append(pk_val)
                state["pk_profiles"][table_name][pk_val] = profile_for_parent(
                    row, columns
                )

        # Flush and finalize this table for every output writer.
        for writer in writers:
            writer.end_table()

        summary[table_name] = row_count
        table_elapsed = time.perf_counter() - table_start
        # Per-table throughput is useful for benchmarking larger generation scales.
        metrics["table_performance"][table_name] = {
            "rows": row_count,
            "elapsed_seconds": round(table_elapsed, 4),
            "rows_per_second": round(row_count / table_elapsed, 2)
            if table_elapsed > 0
            else None,
        }

    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8"
        )
        (out_dir / "metrics.json").write_text(
            json.dumps(
                {"summary": summary, "metrics": _to_jsonable(metrics)},
                indent=2,
            ),
            encoding="utf-8",
        )

    return summary, metrics
