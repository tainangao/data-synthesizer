from collections import Counter, defaultdict
import random
import time

from faker import Faker

from .relationship_rules import build_relationship_context, value_matches
from .schema_utils import pk_column, table_counts, table_order
from .values import (
    apply_nullable,
    non_key_value,
    pk_value,
    profile_for_parent,
    sample_parent_key,
    update_metrics,
)


def generate_data(
    schema: dict,
    records: int,
    seed: int,
    writers: list[object],
    order: list[str] | None = None,
) -> tuple[dict, dict]:
    random.seed(seed)
    fake = Faker()
    fake.seed_instance(seed)

    generation_order = order or table_order(schema)
    tables_by_name = {t["name"]: t for t in schema["tables"]}
    counts = table_counts(schema, generation_order, records)

    state = {
        "pk_values": defaultdict(list),
        "pk_profiles": defaultdict(dict),
        "categorical_options": {},
        "relationship_context": build_relationship_context(schema),
    }
    metrics = {
        "null_counts": Counter(),
        "categorical_counts": defaultdict(Counter),
        "numeric_stats": defaultdict(
            lambda: {"count": 0, "sum": 0.0, "min": None, "max": None}
        ),
        "fk_stats": defaultdict(lambda: {"rows": 0, "nulls": 0, "invalid": 0}),
        "fk_child_counts": defaultdict(Counter),
        "relationship_checks": defaultdict(
            lambda: {"rule": "", "rows": 0, "aligned": 0, "nulls": 0}
        ),
        "table_performance": {},
    }
    summary: dict[str, int] = {}

    for table_name in generation_order:
        table = tables_by_name[table_name]
        columns = table["columns"]
        pk_col = pk_column(table)
        row_count = counts[table_name]
        table_start = time.perf_counter()

        for writer in writers:
            writer.start_table(table)

        for i in range(1, row_count + 1):
            row: dict[str, object] = {}
            parent_profiles: list[dict] = []

            if pk_col is not None:
                row[pk_col["name"]] = pk_value(table_name, pk_col, i)

            for col in columns:
                if col.get("primary_key"):
                    continue
                fk = col.get("foreign_key")
                if not fk:
                    continue

                parent_table = fk["table"]
                parent_pool = state["pk_values"].get(parent_table, [])
                chosen = sample_parent_key(parent_pool)
                row[col["name"]] = chosen

                metric_key = f"{table_name}.{col['name']}"
                metrics["fk_stats"][metric_key]["rows"] += 1
                if chosen is None:
                    metrics["fk_stats"][metric_key]["nulls"] += 1
                    if not col.get("nullable", True):
                        metrics["fk_stats"][metric_key]["invalid"] += 1
                else:
                    metrics["fk_child_counts"][metric_key][chosen] += 1
                    parent_profile = (
                        state["pk_profiles"].get(parent_table, {}).get(chosen)
                    )
                    if parent_profile:
                        parent_profiles.append(parent_profile)

            for col in columns:
                col_name = col["name"]
                if col_name in row:
                    continue
                value, relationship_info = non_key_value(
                    fake=fake,
                    table_name=table_name,
                    col=col,
                    row=row,
                    parent_profiles=parent_profiles,
                    state=state,
                )
                final_value = apply_nullable(col, value)
                row[col_name] = final_value

                if relationship_info is not None:
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

            update_metrics(metrics, table_name, columns, row)

            if pk_col is not None:
                pk_name = pk_col["name"]
                pk_val = row[pk_name]
                state["pk_values"][table_name].append(pk_val)
                state["pk_profiles"][table_name][pk_val] = profile_for_parent(
                    row, columns
                )

        for writer in writers:
            writer.end_table()

        summary[table_name] = row_count
        table_elapsed = time.perf_counter() - table_start
        metrics["table_performance"][table_name] = {
            "rows": row_count,
            "elapsed_seconds": round(table_elapsed, 4),
            "rows_per_second": round(row_count / table_elapsed, 2)
            if table_elapsed > 0
            else None,
        }

    return summary, metrics
