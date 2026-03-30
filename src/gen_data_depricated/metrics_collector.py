def update_metrics(
    metrics: dict,
    table_name: str,
    columns: list[dict],
    row: dict,
) -> None:
    for col in columns:
        col_name = col["name"]
        value = row.get(col_name)
        role = str(col.get("field_role", "")).lower()
        dtype = str(col.get("type", "TEXT")).upper()
        metric_key = f"{table_name}.{col_name}"

        if value is None:
            metrics["null_counts"][metric_key] += 1
            continue

        if role == "categorical":
            metrics["categorical_counts"][metric_key][str(value)] += 1

        if dtype in {"INTEGER", "NUMERIC", "REAL"} or role == "numerical":
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue

            stats = metrics["numeric_stats"][metric_key]
            stats["count"] += 1
            stats["sum"] += number
            stats["min"] = number if stats["min"] is None else min(stats["min"], number)
            stats["max"] = number if stats["max"] is None else max(stats["max"], number)
