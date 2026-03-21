def build_quality_report(schema: dict, summary: dict, metrics: dict, seed: int) -> dict:
    categorical = {}
    for key, counts in metrics["categorical_counts"].items():
        categorical[key] = [
            {"value": value, "count": count} for value, count in counts.most_common(6)
        ]

    numerical = {}
    for key, stats in metrics["numeric_stats"].items():
        if stats["count"] == 0:
            continue
        numerical[key] = {
            "count": stats["count"],
            "min": round(stats["min"], 4) if stats["min"] is not None else None,
            "max": round(stats["max"], 4) if stats["max"] is not None else None,
            "avg": round(stats["sum"] / stats["count"], 4),
        }

    fk_integrity = {}
    for key, stats in metrics["fk_stats"].items():
        fk_integrity[key] = {
            "rows": stats["rows"],
            "nulls": stats["nulls"],
            "invalid": stats["invalid"],
            "valid_rate": round((stats["rows"] - stats["invalid"]) / stats["rows"], 4)
            if stats["rows"]
            else 1.0,
        }

    return {
        "schema_name": schema.get("schema_name"),
        "domain": schema.get("domain"),
        "seed": seed,
        "records_per_table": summary,
        "fk_integrity": fk_integrity,
        "categorical_distributions": categorical,
        "numerical_summaries": numerical,
        "null_counts": dict(metrics["null_counts"]),
    }
