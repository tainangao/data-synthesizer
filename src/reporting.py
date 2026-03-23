import json
from pathlib import Path


def _percentile(values: list[int], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])

    rank = (pct / 100.0) * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def build_quality_report(schema: dict, summary: dict, metrics: dict, seed: int, out_dir: str | Path) -> dict:
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

    parent_child_distribution = {}
    for key, parent_counts in metrics.get("fk_child_counts", {}).items():
        counts = list(parent_counts.values())
        total_children = sum(counts)
        distinct_parents = len(counts)
        p95 = _percentile(counts, 95.0)

        parent_child_distribution[key] = {
            "total_child_rows": total_children,
            "distinct_parent_keys": distinct_parents,
            "avg_children_per_parent": round(total_children / distinct_parents, 4)
            if distinct_parents
            else None,
            "max_children_per_parent": max(counts) if counts else 0,
            "p95_children_per_parent": round(p95, 4) if p95 is not None else None,
        }

    parent_child_consistency = {}
    rule_summary = {}
    for key, stats in metrics.get("relationship_checks", {}).items():
        rows = int(stats.get("rows", 0) or 0)
        nulls = int(stats.get("nulls", 0) or 0)
        aligned = int(stats.get("aligned", 0) or 0)
        non_null_rows = max(rows - nulls, 0)
        rule = str(stats.get("rule", "") or "token_overlap_copy")

        parent_child_consistency[key] = {
            "rule": rule,
            "rows": rows,
            "nulls": nulls,
            "aligned": aligned,
            "alignment_rate": round(aligned / non_null_rows, 4)
            if non_null_rows
            else None,
        }

        aggregate = rule_summary.setdefault(
            rule,
            {"rows": 0, "nulls": 0, "aligned": 0},
        )
        aggregate["rows"] += rows
        aggregate["nulls"] += nulls
        aggregate["aligned"] += aligned

    relationship_rule_summary = {}
    for rule, stats in rule_summary.items():
        non_null_rows = max(stats["rows"] - stats["nulls"], 0)
        relationship_rule_summary[rule] = {
            "rows": stats["rows"],
            "nulls": stats["nulls"],
            "aligned": stats["aligned"],
            "alignment_rate": round(stats["aligned"] / non_null_rows, 4)
            if non_null_rows
            else None,
        }

    report = {
        "schema_name": schema.get("schema_name"),
        "domain": schema.get("domain"),
        "seed": seed,
        "records_per_table": summary,
        "fk_integrity": fk_integrity,
        "parent_child_distribution": parent_child_distribution,
        "parent_child_consistency": parent_child_consistency,
        "relationship_rule_summary": relationship_rule_summary,
        "categorical_distributions": categorical,
        "numerical_summaries": numerical,
        "null_counts": dict(metrics["null_counts"]),
    }
    
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "data_quality_report.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )

    return report