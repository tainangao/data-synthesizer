"""
Comprehensive validation script for assignment submission.
Combines structural checks (FK, nullability, types) with business-rule checks.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pandas as pd


# ============================================================================
# SECTION 1: Path and Data Loading Helpers
# ============================================================================


def resolve_project_root() -> Path:
    """Resolve project root for script and notebook usage."""
    if "__file__" in globals():
        return Path(__file__).resolve().parents[1]

    cwd = Path.cwd()
    if cwd.name == "tests":
        return cwd.parent
    return cwd


def resolve_output_path(output_dir: str | Path = "demo_output") -> Path:
    """Resolve output directory with sensible defaults."""
    output_path = Path(output_dir)
    if output_path.is_absolute():
        return output_path
    return resolve_project_root() / output_path


def load_sqlite_data(
    db_path: Path,
) -> tuple[dict[str, pd.DataFrame], list[str], sqlite3.Connection]:
    """Load all SQLite tables and return dataframes + open connection."""
    conn = sqlite3.connect(db_path)

    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn
    )["name"].tolist()

    data = {}
    for table in tables:
        data[table] = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        print(f"Loaded {table}: {len(data[table])} rows")

    return data, tables, conn


# ============================================================================
# SECTION 2: Structural Validation (Schema Compliance)
# ============================================================================


def validate_fk_integrity(conn: sqlite3.Connection, schema: dict) -> list[dict]:
    """Verify all FK references point to existing parent records."""
    issues = []
    for table in schema["tables"]:
        table_name = table["name"]
        for col in table["columns"]:
            fk = col.get("foreign_key")
            if not fk:
                continue

            query = f"""
            SELECT COUNT(*) AS invalid_count
            FROM {table_name} child
            LEFT JOIN {fk["table"]} parent ON child.{col["name"]} = parent.{fk["column"]}
            WHERE child.{col["name"]} IS NOT NULL AND parent.{fk["column"]} IS NULL
            """
            invalid = conn.execute(query).fetchone()[0]
            if invalid > 0:
                issues.append(
                    {
                        "table": table_name,
                        "column": col["name"],
                        "issue": f"{invalid} orphaned FK values",
                        "ref_table": fk["table"],
                    }
                )
    return issues


def validate_null_constraints(conn: sqlite3.Connection, schema: dict) -> list[dict]:
    """Check non-nullable columns contain no NULL values."""
    issues = []
    for table in schema["tables"]:
        table_name = table["name"]
        for col in table["columns"]:
            if col.get("nullable", True):
                continue

            query = f"SELECT COUNT(*) FROM {table_name} WHERE {col['name']} IS NULL"
            null_count = conn.execute(query).fetchone()[0]
            if null_count > 0:
                issues.append(
                    {
                        "table": table_name,
                        "column": col["name"],
                        "issue": f"{null_count} NULL values in non-nullable column",
                    }
                )
    return issues


def validate_data_types(conn: sqlite3.Connection, schema: dict) -> list[dict]:
    """Ensure numeric/boolean columns contain values of expected SQLite types."""
    issues = []
    for table in schema["tables"]:
        table_name = table["name"]
        for col in table["columns"]:
            col_type = col.get("type", "").upper()

            if col_type in {"INTEGER", "NUMERIC", "REAL"}:
                query = f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE {col["name"]} IS NOT NULL
                  AND TYPEOF({col["name"]}) NOT IN ('integer', 'real')
                """
            elif col_type == "BOOLEAN":
                query = f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE {col["name"]} IS NOT NULL
                  AND TYPEOF({col["name"]}) NOT IN ('integer')
                """
            else:
                continue

            invalid = conn.execute(query).fetchone()[0]
            if invalid > 0:
                issues.append(
                    {
                        "table": table_name,
                        "column": col["name"],
                        "issue": f"{invalid} values with unexpected SQLite type",
                    }
                )
    return issues


# ============================================================================
# SECTION 3: Business Logic - Temporal Ordering
# ============================================================================


def _temporal_cols(table: dict) -> list[str]:
    return [
        col["name"] for col in table["columns"] if col.get("field_role") == "temporal"
    ]


def check_temporal_ordering(data: dict[str, pd.DataFrame], schema: dict) -> list[dict]:
    """Check both intra-table and FK-based temporal ordering constraints."""
    issues = []
    table_defs = {t["name"]: t for t in schema["tables"]}

    # Intra-table ordering for tables with 2+ temporal columns.
    for table in schema["tables"]:
        table_name = table["name"]
        if table_name not in data:
            continue

        temporal_cols = _temporal_cols(table)
        if len(temporal_cols) < 2:
            continue

        df = data[table_name].copy()
        for col_name in temporal_cols:
            df[col_name] = pd.to_datetime(df[col_name], errors="coerce")

        for i in range(len(temporal_cols) - 1):
            col_a = temporal_cols[i]
            col_b = temporal_cols[i + 1]
            violations = df[
                (df[col_a].notna()) & (df[col_b].notna()) & (df[col_a] > df[col_b])
            ]
            if not violations.empty:
                issues.append(
                    {
                        "table": table_name,
                        "issue": f"{col_a} > {col_b} in {len(violations)} rows",
                    }
                )

    # Cross-table ordering via FK: child temporal should not precede parent temporal.
    for child_table in schema["tables"]:
        child_name = child_table["name"]
        if child_name not in data:
            continue

        child_temporals = _temporal_cols(child_table)
        if not child_temporals:
            continue
        child_time_col = child_temporals[0]

        child_df = data[child_name].copy()
        child_df[child_time_col] = pd.to_datetime(
            child_df[child_time_col], errors="coerce"
        )

        for col in child_table["columns"]:
            fk = col.get("foreign_key")
            if not fk:
                continue

            parent_name = fk["table"]
            parent_key = fk["column"]
            child_fk_col = col["name"]

            if parent_name not in data or parent_name not in table_defs:
                continue

            parent_temporals = _temporal_cols(table_defs[parent_name])
            if not parent_temporals:
                continue
            parent_time_col = parent_temporals[0]

            parent_df = data[parent_name][[parent_key, parent_time_col]].copy()
            parent_df[parent_time_col] = pd.to_datetime(
                parent_df[parent_time_col], errors="coerce"
            )

            merged = child_df[[child_fk_col, child_time_col]].merge(
                parent_df,
                left_on=child_fk_col,
                right_on=parent_key,
                how="left",
            )
            violations = merged[
                (merged[child_time_col].notna())
                & (merged[parent_time_col].notna())
                & (merged[child_time_col] < merged[parent_time_col])
            ]
            if not violations.empty:
                issues.append(
                    {
                        "table": child_name,
                        "issue": (
                            f"{child_time_col} < parent {parent_name}.{parent_time_col} "
                            f"in {len(violations)} rows"
                        ),
                    }
                )

    return issues


# ============================================================================
# SECTION 4: Business Logic - State Distributions
# ============================================================================


def _is_geographic_state_field(field_name: str, all_field_names: list[str]) -> bool:
    """Return True when a state-like field likely represents location."""
    if "state" not in field_name:
        return False

    location_hints = [
        "address",
        "billing",
        "shipping",
        "mailing",
        "residence",
        "location",
        "city",
        "country",
        "postal",
        "zip",
        "province",
    ]

    if any(hint in field_name for hint in location_hints):
        return True

    has_location_context = any(
        any(hint in candidate for hint in location_hints)
        for candidate in all_field_names
    )
    if field_name == "state" and has_location_context:
        return True

    if field_name.endswith("_state_code") or field_name.endswith("_state_abbr"):
        return True

    return False


def check_state_transitions(data: dict[str, pd.DataFrame], schema: dict) -> list[dict]:
    """Validate status field distributions for lifecycle/status columns."""
    issues = []

    for table in schema["tables"]:
        table_name = table["name"]
        if table_name not in data:
            continue

        all_col_names = [col["name"].lower() for col in table["columns"]]
        status_cols = []
        for col in table["columns"]:
            col_name = col["name"].lower()
            if col.get("field_role") != "categorical":
                continue
            if "status" in col_name:
                status_cols.append(col["name"])
                continue
            if "state" in col_name and not _is_geographic_state_field(
                col_name, all_col_names
            ):
                status_cols.append(col["name"])

        if not status_cols:
            continue

        df = data[table_name]
        for status_col in status_cols:
            if status_col not in df.columns:
                continue

            status_dist = df[status_col].value_counts(dropna=True)
            unique_count = len(status_dist)

            if unique_count == 1:
                issues.append(
                    {
                        "table": table_name,
                        "column": status_col,
                        "issue": f"Only one status value: {status_dist.index[0]}",
                    }
                )
            elif unique_count > 0:
                max_pct = status_dist.max() / max(len(df), 1)
                if max_pct > 0.95:
                    issues.append(
                        {
                            "table": table_name,
                            "column": status_col,
                            "issue": f"Highly skewed: {max_pct:.1%} in one status",
                        }
                    )

    return issues


# ============================================================================
# SECTION 5: Distribution and Financial Realism
# ============================================================================


def check_distributions(data: dict[str, pd.DataFrame]) -> dict:
    """Collect descriptive stats for numeric columns."""
    stats = {}

    for table_name, df in data.items():
        numeric_cols = df.select_dtypes(include=["number"]).columns
        for col in numeric_cols:
            if col.lower().endswith("_id"):
                continue

            series = pd.to_numeric(df[col], errors="coerce")
            stats[f"{table_name}.{col}"] = {
                "mean": float(series.mean()) if not series.dropna().empty else None,
                "std": float(series.std()) if not series.dropna().empty else None,
                "min": float(series.min()) if not series.dropna().empty else None,
                "max": float(series.max()) if not series.dropna().empty else None,
                "nulls": int(series.isna().sum()),
                "unique": int(series.nunique(dropna=True)),
            }

    return stats


def check_financial_realism(data: dict[str, pd.DataFrame], schema: dict) -> list[dict]:
    """Check numeric scale realism for monetary/rate/ratio fields."""
    issues = []

    monetary_tokens = [
        "income",
        "amount",
        "balance",
        "principal",
        "payment",
        "price",
        "cost",
        "fee",
        "loan",
    ]

    for table in schema["tables"]:
        table_name = table["name"]
        if table_name not in data:
            continue

        df = data[table_name]
        for col in table["columns"]:
            if col.get("field_role") != "numerical":
                continue

            col_name = col["name"]
            if col_name not in df.columns:
                continue

            lower = col_name.lower()
            series = pd.to_numeric(df[col_name], errors="coerce").dropna()
            if series.empty:
                continue

            if "ratio" in lower:
                out_of_bounds = ((series < 0) | (series > 5)).sum()
                if out_of_bounds > 0:
                    issues.append(
                        {
                            "table": table_name,
                            "column": col_name,
                            "issue": f"{int(out_of_bounds)} ratio values outside [0, 5]",
                        }
                    )
                continue

            if "rate" in lower or "yield" in lower or "interest" in lower:
                out_of_bounds = ((series < -5) | (series > 60)).sum()
                if out_of_bounds > 0:
                    issues.append(
                        {
                            "table": table_name,
                            "column": col_name,
                            "issue": f"{int(out_of_bounds)} rate values outside [-5, 60]",
                        }
                    )
                continue

            if any(token in lower for token in monetary_tokens):
                non_positive = (series <= 0).sum()
                if non_positive > 0:
                    issues.append(
                        {
                            "table": table_name,
                            "column": col_name,
                            "issue": f"{int(non_positive)} non-positive monetary values",
                        }
                    )

                if series.mean() < 100:
                    issues.append(
                        {
                            "table": table_name,
                            "column": col_name,
                            "issue": (
                                f"Suspiciously low monetary scale (mean={series.mean():.2f})"
                            ),
                        }
                    )

    return issues


def check_geographic_state_values(
    data: dict[str, pd.DataFrame], schema: dict
) -> list[dict]:
    """Ensure geographic state fields are not filled with lifecycle statuses."""
    issues = []
    status_tokens = {"active", "inactive", "pending", "closed"}

    for table in schema["tables"]:
        table_name = table["name"]
        if table_name not in data:
            continue

        all_names = [col["name"].lower() for col in table["columns"]]
        df = data[table_name]

        for col in table["columns"]:
            col_name = col["name"]
            lower = col_name.lower()
            if col_name not in df.columns:
                continue
            if not _is_geographic_state_field(lower, all_names):
                continue

            values = (
                df[col_name]
                .dropna()
                .astype(str)
                .str.strip()
                .str.lower()
                .replace("", pd.NA)
                .dropna()
            )
            if values.empty:
                continue

            bad = values[values.isin(status_tokens)]
            if not bad.empty:
                issues.append(
                    {
                        "table": table_name,
                        "column": col_name,
                        "issue": f"{len(bad)} values look like lifecycle statuses",
                    }
                )

    return issues


# ============================================================================
# SECTION 6: Semi-Structured Data Validation
# ============================================================================


def check_semi_structured(data: dict[str, pd.DataFrame], schema: dict) -> dict:
    """Validate JSON/XML semi-structured fields from schema."""
    semi_structured = {}

    for table in schema["tables"]:
        table_name = table["name"]
        if table_name not in data:
            continue

        df = data[table_name]
        semi_cols = [
            col["name"]
            for col in table["columns"]
            if col.get("field_role") == "semi_structured"
            or col.get("type", "").upper() in {"JSON", "XML"}
        ]

        for col_name in semi_cols:
            if col_name not in df.columns:
                continue

            values = df[col_name].dropna().astype(str)
            if values.empty:
                continue

            if col_name.endswith("_json") or "json" in col_name.lower():
                valid = 0
                for val in values:
                    try:
                        json.loads(val)
                        valid += 1
                    except (TypeError, json.JSONDecodeError):
                        pass
                semi_structured[f"{table_name}.{col_name}"] = {
                    "type": "JSON",
                    "valid_count": valid,
                    "total_non_null": len(values),
                    "valid_ratio": valid / len(values),
                    "sample": values.iloc[0][:200],
                }
            elif col_name.endswith("_xml") or "xml" in col_name.lower():
                valid = 0
                for val in values:
                    try:
                        ET.fromstring(val)
                        valid += 1
                    except ET.ParseError:
                        pass
                semi_structured[f"{table_name}.{col_name}"] = {
                    "type": "XML",
                    "valid_count": valid,
                    "total_non_null": len(values),
                    "valid_ratio": valid / len(values),
                    "sample": values.iloc[0][:200],
                }

    return semi_structured


# ============================================================================
# SECTION 7: Categorical Relationships
# ============================================================================


def check_categorical_relationships(data: dict[str, pd.DataFrame]) -> dict:
    """Summarize categorical column distributions."""
    categorical_stats = {}

    for table_name, df in data.items():
        cat_cols = df.select_dtypes(include=["object", "string"]).columns
        for col in cat_cols:
            if col.lower().endswith("_id"):
                continue

            value_counts = df[col].value_counts(dropna=False)
            if len(value_counts) <= 30:
                categorical_stats[f"{table_name}.{col}"] = {
                    "unique_values": int(value_counts.size),
                    "distribution": value_counts.head(20).to_dict(),
                    "most_common": value_counts.head(3).to_dict(),
                }

    return categorical_stats


# ============================================================================
# SECTION 8: Run All Validations
# ============================================================================


def run_all_validations(output_dir: str | Path = "demo_output") -> dict | None:
    """Execute all validation checks."""
    output_path = resolve_output_path(output_dir)
    schema_path = output_path / "schema.json"
    db_path = output_path / "sqlite" / "data.db"

    if not schema_path.exists():
        print(f"Schema not found: {schema_path}")
        return None
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return None

    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    print("=" * 80)
    print("ASSIGNMENT SUBMISSION VALIDATION")
    print("=" * 80)
    print(f"Schema: {schema['schema_name']}")

    print("\n[1/9] Loading data...")
    data, tables, conn = load_sqlite_data(db_path)
    print(f"Loaded {len(tables)} tables")

    print("\n[2/9] Checking FK integrity...")
    fk_issues = validate_fk_integrity(conn, schema)
    print(
        "All FK relationships valid"
        if not fk_issues
        else f"Found {len(fk_issues)} FK issues"
    )

    print("\n[3/9] Checking null constraints...")
    null_issues = validate_null_constraints(conn, schema)
    print(
        "All null constraints satisfied"
        if not null_issues
        else f"Found {len(null_issues)} null issues"
    )

    print("\n[4/9] Checking data types...")
    type_issues = validate_data_types(conn, schema)
    print(
        "All data types valid"
        if not type_issues
        else f"Found {len(type_issues)} type issues"
    )

    conn.close()

    print("\n[5/9] Checking temporal ordering...")
    temporal_issues = check_temporal_ordering(data, schema)
    print(
        "Temporal ordering valid"
        if not temporal_issues
        else f"Found {len(temporal_issues)} temporal issues"
    )

    print("\n[6/9] Checking state distributions...")
    state_issues = check_state_transitions(data, schema)
    print(
        "State distributions look reasonable"
        if not state_issues
        else f"Found {len(state_issues)} state warnings"
    )

    print("\n[7/9] Checking semi-structured data...")
    semi_struct = check_semi_structured(data, schema)
    print(
        f"Validated {len(semi_struct)} semi-structured columns"
        if semi_struct
        else "No semi-structured columns found"
    )

    print("\n[8/9] Checking financial realism...")
    financial_issues = check_financial_realism(data, schema)
    geo_state_issues = check_geographic_state_values(data, schema)
    print(
        "Financial and geographic state checks look reasonable"
        if not financial_issues and not geo_state_issues
        else (
            f"Financial warnings: {len(financial_issues)}, "
            f"Geographic state warnings: {len(geo_state_issues)}"
        )
    )

    print("\n[9/9] Analyzing distributions...")
    dist_stats = check_distributions(data)
    cat_stats = check_categorical_relationships(data)
    print(f"Analyzed {len(dist_stats)} numerical, {len(cat_stats)} categorical columns")

    critical_issues = (
        len(fk_issues) + len(null_issues) + len(type_issues) + len(temporal_issues)
    )
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Total tables: {len(tables)}")
    print(f"Total rows: {sum(len(df) for df in data.values())}")
    print("\nCritical checks:")
    print(f"  FK integrity issues: {len(fk_issues)}")
    print(f"  Null constraint issues: {len(null_issues)}")
    print(f"  Data type issues: {len(type_issues)}")
    print(f"  Temporal issues: {len(temporal_issues)}")
    print("\nWarning checks:")
    print(f"  State distribution warnings: {len(state_issues)}")
    print(f"  Financial scale warnings: {len(financial_issues)}")
    print(f"  Geographic state warnings: {len(geo_state_issues)}")
    print(f"  Semi-structured columns checked: {len(semi_struct)}")
    print(
        "\n"
        + ("PASS" if critical_issues == 0 else "FAIL")
        + f": {critical_issues} critical issues"
    )

    return {
        "data": data,
        "fk_issues": fk_issues,
        "null_issues": null_issues,
        "type_issues": type_issues,
        "temporal_issues": temporal_issues,
        "state_issues": state_issues,
        "financial_issues": financial_issues,
        "geo_state_issues": geo_state_issues,
        "dist_stats": dist_stats,
        "semi_struct": semi_struct,
        "cat_stats": cat_stats,
    }


# ============================================================================
# SECTION 9: Interactive Exploration Helper
# ============================================================================


def explore_table(data: dict[str, pd.DataFrame], table_name: str) -> pd.DataFrame:
    """Quick exploration of a specific table."""
    df = data[table_name]
    print(f"\nTable: {table_name}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print("\nFirst 5 rows:")
    print(df.head())
    print("\nData types:")
    print(df.dtypes)
    print("\nMissing values:")
    print(df.isna().sum())
    return df


# ============================================================================
# MAIN EXECUTION
# ============================================================================


if __name__ == "__main__":
    output_dir_arg = sys.argv[1] if len(sys.argv) > 1 else "demo_output"
    results = run_all_validations(output_dir_arg)

    if results:
        report = {
            "timestamp": datetime.now().isoformat(),
            "fk_issues": results["fk_issues"],
            "null_issues": results["null_issues"],
            "type_issues": results["type_issues"],
            "temporal_issues": results["temporal_issues"],
            "state_issues": results["state_issues"],
            "financial_issues": results["financial_issues"],
            "geo_state_issues": results["geo_state_issues"],
            "semi_structured_columns": list(results["semi_struct"].keys()),
            "categorical_columns": list(results["cat_stats"].keys()),
        }

        report_path = resolve_output_path(output_dir_arg) / "validation_report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"\nValidation report saved to {report_path}")

        total_critical = (
            len(results["fk_issues"])
            + len(results["null_issues"])
            + len(results["type_issues"])
            + len(results["temporal_issues"])
        )
        sys.exit(1 if total_critical > 0 else 0)
