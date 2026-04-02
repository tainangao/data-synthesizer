"""Schema-agnostic validation script for generated data."""
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Any
import sys

def load_schema(schema_path: Path) -> Dict:
    with open(schema_path) as f:
        return json.load(f)

def validate_fk_integrity(conn: sqlite3.Connection, schema: Dict) -> List[Dict]:
    """Verify all foreign key references point to existing parent records."""
    issues = []
    for table in schema["tables"]:
        for col in table["columns"]:
            if col.get("foreign_key"):
                fk = col["foreign_key"]
                query = f"""
                SELECT COUNT(*) as invalid_count
                FROM {table["name"]} child
                LEFT JOIN {fk["table"]} parent ON child.{col["name"]} = parent.{fk["column"]}
                WHERE child.{col["name"]} IS NOT NULL AND parent.{fk["column"]} IS NULL
                """
                cursor = conn.execute(query)
                invalid = cursor.fetchone()[0]
                if invalid > 0:
                    issues.append({
                        "type": "fk_integrity",
                        "table": table["name"],
                        "column": col["name"],
                        "invalid_count": invalid,
                        "message": f"Found {invalid} invalid FK references"
                    })
    return issues

def validate_null_constraints(conn: sqlite3.Connection, schema: Dict) -> List[Dict]:
    """Check non-nullable columns contain no NULL values."""
    issues = []
    for table in schema["tables"]:
        for col in table["columns"]:
            if not col["nullable"]:
                query = f"SELECT COUNT(*) FROM {table['name']} WHERE {col['name']} IS NULL"
                cursor = conn.execute(query)
                null_count = cursor.fetchone()[0]
                if null_count > 0:
                    issues.append({
                        "type": "null_constraint",
                        "table": table["name"],
                        "column": col["name"],
                        "null_count": null_count,
                        "message": f"Non-nullable column has {null_count} NULL values"
                    })
    return issues

def validate_data_types(conn: sqlite3.Connection, schema: Dict) -> List[Dict]:
    """Ensure numeric columns contain only numeric values."""
    issues = []
    for table in schema["tables"]:
        for col in table["columns"]:
            if col["type"] in ["INTEGER", "NUMERIC", "REAL"]:
                query = f"""
                SELECT COUNT(*) 
                FROM {table['name']}
                WHERE {col['name']} IS NOT NULL
                AND TYPEOF({col['name']}) NOT IN ('integer', 'real')
                """
                cursor = conn.execute(query)
                invalid = cursor.fetchone()[0]
                if invalid > 0:
                    issues.append({
                        "type": "data_type",
                        "table": table["name"],
                        "column": col["name"],
                        "invalid_count": invalid,
                        "message": f"Expected numeric type, found {invalid} invalid values"
                    })
    return issues

def validate_temporal_ordering(conn: sqlite3.Connection, schema: Dict) -> List[Dict]:
    """Verify date/timestamp columns follow logical chronological order within each row."""
    issues = []
    for table in schema["tables"]:
        date_cols = [c["name"] for c in table["columns"]
                     if c["type"] in ["DATE", "TIMESTAMP"] and c["field_role"] == "temporal"]

        if len(date_cols) >= 2:
            for i in range(len(date_cols) - 1):
                query = f"""
                SELECT COUNT(*) 
                FROM {table['name']}
                WHERE {date_cols[i]} IS NOT NULL
                AND {date_cols[i+1]} IS NOT NULL
                AND {date_cols[i]} > {date_cols[i+1]}
                """
                cursor = conn.execute(query)
                violations = cursor.fetchone()[0]
                if violations > 0:
                    issues.append({
                        "type": "temporal_ordering",
                        "table": table["name"],
                        "columns": [date_cols[i], date_cols[i+1]],
                        "violation_count": violations,
                        "message": f"{date_cols[i]} should be <= {date_cols[i+1]}, found {violations} violations"
                    })
    return issues

def main():
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent.parent / "demo_output"
    schema_path = output_dir / "schema.json"
    db_path = output_dir / "sqlite" / "data.db"

    if not schema_path.exists():
        print(f"❌ Schema not found: {schema_path}")
        return 1

    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return 1

    schema = load_schema(schema_path)
    conn = sqlite3.connect(db_path)

    print(f"🔍 Validating output in {output_dir}")
    print(f"Schema: {schema['schema_name']}\n")

    all_issues = []
    all_issues.extend(validate_fk_integrity(conn, schema))
    all_issues.extend(validate_null_constraints(conn, schema))
    all_issues.extend(validate_data_types(conn, schema))
    all_issues.extend(validate_temporal_ordering(conn, schema))

    conn.close()

    if not all_issues:
        print("✅ All validations passed!")
        return 0
    else:
        print(f"❌ Found {len(all_issues)} issues:\n")
        for issue in all_issues:
            print(f"  [{issue['type']}] {issue['table']}: {issue['message']}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
