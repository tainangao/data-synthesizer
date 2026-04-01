"""
Comprehensive validation script for assignment submission.
Combines structural validation (FK, nulls, types) with business logic checks.
Copy code snippets to Jupyter notebook for interactive validation.
"""

import sqlite3
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# ============================================================================
# SECTION 1: Load Data
# ============================================================================

def load_sqlite_data(db_path="demo_output/sqlite/data.db"):
    """Load all tables from SQLite database."""
    conn = sqlite3.connect(db_path)

    # Get all table names
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table'",
        conn
    )['name'].tolist()

    data = {}
    for table in tables:
        data[table] = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        print(f"Loaded {table}: {len(data[table])} rows")

    conn.close()
    return data, tables, sqlite3.connect(db_path)


# ============================================================================
# SECTION 2: Structural Validation (Schema Compliance)
# ============================================================================

def validate_fk_integrity(conn, schema):
    """Verify all FK references point to existing parent records."""
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
                        'table': table["name"],
                        'column': col["name"],
                        'issue': f"{invalid} orphaned FK values",
                        'ref_table': fk["table"]
                    })
    return issues

def validate_null_constraints(conn, schema):
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
                        'table': table["name"],
                        'column': col["name"],
                        'issue': f"{null_count} NULL values in non-nullable column"
                    })
    return issues

def validate_data_types(conn, schema):
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
                        'table': table["name"],
                        'column': col["name"],
                        'issue': f"{invalid} non-numeric values"
                    })
    return issues


# ============================================================================
# SECTION 3: Business Logic - Temporal Ordering
# ============================================================================

def check_temporal_ordering(data, schema_path="demo_output/schema.json"):
    """Check date field ordering constraints from schema."""
    with open(schema_path) as f:
        schema = json.load(f)

    issues = []

    for table in schema['tables']:
        table_name = table['name']
        if table_name not in data:
            continue

        df = data[table_name].copy()

        # Find temporal columns from schema
        temporal_cols = [
            col['name'] for col in table['columns']
            if col.get('field_role') == 'temporal'
        ]

        if len(temporal_cols) >= 2:
            # Convert to datetime
            for col in temporal_cols:
                df[col] = pd.to_datetime(df[col], errors='coerce')

            # Check sequential ordering
            for i in range(len(temporal_cols) - 1):
                col1, col2 = temporal_cols[i], temporal_cols[i+1]
                violations = df[(df[col1].notna()) & (df[col2].notna()) & (df[col1] > df[col2])]

                if len(violations) > 0:
                    issues.append({
                        'table': table_name,
                        'issue': f"{col1} > {col2} in {len(violations)} rows"
                    })

    return issues


# ============================================================================
# SECTION 4: Business Logic - State Transitions
# ============================================================================

def check_state_transitions(data, schema_path="demo_output/schema.json"):
    """Validate status field distributions are realistic."""
    with open(schema_path) as f:
        schema = json.load(f)

    issues = []

    for table in schema['tables']:
        table_name = table['name']
        if table_name not in data:
            continue

        df = data[table_name]

        # Find categorical status columns
        status_cols = [
            col['name'] for col in table['columns']
            if col.get('field_role') == 'categorical' and
            ('status' in col['name'].lower() or 'state' in col['name'].lower())
        ]

        for status_col in status_cols:
            if status_col not in df.columns:
                continue

            status_dist = df[status_col].value_counts()
            unique_count = len(status_dist)

            # Check for reasonable variety
            if unique_count == 1:
                issues.append({
                    'table': table_name,
                    'column': status_col,
                    'issue': f'Only one status value: {status_dist.index[0]}'
                })
            elif unique_count > 0:
                # Check if distribution is too uniform or too skewed
                max_pct = status_dist.max() / len(df)
                if max_pct > 0.95:
                    issues.append({
                        'table': table_name,
                        'column': status_col,
                        'issue': f'Highly skewed: {max_pct:.1%} in one status'
                    })

    return issues


# ============================================================================
# SECTION 5: Distribution Realism
# ============================================================================

def check_distributions(data):
    """Check if numerical distributions look realistic."""
    stats = {}

    for table_name, df in data.items():
        numeric_cols = df.select_dtypes(include=['number']).columns

        for col in numeric_cols:
            if col.lower().endswith('_id'):
                continue  # Skip ID columns

            stats[f"{table_name}.{col}"] = {
                'mean': df[col].mean(),
                'std': df[col].std(),
                'min': df[col].min(),
                'max': df[col].max(),
                'nulls': df[col].isna().sum(),
                'unique': df[col].nunique()
            }

    return stats


# ============================================================================
# SECTION 6: Semi-Structured Data Validation
# ============================================================================

def check_semi_structured(data, schema_path="demo_output/schema.json"):
    """Validate JSON/XML/text fields from schema."""
    with open(schema_path) as f:
        schema = json.load(f)

    semi_structured = {}

    for table in schema['tables']:
        table_name = table['name']
        if table_name not in data:
            continue

        df = data[table_name]

        # Find semi-structured columns from schema
        semi_cols = [
            col['name'] for col in table['columns']
            if col.get('field_role') in ['json', 'xml', 'semi_structured']
        ]

        for col in semi_cols:
            if col not in df.columns:
                continue

            sample = df[col].dropna().head(1)
            if len(sample) > 0:
                val = str(sample.iloc[0])

                # Validate JSON
                if col.endswith('_json') or 'json' in col.lower():
                    try:
                        json.loads(val)
                        semi_structured[f"{table_name}.{col}"] = {
                            'type': 'JSON',
                            'valid': True,
                            'sample': val[:200]
                        }
                    except:
                        semi_structured[f"{table_name}.{col}"] = {
                            'type': 'JSON',
                            'valid': False,
                            'sample': val[:200]
                        }
                # Validate XML
                elif col.endswith('_xml') or 'xml' in col.lower():
                    semi_structured[f"{table_name}.{col}"] = {
                        'type': 'XML',
                        'valid': val.startswith('<'),
                        'sample': val[:200]
                    }

    return semi_structured


# ============================================================================
# SECTION 7: Categorical Relationships
# ============================================================================

def check_categorical_relationships(data):
    """Check if categorical fields have realistic distributions."""
    categorical_stats = {}

    for table_name, df in data.items():
        # Identify categorical columns (both object and string dtypes)
        cat_cols = df.select_dtypes(include=['object', 'string']).columns

        for col in cat_cols:
            if col.lower().endswith('_id'):
                continue

            value_counts = df[col].value_counts()

            if len(value_counts) <= 20:  # Reasonable number of categories
                categorical_stats[f"{table_name}.{col}"] = {
                    'unique_values': len(value_counts),
                    'distribution': value_counts.to_dict(),
                    'most_common': value_counts.head(3).to_dict()
                }

    return categorical_stats


# ============================================================================
# SECTION 8: Run All Validations
# ============================================================================

def run_all_validations(output_dir="demo_output"):
    """Execute all validation checks."""
    output_path = Path(output_dir)
    schema_path = output_path / "schema.json"
    db_path = output_path / "sqlite" / "data.db"

    if not schema_path.exists():
        print(f"❌ Schema not found: {schema_path}")
        return None

    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return None

    with open(schema_path) as f:
        schema = json.load(f)

    print("=" * 80)
    print("ASSIGNMENT SUBMISSION VALIDATION")
    print("=" * 80)
    print(f"Schema: {schema['schema_name']}")

    # Load data
    print("\n[1/8] Loading data...")
    data, tables, conn = load_sqlite_data(str(db_path))
    print(f"✓ Loaded {len(tables)} tables")

    # Structural validations
    print("\n[2/8] Checking FK integrity...")
    fk_issues = validate_fk_integrity(conn, schema)
    if fk_issues:
        print(f"✗ Found {len(fk_issues)} FK integrity issues:")
        for issue in fk_issues[:5]:
            print(f"  - {issue}")
    else:
        print("✓ All FK relationships valid")

    print("\n[3/8] Checking null constraints...")
    null_issues = validate_null_constraints(conn, schema)
    if null_issues:
        print(f"✗ Found {len(null_issues)} null constraint violations:")
        for issue in null_issues[:5]:
            print(f"  - {issue}")
    else:
        print("✓ All null constraints satisfied")

    print("\n[4/8] Checking data types...")
    type_issues = validate_data_types(conn, schema)
    if type_issues:
        print(f"✗ Found {len(type_issues)} data type issues:")
        for issue in type_issues[:5]:
            print(f"  - {issue}")
    else:
        print("✓ All data types valid")

    conn.close()

    # Business logic validations
    print("\n[5/8] Checking temporal ordering...")
    temporal_issues = check_temporal_ordering(data, schema_path)
    if temporal_issues:
        print(f"✗ Found {len(temporal_issues)} temporal ordering issues:")
        for issue in temporal_issues[:5]:
            print(f"  - {issue}")
    else:
        print("✓ Temporal ordering valid")

    print("\n[6/8] Checking state distributions...")
    state_issues = check_state_transitions(data, schema_path)
    if state_issues:
        print(f"⚠ Found {len(state_issues)} distribution warnings:")
        for issue in state_issues[:5]:
            print(f"  - {issue}")
    else:
        print("✓ State distributions look reasonable")

    print("\n[7/8] Checking semi-structured data...")
    semi_struct = check_semi_structured(data, schema_path)
    if semi_struct:
        print(f"✓ Found {len(semi_struct)} semi-structured columns:")
        for col, info in list(semi_struct.items())[:3]:
            status = '✓' if info.get('valid', True) else '✗'
            print(f"  {status} {col}: {info['type']}")
    else:
        print("⚠ No semi-structured data found")

    print("\n[8/8] Analyzing distributions...")
    dist_stats = check_distributions(data)
    cat_stats = check_categorical_relationships(data)
    print(f"✓ Analyzed {len(dist_stats)} numerical, {len(cat_stats)} categorical columns")

    # Summary
    total_issues = len(fk_issues) + len(null_issues) + len(type_issues) + len(temporal_issues)
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Total tables: {len(tables)}")
    print(f"Total rows: {sum(len(df) for df in data.values())}")
    print(f"\nStructural Issues:")
    print(f"  FK integrity: {len(fk_issues)}")
    print(f"  Null constraints: {len(null_issues)}")
    print(f"  Data types: {len(type_issues)}")
    print(f"\nBusiness Logic:")
    print(f"  Temporal ordering: {len(temporal_issues)}")
    print(f"  State warnings: {len(state_issues)}")
    print(f"  Semi-structured columns: {len(semi_struct)}")
    print(f"\n{'✅ PASS' if total_issues == 0 else '❌ FAIL'}: {total_issues} critical issues found")

    return {
        'data': data,
        'fk_issues': fk_issues,
        'null_issues': null_issues,
        'type_issues': type_issues,
        'temporal_issues': temporal_issues,
        'state_issues': state_issues,
        'dist_stats': dist_stats,
        'semi_struct': semi_struct,
        'cat_stats': cat_stats
    }


# ============================================================================
# SECTION 9: Interactive Exploration (for Jupyter)
# ============================================================================

def explore_table(data, table_name):
    """Quick exploration of a specific table."""
    df = data[table_name]
    print(f"\nTable: {table_name}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print(f"\nFirst 5 rows:")
    print(df.head())
    print(f"\nData types:")
    print(df.dtypes)
    print(f"\nMissing values:")
    print(df.isna().sum())
    return df


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "demo_output"
    results = run_all_validations(output_dir)

    if results:
        # Save validation report
        report = {
            'timestamp': datetime.now().isoformat(),
            'fk_issues': results['fk_issues'],
            'null_issues': results['null_issues'],
            'type_issues': results['type_issues'],
            'temporal_issues': results['temporal_issues'],
            'state_issues': results['state_issues'],
            'semi_structured_columns': list(results['semi_struct'].keys()),
            'categorical_columns': list(results['cat_stats'].keys())
        }

        report_path = Path(output_dir) / "validation_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"\n✓ Validation report saved to {report_path}")

        # Exit with error code if critical issues found
        total_issues = len(results['fk_issues']) + len(results['null_issues']) + len(results['type_issues']) + len(results['temporal_issues'])
        sys.exit(1 if total_issues > 0 else 0)
