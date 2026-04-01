"""Schema-agnostic pytest tests for validating generated data."""
import pandas as pd
import sqlite3
import json
from pathlib import Path
import pytest

OUTPUT_DIR = Path("demo_output")
DB_PATH = OUTPUT_DIR / "sqlite" / "data.db"
SCHEMA_PATH = OUTPUT_DIR / "schema.json"

@pytest.fixture
def schema():
    with open(SCHEMA_PATH) as f:
        return json.load(f)

@pytest.fixture
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    yield conn
    conn.close()

class TestFKIntegrity:
    """Test foreign key relationships are valid."""

    def test_all_fk_references_valid(self, db_conn, schema):
        """Verify all FK values point to existing parent records."""
        for table in schema["tables"]:
            for col in table["columns"]:
                if col.get("foreign_key"):
                    fk = col["foreign_key"]
                    query = f"""
                    SELECT COUNT(*) FROM {table["name"]} child
                    LEFT JOIN {fk["table"]} parent ON child.{col["name"]} = parent.{fk["column"]}
                    WHERE child.{col["name"]} IS NOT NULL AND parent.{fk["column"]} IS NULL
                    """
                    cursor = db_conn.execute(query)
                    invalid = cursor.fetchone()[0]
                    assert invalid == 0, f"Found {invalid} invalid FK references in {table['name']}.{col['name']}"

class TestNullConstraints:
    """Test non-nullable columns have no NULLs."""

    def test_non_nullable_columns(self, db_conn, schema):
        """Check all non-nullable columns contain no NULL values."""
        for table in schema["tables"]:
            non_nullable = [col["name"] for col in table["columns"] if not col["nullable"]]
            if non_nullable:
                conditions = " OR ".join([f"{col} IS NULL" for col in non_nullable])
                query = f"SELECT COUNT(*) FROM {table['name']} WHERE {conditions}"
                cursor = db_conn.execute(query)
                null_count = cursor.fetchone()[0]
                assert null_count == 0, f"Found {null_count} NULL values in non-nullable columns of {table['name']}"

class TestDataTypes:
    """Test data type consistency."""

    def test_numeric_columns(self, db_conn, schema):
        """Ensure numeric columns contain only numeric values."""
        for table in schema["tables"]:
            for col in table["columns"]:
                if col["type"] in ["INTEGER", "NUMERIC", "REAL"]:
                    query = f"""
                    SELECT COUNT(*) FROM {table['name']}
                    WHERE {col['name']} IS NOT NULL
                    AND TYPEOF({col['name']}) NOT IN ('integer', 'real')
                    """
                    cursor = db_conn.execute(query)
                    invalid = cursor.fetchone()[0]
                    assert invalid == 0, f"Found {invalid} non-numeric values in {table['name']}.{col['name']}"

class TestBusinessLogic:
    """Test domain-specific business rules."""

    def test_positive_amounts(self, db_conn, schema):
        """Check amount/balance columns are positive."""
        for table in schema["tables"]:
            for col in table["columns"]:
                if any(keyword in col["name"].lower() for keyword in ["amount", "balance", "income", "salary"]):
                    if col["type"] in ["INTEGER", "NUMERIC", "REAL"]:
                        query = f"SELECT COUNT(*) FROM {table['name']} WHERE {col['name']} < 0"
                        cursor = db_conn.execute(query)
                        negative = cursor.fetchone()[0]
                        assert negative == 0, f"Found {negative} negative values in {table['name']}.{col['name']}"

    def test_semi_structured_json_valid(self, db_conn, schema):
        """Validate JSON columns contain valid JSON."""
        for table in schema["tables"]:
            for col in table["columns"]:
                if col.get("field_role") == "json" or "json" in col["name"].lower():
                    query = f"SELECT {col['name']} FROM {table['name']} WHERE {col['name']} IS NOT NULL"
                    cursor = db_conn.execute(query)
                    for row in cursor.fetchall():
                        try:
                            json.loads(row[0])
                        except json.JSONDecodeError:
                            pytest.fail(f"Invalid JSON in {table['name']}.{col['name']}: {row[0][:100]}")

class TestDataQuality:
    """Test data quality and distributions."""

    def test_tables_not_empty(self, db_conn, schema):
        """Ensure all tables have data."""
        for table in schema["tables"]:
            cursor = db_conn.execute(f"SELECT COUNT(*) FROM {table['name']}")
            count = cursor.fetchone()[0]
            assert count > 0, f"{table['name']} should not be empty"

    def test_temporal_ordering(self, db_conn, schema):
        """Check date columns follow logical sequence."""
        for table in schema["tables"]:
            temporal_cols = [col["name"] for col in table["columns"] if col.get("field_role") == "temporal"]
            if len(temporal_cols) >= 2:
                for i in range(len(temporal_cols) - 1):
                    query = f"""
                    SELECT COUNT(*) FROM {table['name']}
                    WHERE {temporal_cols[i]} IS NOT NULL
                    AND {temporal_cols[i+1]} IS NOT NULL
                    AND {temporal_cols[i]} > {temporal_cols[i+1]}
                    """
                    cursor = db_conn.execute(query)
                    violations = cursor.fetchone()[0]
                    assert violations == 0, f"Found {violations} temporal ordering violations in {table['name']}"
