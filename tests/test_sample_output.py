"""Pytest tests for validating sample output data."""
import pandas as pd
import sqlite3
import json
from pathlib import Path
import pytest

OUTPUT_DIR = Path("demo_output")
CSV_DIR = OUTPUT_DIR / "csv"
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

    def test_creditapplications_customer_fk(self, db_conn):
        cursor = db_conn.execute("""
            SELECT COUNT(*) FROM CreditApplications ca
            LEFT JOIN Customers c ON ca.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """)
        assert cursor.fetchone()[0] == 0, "Invalid customer_id references in CreditApplications"

    def test_creditscores_customer_fk(self, db_conn):
        cursor = db_conn.execute("""
            SELECT COUNT(*) FROM CreditScores cs
            LEFT JOIN Customers c ON cs.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """)
        assert cursor.fetchone()[0] == 0, "Invalid customer_id references in CreditScores"

class TestNullConstraints:
    """Test non-nullable columns have no NULLs."""

    def test_customers_required_fields(self, db_conn):
        cursor = db_conn.execute("""
            SELECT COUNT(*) FROM Customers
            WHERE customer_id IS NULL OR first_name IS NULL OR last_name IS NULL
            OR date_of_birth IS NULL OR email IS NULL OR nationality IS NULL
            OR employment_status IS NULL OR annual_income IS NULL
        """)
        assert cursor.fetchone()[0] == 0, "Required fields in Customers contain NULLs"

    def test_creditapplications_required_fields(self, db_conn):
        cursor = db_conn.execute("""
            SELECT COUNT(*) FROM CreditApplications
            WHERE application_id IS NULL OR customer_id IS NULL OR application_date IS NULL
            OR loan_type IS NULL OR requested_amount IS NULL OR loan_term_months IS NULL
            OR loan_purpose IS NULL OR application_status IS NULL OR debt_to_income_ratio IS NULL
        """)
        assert cursor.fetchone()[0] == 0, "Required fields in CreditApplications contain NULLs"

class TestBusinessLogic:
    """Test domain-specific business rules."""

    def test_positive_amounts(self, db_conn):
        cursor = db_conn.execute("SELECT COUNT(*) FROM CreditApplications WHERE requested_amount <= 0")
        assert cursor.fetchone()[0] == 0, "Loan amounts must be positive"

    def test_positive_income(self, db_conn):
        cursor = db_conn.execute("SELECT COUNT(*) FROM Customers WHERE annual_income <= 0")
        assert cursor.fetchone()[0] == 0, "Annual income must be positive"

    def test_credit_score_range(self, db_conn):
        cursor = db_conn.execute("SELECT COUNT(*) FROM CreditScores WHERE score_value < 300 OR score_value > 850")
        assert cursor.fetchone()[0] == 0, "Credit scores should be in range 300-850"

    def test_debt_to_income_ratio(self, db_conn):
        cursor = db_conn.execute("SELECT COUNT(*) FROM CreditApplications WHERE debt_to_income_ratio < 0 OR debt_to_income_ratio > 1")
        assert cursor.fetchone()[0] == 0, "Debt-to-income ratio should be between 0 and 1"

    def test_loan_term_positive(self, db_conn):
        cursor = db_conn.execute("SELECT COUNT(*) FROM CreditApplications WHERE loan_term_months <= 0")
        assert cursor.fetchone()[0] == 0, "Loan term must be positive"

class TestDataQuality:
    """Test data quality and distributions."""

    def test_tables_not_empty(self, db_conn):
        for table in ["Customers", "CreditApplications", "CreditScores"]:
            cursor = db_conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            assert count > 0, f"{table} should not be empty"

    def test_semi_structured_json_valid(self, db_conn):
        cursor = db_conn.execute("SELECT additional_data FROM CreditApplications WHERE additional_data IS NOT NULL")
        for row in cursor.fetchall():
            try:
                json.loads(row[0])
            except json.JSONDecodeError:
                pytest.fail(f"Invalid JSON in additional_data: {row[0]}")
