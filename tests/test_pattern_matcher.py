"""Tests for scenario-specific pattern matching rules."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.gen_config.pattern_matcher import build_credit_config, find_status_field


def test_credit_config_debt_to_income_ratio_distribution():
    """Debt-to-income ratio should be modeled as a bounded ratio, not income."""
    schema = {
        "tables": [
            {
                "name": "LoanApplications",
                "columns": [
                    {
                        "name": "application_id",
                        "type": "INTEGER",
                        "field_role": "identifier",
                    },
                    {
                        "name": "application_status",
                        "type": "TEXT",
                        "field_role": "categorical",
                    },
                    {
                        "name": "debt_to_income_ratio",
                        "type": "NUMERIC",
                        "field_role": "numerical",
                    },
                    {
                        "name": "annual_income",
                        "type": "NUMERIC",
                        "field_role": "numerical",
                    },
                ],
            }
        ]
    }

    mapping = build_credit_config(schema)
    distributions = mapping["key_distributions"]["LoanApplications"]

    dti = distributions["debt_to_income_ratio"]
    assert dti["distribution"] == "normal"
    assert dti["params"]["mean"] < 1

    income = distributions["annual_income"]
    assert income["distribution"] == "lognormal"
    assert income["params"]["mean"] == 60000


def test_find_status_field_skips_geographic_state():
    """Address state should not be interpreted as lifecycle state."""
    table = {
        "name": "Customers",
        "columns": [
            {"name": "customer_id", "type": "INTEGER", "field_role": "identifier"},
            {"name": "state", "type": "TEXT", "field_role": "categorical"},
            {"name": "country", "type": "TEXT", "field_role": "categorical"},
            {"name": "city", "type": "TEXT", "field_role": "categorical"},
        ],
    }

    assert find_status_field(table) is None


def test_find_status_field_prefers_explicit_status():
    """Explicit *_status fields should be selected over location fields."""
    table = {
        "name": "Accounts",
        "columns": [
            {"name": "address_state", "type": "TEXT", "field_role": "categorical"},
            {"name": "country", "type": "TEXT", "field_role": "categorical"},
            {"name": "account_status", "type": "TEXT", "field_role": "categorical"},
        ],
    }

    assert find_status_field(table) == "account_status"
