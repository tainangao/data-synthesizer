"""Test business day calculation for T+2 settlement dates."""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.gen_data.value_generators import add_business_days


def test_add_business_days_no_weekend():
    """Monday + 2 business days = Wednesday."""
    monday = datetime(2026, 3, 30)  # Monday
    result = add_business_days(monday, 2)
    assert result == datetime(2026, 4, 1)  # Wednesday


def test_add_business_days_over_weekend():
    """Thursday + 2 business days = Monday (skips weekend)."""
    thursday = datetime(2026, 4, 2)  # Thursday
    result = add_business_days(thursday, 2)
    assert result == datetime(2026, 4, 6)  # Monday


def test_add_business_days_from_friday():
    """Friday + 2 business days = Tuesday (skips weekend)."""
    friday = datetime(2026, 4, 3)  # Friday
    result = add_business_days(friday, 2)
    assert result == datetime(2026, 4, 7)  # Tuesday


def test_add_business_days_zero():
    """Adding 0 business days returns the same date."""
    date = datetime(2026, 4, 1)
    result = add_business_days(date, 0)
    assert result == date
