"""Example-based tests for gen_data module.

These tests serve as executable documentation, demonstrating how the
data generation system works with real (but minimal) data.
"""

import random
from datetime import datetime

import polars as pl
import pytest
from faker import Faker

from gen_data.event_emitter import calculate_lambdas_batch, sample_event_counts_batch
from gen_data.state_machine import apply_state_machine, apply_adjustments
from gen_data.value_generators import (
    generate_numerical_column,
    generate_temporal_column,
    find_inheritable_field,
)


def test_semantic_field_generation():
    """Demonstrate how semantic field patterns drive generation logic."""
    rng = random.Random(42)

    # Test 1: "score" field generates credit score range (mean=650, std=100)
    score_col = {"name": "credit_score", "field_role": "numerical"}
    scores = generate_numerical_column(score_col, None, 5, rng)
    assert all(400 < s < 900 for s in scores), "Scores should be in credit score range"

    # Test 2: "amount" field generates lognormal distribution (right-skewed)
    amount_col = {"name": "transaction_amount", "field_role": "numerical"}
    amounts = generate_numerical_column(amount_col, None, 5, rng)
    assert all(a > 0 for a in amounts), "Amounts should be positive"
    assert max(amounts) > sum(amounts) / len(amounts), "Should be right-skewed"

    # Test 3: "age" field generates adult ages (mean=35, std=12)
    age_col = {"name": "customer_age", "field_role": "numerical"}
    ages = generate_numerical_column(age_col, None, 5, rng)
    assert all(10 < a < 90 for a in ages), "Ages should be in reasonable range"


def test_temporal_anchor_within_row():
    """Demonstrate within-row temporal anchoring: end_date relative to start_date."""
    rng = random.Random(42)

    # Generate start dates
    start_col = {"name": "start_date", "field_role": "temporal"}
    start_dates = generate_temporal_column(
        start_col, 3, rng,
        simulation_start="2024-01-01",
        simulation_end="2024-12-31"
    )

    # Generate end dates anchored to start dates
    end_col = {"name": "end_date", "field_role": "temporal"}
    end_dates = generate_temporal_column(
        end_col, 3, rng,
        simulation_start="2024-01-01",
        simulation_end="2024-12-31",
        anchor_series=start_dates  # Key: anchor to start_dates
    )

    # Verify: end_date > start_date for all rows
    for start, end in zip(start_dates, end_dates):
        assert end > start, f"End date {end} should be after start date {start}"
        # Should be 30-365 days later based on "end" pattern
        delta_days = (end - start).days
        assert 30 <= delta_days <= 365, f"Delta should be 30-365 days, got {delta_days}"


def test_temporal_floor_from_parent():
    """Demonstrate parent temporal floor: child dates >= parent dates."""
    rng = random.Random(42)

    # Parent dates (e.g., account_open_date)
    parent_dates = [
        datetime(2024, 1, 15),
        datetime(2024, 3, 20),
        datetime(2024, 6, 10),
    ]

    # Generate child dates (e.g., transaction_date) with parent floor
    child_col = {"name": "transaction_date", "field_role": "temporal"}
    child_dates = generate_temporal_column(
        child_col, 3, rng,
        simulation_start="2024-01-01",
        simulation_end="2024-12-31",
        parent_temporal=parent_dates  # Key: parent floor constraint
    )

    # Verify: child_date >= parent_date for all rows
    for parent, child in zip(parent_dates, child_dates):
        assert child >= parent, f"Child date {child} should be >= parent date {parent}"


def test_value_inheritance():
    """Demonstrate FK-based value inheritance: child copies parent values."""
    # Simulate parent DataFrame (e.g., Account table)
    parent_df_columns = ["account_id", "currency", "segment"]

    # Child column that should inherit from parent
    child_col = {"name": "transaction_currency", "field_role": "categorical"}

    # Test semantic matching: "transaction_currency" should match "currency"
    match = find_inheritable_field(child_col, parent_df_columns)
    assert match == "currency", "Should match 'currency' via semantic token"

    # Test exact match
    child_col_exact = {"name": "currency", "field_role": "categorical"}
    match_exact = find_inheritable_field(child_col_exact, parent_df_columns)
    assert match_exact == "currency", "Should match 'currency' exactly"


def test_state_machine_adjustments():
    """Demonstrate probability modulation based on row features."""
    rng = random.Random(42)

    # Base transition probabilities
    base_probs = {"Approved": 0.7, "Declined": 0.3}

    # Row with low risk score
    low_risk_row = {"risk_score": 600}
    adjustments = {
        "Declined": [{"field": "risk_score", "direction": "higher_increases", "strength": "strong"}]
    }
    adjusted_low = apply_adjustments(base_probs, low_risk_row, adjustments, {"risk_score": (300, 850)})

    # Row with high risk score
    high_risk_row = {"risk_score": 800}
    adjusted_high = apply_adjustments(base_probs, high_risk_row, adjustments, {"risk_score": (300, 850)})

    # Verify: higher risk score increases "Declined" probability
    assert adjusted_high["Declined"] > adjusted_low["Declined"], \
        "Higher risk should increase Declined probability"


def test_event_emission_poisson():
    """Demonstrate Poisson-based event generation with lambda modifiers."""
    # Create parent DataFrame with varying balance values
    parent_df = pl.DataFrame({
        "account_id": [1, 2, 3],
        "balance": [1000.0, 50000.0, 100000.0],  # Low, medium, high
    })

    # Calculate lambdas with balance modifier
    lambda_base = 2.0
    lambda_modifiers = [{"field": "balance", "effect": "higher_increases"}]
    lambdas = calculate_lambdas_batch(lambda_base, lambda_modifiers, parent_df)

    # Verify: higher balance increases lambda
    assert lambdas[0] < lambdas[1] < lambdas[2], \
        "Lambda should increase with balance"

    # Sample event counts
    event_counts = sample_event_counts_batch(lambdas, rng_seed=42)
    assert len(event_counts) == 3, "Should have one count per parent"
    assert all(c >= 0 for c in event_counts), "Counts should be non-negative"
