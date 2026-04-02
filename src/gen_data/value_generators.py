"""Field value generation using field_role and config distributions.

Provides both batch column generators (for Polars integration) and
single-value generators (for event row fallback).
"""

import json
import random
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any

from faker import Faker


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def add_business_days(start_date: datetime, days: int) -> datetime:
    """Add business days to a date, skipping weekends."""
    current = start_date
    while days > 0:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Monday=0, Sunday=6
            days -= 1
    return current


# ---------------------------------------------------------------------------
# Batch column generators — return list[T] of `count` values
# ---------------------------------------------------------------------------


def generate_identifier_column(
    col: dict, count: int, rng: random.Random, fake: Faker
) -> list:
    """Batch-generate identifier values."""
    col_name = col["name"].lower()
    if "uuid" in col_name or "guid" in col_name:
        return [fake.uuid4() for _ in range(count)]
    return list(range(1, count + 1))


def generate_numerical_column(
    col: dict, config_dist: dict | None, count: int, rng: random.Random
) -> list[float]:
    """Batch-generate numerical values from config or semantic defaults."""
    if config_dist:
        dist_type = config_dist.get("distribution", "normal")
        params = config_dist.get("params", {})

        if dist_type == "normal":
            mean, std = params.get("mean", 0), params.get("std", 1)
            return [rng.gauss(mean, std) for _ in range(count)]
        elif dist_type == "lognormal":
            mu, sigma = params.get("mu", 2), params.get("sigma", 0.5)
            return [rng.lognormvariate(mu, sigma) for _ in range(count)]
        elif dist_type == "uniform":
            low, high = params.get("low", 0), params.get("high", 100)
            return [rng.uniform(low, high) for _ in range(count)]

    # Semantic defaults: column name patterns drive distribution choice
    # This allows schema-driven generation without explicit config for every field
    col_name = col["name"].lower()
    if "score" in col_name or "rating" in col_name:
        return [rng.gauss(650, 100) for _ in range(count)]  # Credit score range
    elif "rate" in col_name or "yield" in col_name or "interest" in col_name:
        return [rng.gauss(5.5, 3.0) for _ in range(count)]  # Percentage rates
    elif "income" in col_name or "salary" in col_name or "revenue" in col_name or "earnings" in col_name:
        return [round(rng.lognormvariate(11.0, 0.6), 2) for _ in range(count)]  # Annual income ~$60k median
    elif "limit" in col_name or "credit_limit" in col_name:
        return [round(rng.lognormvariate(9.5, 0.7), 2) for _ in range(count)]  # Credit limit ~$13k median
    elif "amount" in col_name or "balance" in col_name or "principal" in col_name or "payment" in col_name or "price" in col_name or "cost" in col_name or "fee" in col_name:
        return [round(rng.lognormvariate(6.5, 1.0), 2) for _ in range(count)]  # General monetary ~$665 median
    elif "age" in col_name:
        return [int(rng.gauss(35, 12)) for _ in range(count)]  # Adult age distribution
    elif "quantity" in col_name or "count" in col_name:
        return [int(rng.lognormvariate(2, 0.5)) for _ in range(count)]  # Small positive integers

    return [round(rng.gauss(50, 20), 2) for _ in range(count)]


def generate_risk_from_segment(segment: str, rng: random.Random) -> str:
    """Generate correlated risk based on segment."""
    risk_map = {
        "Retail": (["Low", "Medium", "High"], [50, 35, 15]),
        "Mass Affluent": (["Low", "Medium", "High"], [70, 25, 5]),
        "HNW": (["Low", "Medium", "High"], [85, 13, 2]),
    }
    categories, weights = risk_map.get(segment, (["Low", "Medium", "High"], [60, 30, 10]))
    return rng.choices(categories, weights=weights)[0]


def generate_categorical_column(
    col: dict, config_dist: dict | None, count: int, rng: random.Random, fake: Faker
) -> list[str]:
    """Batch-generate categorical values from config or semantic defaults."""
    if config_dist:
        params = config_dist.get("params", {})
        categories = params.get("categories", [])
        weights = params.get("weights")
        if categories:
            return rng.choices(categories, weights=weights, k=count)

    col_name = col["name"].lower()
    if "gender" in col_name or "sex" in col_name:
        return rng.choices(["Male", "Female"], weights=[50, 50], k=count)
    elif "marital" in col_name:
        return rng.choices(["Single", "Married", "Divorced", "Widowed"], weights=[35, 45, 15, 5], k=count)
    elif "employment" in col_name:
        return rng.choices(["Employed", "Self-Employed", "Unemployed", "Retired", "Student"], weights=[55, 20, 10, 10, 5], k=count)
    elif "status" in col_name or "state" in col_name:
        return rng.choices(["Active", "Inactive", "Pending", "Closed"], weights=[70, 10, 12, 8], k=count)
    elif "profile" in col_name or "behavior" in col_name:
        return rng.choices(["Conservative", "Moderate", "Aggressive"], weights=[50, 35, 15], k=count)
    elif "segment" in col_name:
        return rng.choices(["Retail", "Mass Affluent", "HNW"], weights=[80, 15, 5], k=count)
    elif "risk" in col_name:
        return rng.choices(["Low", "Medium", "High"], weights=[60, 30, 10], k=count)
    elif "type" in col_name:
        return rng.choices(["Standard", "Premium", "Enterprise"], weights=[60, 30, 10], k=count)
    elif "currency" in col_name:
        return rng.choices(["USD", "EUR", "GBP", "JPY"], k=count)
    elif "country" in col_name:
        return [fake.country_code() for _ in range(count)]

    return rng.choices(["A", "B", "C"], k=count)


def generate_text_column(
    col: dict, count: int, fake: Faker
) -> list[str]:
    """Batch-generate text values using Faker (inherently sequential)."""
    col_name = col["name"].lower()

    if "first" in col_name and "name" in col_name:
        return [fake.first_name() for _ in range(count)]
    elif "last" in col_name and "name" in col_name:
        return [fake.last_name() for _ in range(count)]
    elif "name" in col_name:
        return [fake.name() for _ in range(count)]
    elif "email" in col_name:
        return [fake.email() for _ in range(count)]
    elif "phone" in col_name:
        return [fake.phone_number() for _ in range(count)]
    elif "address" in col_name:
        return [fake.address().replace("\n", ", ") for _ in range(count)]
    elif "city" in col_name:
        return [fake.city() for _ in range(count)]
    elif "company" in col_name:
        return [fake.company() for _ in range(count)]
    elif "description" in col_name or "comment" in col_name:
        return [fake.sentence() for _ in range(count)]

    return [fake.word() for _ in range(count)]


def generate_temporal_column(
    col: dict,
    count: int,
    rng: random.Random,
    simulation_start: str | None = None,
    simulation_end: str | None = None,
    anchor_series: list[datetime] | None = None,
    parent_temporal: list | None = None,
) -> list[datetime]:
    """Batch-generate temporal values with two types of constraints.

    Args:
        anchor_series: Per-row anchor from an earlier column in the same table
            (e.g. start_date → used to generate end_date as start_date + offset).
            This creates within-row temporal relationships.
        parent_temporal: Per-row floor from the parent table's date column
            (e.g. account_open_date → ensures transaction_date ≥ account_open_date).
            This enforces parent-child temporal ordering.
    """
    col_name = col["name"].lower()

    start_dt = datetime.fromisoformat(simulation_start) if simulation_start else datetime(2020, 1, 1)
    end_dt = datetime.fromisoformat(simulation_end) if simulation_end else datetime(2024, 12, 31)

    # Birth dates — not affected by parent floor
    if "birth" in col_name or "dob" in col_name:
        now = datetime.now()
        return [now - timedelta(days=int(rng.gauss(35, 12)) * 365) for _ in range(count)]

    # Relative dates anchored within the same row (e.g., end_date = start_date + offset)
    if anchor_series is not None:
        if "settlement" in col_name:
            # T+2 business days for settlement dates
            return [add_business_days(a, 2) for a in anchor_series]
        elif "end" in col_name or "close" in col_name or "maturity" in col_name:
            return [a + timedelta(days=rng.randint(30, 365)) for a in anchor_series]
        elif "due" in col_name or "scheduled" in col_name:
            return [a + timedelta(days=rng.randint(1, 90)) for a in anchor_series]

    # Use parent temporal as a per-row floor so child dates ≥ parent dates
    # Example: transaction_date must be >= account_open_date
    delta_days = (end_dt - start_dt).days
    results = []
    for i in range(count):
        floor = None
        if parent_temporal is not None and i < len(parent_temporal):
            pt = parent_temporal[i]
            if pt is not None:
                floor = pt if isinstance(pt, datetime) else datetime.combine(pt, datetime.min.time())
        effective_start = max(floor, start_dt) if floor else start_dt
        effective_delta = max((end_dt - effective_start).days, 0)
        results.append(effective_start + timedelta(days=rng.randint(0, effective_delta) if effective_delta > 0 else 0))
    return results


def generate_semi_structured_column(
    col: dict, count: int, rng: random.Random, fake: Faker
) -> list[str]:
    """Batch-generate JSON or XML semi-structured data."""
    col_name = col["name"].lower()
    col_type = col.get("type", "JSON").upper()

    results = []
    for _ in range(count):
        if col_type == "XML":
            results.append(_generate_xml(col_name, rng, fake))
        elif "preference" in col_name or "settings" in col_name:
            results.append(json.dumps({
                "language": rng.choice(["en", "es", "fr", "de"]),
                "notifications": rng.choice([True, False]),
                "theme": rng.choice(["light", "dark"]),
                "marketing_opt_in": rng.choice([True, False]),
            }))
        elif "risk" in col_name and "model" in col_name:
            results.append(json.dumps({
                "score": round(rng.gauss(650, 100), 2),
                "tier": rng.choice(["Low", "Medium", "High"]),
                "factors": [fake.word() for _ in range(rng.randint(2, 4))],
                "model_version": f"v{rng.randint(1, 5)}.{rng.randint(0, 9)}",
            }))
        elif "address" in col_name or "location" in col_name:
            results.append(json.dumps({
                "street": fake.street_address(),
                "city": fake.city(),
                "country": fake.country_code(),
                "postal_code": fake.postcode(),
            }))
        elif "metadata" in col_name or "attribute" in col_name or "data" in col_name:
            results.append(json.dumps({
                "source": rng.choice(["web", "mobile", "branch", "api"]),
                "created_by": fake.user_name(),
                "tags": [fake.word() for _ in range(rng.randint(1, 3))],
            }))
        else:
            results.append(json.dumps({
                "value": fake.word(),
                "count": rng.randint(1, 10),
                "active": rng.choice([True, False]),
            }))
    return results


def _generate_xml(col_name: str, rng: random.Random, fake: Faker) -> str:
    """Generate domain-aware XML based on column name semantics."""
    if "transaction" in col_name or "payment" in col_name:
        root = ET.Element("transaction")
        ET.SubElement(root, "amount", currency=rng.choice(["USD", "EUR", "GBP"])).text = str(round(rng.lognormvariate(6, 1), 2))
        ET.SubElement(root, "channel").text = rng.choice(["online", "branch", "mobile", "atm"])
        ET.SubElement(root, "reference").text = fake.bothify("TXN-????-########")
    elif "risk" in col_name or "score" in col_name:
        root = ET.Element("risk_assessment")
        ET.SubElement(root, "score").text = str(round(rng.gauss(650, 100), 1))
        ET.SubElement(root, "grade").text = rng.choice(["A", "B", "C", "D"])
        factors = ET.SubElement(root, "factors")
        for _ in range(rng.randint(1, 3)):
            ET.SubElement(factors, "factor").text = fake.word()
    elif "profile" in col_name or "customer" in col_name:
        root = ET.Element("profile")
        ET.SubElement(root, "segment").text = rng.choice(["Retail", "Affluent", "Premium"])
        ET.SubElement(root, "channel").text = rng.choice(["online", "branch", "referral"])
        ET.SubElement(root, "since").text = str(rng.randint(2010, 2024))
    elif "order" in col_name or "trade" in col_name:
        root = ET.Element("order_details")
        ET.SubElement(root, "instrument").text = fake.lexify("????").upper()
        ET.SubElement(root, "quantity").text = str(rng.randint(1, 10000))
        ET.SubElement(root, "price").text = str(round(rng.uniform(1, 500), 2))
    else:
        root = ET.Element("data")
        ET.SubElement(root, "id").text = fake.bothify("??-######")
        ET.SubElement(root, "value").text = fake.word()
        ET.SubElement(root, "status").text = rng.choice(["active", "inactive", "pending"])
    return ET.tostring(root, encoding="unicode")


def generate_boolean_column(
    col: dict, count: int, rng: random.Random
) -> list[bool]:
    """Batch-generate boolean values."""
    col_name = col["name"].lower()
    if "active" in col_name or "enabled" in col_name:
        return rng.choices([True, False], weights=[80, 20], k=count)
    elif "default" in col_name:
        return rng.choices([True, False], weights=[20, 80], k=count)
    return rng.choices([True, False], k=count)


# ---------------------------------------------------------------------------
# Column dispatch — used by data_generator to build one column at a time
# ---------------------------------------------------------------------------


def generate_column(
    col: dict,
    config_dist: dict | None,
    count: int,
    rng: random.Random,
    fake: Faker,
    simulation_start: str | None = None,
    simulation_end: str | None = None,
    anchor_series: list[datetime] | None = None,
    parent_temporal: list | None = None,
) -> list:
    """Dispatch to the appropriate batch generator based on field_role."""
    role = col.get("field_role", "text")

    if role == "identifier":
        return generate_identifier_column(col, count, rng, fake)
    elif role == "numerical":
        return generate_numerical_column(col, config_dist, count, rng)
    elif role == "categorical":
        return generate_categorical_column(col, config_dist, count, rng, fake)
    elif role == "text":
        return generate_text_column(col, count, fake)
    elif role == "temporal":
        return generate_temporal_column(
            col, count, rng, simulation_start, simulation_end, anchor_series, parent_temporal
        )
    elif role == "semi_structured":
        return generate_semi_structured_column(col, count, rng, fake)
    elif role == "boolean":
        return generate_boolean_column(col, count, rng)

    return [None] * count


# ---------------------------------------------------------------------------
# Inheritance helpers — for joining parent values into child DataFrames
# ---------------------------------------------------------------------------


def find_inheritable_field(col: dict, parent_columns: list[str]) -> str | None:
    """Find a parent column that semantically matches this child column.

    Uses two strategies:
    1. Exact name match (e.g., child "currency" → parent "currency")
    2. Semantic token match (e.g., child "account_currency" → parent "currency")

    This enables value inheritance where child records copy parent values via FK joins,
    maintaining consistency (e.g., all transactions inherit account's currency).

    Returns the parent column name, or None.
    """
    col_name = col["name"].lower()

    # Direct exact match
    if col["name"] in parent_columns:
        return col["name"]

    # Semantic token match — token must appear as a whole word (bounded by _ or string edges)
    # to avoid false matches like "state" matching "address_state".
    import re
    tokens = ["currency", "country", "segment", "risk", "type", "channel", "status", "grade"]
    for token in tokens:
        pattern = r"(^|_)" + token + r"(_|$)"
        if re.search(pattern, col_name):
            for pcol in parent_columns:
                if re.search(pattern, pcol.lower()):
                    return pcol
    return None
