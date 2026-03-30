"""Field value generation using field_role and config distributions."""

import json
import random
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any

from faker import Faker


def generate_identifier(
    col: dict, rng: random.Random, fake: Faker, row_idx: int
) -> Any:
    """Generate identifier values (PK, unique IDs)."""
    col_name = col["name"].lower()

    if "uuid" in col_name or "guid" in col_name:
        return fake.uuid4()

    # Auto-increment integer IDs
    return row_idx + 1


def generate_numerical(
    col: dict,
    config_dist: dict | None,
    rng: random.Random,
    fake: Faker,
) -> float | int:
    """Generate numerical values from config or semantic defaults."""
    if config_dist:
        dist_type = config_dist.get("distribution", "normal")
        params = config_dist.get("params", {})

        if dist_type == "normal":
            mean = params.get("mean", 0)
            std = params.get("std", 1)
            return rng.gauss(mean, std)
        elif dist_type == "lognormal":
            mean = params.get("mean", 1)
            sigma = params.get("sigma", 0.5)
            return rng.lognormvariate(mean, sigma)
        elif dist_type == "uniform":
            low = params.get("low", 0)
            high = params.get("high", 100)
            return rng.uniform(low, high)

    # Semantic defaults
    col_name = col["name"].lower()

    if "score" in col_name or "rating" in col_name:
        return rng.gauss(650, 100)
    elif "rate" in col_name or "yield" in col_name or "interest" in col_name:
        return rng.gauss(5.5, 3.0)
    elif "amount" in col_name or "balance" in col_name or "principal" in col_name:
        return rng.lognormvariate(10, 0.8)
    elif "age" in col_name:
        return int(rng.gauss(35, 12))
    elif "quantity" in col_name or "count" in col_name:
        return int(rng.lognormvariate(2, 0.5))

    return rng.gauss(50, 20)


def generate_categorical(
    col: dict,
    config_dist: dict | None,
    rng: random.Random,
    fake: Faker,
) -> str:
    """Generate categorical values from config or semantic defaults."""
    if config_dist:
        dist_type = config_dist.get("distribution", "categorical")
        params = config_dist.get("params", {})

        if dist_type == "categorical" or dist_type == "choice":
            categories = params.get("categories", [])
            weights = params.get("weights")
            if categories:
                return rng.choices(categories, weights=weights, k=1)[0]

    # Semantic defaults
    col_name = col["name"].lower()

    if "status" in col_name or "state" in col_name:
        return rng.choices(
            ["Active", "Inactive", "Pending", "Closed"],
            weights=[70, 10, 12, 8],
            k=1
        )[0]
    elif "segment" in col_name:
        return rng.choices(
            ["Mass", "Affluent", "Premium"],
            weights=[60, 30, 10],
            k=1
        )[0]
    elif "risk" in col_name:
        return rng.choices(
            ["Low", "Medium", "High"],
            weights=[60, 30, 10],
            k=1
        )[0]
    elif "type" in col_name:
        return rng.choices(
            ["Standard", "Premium", "Enterprise"],
            weights=[60, 30, 10],
            k=1
        )[0]
    elif "currency" in col_name:
        return rng.choice(["USD", "EUR", "GBP", "JPY"])
    elif "country" in col_name:
        return fake.country_code()

    return rng.choice(["A", "B", "C"])


def generate_text(
    col: dict,
    rng: random.Random,
    fake: Faker,
) -> str:
    """Generate text values using Faker."""
    col_name = col["name"].lower()

    if "first" in col_name and "name" in col_name:
        return fake.first_name()
    elif "last" in col_name and "name" in col_name:
        return fake.last_name()
    elif "name" in col_name:
        return fake.name()
    elif "email" in col_name:
        return fake.email()
    elif "phone" in col_name:
        return fake.phone_number()
    elif "address" in col_name:
        return fake.address().replace("\n", ", ")
    elif "city" in col_name:
        return fake.city()
    elif "company" in col_name:
        return fake.company()
    elif "description" in col_name or "comment" in col_name:
        return fake.sentence()

    return fake.word()


def generate_temporal(
    col: dict,
    config_dist: dict | None,
    rng: random.Random,
    fake: Faker,
    simulation_start: str | None = None,
    simulation_end: str | None = None,
    temporal_anchor: datetime | None = None,
) -> datetime:
    """Generate temporal values."""
    col_name = col["name"].lower()

    # Parse simulation dates
    start_dt = datetime.fromisoformat(simulation_start) if simulation_start else datetime(2020, 1, 1)
    end_dt = datetime.fromisoformat(simulation_end) if simulation_end else datetime(2024, 12, 31)

    # Use anchor for relative dates
    if temporal_anchor:
        if "end" in col_name or "close" in col_name or "maturity" in col_name:
            days_offset = rng.randint(30, 365)
            return temporal_anchor + timedelta(days=days_offset)
        elif "due" in col_name or "scheduled" in col_name:
            days_offset = rng.randint(1, 90)
            return temporal_anchor + timedelta(days=days_offset)

    # Birth dates
    if "birth" in col_name or "dob" in col_name:
        age_years = int(rng.gauss(35, 12))
        return datetime.now() - timedelta(days=age_years * 365)

    # Random date in simulation range
    delta = end_dt - start_dt
    random_days = rng.randint(0, delta.days)
    return start_dt + timedelta(days=random_days)


def generate_semi_structured(
    col: dict,
    rng: random.Random,
    fake: Faker,
) -> str:
    """Generate JSON or XML semi-structured data."""
    col_name = col["name"].lower()
    col_type = col.get("type", "JSON").upper()

    if col_type == "JSON":
        # Semantic JSON generation
        if "preference" in col_name or "settings" in col_name:
            return json.dumps({
                "language": rng.choice(["en", "es", "fr", "de"]),
                "notifications": rng.choice([True, False]),
                "theme": rng.choice(["light", "dark"])
            })
        elif "risk" in col_name and "model" in col_name:
            return json.dumps({
                "score": round(rng.gauss(650, 100), 2),
                "tier": rng.choice(["Low", "Medium", "High"]),
                "factors": [fake.word() for _ in range(rng.randint(2, 4))]
            })
        elif "metadata" in col_name or "data" in col_name:
            return json.dumps({
                "key1": fake.word(),
                "key2": rng.randint(1, 100),
                "key3": rng.choice([True, False])
            })

        # Generic JSON
        return json.dumps({"value": fake.word(), "count": rng.randint(1, 10)})

    elif col_type == "XML":
        # Generate well-formed XML
        root = ET.Element("data")
        ET.SubElement(root, "field1").text = fake.word()
        ET.SubElement(root, "field2").text = str(rng.randint(1, 100))
        return ET.tostring(root, encoding="unicode")

    return "{}"


def generate_boolean(
    col: dict,
    rng: random.Random,
) -> bool:
    """Generate boolean values."""
    col_name = col["name"].lower()

    if "active" in col_name or "enabled" in col_name:
        return rng.choices([True, False], weights=[80, 20], k=1)[0]
    elif "default" in col_name:
        return rng.choices([True, False], weights=[20, 80], k=1)[0]

    return rng.choice([True, False])


def inherit_from_parent(
    col: dict,
    parent_profile: dict | None,
) -> Any | None:
    """Inherit value from parent if semantic match exists."""
    if not parent_profile:
        return None

    col_name = col["name"].lower()

    # Direct matches
    if col_name in parent_profile:
        return parent_profile[col_name]

    # Semantic matches
    for parent_key, parent_val in parent_profile.items():
        parent_key_lower = parent_key.lower()

        # Currency, country, segment inheritance
        if "currency" in col_name and "currency" in parent_key_lower:
            return parent_val
        elif "country" in col_name and "country" in parent_key_lower:
            return parent_val
        elif "segment" in col_name and "segment" in parent_key_lower:
            return parent_val

    return None


def generate_field_value(
    col: dict,
    config_dist: dict | None,
    rng: random.Random,
    fake: Faker,
    row_idx: int,
    parent_profile: dict | None = None,
    simulation_start: str | None = None,
    simulation_end: str | None = None,
    temporal_anchor: datetime | None = None,
) -> Any:
    """Generate a field value based on field_role and config."""
    field_role = col.get("field_role", "text")

    # Try inheritance first for non-identifier fields
    if field_role != "identifier" and parent_profile:
        inherited = inherit_from_parent(col, parent_profile)
        if inherited is not None and rng.random() < 0.65:
            return inherited

    # Generate by field_role
    if field_role == "identifier":
        return generate_identifier(col, rng, fake, row_idx)
    elif field_role == "numerical":
        return generate_numerical(col, config_dist, rng, fake)
    elif field_role == "categorical":
        return generate_categorical(col, config_dist, rng, fake)
    elif field_role == "text":
        return generate_text(col, rng, fake)
    elif field_role == "temporal":
        return generate_temporal(col, config_dist, rng, fake, simulation_start, simulation_end, temporal_anchor)
    elif field_role == "semi_structured":
        return generate_semi_structured(col, rng, fake)
    elif field_role == "boolean":
        return generate_boolean(col, rng)

    return None
