"""Minimal test cases for debugging _generate_entity_table and _generate_event_table."""

import random
from datetime import datetime

import polars as pl
from faker import Faker

from gen_data.data_generator import _generate_entity_table, _generate_event_table


def test_generate_entity_table():
    """Debug entity table generation with minimal parent-child relationship."""
    rng = random.Random(42)
    fake = Faker()
    fake.seed_instance(42)

    # Parent table schema
    parent_table = {
        "name": "customers",
        "columns": [
            {"name": "customer_id", "type": "integer", "primary_key": True, "field_role": "identifier"},
            {"name": "name", "type": "text", "field_role": "text"},
            {"name": "created_at", "type": "date", "field_role": "temporal"},
        ]
    }

    state = {"pk_values": {}, "table_dfs": {}}

    # Generate parent
    parent_df = _generate_entity_table(
        table=parent_table,
        count=3,
        entities={},
        state_machines={},
        constraints=[],
        state=state,
        simulation={"start_date": "2024-01-01", "end_date": "2024-12-31"},
        rng=rng,
        fake=fake,
    )

    print("\n=== PARENT TABLE (customers) ===")
    print(parent_df)
    print(f"Shape: {parent_df.shape}")

    # Store parent state
    state["pk_values"]["customers"] = parent_df["customer_id"].to_list()
    state["table_dfs"]["customers"] = parent_df

    # Child table schema with FK
    child_table = {
        "name": "accounts",
        "columns": [
            {"name": "account_id", "type": "integer", "primary_key": True, "field_role": "identifier"},
            {"name": "customer_id", "type": "integer", "foreign_key": {"table": "customers", "column": "customer_id"}},
            {"name": "balance", "type": "decimal", "field_role": "numerical"},
            {"name": "opened_at", "type": "date", "field_role": "temporal"},
        ]
    }

    # Generate child
    child_df = _generate_entity_table(
        table=child_table,
        count=5,
        entities={},
        state_machines={},
        constraints=[],
        state=state,
        simulation={"start_date": "2024-01-01", "end_date": "2024-12-31"},
        rng=rng,
        fake=fake,
    )

    print("\n=== CHILD TABLE (accounts) ===")
    print(child_df)
    print(f"Shape: {child_df.shape}")

    assert len(parent_df) == 3
    assert len(child_df) == 5
    assert all(cid in state["pk_values"]["customers"] for cid in child_df["customer_id"].to_list())


def test_generate_event_table():
    """Debug event table generation with Poisson emission."""
    rng = random.Random(42)
    fake = Faker()
    fake.seed_instance(42)

    # Parent entity with status
    parent_df = pl.DataFrame({
        "account_id": [1, 2, 3],
        "status": ["active", "active", "closed"],
        "opened_at": [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1)],
    })

    state = {
        "pk_values": {"accounts": [1, 2, 3]},
        "table_dfs": {"accounts": parent_df},
    }

    # Event table schema
    event_table = {
        "name": "transactions",
        "columns": [
            {"name": "transaction_id", "type": "integer", "primary_key": True, "field_role": "identifier"},
            {"name": "account_id", "type": "integer", "foreign_key": {"table": "accounts", "column": "account_id"}},
            {"name": "amount", "type": "decimal", "field_role": "numerical"},
            {"name": "transaction_date", "type": "date", "field_role": "temporal"},
        ]
    }

    # Event config
    event_config = {
        "emitted_by": "accounts",
        "emit_when_states": ["active"],
        "frequency": {
            "lambda_base": 2.0,
            "lambda_modifiers": [],
        }
    }

    # Generate events
    event_df = _generate_event_table(
        table=event_table,
        event_config=event_config,
        entities={},
        constraints=[],
        state=state,
        simulation={"start_date": "2024-01-01", "end_date": "2024-12-31"},
        rng=rng,
        fake=fake,
        state_machines={},
    )

    print("\n=== PARENT TABLE (accounts) ===")
    print(parent_df)

    print("\n=== EVENT TABLE (transactions) ===")
    print(event_df)
    print(f"Shape: {event_df.shape}")

    # Only active accounts should emit events
    assert all(aid in [1, 2] for aid in event_df["account_id"].to_list())
    assert 3 not in event_df["account_id"].to_list()
