"""Data generation orchestrator."""

import random
from datetime import datetime
from pathlib import Path
from typing import Any

from faker import Faker

from .event_emitter import calculate_lambda, sample_event_count, should_emit_events
from .state_machine import apply_state_machine
from .value_generators import generate_field_value


def generate_data(
    schema: dict,
    config: dict,
    writers: list,
    seed: int | None = None,
) -> dict[str, int]:
    """Generate synthetic data from schema and config.

    Args:
        schema: JSON schema from gen_schema
        config: Scenario config from gen_config
        writers: List of writer instances (CSV, SQLite, Parquet, Delta)
        seed: Random seed for reproducibility

    Returns:
        Dictionary of table_name: row_count
    """
    # Initialize state
    seed = seed or config.get("seed", 42)
    rng = random.Random(seed)
    fake = Faker()
    fake.seed_instance(seed)

    state = {
        "pk_values": {},  # {table_name: [pk_values]}
        "pk_profiles": {},  # {table_name: {pk: {field: value}}}
        "rng": rng,
        "fake": fake,
    }

    tables_by_name = {t["name"]: t for t in schema["tables"]}
    generation_order = config["generation_order"]
    table_counts = config["table_counts"]
    entities = config.get("entities", {})
    state_machines = config.get("state_machines", {})
    events = config.get("events", {})
    simulation = config.get("simulation", {})

    row_counts = {}

    # Generate entity tables
    for table_name in generation_order:
        if table_name not in tables_by_name:
            continue

        table = tables_by_name[table_name]
        count = table_counts.get(table_name, 100)

        # Skip event tables (generated later)
        if table_name in events:
            continue

        # Start table in all writers
        for writer in writers:
            writer.start_table(table)

        # Generate rows
        for row_idx in range(count):
            row = _generate_row(
                table, row_idx, entities, state_machines, state, simulation
            )

            # Write row
            for writer in writers:
                writer.write_row(row)

            # Store PK and profile
            _store_row_state(table, row, state)

        # End table in all writers
        for writer in writers:
            writer.end_table()

        row_counts[table_name] = count

    # Generate event tables
    for event_table_name, event_config in events.items():
        if event_table_name not in tables_by_name:
            continue

        table = tables_by_name[event_table_name]

        # Start table in all writers
        for writer in writers:
            writer.start_table(table)

        event_count = _emit_events(
            table, event_config, state, entities, simulation, writers
        )

        # End table in all writers
        for writer in writers:
            writer.end_table()

        row_counts[event_table_name] = event_count

    # Close all writers
    for writer in writers:
        writer.close()

    return row_counts


def _generate_row(
    table: dict,
    row_idx: int,
    entities: dict,
    state_machines: dict,
    state: dict,
    simulation: dict,
) -> dict[str, Any]:
    """Generate a single row for a table."""
    row = {}
    rng = state["rng"]
    fake = state["fake"]
    table_name = table["name"]
    entity_config = entities.get(table_name, {})
    field_configs = entity_config.get("fields", {})

    # Get parent profile if FK exists
    parent_profile = None
    temporal_anchor = None

    # First pass: Generate PK and FK
    for col in table["columns"]:
        col_name = col["name"]

        if col.get("primary_key"):
            row[col_name] = generate_field_value(
                col, None, rng, fake, row_idx
            )
        elif col.get("foreign_key"):
            fk = col["foreign_key"]
            parent_table = fk["table"]
            if parent_table in state["pk_values"]:
                parent_pks = state["pk_values"][parent_table]
                if parent_pks:
                    parent_pk = rng.choice(parent_pks)
                    row[col_name] = parent_pk
                    # Get parent profile
                    if parent_table in state["pk_profiles"]:
                        parent_profile = state["pk_profiles"][parent_table].get(parent_pk, {})

    # Second pass: Generate other fields
    for col in table["columns"]:
        col_name = col["name"]

        if col_name in row:
            continue

        config_dist = field_configs.get(col_name)
        sim_start = simulation.get("start_date")
        sim_end = simulation.get("end_date")

        value = generate_field_value(
            col, config_dist, rng, fake, row_idx,
            parent_profile, sim_start, sim_end, temporal_anchor
        )

        row[col_name] = value

        # Use first temporal field as anchor
        if temporal_anchor is None and col.get("field_role") == "temporal":
            if isinstance(value, datetime):
                temporal_anchor = value

    # Apply state machine if configured
    if table_name in state_machines:
        sm = state_machines[table_name]
        state_field = sm["state_field"]
        if state_field in row:
            row[state_field] = apply_state_machine(sm, row, rng)

    # Apply nullability
    for col in table["columns"]:
        col_name = col["name"]
        if col.get("nullable", True) and not col.get("primary_key"):
            if rng.random() < 0.05:  # 5% null rate
                row[col_name] = None

    return row


def _store_row_state(table: dict, row: dict, state: dict) -> None:
    """Store PK and profile for FK sampling and inheritance."""
    table_name = table["name"]

    # Find PK column
    pk_col = None
    for col in table["columns"]:
        if col.get("primary_key"):
            pk_col = col["name"]
            break

    if not pk_col or pk_col not in row:
        return

    pk_value = row[pk_col]

    # Store PK value
    if table_name not in state["pk_values"]:
        state["pk_values"][table_name] = []
    state["pk_values"][table_name].append(pk_value)

    # Store compact profile (key fields for inheritance)
    if table_name not in state["pk_profiles"]:
        state["pk_profiles"][table_name] = {}

    profile = {}
    for col in table["columns"]:
        col_name = col["name"]
        field_role = col.get("field_role")
        # Store categorical, numerical, and temporal fields
        if field_role in ["categorical", "numerical", "temporal"] and col_name in row:
            profile[col_name] = row[col_name]

    state["pk_profiles"][table_name][pk_value] = profile


def _emit_events(
    table: dict,
    event_config: dict,
    state: dict,
    entities: dict,
    simulation: dict,
    writers: list,
) -> int:
    """Emit events for a parent entity table."""
    parent_table = event_config["emitted_by"]
    emit_when_states = event_config.get("emit_when_states", [])
    frequency = event_config.get("frequency", {})
    lambda_base = frequency.get("lambda_base", 1.0)
    lambda_modifiers = frequency.get("lambda_modifiers", [])

    rng = state["rng"]
    fake = state["fake"]
    table_name = table["name"]
    entity_config = entities.get(table_name, {})
    field_configs = entity_config.get("fields", {})

    # Get parent state field if state machine exists
    parent_state_field = None
    if parent_table in state.get("pk_profiles", {}):
        for pk, profile in state["pk_profiles"][parent_table].items():
            for key in profile.keys():
                if "status" in key.lower() or "state" in key.lower():
                    parent_state_field = key
                    break
            break

    event_count = 0
    parent_pks = state["pk_values"].get(parent_table, [])

    for parent_pk in parent_pks:
        parent_profile = state["pk_profiles"].get(parent_table, {}).get(parent_pk, {})

        # Check if parent state allows emission
        if parent_state_field and parent_state_field in parent_profile:
            parent_state = parent_profile[parent_state_field]
            if not should_emit_events(parent_state, emit_when_states):
                continue

        # Calculate lambda
        lambda_val = calculate_lambda(lambda_base, lambda_modifiers, parent_profile)

        # Sample event count
        num_events = sample_event_count(lambda_val, rng)

        # Generate events
        for i in range(num_events):
            row = _generate_event_row(
                table, i, parent_table, parent_pk, parent_profile,
                field_configs, simulation, state
            )

            for writer in writers:
                writer.write_row(row)

            event_count += 1

    return event_count


def _generate_event_row(
    table: dict,
    row_idx: int,
    parent_table: str,
    parent_pk: Any,
    parent_profile: dict,
    field_configs: dict,
    simulation: dict,
    state: dict,
) -> dict[str, Any]:
    """Generate a single event row."""
    row = {}
    rng = state["rng"]
    fake = state["fake"]
    temporal_anchor = None

    # First pass: PK and FK
    for col in table["columns"]:
        col_name = col["name"]

        if col.get("primary_key"):
            row[col_name] = generate_field_value(col, None, rng, fake, row_idx)
        elif col.get("foreign_key"):
            fk = col["foreign_key"]
            if fk["table"] == parent_table:
                row[col_name] = parent_pk

    # Second pass: other fields
    for col in table["columns"]:
        col_name = col["name"]

        if col_name in row:
            continue

        config_dist = field_configs.get(col_name)
        sim_start = simulation.get("start_date")
        sim_end = simulation.get("end_date")

        value = generate_field_value(
            col, config_dist, rng, fake, row_idx,
            parent_profile, sim_start, sim_end, temporal_anchor
        )

        row[col_name] = value

        if temporal_anchor is None and col.get("field_role") == "temporal":
            if isinstance(value, datetime):
                temporal_anchor = value

    return row


