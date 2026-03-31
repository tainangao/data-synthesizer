"""Data generation orchestrator using Polars for batch column generation."""

import logging
import random
from datetime import datetime
from typing import Any

import numpy as np
import polars as pl
from faker import Faker

from .event_emitter import (
    calculate_lambdas_batch,
    filter_eligible_parents,
    sample_event_counts_batch,
)
from .state_machine import apply_state_machine_batch
from .value_generators import find_inheritable_field, generate_column

logger = logging.getLogger(__name__)


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
    seed = seed or config.get("seed", 42)
    rng = random.Random(seed)
    fake = Faker()
    fake.seed_instance(seed)

    tables_by_name = {t["name"]: t for t in schema["tables"]}
    generation_order = config["generation_order"]
    table_counts = config["table_counts"]
    entities = config.get("entities", {})
    state_machines = config.get("state_machines", {})
    events = config.get("events", {})
    simulation = config.get("simulation", {})

    # State shared across tables for FK sampling and inheritance
    state: dict[str, Any] = {
        "pk_values": {},          # {table: [pk_values]}
        "table_dfs": {},          # {table: pl.DataFrame}  — for joins / inheritance
    }

    row_counts: dict[str, int] = {}

    # ── Phase 1: Entity tables ──────────────────────────────────────────
    for table_name in generation_order:
        if table_name not in tables_by_name or table_name in events:
            continue

        table = tables_by_name[table_name]
        count = table_counts.get(table_name, 100)

        logger.info(f"Generating {count} rows for entity table: {table_name}")
        df = _generate_entity_table(table, count, entities, state_machines, state, simulation, rng, fake)

        # Store PK pool and DataFrame for child tables
        _store_table_state(table, df, state)

        # Write to all formats
        for writer in writers:
            writer.write_dataframe(table, df)

        row_counts[table_name] = len(df)

    # ── Phase 2: Event tables ───────────────────────────────────────────
    for event_table_name, event_config in events.items():
        if event_table_name not in tables_by_name:
            continue

        table = tables_by_name[event_table_name]

        logger.info(f"Generating events for table: {event_table_name}")
        df = _generate_event_table(table, event_config, entities, state, simulation, rng, fake)

        for writer in writers:
            writer.write_dataframe(table, df)

        row_counts[event_table_name] = len(df)

    # Close all writers
    for writer in writers:
        writer.close()

    return row_counts


# ── Entity table generation ─────────────────────────────────────────────


def _generate_entity_table(
    table: dict,
    count: int,
    entities: dict,
    state_machines: dict,
    state: dict,
    simulation: dict,
    rng: random.Random,
    fake: Faker,
) -> pl.DataFrame:
    """Generate a full entity table as a Polars DataFrame."""
    table_name = table["name"]
    field_configs = entities.get(table_name, {}).get("fields", {})
    sim_start = simulation.get("start_date")
    sim_end = simulation.get("end_date")

    columns: dict[str, list] = {}
    temporal_anchor: list[datetime] | None = None

    # ── Pass 1: PK and FK columns ───────────────────────────────────
    for col in table["columns"]:
        col_name = col["name"]

        if col.get("primary_key"):
            columns[col_name] = generate_column(col, None, count, rng, fake)

        elif col.get("foreign_key"):
            fk = col["foreign_key"]
            parent_table = fk["table"]
            parent_pks = state["pk_values"].get(parent_table, [])
            if parent_pks:
                columns[col_name] = rng.choices(parent_pks, k=count)
            else:
                columns[col_name] = [None] * count

    # ── Pass 2: Remaining columns ───────────────────────────────────
    for col in table["columns"]:
        col_name = col["name"]
        if col_name in columns:
            continue

        role = col.get("field_role", "text")

        # Try inheritance from parent via FK join
        if role != "identifier":
            inherited = _try_inherit_column(col, table, columns, state)
            if inherited is not None:
                columns[col_name] = inherited
                continue

        # Generate column
        config_dist = field_configs.get(col_name)
        values = generate_column(
            col, config_dist, count, rng, fake, sim_start, sim_end, temporal_anchor
        )
        columns[col_name] = values

        # Track first temporal column as anchor for relative dates
        if temporal_anchor is None and role == "temporal":
            temporal_anchor = values

    # ── Assemble DataFrame ──────────────────────────────────────────
    # Build in schema column order
    col_order = [c["name"] for c in table["columns"]]
    df = pl.DataFrame({name: columns[name] for name in col_order if name in columns})

    # ── Apply state machine (overwrites status column) ──────────────
    if table_name in state_machines:
        sm = state_machines[table_name]
        state_col = apply_state_machine_batch(sm, df, rng)
        df = df.with_columns(state_col)

    # ── Apply nullability mask ──────────────────────────────────────
    df = _apply_nulls(df, table, rng, count)

    return df


def _try_inherit_column(
    col: dict,
    table: dict,
    columns: dict[str, list],
    state: dict,
) -> list | None:
    """Try to inherit a column's values from the parent table via FK join."""
    # Find the FK column in this table that links to a parent
    for fk_col in table["columns"]:
        fk = fk_col.get("foreign_key")
        if not fk:
            continue

        parent_table = fk["table"]
        parent_df = state["table_dfs"].get(parent_table)
        if parent_df is None:
            continue

        # Check if parent has a matching column
        match = find_inheritable_field(col, parent_df.columns)
        if match is None:
            continue

        # Join: use FK column values to look up parent values
        fk_col_name = fk_col["name"]
        if fk_col_name not in columns:
            continue

        parent_pk_col = fk["column"]
        fk_values = columns[fk_col_name]

        # Build a lookup from parent PK → parent value
        parent_lookup = dict(
            zip(
                parent_df[parent_pk_col].to_list(),
                parent_df[match].to_list(),
            )
        )
        return [parent_lookup.get(fk_val) for fk_val in fk_values]

    return None


def _apply_nulls(
    df: pl.DataFrame, table: dict, rng: random.Random, count: int
) -> pl.DataFrame:
    """Apply 5% null rate to nullable, non-PK columns."""
    null_exprs = []
    for col in table["columns"]:
        col_name = col["name"]
        if col_name not in df.columns:
            continue
        if not col.get("nullable", True) or col.get("primary_key") or col.get("foreign_key"):
            continue

        # Generate mask: True = keep, False = null
        mask = [rng.random() >= 0.05 for _ in range(count)]
        null_exprs.append(
            pl.when(pl.Series(mask)).then(pl.col(col_name)).otherwise(None).alias(col_name)
        )

    if null_exprs:
        df = df.with_columns(null_exprs)

    return df


# ── State management ────────────────────────────────────────────────────


def _store_table_state(table: dict, df: pl.DataFrame, state: dict) -> None:
    """Store PK pool and DataFrame for child tables."""
    table_name = table["name"]

    # Find PK column
    pk_col = None
    for col in table["columns"]:
        if col.get("primary_key"):
            pk_col = col["name"]
            break

    if pk_col and pk_col in df.columns:
        state["pk_values"][table_name] = df[pk_col].to_list()

    # Store full DataFrame for inheritance joins and event emission
    state["table_dfs"][table_name] = df


# ── Event table generation ──────────────────────────────────────────────


def _generate_event_table(
    table: dict,
    event_config: dict,
    entities: dict,
    state: dict,
    simulation: dict,
    rng: random.Random,
    fake: Faker,
) -> pl.DataFrame:
    """Generate event rows based on parent entity states and Poisson distribution."""
    parent_table = event_config["emitted_by"]
    emit_when_states = event_config.get("emit_when_states", [])
    frequency = event_config.get("frequency", {})
    lambda_base = frequency.get("lambda_base", 1.0)
    lambda_modifiers = frequency.get("lambda_modifiers", [])

    parent_df = state["table_dfs"].get(parent_table, pl.DataFrame())
    if parent_df.is_empty():
        return pl.DataFrame()

    # Find parent PK and state field
    parent_pk_col = None
    for t in [t for t in [state] if True]:  # just need pk from pk_values
        pass
    # Look up parent PK column from the event table's FK
    parent_pk_col = _find_parent_pk_col(table, parent_table)

    # Find parent state field
    parent_state_field = _find_state_field(parent_df)

    # Filter to eligible parents
    eligible = filter_eligible_parents(parent_df, parent_state_field, emit_when_states)
    if eligible.is_empty():
        return pl.DataFrame()

    # Vectorized lambda + Poisson
    lambdas = calculate_lambdas_batch(lambda_base, lambda_modifiers, eligible)
    event_counts = sample_event_counts_batch(lambdas, rng.randint(0, 2**31))

    # Build event rows
    total_events = int(event_counts.sum())
    if total_events == 0:
        return pl.DataFrame()

    logger.info(f"  Emitting {total_events} events from {len(eligible)} eligible parents")

    return _build_event_dataframe(
        table, eligible, parent_pk_col, parent_table,
        event_counts, total_events, entities, simulation, rng, fake
    )


def _find_parent_pk_col(table: dict, parent_table: str) -> str | None:
    """Find the parent PK column referenced by this event table's FK."""
    for col in table["columns"]:
        fk = col.get("foreign_key")
        if fk and fk["table"] == parent_table:
            return fk["column"]
    return None


def _find_state_field(df: pl.DataFrame) -> str | None:
    """Find a status/state field in a DataFrame."""
    for col_name in df.columns:
        lower = col_name.lower()
        if "status" in lower or "state" in lower:
            if "employment" not in lower and "marital" not in lower:
                return col_name
    return None


def _build_event_dataframe(
    table: dict,
    eligible: pl.DataFrame,
    parent_pk_col: str | None,
    parent_table: str,
    event_counts: np.ndarray,
    total_events: int,
    entities: dict,
    simulation: dict,
    rng: random.Random,
    fake: Faker,
) -> pl.DataFrame:
    """Build the event DataFrame by repeating parent PKs and generating fields."""
    table_name = table["name"]
    field_configs = entities.get(table_name, {}).get("fields", {})
    sim_start = simulation.get("start_date")
    sim_end = simulation.get("end_date")

    # Repeat parent PKs according to event counts
    parent_pks = eligible[parent_pk_col].to_list() if parent_pk_col else []
    repeated_pks = []
    for pk, cnt in zip(parent_pks, event_counts):
        repeated_pks.extend([pk] * int(cnt))

    columns: dict[str, list] = {}

    # Generate columns
    for col in table["columns"]:
        col_name = col["name"]

        if col.get("primary_key"):
            columns[col_name] = generate_column(col, None, total_events, rng, fake)
        elif col.get("foreign_key"):
            fk = col["foreign_key"]
            if fk["table"] == parent_table:
                columns[col_name] = repeated_pks
            else:
                columns[col_name] = [None] * total_events
        else:
            config_dist = field_configs.get(col_name)
            columns[col_name] = generate_column(
                col, config_dist, total_events, rng, fake, sim_start, sim_end
            )

    col_order = [c["name"] for c in table["columns"]]
    return pl.DataFrame({name: columns[name] for name in col_order if name in columns})
