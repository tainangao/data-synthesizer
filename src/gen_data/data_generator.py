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

    tables_by_name: dict = {t["name"]: t for t in schema["tables"]}
    generation_order: list[str] = config["generation_order"]
    table_counts: dict[str, int] = config["table_counts"]
    entities: dict = config.get("entities", {})
    state_machines: dict = config.get("state_machines", {})
    events: dict = config.get("events", {})
    simulation: dict = config.get("simulation", {})
    constraints: list[dict] = config.get("constraints", [])

    # State shared across tables for FK sampling and inheritance
    state: dict[str, Any] = {
        "pk_values": {},   # {table: [pk_values]}
        "table_dfs": {},   # {table: pl.DataFrame} — for joins / inheritance
    }

    row_counts: dict[str, int] = {}

    # ── Phase 1: Entity tables ──────────────────────────────────────────
    for table_name in generation_order:
        if table_name not in tables_by_name or table_name in events:
            continue

        table = tables_by_name[table_name]
        count = table_counts.get(table_name, 100)

        logger.info(f"Generating {count} rows for entity table: {table_name}")
        df = _generate_entity_table(
            table, count, entities, state_machines, constraints, state, simulation, rng, fake
        )

        # Store PK pool and DataFrame for child tables
        _store_table_state(table, df, state)

        for writer in writers:
            writer.write_dataframe(table, df)

        row_counts[table_name] = len(df)

    # ── Phase 2: Event tables ───────────────────────────────────────────
    for event_table_name, event_config in events.items():
        if event_table_name not in tables_by_name:
            continue

        table = tables_by_name[event_table_name]

        logger.info(f"Generating events for table: {event_table_name}")
        df = _generate_event_table(
            table, event_config, entities, constraints, state, simulation, rng, fake
        )

        # Update parent balance if applicable
        _update_parent_balance(table, df, event_config, state, tables_by_name, writers)

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
    constraints: list,
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
                logger.warning(
                    f"No parent PKs found for FK {table_name}.{col_name} → {parent_table}. "
                    "Check generation_order in config."
                )
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

        # Use parent's temporal column as floor for child temporal fields
        parent_temporal = _get_parent_temporal_anchor(table, columns, state)

        config_dist = field_configs.get(col_name)
        values = generate_column(
            col, config_dist, count, rng, fake, sim_start, sim_end,
            anchor_series=temporal_anchor,
            parent_temporal=parent_temporal,
        )
        columns[col_name] = values

        # Track first temporal column as within-row anchor for relative dates
        if temporal_anchor is None and role == "temporal":
            temporal_anchor = values

    # ── Assemble DataFrame ──────────────────────────────────────────
    col_order = [c["name"] for c in table["columns"]]
    df = pl.DataFrame({name: columns[name] for name in col_order if name in columns})

    # ── Apply state machine (overwrites status column) ──────────────
    if table_name in state_machines:
        sm = state_machines[table_name]
        state_col = apply_state_machine_batch(sm, df, rng)
        df = df.with_columns(state_col)

    # ── Enforce temporal ordering constraints ───────────────────────
    df = _apply_temporal_constraints(df, table, constraints)

    # ── Apply nullability mask ──────────────────────────────────────
    df = _apply_nulls(df, table, rng, count)

    return df


def _get_parent_temporal_anchor(
    table: dict,
    columns: dict[str, list],
    state: dict,
) -> list[datetime] | None:
    """Return the parent table's first temporal column as a per-row floor for child dates.

    This ensures child temporal fields occur after parent temporal fields.
    Example: transaction_date >= account_open_date

    Returns a list of datetime values (one per child row) by looking up the parent's
    temporal column via the FK relationship.
    """
    for col in table["columns"]:
        fk = col.get("foreign_key")
        if not fk:
            continue
        parent_table = fk["table"]
        parent_df = state["table_dfs"].get(parent_table)
        if parent_df is None:
            continue
        fk_col_name = col["name"]
        if fk_col_name not in columns:
            continue

        # Find the first temporal column in parent
        for parent_col_name in parent_df.columns:
            if parent_df[parent_col_name].dtype in (pl.Date, pl.Datetime):
                parent_pk_col = fk["column"]
                fk_values = columns[fk_col_name]
                parent_lookup = dict(
                    zip(parent_df[parent_pk_col].to_list(), parent_df[parent_col_name].to_list())
                )
                return [parent_lookup.get(fk_val) for fk_val in fk_values]
    return None


def _try_inherit_column(
    col: dict,
    table: dict,
    columns: dict[str, list],
    state: dict,
) -> list | None:
    """Try to inherit a column's values from the parent table via FK join.

    Value inheritance reduces redundancy and maintains consistency across related tables.
    Example: Transaction.currency inherits from Account.currency via account_id FK.

    Process:
    1. Find an FK column in this table
    2. Get the parent DataFrame from state
    3. Use semantic matching to find a compatible parent column
    4. Build a lookup dict: {parent_pk: parent_value}
    5. Map child FK values to parent values

    Returns the inherited values list, or None if inheritance isn't possible.
    """
    for fk_col in table["columns"]:
        fk = fk_col.get("foreign_key")
        if not fk:
            continue

        parent_table = fk["table"]
        parent_df = state["table_dfs"].get(parent_table)
        if parent_df is None:
            continue

        match = find_inheritable_field(col, parent_df.columns)
        if match is None:
            continue

        fk_col_name = fk_col["name"]
        if fk_col_name not in columns:
            continue

        parent_pk_col = fk["column"]
        fk_values = columns[fk_col_name]

        parent_lookup = dict(
            zip(
                parent_df[parent_pk_col].to_list(),
                parent_df[match].to_list(),
            )
        )
        inherited = [parent_lookup.get(fk_val) for fk_val in fk_values]

        # Don't inherit if result contains nulls and the child column is non-nullable
        if not col.get("nullable", True) and any(v is None for v in inherited):
            continue

        return inherited

    return None


def _apply_temporal_constraints(
    df: pl.DataFrame, table: dict, constraints: list
) -> pl.DataFrame:
    """Enforce temporal_order constraints: sort date columns so earlier fields come first.

    For each temporal_order constraint that references columns present in this table,
    ensure col[0] <= col[1] <= col[2] ... row-by-row by sorting the values.

    Example: If constraint specifies [application_date, approval_date, disbursement_date],
    this ensures approval doesn't happen before application, etc.
    """
    table_col_names = {c["name"] for c in table["columns"]}

    for constraint in constraints:
        if constraint.get("type") != "temporal_order":
            continue
        fields = constraint.get("params", {}).get("fields", [])
        # Only apply if ALL fields are in this table
        present = [f for f in fields if f in table_col_names and f in df.columns]
        if len(present) < 2:
            continue

        # Sort date values per row so they're in ascending order
        rows = df.select(present).to_pandas()
        sorted_rows = rows.apply(lambda r: sorted(r, key=lambda x: (x is None, x)), axis=1, result_type="expand")
        sorted_rows.columns = present
        for field in present:
            df = df.with_columns(pl.Series(field, sorted_rows[field].tolist()))

    return df


def _apply_nulls(
    df: pl.DataFrame, table: dict, rng: random.Random, count: int
) -> pl.DataFrame:
    """Apply 5% null rate to nullable, non-PK/non-FK columns."""
    null_exprs = []
    for col in table["columns"]:
        col_name = col["name"]
        if col_name not in df.columns:
            continue
        if not col.get("nullable", True) or col.get("primary_key") or col.get("foreign_key"):
            continue

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

    pk_col = next((c["name"] for c in table["columns"] if c.get("primary_key")), None)
    if pk_col and pk_col in df.columns:
        state["pk_values"][table_name] = df[pk_col].to_list()

    state["table_dfs"][table_name] = df


# ── Event table generation ──────────────────────────────────────────────


def _generate_event_table(
    table: dict,
    event_config: dict,
    entities: dict,
    constraints: list,
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
        logger.warning(f"No parent data found for event table {table['name']} (emitted_by: {parent_table})")
        return pl.DataFrame()

    parent_pk_col = _find_parent_pk_col(table, parent_table)
    parent_state_field = _find_state_field(parent_df)

    eligible = filter_eligible_parents(parent_df, parent_state_field, emit_when_states)
    if eligible.is_empty():
        logger.info(f"  No eligible parents for {table['name']} (states: {emit_when_states})")
        return pl.DataFrame()

    lambdas = calculate_lambdas_batch(lambda_base, lambda_modifiers, eligible)
    event_counts = sample_event_counts_batch(lambdas, rng.randint(0, 2**31))

    total_events = int(event_counts.sum())
    if total_events == 0:
        return pl.DataFrame()

    logger.info(f"  Emitting {total_events} events from {len(eligible)} eligible parents")

    df = _build_event_dataframe(
        table, eligible, parent_pk_col, parent_table,
        event_counts, total_events, entities, state, simulation, rng, fake
    )

    # Enforce temporal constraints on event table too
    df = _apply_temporal_constraints(df, table, constraints)

    return df


def _find_parent_pk_col(table: dict, parent_table: str) -> str | None:
    """Find the parent PK column referenced by this event table's FK."""
    for col in table["columns"]:
        fk = col.get("foreign_key")
        if fk and fk["table"] == parent_table:
            return fk["column"]
    return None


def _find_state_field(df: pl.DataFrame) -> str | None:
    """Find a status/state field in a DataFrame by column name heuristics."""
    for col_name in df.columns:
        lower = col_name.lower()
        if ("status" in lower or "state" in lower) and "employment" not in lower and "marital" not in lower:
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
    state: dict,
    simulation: dict,
    rng: random.Random,
    fake: Faker,
) -> pl.DataFrame:
    """Build the event DataFrame by repeating parent PKs and generating fields."""
    table_name = table["name"]
    field_configs = entities.get(table_name, {}).get("fields", {})
    sim_start = simulation.get("start_date")
    sim_end = simulation.get("end_date")

    # Repeat parent row indices so we can look up any parent column per event
    parent_pk_values = eligible[parent_pk_col].to_list() if parent_pk_col else []
    repeated_parent_pks: list = []
    for pk, cnt in zip(parent_pk_values, event_counts):
        repeated_parent_pks.extend([pk] * int(cnt))

    # Build per-event lookup into parent DataFrame
    parent_pk_series = eligible[parent_pk_col].to_list() if parent_pk_col else []
    parent_row_by_pk: dict = {}
    if parent_pk_col:
        for row in eligible.iter_rows(named=True):
            parent_row_by_pk[row[parent_pk_col]] = row

    # Temporal anchor from parent (events happen after parent's date)
    parent_temporal_col = _find_temporal_col(eligible)
    parent_temporal_lookup: dict = {}
    if parent_temporal_col and parent_pk_col:
        parent_temporal_lookup = dict(
            zip(eligible[parent_pk_col].to_list(), eligible[parent_temporal_col].to_list())
        )
    parent_temporal_anchor = [parent_temporal_lookup.get(pk) for pk in repeated_parent_pks] if parent_temporal_lookup else None

    columns: dict[str, list] = {}
    within_row_temporal_anchor: list | None = None

    for col in table["columns"]:
        col_name = col["name"]

        if col.get("primary_key"):
            columns[col_name] = generate_column(col, None, total_events, rng, fake)

        elif col.get("foreign_key"):
            fk = col["foreign_key"]
            if fk["table"] == parent_table:
                # Direct parent FK
                columns[col_name] = repeated_parent_pks
            else:
                # Propagate grandparent FK from parent row
                columns[col_name] = _propagate_fk_from_parent(
                    fk, repeated_parent_pks, parent_row_by_pk, total_events
                )
        else:
            config_dist = field_configs.get(col_name)
            values = generate_column(
                col, config_dist, total_events, rng, fake, sim_start, sim_end,
                anchor_series=within_row_temporal_anchor,
                parent_temporal=parent_temporal_anchor,
            )
            columns[col_name] = values

            role = col.get("field_role", "text")
            if within_row_temporal_anchor is None and role == "temporal":
                within_row_temporal_anchor = values

    col_order = [c["name"] for c in table["columns"]]
    return pl.DataFrame({name: columns[name] for name in col_order if name in columns})


def _propagate_fk_from_parent(
    fk: dict,
    repeated_parent_pks: list,
    parent_row_by_pk: dict,
    total_events: int,
) -> list:
    """For a non-parent FK, look up the value from the parent row.

    Handles grandparent FK propagation in event tables.
    Example: Transaction.customer_id is found by looking up Account.customer_id
    for each repeated Account row.

    This maintains FK relationships across 3+ levels: Customer → Account → Transaction
    """
    fk_col_in_parent = fk["column"]  # The column name we need in the parent row
    result = []
    for parent_pk in repeated_parent_pks:
        parent_row = parent_row_by_pk.get(parent_pk, {})
        # Try exact column name match first, then partial match
        val = parent_row.get(fk_col_in_parent)
        if val is None:
            # Try matching by FK table name as prefix/suffix in parent columns
            for k, v in parent_row.items():
                if fk["table"].lower() in k.lower() or k.lower() in fk["table"].lower():
                    val = v
                    break
        result.append(val)
    return result


def _find_temporal_col(df: pl.DataFrame) -> str | None:
    """Find the first temporal (Date or Datetime) column in a DataFrame."""
    for col_name in df.columns:
        if df[col_name].dtype in (pl.Date, pl.Datetime):
            return col_name
    return None


def _update_parent_balance(
    event_table: dict,
    event_df: pl.DataFrame,
    event_config: dict,
    state: dict,
    tables_by_name: dict,
    writers: list,
) -> None:
    """Update parent balance based on event amounts (credits - debits)."""
    if event_df.is_empty():
        return

    parent_table_name = event_config["emitted_by"]
    parent_df = state["table_dfs"].get(parent_table_name)
    if parent_df is None:
        return

    # Find balance column in parent
    balance_col = None
    for col in parent_df.columns:
        if "balance" in col.lower():
            balance_col = col
            break
    if not balance_col:
        return

    # Find amount column in events
    amount_col = None
    for col in event_df.columns:
        if "amount" in col.lower():
            amount_col = col
            break
    if not amount_col:
        return

    # Find parent FK column
    parent_fk_col = None
    for col in event_table["columns"]:
        fk = col.get("foreign_key")
        if fk and fk["table"] == parent_table_name:
            parent_fk_col = col["name"]
            break
    if not parent_fk_col or parent_fk_col not in event_df.columns:
        return

    # Calculate balance adjustments
    parent_pk_col = None
    for col in tables_by_name[parent_table_name]["columns"]:
        if col.get("primary_key"):
            parent_pk_col = col["name"]
            break
    if not parent_pk_col:
        return

    # Sum amounts by parent
    balance_changes = event_df.group_by(parent_fk_col).agg(
        pl.col(amount_col).sum().alias("total_amount")
    )

    # Update parent balance
    updated_parent = parent_df.join(
        balance_changes, left_on=parent_pk_col, right_on=parent_fk_col, how="left"
    ).with_columns(
        (pl.col(balance_col) + pl.col("total_amount").fill_null(0)).alias(balance_col)
    ).drop("total_amount")

    state["table_dfs"][parent_table_name] = updated_parent

    # Rewrite parent table with updated balances
    parent_table_def = tables_by_name[parent_table_name]
    for writer in writers:
        writer.write_dataframe(parent_table_def, updated_parent)
