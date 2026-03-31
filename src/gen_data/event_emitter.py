"""Event emission with Poisson distribution and lambda modifiers."""

import random
from typing import Any

import numpy as np
import polars as pl


STRENGTH_FACTOR = 0.5  # Default modifier strength


def calculate_lambdas_batch(
    lambda_base: float,
    lambda_modifiers: list[dict],
    parent_df: pl.DataFrame,
) -> np.ndarray:
    """Vectorized lambda calculation for all parent rows.

    Returns an ndarray of adjusted lambda values, one per parent row.
    """
    n = len(parent_df)
    lambdas = np.full(n, lambda_base, dtype=np.float64)

    for modifier in lambda_modifiers:
        field = modifier["field"]
        if field not in parent_df.columns:
            continue

        col = parent_df[field]
        if col.dtype not in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8):
            continue

        values = col.to_numpy().astype(np.float64)

        # Normalize to 0-1
        vmin, vmax = np.nanmin(values), np.nanmax(values)
        if vmax > vmin:
            normalized = (values - vmin) / (vmax - vmin)
        else:
            normalized = np.full(n, 0.5)

        effect = modifier.get("effect", "higher_increases")
        if effect == "higher_increases":
            lambdas *= (1 + STRENGTH_FACTOR * normalized)
        elif effect == "higher_decreases":
            lambdas *= (1 - STRENGTH_FACTOR * normalized)

    return np.clip(lambdas, 0.1, None)


def sample_event_counts_batch(
    lambdas: np.ndarray, rng_seed: int
) -> np.ndarray:
    """Batch Poisson sampling for all parents at once."""
    gen = np.random.default_rng(rng_seed)
    return gen.poisson(lambdas)


def filter_eligible_parents(
    parent_df: pl.DataFrame,
    state_field: str | None,
    emit_when_states: list[str],
) -> pl.DataFrame:
    """Filter parents to only those in eligible states."""
    if not emit_when_states or not state_field or state_field not in parent_df.columns:
        return parent_df
    return parent_df.filter(pl.col(state_field).is_in(emit_when_states))
