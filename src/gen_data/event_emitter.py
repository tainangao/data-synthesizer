"""Event emission with Poisson distribution and lambda modifiers."""
import numpy as np
import polars as pl


STRENGTH_FACTOR = 0.5  # Default modifier strength


def calculate_lambdas_batch(
    lambda_base: float,
    lambda_modifiers: list[dict],
    parent_df: pl.DataFrame,
    temporal_col: str | None = None,
) -> np.ndarray:
    """Vectorized lambda calculation for all parent rows.

    Adjusts Poisson lambda based on parent row features and temporal patterns.
    Example: Higher account_balance increases transaction frequency.

    Process:
    1. Start with base lambda for all rows
    2. For each modifier, normalize feature values to 0-1
    3. Apply effect: higher_increases multiplies lambda by (1 + 0.5 * normalized)
    4. Apply seasonality if temporal column provided
    5. Clip to minimum 0.1 to avoid zero events

    Returns an ndarray of adjusted lambda values, one per parent row.
    """
    n = len(parent_df)
    lambdas = np.full(n, lambda_base, dtype=np.float64)

    for modifier in lambda_modifiers:
        field = modifier["field"]
        if field not in parent_df.columns:
            continue

        col = parent_df[field]

        # Handle categorical profile fields
        if col.dtype == pl.Utf8 or col.dtype == pl.String:
            profile_multipliers = {"Conservative": 0.7, "Moderate": 1.0, "Aggressive": 1.5,
                                   "Mass": 0.8, "Affluent": 1.2, "Premium": 1.8}
            values = col.to_list()
            multipliers = np.array([profile_multipliers.get(v, 1.0) for v in values])
            lambdas *= multipliers
            continue

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

    # Apply seasonality
    if temporal_col and temporal_col in parent_df.columns:
        lambdas = _apply_seasonality(lambdas, parent_df[temporal_col])

    # Clip to valid Poisson range AFTER all adjustments
    return np.clip(lambdas, 0.1, 1000.0)


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
    terminal_states: list[str] | None = None,
) -> pl.DataFrame:
    """Filter parents to only those in eligible states, excluding terminal states."""
    if not state_field or state_field not in parent_df.columns:
        return parent_df

    # Filter by emit_when_states if provided
    if emit_when_states:
        parent_df = parent_df.filter(pl.col(state_field).is_in(emit_when_states))

    # Exclude terminal states
    if terminal_states:
        parent_df = parent_df.filter(~pl.col(state_field).is_in(terminal_states))

    return parent_df


def _apply_seasonality(lambdas: np.ndarray, temporal_col: pl.Series) -> np.ndarray:
    """Apply monthly seasonality patterns to lambda values."""
    if temporal_col.dtype not in (pl.Date, pl.Datetime):
        return lambdas

    dates = temporal_col.to_numpy()
    months = np.array([d.month if hasattr(d, 'month') else 1 for d in dates])

    # Monthly multipliers (salary cycles, end-of-month patterns)
    # Higher activity at month start (1-5) and end (25-31)
    day_of_month = np.array([d.day if hasattr(d, 'day') else 15 for d in dates])
    seasonal_factor = np.where(
        (day_of_month <= 5) | (day_of_month >= 25),
        1.3,  # 30% increase at month boundaries
        1.0
    )

    return lambdas * seasonal_factor
