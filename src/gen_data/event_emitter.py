"""Event emission with Poisson distribution and lambda modifiers."""

from typing import Any


def calculate_lambda(
    lambda_base: float,
    lambda_modifiers: list[dict],
    parent_row: dict[str, Any],
    field_ranges: dict[str, tuple[float, float]] | None = None,
) -> float:
    """Calculate adjusted lambda for Poisson distribution.

    Args:
        lambda_base: Base lambda value
        lambda_modifiers: List of {field, effect} modifiers
        parent_row: Parent entity row values
        field_ranges: Optional field ranges for normalization

    Returns:
        Adjusted lambda value
    """
    lambda_val = lambda_base

    for modifier in lambda_modifiers:
        field = modifier["field"]
        if field not in parent_row:
            continue

        value = parent_row[field]
        if not isinstance(value, (int, float)):
            continue

        # Normalize value
        if field_ranges and field in field_ranges:
            min_val, max_val = field_ranges[field]
            if max_val == min_val:
                normalized = 0.5
            else:
                normalized = max(0.0, min(1.0, (float(value) - min_val) / (max_val - min_val)))
        else:
            normalized = 0.5

        # Apply modifier
        effect = modifier.get("effect", "higher_increases")
        if effect == "higher_increases":
            lambda_val *= (1 + 0.5 * normalized)
        elif effect == "higher_decreases":
            lambda_val *= (1 - 0.5 * normalized)

    return max(0.1, lambda_val)


def should_emit_events(
    parent_state: Any,
    emit_when_states: list[str],
) -> bool:
    """Check if parent state allows event emission."""
    if not emit_when_states:
        return True
    return str(parent_state) in emit_when_states


def sample_event_count(lambda_val: float, rng: Any) -> int:
    """Sample event count from Poisson distribution."""
    # Use numpy if available, otherwise approximate
    try:
        import numpy as np
        return int(np.random.poisson(lambda_val))
    except ImportError:
        # Simple approximation using exponential distribution
        count = 0
        p = 1.0
        L = 2.71828 ** (-lambda_val)
        while p > L:
            count += 1
            p *= rng.random()
        return count - 1
