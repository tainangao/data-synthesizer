"""State machine transitions with feature-based probability adjustments."""

from typing import Any


STRENGTH_MAP = {"weak": 0.2, "moderate": 0.5, "strong": 1.0}


def normalize_value(value: Any, min_val: float = 0.0, max_val: float = 1.0) -> float:
    """Normalize a value to 0-1 range."""
    if not isinstance(value, (int, float)):
        return 0.5
    if max_val == min_val:
        return 0.5
    return max(0.0, min(1.0, (float(value) - min_val) / (max_val - min_val)))


def apply_adjustments(
    base_probs: dict[str, float],
    row_values: dict[str, Any],
    adjustments: list[dict],
    field_ranges: dict[str, tuple[float, float]] | None = None,
) -> dict[str, float]:
    """Apply feature-based adjustments to base probabilities.

    Args:
        base_probs: {to_state: base_prob}
        row_values: Current row field values
        adjustments: List of {field, direction, strength} per to_state
        field_ranges: Optional {field: (min, max)} for normalization

    Returns:
        Adjusted probabilities (not normalized)
    """
    adjusted = dict(base_probs)

    for to_state, adj_list in adjustments.items():
        if to_state not in adjusted:
            continue

        prob = adjusted[to_state]

        for adj in adj_list:
            field = adj["field"]
            if field not in row_values:
                continue

            value = row_values[field]

            # Normalize value
            if field_ranges and field in field_ranges:
                min_val, max_val = field_ranges[field]
                normalized = normalize_value(value, min_val, max_val)
            else:
                normalized = normalize_value(value)

            # Apply adjustment
            strength = STRENGTH_MAP.get(adj["strength"], 0.5)
            direction = adj["direction"]

            if direction == "higher_increases":
                prob *= (1 + strength * normalized)
            elif direction == "higher_decreases":
                prob *= (1 - strength * normalized)

        adjusted[to_state] = max(0.0, prob)

    return adjusted


def normalize_probs(probs: dict[str, float]) -> dict[str, float]:
    """Normalize probabilities to sum to 1.0."""
    total = sum(probs.values())
    if total == 0:
        return {k: 1.0 / len(probs) for k in probs}
    return {k: v / total for k, v in probs.items()}


def sample_state(probs: dict[str, float], rng: Any) -> str:
    """Sample a state using weighted choice."""
    states = list(probs.keys())
    weights = list(probs.values())
    return rng.choices(states, weights=weights, k=1)[0]


def apply_state_machine(
    state_machine: dict,
    row_values: dict[str, Any],
    rng: Any,
    field_ranges: dict[str, tuple[float, float]] | None = None,
) -> str:
    """Apply state machine to generate a state for a row.

    Args:
        state_machine: Config state machine definition
        row_values: Current row field values
        rng: Random number generator
        field_ranges: Optional field ranges for normalization

    Returns:
        Sampled state
    """
    initial_state = state_machine["initial_state"]
    transitions = state_machine["transitions"]

    if initial_state not in transitions:
        return initial_state

    # Get transitions from initial state
    trans = transitions[initial_state]

    # Build base probs and adjustments
    base_probs = {}
    adjustments = {}

    for to_state, config in trans.items():
        base_probs[to_state] = config["base_prob"]
        if "adjustments" in config and config["adjustments"]:
            adjustments[to_state] = config["adjustments"]

    # Apply adjustments
    adjusted = apply_adjustments(base_probs, row_values, adjustments, field_ranges)

    # Normalize and sample
    normalized = normalize_probs(adjusted)
    return sample_state(normalized, rng)
