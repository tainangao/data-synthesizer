"""Translates JSON schema to scenario config using pattern matching."""

import json
from pathlib import Path

from src.gen_schema.schema_utils import table_order, table_counts
from .config_validator import validate_config
from .pattern_matcher import generate_behavioral_mapping

def _build_full_config(schema: dict, mapping: dict, base_records: int, seed: int) -> dict:
    """Build full config from schema and behavioral mapping."""
    # Get table order and counts
    order = table_order(schema)
    counts = table_counts(schema, order, base_records)

    # Build entities from schema tables
    entities = {}
    for table in schema["tables"]:
        table_name = table["name"]
        entities[table_name] = {
            "fields": mapping["key_distributions"].get(table_name, {})
        }

    # State machines and events already in correct format
    state_machines = {}
    for sm in mapping["state_machines"]:
        transitions = {}
        for from_state, to_list in sm["transitions"].items():
            transitions[from_state] = {}
            for trans in to_list:
                transitions[from_state][trans["to_state"]] = {
                    "base_prob": trans["base_prob"],
                    "adjustments": trans.get("adjustments", [])
                }

        state_machines[sm["entity"]] = {
            "state_field": sm["state_field"],
            "initial_state": sm["initial_state"],
            "terminal_states": sm["terminal_states"],
            "transitions": transitions
        }

    # Build events
    events = {}
    for evt in mapping["events"]:
        events[evt["event_table"]] = {
            "emitted_by": evt["emitted_by"],
            "emit_when_states": evt["emit_when_states"],
            "frequency": {
                "distribution": "poisson",
                "lambda_base": evt["lambda_base"],
                "lambda_modifiers": evt.get("lambda_modifiers", [])
            },
            "fields": {}
        }

    # Build constraints
    constraints = mapping["constraints"]

    return {
        "scenario_name": mapping["scenario_name"],
        "seed": seed,
        "simulation": {
            "start_date": mapping["start_date"],
            "end_date": mapping["end_date"]
        },
        "entities": entities,
        "generation_order": order,
        "table_counts": counts,
        "state_machines": state_machines,
        "events": events,
        "constraints": constraints
    }


def translate_schema_to_config(
    schema: dict,
    base_records: int = 1000,
    seed: int = 42,
    logic_file: Path | None = None  # noqa: ARG001
) -> dict:
    """Translate JSON schema to scenario config.

    Args:
        schema: JSON schema from Part 1
        base_records: Base record count for table_counts calculation
        seed: Random seed for reproducibility
        logic_file: Unused (kept for compatibility)

    Returns:
        Scenario configuration dict
    """
    # Generate behavioral mapping using pattern matching
    mapping = generate_behavioral_mapping(schema)

    # Build full config
    config = _build_full_config(schema, mapping, base_records, seed)

    return config


def save_config(config: dict, output_path: Path) -> None:
    """Save config to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(config, indent=2), encoding="utf-8")


def translate_and_validate(
    schema: dict,
    base_records: int = 1000,
    seed: int = 42,
    output_path: Path | None = None
) -> tuple[dict, list[str]]:
    """Translate schema to config and validate.

    Returns:
        Tuple of (config, validation_errors)
    """
    config = translate_schema_to_config(schema, base_records, seed)
    errors = validate_config(config)

    if output_path and not errors:
        save_config(config, output_path)

    return config, errors

