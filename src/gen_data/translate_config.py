"""Translates JSON schema to scenario config using LLM."""

import json
from pathlib import Path

from src.gen_schema.schema_utils import table_order, table_counts
from src.utils.gemini_client import GeminiClient
from .config_validator import validate_config
from .behavioral_mapping import BehavioralMapping

MAX_CONFIG_ATTEMPTS = 3


def _build_system_prompt(logic_content: str) -> str:
    """Build system prompt for behavioral mapping."""
    return f"""You are a behavioral mapping generator for synthetic data simulation.

# Business Logic Reference
{logic_content}

# Your Task
Given a JSON schema, identify the business scenario and produce a behavioral mapping with:
1. State machines for entities with lifecycle states
2. Event emission rules
3. Constraints
4. Key field distributions (only for behavioral drivers like credit_score, income, segment)

# Output Format
Output ONLY valid JSON (no markdown, no prose):
{{
  "scenario_name": "string",
  "start_date": "YYYY-MM-DD",
  "end_date": "YYYY-MM-DD",
  "state_machines": [
    {{
      "entity": "table_name",
      "state_field": "status",
      "initial_state": "Pending",
      "terminal_states": ["Closed"],
      "transitions": {{
        "Pending": [
          {{
            "to_state": "Active",
            "base_prob": 0.85,
            "adjustments": [
              {{"field": "credit_score", "direction": "higher_increases", "strength": "strong"}}
            ]
          }}
        ]
      }}
    }}
  ],
  "events": [
    {{
      "event_table": "table_name",
      "emitted_by": "entity_table",
      "emit_when_states": ["Active"],
      "lambda_base": 1.0,
      "lambda_modifiers": [
        {{"field": "income", "effect": "higher_increases"}}
      ]
    }}
  ],
  "constraints": [
    {{
      "type": "temporal_order",
      "params": {{"fields": ["date1", "date2"]}}
    }}
  ],
  "key_distributions": {{
    "Customers": {{
      "credit_score": {{"distribution": "normal", "params": {{"mean": 650, "std": 100}}}},
      "income": {{"distribution": "lognormal", "params": {{"mean": 60000, "sigma": 0.5}}}}
    }}
  }}
}}

Match patterns from business logic. Use realistic probabilities. Output JSON only."""


def _generate_behavioral_mapping(schema: dict, logic_content: str) -> BehavioralMapping:
    """Generate behavioral mapping from schema using LLM."""
    system_prompt = _build_system_prompt(logic_content)
    user_prompt = f"Generate behavioral mapping for:\n\n{json.dumps(schema, indent=2)}"

    client = GeminiClient()

    for attempt in range(1, MAX_CONFIG_ATTEMPTS + 1):
        try:
            response = client.chat(user_prompt=user_prompt, system_prompt=system_prompt)

            # Strip markdown fences
            response = response.strip()
            if response.startswith("```"):
                lines = response.split("\n")
                response = "\n".join(lines[1:-1]) if len(lines) > 2 else response
                if response.startswith("json"):
                    response = response[4:].strip()

            mapping_dict = json.loads(response)
            return BehavioralMapping(**mapping_dict)

        except Exception as e:
            if attempt < MAX_CONFIG_ATTEMPTS:
                print(f"⚠️  Attempt {attempt} failed: {e}. Retrying...")
                user_prompt = f"{user_prompt}\n\nPrevious attempt failed: {e}\n\nFix and output valid JSON only."
            else:
                raise ValueError(f"Failed after {MAX_CONFIG_ATTEMPTS} attempts: {e}")

    raise ValueError("Unreachable")


def _build_full_config(schema: dict, mapping: BehavioralMapping, base_records: int, seed: int) -> dict:
    """Build full config from schema and behavioral mapping."""
    # Get table order and counts
    order = table_order(schema)
    counts = table_counts(schema, order, base_records)

    # Build entities from schema tables
    entities = {}
    for table in schema["tables"]:
        table_name = table["name"]
        entities[table_name] = {
            "fields": mapping.key_distributions.get(table_name, {})
        }

    # Build state machines
    state_machines = {}
    for sm in mapping.state_machines:
        transitions = {}
        for from_state, to_list in sm.transitions.items():
            transitions[from_state] = {}
            for trans in to_list:
                transitions[from_state][trans.to_state] = {
                    "base_prob": trans.base_prob,
                    "adjustments": trans.adjustments
                }

        state_machines[sm.entity] = {
            "state_field": sm.state_field,
            "initial_state": sm.initial_state,
            "terminal_states": sm.terminal_states,
            "transitions": transitions
        }

    # Build events
    events = {}
    for evt in mapping.events:
        events[evt.event_table] = {
            "emitted_by": evt.emitted_by,
            "emit_when_states": evt.emit_when_states,
            "frequency": {
                "distribution": "poisson",
                "lambda_base": evt.lambda_base,
                "lambda_modifiers": evt.lambda_modifiers
            },
            "fields": {}
        }

    # Build constraints
    constraints = []
    for const in mapping.constraints:
        constraint = {"type": const.type}
        constraint.update(const.params)
        constraints.append(constraint)

    return {
        "scenario_name": mapping.scenario_name,
        "seed": seed,
        "simulation": {
            "start_date": mapping.start_date,
            "end_date": mapping.end_date
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
    logic_file: Path | None = None
) -> dict:
    """Translate JSON schema to scenario config.

    Args:
        schema: JSON schema from Part 1
        base_records: Base record count for table_counts calculation
        seed: Random seed for reproducibility
        logic_file: Path to business logic markdown file

    Returns:
        Scenario configuration dict
    """
    # Load business logic
    if logic_file is None:
        logic_file = Path(__file__).parent / "business_logic.md"

    logic_content = logic_file.read_text(encoding="utf-8") if logic_file.exists() else ""

    # Generate behavioral mapping from LLM
    mapping = _generate_behavioral_mapping(schema, logic_content)

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

