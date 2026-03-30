"""Translates JSON schema to scenario config using LLM."""

import json
from pathlib import Path

from src.gen_schema.schema_utils import table_order, table_counts
from src.utils.gemini_client import GeminiClient
from .config_validator import validate_config

MAX_CONFIG_ATTEMPTS = 3


def _build_system_prompt(logic_content: str) -> str:
    """Build system prompt for config translation."""
    return f"""You are a synthetic data configuration generator. Your task is to convert a JSON schema into a scenario configuration that drives a simulation engine.

# Business Logic Reference
{logic_content}

# Your Task
Given a JSON schema with tables and columns, produce a valid scenario_config.json that:
1. Identifies entity types (customers, accounts, loans, orders, etc.)
2. Maps entities to lifecycle state machines based on the business logic above
3. Defines event tables (transactions, interactions, repayments) emitted by entities
4. Sets up feature-conditional transitions (e.g., credit score affects default probability)
5. Applies temporal constraints and balance rules

# Config Structure
Output ONLY valid JSON (no markdown fences, no prose) with this structure:
{{
  "scenario_name": "string",
  "seed": 42,
  "simulation": {{
    "start_date": "YYYY-MM-DD",
    "end_date": "YYYY-MM-DD"
  }},
  "entities": {{
    "<entity_name>": {{
      "count": 1000,
      "fields": {{
        "<field_name>": {{
          "distribution": "normal|lognormal|uniform|poisson|choice|constant|date_offset",
          "params": {{}}
        }}
      }}
    }}
  }},
  "state_machines": {{
    "<entity_name>": {{
      "state_field": "<field_name>",
      "initial_state": "string",
      "terminal_states": ["string"],
      "transitions": {{
        "<from_state>": {{
          "<to_state>": {{
            "base_prob": 0.0,
            "adjustments": [
              {{
                "field": "<field_name>",
                "direction": "higher_increases|higher_decreases",
                "strength": "weak|moderate|strong"
              }}
            ]
          }}
        }}
      }}
    }}
  }},
  "events": {{
    "<event_table_name>": {{
      "emitted_by": "<entity_name>",
      "emit_when_states": ["string"],
      "frequency": {{
        "distribution": "poisson",
        "lambda_base": 1.0,
        "lambda_modifiers": [
          {{
            "field": "<field_name>",
            "effect": "higher_increases|higher_decreases"
          }}
        ]
      }},
      "fields": {{
        "<field_name>": {{
          "distribution": "string",
          "params": {{}}
        }}
      }}
    }}
  }},
  "constraints": [
    {{
      "type": "temporal_order",
      "fields": ["date_field_1", "date_field_2"]
    }},
    {{
      "type": "no_events_after_terminal",
      "entity": "<entity_name>",
      "event_table": "<event_table_name>"
    }},
    {{
      "type": "running_balance",
      "credit_field": "<field_name>",
      "debit_field": "<field_name>",
      "balance_field": "<field_name>"
    }}
  ]
}}

# Distribution Parameters
- normal: {{"mean": float, "std": float}}
- lognormal: {{"mean": float, "sigma": float}}
- uniform: {{"low": float, "high": float}}
- poisson: {{"lambda": float}}
- choice: {{"values": [any], "weights": [float]}}
- constant: {{"value": any}}
- date_offset: {{"min_days": int, "max_days": int}}

# Important
- Match scenario patterns from the business logic (CRM/Trading/Credit)
- Use realistic transition probabilities from the reference matrices
- Apply feature-based adjustments where appropriate
- Output ONLY the JSON config, nothing else"""


def translate_schema_to_config(
    schema: dict,
    base_records: int = 1000,
    seed: int = 42,
    logic_file: Path | None = None
) -> dict:
    """Translate JSON schema to scenario config using LLM with retry logic.

    Args:
        schema: JSON schema from Part 1
        base_records: Base record count for table_counts calculation
        seed: Random seed for reproducibility
        logic_file: Path to business logic markdown file

    Returns:
        Scenario configuration dict

    Raises:
        ValueError: If config generation fails after MAX_CONFIG_ATTEMPTS
    """
    # Load business logic
    if logic_file is None:
        logic_file = Path(__file__).parent.parent.parent / "biz_logic" / "logic2.md"

    logic_content = logic_file.read_text(encoding="utf-8") if logic_file.exists() else ""

    # Build prompts
    system_prompt = _build_system_prompt(logic_content)
    user_prompt = f"Convert this schema to a scenario config:\n\n{json.dumps(schema, indent=2)}"

    client = GeminiClient()
    last_error = None

    for attempt in range(1, MAX_CONFIG_ATTEMPTS + 1):
        try:
            # Call LLM
            response = client.chat(user_prompt=user_prompt, system_prompt=system_prompt)

            # Parse response (strip markdown fences if present)
            response = response.strip()
            if response.startswith("```"):
                lines = response.split("\n")
                response = "\n".join(lines[1:-1]) if len(lines) > 2 else response
                if response.startswith("json"):
                    response = response[4:].strip()

            config = json.loads(response)

            # Add computed fields
            config["seed"] = seed
            order = table_order(schema)
            config["generation_order"] = order
            config["table_counts"] = table_counts(schema, order, base_records)

            # Validate before returning
            errors = validate_config(config)
            if not errors:
                return config

            # If validation failed, add feedback to prompt for next attempt
            last_error = f"Validation errors: {'; '.join(errors)}"
            user_prompt = f"{user_prompt}\n\nPrevious attempt failed with errors:\n{last_error}\n\nPlease fix these issues."

        except json.JSONDecodeError as e:
            last_error = f"JSON parse error: {str(e)}"
            user_prompt = f"{user_prompt}\n\nPrevious attempt returned invalid JSON: {str(e)}\n\nPlease output valid JSON only."
        except Exception as e:
            last_error = str(e)

        if attempt < MAX_CONFIG_ATTEMPTS:
            print(f"⚠️  Attempt {attempt} failed: {last_error}. Retrying...")

    raise ValueError(f"Failed to generate valid config after {MAX_CONFIG_ATTEMPTS} attempts. Last error: {last_error}")


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

    if output_path:
        save_config(config, output_path)

    return config, errors
