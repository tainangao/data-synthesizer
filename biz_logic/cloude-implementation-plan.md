# Synthetic Data Generator — Implementation Prompt

## Context
I have a two-part synthetic data generation system. Part 1 (already complete) uses an LLM to generate a JSON schema describing multiple tables for a given business scenario. Part 2 (your task) must generate realistic synthetic data from that schema.

The architecture to implement is a **two-step pipeline**:
1. **Schema → Config translation** (LLM-assisted, runs once): parse the JSON schema and emit a structured `scenario_config.json` describing state machines, distributions, and field mappings
2. **Simulation Engine** (deterministic, runs at scale): consume the config and simulate entity lifecycles day-by-day, producing output tables

---

## Step 1: Schema → Config Translator (`translate_config.py`)

Write a script that:
- Accepts a `schema.json` as input (the output of Part 1)
- Sends it to the Anthropic API (claude-sonnet-4-20250514) along with a system prompt instructing the model to emit **only** a valid `scenario_config.json` (no prose, no markdown fences)
- Saves the resulting config to disk

### Config Schema (`scenario_config.json`)
The LLM must produce a config with the following top-level structure:

```json
{
  "scenario_name": "string",
  "seed": 42,
  "simulation": {
    "start_date": "YYYY-MM-DD",
    "end_date": "YYYY-MM-DD"
  },
  "entities": {
    "<entity_name>": {
      "count": 1000,
      "fields": {
        "<field_name>": {
          "distribution": "normal | lognormal | uniform | poisson | choice | constant | date_offset",
          "params": {}
        }
      }
    }
  },
  "state_machines": {
    "<entity_name>": {
      "state_field": "<field_name>",
      "initial_state": "string",
      "terminal_states": ["string"],
      "transitions": {
        "<from_state>": {
          "<to_state>": {
            "base_prob": 0.0,
            "adjustments": [
              {
                "field": "<field_name>",
                "direction": "higher_increases | higher_decreases",
                "strength": "weak | moderate | strong"
              }
            ]
          }
        }
      }
    }
  },
  "events": {
    "<event_table_name>": {
      "emitted_by": "<entity_name>",
      "emit_when_states": ["string"],
      "frequency": {
        "distribution": "poisson",
        "lambda_base": 1.0,
        "lambda_modifiers": [
          {
            "field": "<field_name>",
            "effect": "higher_increases | higher_decreases"
          }
        ]
      },
      "fields": {
        "<field_name>": {
          "distribution": "string",
          "params": {}
        }
      }
    }
  },
  "constraints": [
    {
      "type": "temporal_order",
      "fields": ["date_field_1", "date_field_2"]
    },
    {
      "type": "no_events_after_terminal",
      "entity": "<entity_name>",
      "event_table": "<event_table_name>"
    },
    {
      "type": "running_balance",
      "credit_field": "<field_name>",
      "debit_field": "<field_name>",
      "balance_field": "<field_name>"
    }
  ]
}
```

---

## Step 2: Simulation Engine (`engine.py`)

Build a **scenario-agnostic** simulation engine. It must:

### 2a. Entity Generation
- Spawn entities defined in `config["entities"]`
- Sample each field using the distribution specified:
  - `normal` → `numpy.random.normal(mean, std)`
  - `lognormal` → `numpy.random.lognormal(mean, sigma)`
  - `uniform` → `numpy.random.uniform(low, high)`
  - `poisson` → `numpy.random.poisson(lambda)`
  - `choice` → `numpy.random.choice(values, p=weights)`
  - `constant` → fixed value
  - `date_offset` → `start_date + random offset in days`
- Respect the global `seed` for reproducibility

### 2b. State Machine Runner
- For each simulated day, for each entity not in a terminal state:
  - Look up the transition probabilities from `config["state_machines"]`
  - Apply feature-based adjustments:
    - `higher_increases`: multiply base prob by `(1 + strength_factor * normalized_field_value)`
    - `higher_decreases`: multiply base prob by `(1 - strength_factor * normalized_field_value)`
    - Strength factors: `weak=0.2`, `moderate=0.5`, `strong=1.0`
  - Re-normalize probabilities across all target states
  - Sample next state using `numpy.random.choice`
  - Track state history with timestamps

### 2c. Event Generator
- On each simulated day, for each entity in an emit-eligible state:
  - Compute λ for that entity by applying `lambda_modifiers` to `lambda_base`
  - Draw number of events from `Poisson(λ)`
  - For each event, sample all event fields per their distributions
  - Stamp each event with the entity ID and current simulation date

### 2d. Constraint Enforcer (post-processing pass)
- `temporal_order`: assert and sort rows so date ordering is respected across specified fields
- `no_events_after_terminal`: drop any events timestamped after the entity's terminal state entry date
- `running_balance`: recompute balance column as cumulative `credits - debits` per entity, sorted by date
- Log a warning (do not raise) for any constraint violation found before correction

### 2e. Output
- Write each entity table and each event table as a separate `.csv` file into an `./output/` directory
- Print a summary: row counts per table, date range, state distribution per entity type

---

## Entry Point (`main.py`)

```python
# Usage:
# python main.py --schema schema.json --config scenario_config.json --output ./output
# python main.py --schema schema.json --translate-only   # runs Step 1 only
# python main.py --config scenario_config.json           # runs Step 2 only (skip translation)
```

---

## Additional Requirements

- Use only standard libraries + `numpy`, `pandas`, `faker`, `anthropic`
- The engine must be **fully stateless between runs** — same seed = same output
- All classes and functions must have docstrings
- Add a `validate_config(config: dict) -> list[str]` function that returns a list of human-readable errors if the config is malformed
- Keep `translate_config.py`, `engine.py`, and `main.py` as separate files

---

## Reference: Business Logic

The following scenarios and their logic rules should inform the system prompt you write for the config translator. The LLM should use these to populate transition matrices, distributions, and constraints correctly when it sees a matching schema.

```
[PASTE CONTENTS OF logic2.md HERE]
```
