"""Rule-based pattern matching for schema-to-config translation."""

import json
from datetime import datetime, timedelta
from pathlib import Path


def load_scenario_config(scenario: str, config_type: str) -> dict | None:
    """Load scenario config from scenarios directory."""
    scenarios_dir = Path(__file__).parent / "scenarios"
    config_file = scenarios_dir / f"{scenario}_{config_type}.json"
    if config_file.exists():
        return json.loads(config_file.read_text())
    return None


def detect_scenario(schema: dict) -> str:
    """Detect business scenario from schema."""
    tables = {t["name"].lower() for t in schema["tables"]}

    # Check for loan/credit patterns (partial match)
    if any(x in name for name in tables for x in ["loan", "credit", "repayment", "delinquenc"]):
        return "credit_risk"

    # Check for trading patterns (partial match)
    if any(x in name for name in tables for x in ["order", "trade", "execution", "settlement", "portfolio"]):
        return "trading"

    # Default to CRM
    return "crm"


def find_status_field(table: dict) -> str | None:
    """Find status/state field in table."""
    columns = table.get("columns", table.get("fields", []))
    for col in columns:
        name = col["name"].lower()
        # Match exact patterns: status, state, or *_status/*_state
        if name == "status" or name == "state" or name.endswith("_status") or name.endswith("_state"):
            # Exclude employment_status, marital_status, etc.
            if name not in ["employment_status", "marital_status", "relationship_status"]:
                return col["name"]
    return None


def find_date_fields(table: dict) -> list[str]:
    """Find date fields in table."""
    dates = []
    columns = table.get("columns", table.get("fields", []))
    for col in columns:
        if col["type"] in ["DATE", "TIMESTAMP"]:
            dates.append(col["name"])
    return dates


def find_fk_relationships(schema: dict) -> dict[str, str]:
    """Find parent-child relationships via foreign keys. Returns {child_table: parent_table}."""
    relationships = {}
    for table in schema["tables"]:
        columns = table.get("columns", table.get("fields", []))
        for col in columns:
            fk = col.get("foreign_key")
            if fk and fk != "null" and fk is not None:
                if "." in str(fk):
                    parent_table = str(fk).split(".")[0]
                    relationships[table["name"]] = parent_table
                    break
    return relationships


def detect_event_tables(schema: dict, entity_table: str) -> list[str]:
    """Detect event tables by name patterns."""
    # These are event/fact table prefixes — must match a word segment, not a substring
    # e.g. "TradeExecutions" matches "execution", not "trade" (which would match "Traders")
    event_patterns = ["transaction", "interaction", "payment", "repayment", "execution", "settlement", "history"]
    entity_patterns = ["trader", "customer", "instrument", "portfolio", "borrower", "account"]
    event_tables = []

    for table in schema["tables"]:
        table_name = table["name"]
        table_lower = table_name.lower()
        if table_name == entity_table:
            continue
        # Skip tables that look like entity/master tables
        if any(pattern in table_lower for pattern in entity_patterns):
            continue
        if any(pattern in table_lower for pattern in event_patterns):
            event_tables.append(table_name)

    return event_tables


def find_categorical_fields(table: dict) -> list[str]:
    """Find categorical fields for adjustments."""
    categoricals = []
    columns = table.get("columns", table.get("fields", []))
    for col in columns:
        if col.get("field_role") == "categorical":
            name = col["name"].lower()
            # Exclude status fields and IDs
            if "status" not in name and "state" not in name and "id" not in name:
                categoricals.append(col["name"])
    return categoricals


def find_numerical_fields(table: dict) -> list[str]:
    """Find numerical fields for distributions and adjustments."""
    numericals = []
    columns = table.get("columns", table.get("fields", []))
    for col in columns:
        if col.get("field_role") == "numerical":
            name = col["name"].lower()
            # Exclude IDs
            if "id" not in name:
                numericals.append(col["name"])
    return numericals


def build_adjustments(table: dict, scenario: str) -> list[dict]:
    """Build probability adjustments based on table fields."""
    adjustments = []

    # Add numerical field adjustments (e.g., credit_score, income, balance)
    numericals = find_numerical_fields(table)
    for field in numericals[:2]:  # Limit to 2 most relevant
        field_lower = field.lower()
        if any(x in field_lower for x in ["score", "income", "balance", "amount"]):
            adjustments.append({
                "field": field,
                "direction": "higher_increases",
                "strength": "moderate"
            })

    # Add categorical field adjustments (e.g., segment, risk_level)
    categoricals = find_categorical_fields(table)
    for field in categoricals[:1]:  # Limit to 1
        field_lower = field.lower()
        if any(x in field_lower for x in ["segment", "type", "category", "risk"]):
            adjustments.append({
                "field": field,
                "direction": "higher_increases",
                "strength": "weak"
            })

    return adjustments


def build_lambda_modifiers(table: dict) -> list[dict]:
    """Build lambda modifiers for event frequency."""
    modifiers = []

    # Add numerical modifiers
    numericals = find_numerical_fields(table)
    for field in numericals[:2]:  # Top 2
        field_lower = field.lower()
        if any(x in field_lower for x in ["income", "balance", "score", "value", "amount", "principal", "rate"]):
            modifiers.append({
                "field": field,
                "effect": "higher_increases"
            })

    # Add categorical modifiers
    categoricals = find_categorical_fields(table)
    for field in categoricals[:1]:
        field_lower = field.lower()
        if any(x in field_lower for x in ["segment", "type", "tier", "category", "risk"]):
            modifiers.append({
                "field": field,
                "effect": "higher_increases"
            })

    return modifiers


def build_crm_config(schema: dict) -> dict:
    """Build CRM scenario config."""
    tables = {t["name"]: t for t in schema["tables"]}

    state_machines = []
    events = []
    constraints = []
    key_distributions = {}

    # Find entity table with status for state machine (prefer parent tables)
    entity_table = None
    relationships = find_fk_relationships(schema)

    for table_name, table in tables.items():
        status_field = find_status_field(table)
        if status_field and table_name not in relationships:  # Parent table
            entity_table = table_name

            # Try to load scenario config, fallback to generated
            scenario_config = load_scenario_config("crm", "account")
            if scenario_config and "state_machine" in scenario_config:
                sm = scenario_config["state_machine"]
                # Convert transitions from dict format to list format
                transitions_list = {}
                for from_state, to_states in sm["transitions"].items():
                    transitions_list[from_state] = []
                    for to_state, config in to_states.items():
                        transitions_list[from_state].append({
                            "to_state": to_state,
                            "base_prob": config["base_prob"],
                            "adjustments": config.get("adjustments", [])
                        })
                state_machines.append({
                    "entity": table_name,
                    "state_field": status_field,
                    "initial_state": sm["initial_state"],
                    "terminal_states": ["Closed", "Rejected"],
                    "transitions": transitions_list
                })
            else:
                adjustments = build_adjustments(table, "crm")
                state_machines.append({
                    "entity": table_name,
                    "state_field": status_field,
                    "initial_state": "Pending",
                    "terminal_states": ["Closed"],
                    "transitions": {
                        "Pending": [
                            {"to_state": "Active", "base_prob": 0.85, "adjustments": adjustments},
                            {"to_state": "Closed", "base_prob": 0.15, "adjustments": []}
                        ],
                        "Active": [
                            {"to_state": "Active", "base_prob": 0.85, "adjustments": []},
                            {"to_state": "Dormant", "base_prob": 0.14, "adjustments": adjustments},
                            {"to_state": "Closed", "base_prob": 0.01, "adjustments": []}
                        ],
                        "Dormant": [
                            {"to_state": "Active", "base_prob": 0.1, "adjustments": []},
                            {"to_state": "Dormant", "base_prob": 0.8, "adjustments": []},
                            {"to_state": "Closed", "base_prob": 0.1, "adjustments": adjustments}
                        ]
                    }
                })
            break

    # Find event tables
    if entity_table:
        event_tables = detect_event_tables(schema, entity_table)

        # Try to load scenario event config
        event_config = load_scenario_config("crm", "transactions")
        if event_config and "event" in event_config:
            for event_table in event_tables:
                evt = event_config["event"].copy()
                events.append({
                    "event_table": event_table,
                    "emitted_by": entity_table,
                    "emit_when_states": evt.get("emit_when_states", ["Active"]),
                    "lambda_base": evt["frequency"]["lambda_base"],
                    "lambda_modifiers": evt["frequency"].get("lambda_modifiers", [])
                })
        else:
            entity_table_obj = tables[entity_table]
            modifiers = build_lambda_modifiers(entity_table_obj)
            for event_table in event_tables:
                events.append({
                    "event_table": event_table,
                    "emitted_by": entity_table,
                    "emit_when_states": ["Active"],
                    "lambda_base": 5.0,
                    "lambda_modifiers": modifiers
                })

    # Add key distributions from schema fields
    for table_name, table in tables.items():
        numericals = find_numerical_fields(table)
        categoricals = find_categorical_fields(table)

        if numericals or categoricals:
            if table_name not in key_distributions:
                key_distributions[table_name] = {}

            # Add numerical distributions
            for field in numericals[:3]:
                field_lower = field.lower()
                if "age" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "normal", "params": {"mean": 35, "std": 12}
                    }
                elif "income" in field_lower or "salary" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "lognormal", "params": {"mean": 60000, "sigma": 0.5}
                    }
                elif "balance" in field_lower or "amount" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "lognormal", "params": {"mean": 5000, "sigma": 0.8}
                    }

            # Add categorical distributions
            for field in categoricals[:2]:
                field_lower = field.lower()
                if "segment" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "categorical",
                        "params": {"categories": ["Mass", "Affluent", "SME"]}
                    }
                elif "type" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "categorical",
                        "params": {"categories": ["Standard", "Premium", "Enterprise"]}
                    }

    # Date constraints
    for table in tables.values():
        dates = find_date_fields(table)
        if len(dates) >= 2:
            constraints.append({
                "type": "temporal_order",
                "params": {"fields": dates}
            })

    return {
        "state_machines": state_machines,
        "events": events,
        "constraints": constraints,
        "key_distributions": key_distributions
    }


def build_credit_config(schema: dict) -> dict:
    """Build credit risk scenario config."""
    tables = {t["name"]: t for t in schema["tables"]}

    state_machines = []
    events = []
    constraints = []
    key_distributions = {}

    # Find loan/application tables with status - support multiple state machines
    loan_table = None
    for table_name, table in tables.items():
        if any(x in table_name.lower() for x in ["loan", "application"]):
            status_field = find_status_field(table)
            if status_field:
                adjustments = build_adjustments(table, "credit_risk")

                # Use appropriate states based on table type
                if "application" in table_name.lower():
                    state_machines.append({
                        "entity": table_name,
                        "state_field": status_field,
                        "initial_state": "Pending",
                        "terminal_states": ["Approved", "Rejected"],
                        "transitions": {
                            "Pending": [
                                {"to_state": "Approved", "base_prob": 0.7, "adjustments": adjustments},
                                {"to_state": "Rejected", "base_prob": 0.3, "adjustments": adjustments}
                            ]
                        }
                    })
                else:
                    # Loans table with full lifecycle
                    loan_table = table_name

                    # Try to load scenario config
                    scenario_config = load_scenario_config("credit_risk", "loan")
                    if scenario_config and "state_machine" in scenario_config:
                        sm = scenario_config["state_machine"]
                        # Convert transitions from dict format to list format
                        transitions_list = {}
                        for from_state, to_states in sm["transitions"].items():
                            transitions_list[from_state] = []
                            for to_state, config in to_states.items():
                                transitions_list[from_state].append({
                                    "to_state": to_state,
                                    "base_prob": config["base_prob"],
                                    "adjustments": config.get("adjustments", [])
                                })
                        state_machines.append({
                            "entity": table_name,
                            "state_field": status_field,
                            "initial_state": sm["initial_state"],
                            "terminal_states": ["Paid in Full", "Charged-off"],
                            "transitions": transitions_list
                        })
                    else:
                        state_machines.append({
                            "entity": table_name,
                            "state_field": status_field,
                            "initial_state": "Current",
                            "terminal_states": ["Paid in Full", "Charged-off"],
                            "transitions": {
                            "Current": [
                                {"to_state": "Current", "base_prob": 0.94, "adjustments": []},
                                {"to_state": "Delinquent", "base_prob": 0.03, "adjustments": adjustments},
                                {"to_state": "Paid in Full", "base_prob": 0.03, "adjustments": []}
                            ],
                            "Delinquent": [
                                {"to_state": "Current", "base_prob": 0.2, "adjustments": []},
                                {"to_state": "Delinquent", "base_prob": 0.6, "adjustments": []},
                                {"to_state": "Default", "base_prob": 0.15, "adjustments": adjustments},
                                {"to_state": "Paid in Full", "base_prob": 0.05, "adjustments": []}
                            ],
                            "Default": [
                                {"to_state": "Current", "base_prob": 0.05, "adjustments": []},
                                {"to_state": "Delinquent", "base_prob": 0.1, "adjustments": []},
                                {"to_state": "Default", "base_prob": 0.7, "adjustments": []},
                                {"to_state": "Charged-off", "base_prob": 0.15, "adjustments": adjustments}
                            ]
                        }
                    })

    # Find payment/repayment event tables
    if loan_table:
        event_tables = detect_event_tables(schema, loan_table)

        # Try to load scenario event config
        event_config = load_scenario_config("credit_risk", "payments")
        if event_config and "event" in event_config:
            for event_table in event_tables:
                evt = event_config["event"].copy()
                events.append({
                    "event_table": event_table,
                    "emitted_by": loan_table,
                    "emit_when_states": evt.get("emit_when_states", ["Current", "Delinquent"]),
                    "lambda_base": evt["frequency"]["lambda_base"],
                    "lambda_modifiers": evt["frequency"].get("lambda_modifiers", [])
                })
        else:
            loan_table_obj = tables[loan_table]
            modifiers = build_lambda_modifiers(loan_table_obj)
            for event_table in event_tables:
                events.append({
                    "event_table": event_table,
                    "emitted_by": loan_table,
                    "emit_when_states": ["Current", "Delinquent"],
                    "lambda_base": 1.0,
                    "lambda_modifiers": modifiers
                })

    # Add key distributions from schema fields
    for table_name, table in tables.items():
        numericals = find_numerical_fields(table)
        categoricals = find_categorical_fields(table)

        if numericals or categoricals:
            if table_name not in key_distributions:
                key_distributions[table_name] = {}

            # Add numerical distributions
            for field in numericals[:3]:
                field_lower = field.lower()
                if "score" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "normal", "params": {"mean": 650, "std": 100}
                    }
                elif "income" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "lognormal", "params": {"mean": 60000, "sigma": 0.5}
                    }
                elif "amount" in field_lower or "principal" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "lognormal", "params": {"mean": 25000, "sigma": 0.6}
                    }

            # Add categorical distributions
            for field in categoricals[:2]:
                field_lower = field.lower()
                if "segment" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "categorical",
                        "params": {"categories": ["Prime", "Near-Prime", "Subprime"]}
                    }
                elif "risk" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "categorical",
                        "params": {"categories": ["Low", "Medium", "High"]}
                    }

    # Date constraints
    for table in tables.values():
        dates = find_date_fields(table)
        if len(dates) >= 2:
            constraints.append({
                "type": "temporal_order",
                "params": {"fields": dates}
            })

    return {
        "state_machines": state_machines,
        "events": events,
        "constraints": constraints,
        "key_distributions": key_distributions
    }


def build_trading_config(schema: dict) -> dict:
    """Build trading scenario config."""
    tables = {t["name"]: t for t in schema["tables"]}

    state_machines = []
    events = []
    constraints = []
    key_distributions = {}

    # Find order table with status
    order_table = None
    for table_name, table in tables.items():
        if "order" in table_name.lower():
            status_field = find_status_field(table)
            if status_field:
                order_table = table_name

                # Try to load scenario config
                scenario_config = load_scenario_config("trading", "order")
                if scenario_config and "state_machine" in scenario_config:
                    sm = scenario_config["state_machine"]
                    # Convert transitions from dict format to list format
                    transitions_list = {}
                    for from_state, to_states in sm["transitions"].items():
                        transitions_list[from_state] = []
                        for to_state, config in to_states.items():
                            transitions_list[from_state].append({
                                "to_state": to_state,
                                "base_prob": config["base_prob"],
                                "adjustments": config.get("adjustments", [])
                            })
                    state_machines.append({
                        "entity": table_name,
                        "state_field": status_field,
                        "initial_state": sm["initial_state"],
                        "terminal_states": ["Filled", "Cancelled"],
                        "transitions": transitions_list
                    })
                else:
                    adjustments = build_adjustments(table, "trading")
                    state_machines.append({
                        "entity": table_name,
                        "state_field": status_field,
                        "initial_state": "Open",
                        "terminal_states": ["Filled", "Cancelled"],
                        "transitions": {
                        "Open": [
                            {"to_state": "Open", "base_prob": 0.4, "adjustments": []},
                            {"to_state": "Partial Fill", "base_prob": 0.2, "adjustments": adjustments},
                            {"to_state": "Filled", "base_prob": 0.3, "adjustments": adjustments},
                            {"to_state": "Cancelled", "base_prob": 0.1, "adjustments": []}
                        ],
                        "Partial Fill": [
                            {"to_state": "Partial Fill", "base_prob": 0.3, "adjustments": []},
                            {"to_state": "Filled", "base_prob": 0.6, "adjustments": adjustments},
                            {"to_state": "Cancelled", "base_prob": 0.1, "adjustments": []}
                        ]
                    }
                })
                break

    # Find execution/trade event tables
    if order_table:
        event_tables = detect_event_tables(schema, order_table)

        # Try to load scenario event config
        event_config = load_scenario_config("trading", "orders")
        if event_config and "event" in event_config:
            for event_table in event_tables:
                evt = event_config["event"].copy()
                events.append({
                    "event_table": event_table,
                    "emitted_by": order_table,
                    "emit_when_states": evt.get("emit_when_states", ["Open", "Partial Fill"]),
                    "lambda_base": evt["frequency"]["lambda_base"],
                    "lambda_modifiers": evt["frequency"].get("lambda_modifiers", [])
                })
        else:
            order_table_obj = tables[order_table]
            modifiers = build_lambda_modifiers(order_table_obj)
            for event_table in event_tables:
                events.append({
                    "event_table": event_table,
                    "emitted_by": order_table,
                    "emit_when_states": ["Open", "Partial Fill"],
                    "lambda_base": 2.0,
                    "lambda_modifiers": modifiers
                })

    # Add key distributions from schema fields
    for table_name, table in tables.items():
        numericals = find_numerical_fields(table)
        categoricals = find_categorical_fields(table)

        if numericals or categoricals:
            if table_name not in key_distributions:
                key_distributions[table_name] = {}

            # Add numerical distributions
            for field in numericals[:3]:
                field_lower = field.lower()
                if "quantity" in field_lower or "volume" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "lognormal", "params": {"mean": 1000, "sigma": 0.8}
                    }
                elif "price" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "lognormal", "params": {"mean": 100, "sigma": 0.5}
                    }

            # Add categorical distributions
            for field in categoricals[:2]:
                field_lower = field.lower()
                if "type" in field_lower and "trader" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "categorical",
                        "params": {"categories": ["Retail", "Institutional", "HFT"]}
                    }
                elif "side" in field_lower:
                    key_distributions[table_name][field] = {
                        "distribution": "categorical",
                        "params": {"categories": ["Buy", "Sell"]}
                    }

    # Date constraints
    for table in tables.values():
        dates = find_date_fields(table)
        if len(dates) >= 2:
            constraints.append({
                "type": "temporal_order",
                "params": {"fields": dates}
            })

    return {
        "state_machines": state_machines,
        "events": events,
        "constraints": constraints,
        "key_distributions": key_distributions
    }

    return {
        "state_machines": state_machines,
        "events": events,
        "constraints": constraints,
        "key_distributions": {}
    }


def generate_behavioral_mapping(schema: dict) -> dict:
    """Generate behavioral mapping using pattern matching."""
    scenario = detect_scenario(schema)

    # Get scenario-specific config
    if scenario == "credit_risk":
        mapping = build_credit_config(schema)
    elif scenario == "trading":
        mapping = build_trading_config(schema)
    else:
        mapping = build_crm_config(schema)

    # Add metadata
    today = datetime.now()
    mapping["scenario_name"] = scenario
    mapping["start_date"] = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    mapping["end_date"] = today.strftime("%Y-%m-%d")

    return mapping
