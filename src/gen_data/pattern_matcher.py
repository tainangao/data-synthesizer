"""Rule-based pattern matching for schema-to-config translation."""

from datetime import datetime, timedelta


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


def build_crm_config(schema: dict) -> dict:
    """Build CRM scenario config."""
    tables = {t["name"]: t for t in schema["tables"]}

    state_machines = []
    events = []
    constraints = []

    # Find any table with status field for state machine
    for table_name, table in tables.items():
        status_field = find_status_field(table)
        if status_field:
            state_machines.append({
                "entity": table_name,
                "state_field": status_field,
                "initial_state": "Pending",
                "terminal_states": ["Closed"],
                "transitions": {
                    "Pending": [
                        {"to_state": "Active", "base_prob": 0.85, "adjustments": []},
                        {"to_state": "Closed", "base_prob": 0.15, "adjustments": []}
                    ],
                    "Active": [
                        {"to_state": "Active", "base_prob": 0.85, "adjustments": []},
                        {"to_state": "Dormant", "base_prob": 0.14, "adjustments": []},
                        {"to_state": "Closed", "base_prob": 0.01, "adjustments": []}
                    ],
                    "Dormant": [
                        {"to_state": "Active", "base_prob": 0.1, "adjustments": []},
                        {"to_state": "Dormant", "base_prob": 0.8, "adjustments": []},
                        {"to_state": "Closed", "base_prob": 0.1, "adjustments": []}
                    ]
                }
            })
            break  # Use first table with status

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
        "key_distributions": {}
    }

    return {
        "state_machines": state_machines,
        "events": events,
        "constraints": constraints,
        "key_distributions": {}
    }


def build_credit_config(schema: dict) -> dict:
    """Build credit risk scenario config."""
    tables = {t["name"]: t for t in schema["tables"]}

    state_machines = []
    events = []
    constraints = []

    # Find any table with status field for state machine
    for table_name, table in tables.items():
        status_field = find_status_field(table)
        if status_field:
            state_machines.append({
                "entity": table_name,
                "state_field": status_field,
                "initial_state": "Current",
                "terminal_states": ["Paid in Full", "Charged-off"],
                "transitions": {
                    "Current": [
                        {"to_state": "Current", "base_prob": 0.94, "adjustments": []},
                        {"to_state": "Delinquent", "base_prob": 0.03, "adjustments": []},
                        {"to_state": "Paid in Full", "base_prob": 0.03, "adjustments": []}
                    ],
                    "Delinquent": [
                        {"to_state": "Current", "base_prob": 0.2, "adjustments": []},
                        {"to_state": "Delinquent", "base_prob": 0.6, "adjustments": []},
                        {"to_state": "Default", "base_prob": 0.15, "adjustments": []},
                        {"to_state": "Paid in Full", "base_prob": 0.05, "adjustments": []}
                    ],
                    "Default": [
                        {"to_state": "Current", "base_prob": 0.05, "adjustments": []},
                        {"to_state": "Delinquent", "base_prob": 0.1, "adjustments": []},
                        {"to_state": "Default", "base_prob": 0.7, "adjustments": []},
                        {"to_state": "Charged-off", "base_prob": 0.15, "adjustments": []}
                    ]
                }
            })
            break  # Use first table with status

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
        "key_distributions": {}
    }

    return {
        "state_machines": state_machines,
        "events": events,
        "constraints": constraints,
        "key_distributions": {}
    }


def build_trading_config(schema: dict) -> dict:
    """Build trading scenario config."""
    tables = {t["name"]: t for t in schema["tables"]}

    state_machines = []
    events = []
    constraints = []

    # Find any table with status field for state machine
    for table_name, table in tables.items():
        status_field = find_status_field(table)
        if status_field:
            state_machines.append({
                "entity": table_name,
                "state_field": status_field,
                "initial_state": "Open",
                "terminal_states": ["Filled", "Cancelled"],
                "transitions": {
                    "Open": [
                        {"to_state": "Open", "base_prob": 0.4, "adjustments": []},
                        {"to_state": "Partial Fill", "base_prob": 0.2, "adjustments": []},
                        {"to_state": "Filled", "base_prob": 0.3, "adjustments": []},
                        {"to_state": "Cancelled", "base_prob": 0.1, "adjustments": []}
                    ],
                    "Partial Fill": [
                        {"to_state": "Partial Fill", "base_prob": 0.3, "adjustments": []},
                        {"to_state": "Filled", "base_prob": 0.6, "adjustments": []},
                        {"to_state": "Cancelled", "base_prob": 0.1, "adjustments": []}
                    ]
                }
            })
            break  # Use first table with status

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
        "key_distributions": {}
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
