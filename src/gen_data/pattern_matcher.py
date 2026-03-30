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
    event_patterns = ["transaction", "interaction", "payment", "repayment", "execution", "trade", "history"]
    event_tables = []

    for table in schema["tables"]:
        table_name = table["name"]
        if table_name != entity_table:
            if any(pattern in table_name.lower() for pattern in event_patterns):
                event_tables.append(table_name)

    return event_tables


def find_categorical_fields(table: dict) -> list[str]:
    """Find categorical fields for adjustments."""
    categoricals = []
    columns = table.get("columns", table.get("fields", []))
    for col in columns:
        if col.get("field_role") == "categorical":
            categoricals.append(col["name"])
    return categoricals


def find_numerical_fields(table: dict) -> list[str]:
    """Find numerical fields for distributions."""
    numericals = []
    columns = table.get("columns", table.get("fields", []))
    for col in columns:
        if col.get("field_role") == "numerical":
            numericals.append(col["name"])
    return numericals


def build_crm_config(schema: dict) -> dict:
    """Build CRM scenario config."""
    tables = {t["name"]: t for t in schema["tables"]}

    state_machines = []
    events = []
    constraints = []
    key_distributions = {}

    # Find entity table with status for state machine
    entity_table = None
    for table_name, table in tables.items():
        status_field = find_status_field(table)
        if status_field and table_name not in relationships:  # Parent table
            entity_table = table_name
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
            break

    # Find event tables
    if entity_table:
        event_tables = detect_event_tables(schema, entity_table)
        for event_table in event_tables:
            events.append({
                "event_table": event_table,
                "emitted_by": entity_table,
                "emit_when_states": ["Active"],
                "lambda_base": 5.0,
                "lambda_modifiers": []
            })

    # Add key distributions for customer table
    customer_table = next((n for n in tables if "customer" in n.lower()), None)
    if customer_table:
        key_distributions[customer_table] = {
            "age": {"distribution": "normal", "params": {"mean": 35, "std": 12}},
            "income": {"distribution": "lognormal", "params": {"mean": 60000, "sigma": 0.5}}
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


def build_credit_config(schema: dict) -> dict:
    """Build credit risk scenario config."""
    tables = {t["name"]: t for t in schema["tables"]}

    state_machines = []
    events = []
    constraints = []
    key_distributions = {}

    # Find loan/application table with status
    loan_table = None
    for table_name, table in tables.items():
        if any(x in table_name.lower() for x in ["loan", "application"]):
            status_field = find_status_field(table)
            if status_field:
                loan_table = table_name
                # Use appropriate states based on table type
                if "application" in table_name.lower():
                    state_machines.append({
                        "entity": table_name,
                        "state_field": status_field,
                        "initial_state": "Pending",
                        "terminal_states": ["Approved", "Rejected"],
                        "transitions": {
                            "Pending": [
                                {"to_state": "Approved", "base_prob": 0.7, "adjustments": []},
                                {"to_state": "Rejected", "base_prob": 0.3, "adjustments": []}
                            ]
                        }
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
                break

    # Find payment/repayment event tables
    if loan_table:
        event_tables = detect_event_tables(schema, loan_table)
        for event_table in event_tables:
            events.append({
                "event_table": event_table,
                "emitted_by": loan_table,
                "emit_when_states": ["Current", "Delinquent"],
                "lambda_base": 1.0,
                "lambda_modifiers": []
            })

    # Add key distributions for customer table
    customer_table = next((n for n in tables if "customer" in n.lower()), None)
    if customer_table:
        key_distributions[customer_table] = {
            "credit_score": {"distribution": "normal", "params": {"mean": 650, "std": 100}},
            "income": {"distribution": "lognormal", "params": {"mean": 60000, "sigma": 0.5}}
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
                break

    # Find execution/trade event tables
    if order_table:
        event_tables = detect_event_tables(schema, order_table)
        for event_table in event_tables:
            events.append({
                "event_table": event_table,
                "emitted_by": order_table,
                "emit_when_states": ["Open", "Partial Fill"],
                "lambda_base": 2.0,
                "lambda_modifiers": []
            })

    # Add key distributions for trader table
    trader_table = next((n for n in tables if "trader" in n.lower() or "customer" in n.lower()), None)
    if trader_table:
        key_distributions[trader_table] = {
            "trader_type": {"distribution": "categorical", "params": {"categories": ["Retail", "Institutional", "HFT"]}}
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
