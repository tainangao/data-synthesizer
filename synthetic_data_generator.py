import argparse
import csv
import json
import random
import re
from collections import defaultdict
from datetime import date
from pathlib import Path

from faker import Faker


def _pk_column(table: dict) -> str | None:
    for col in table["columns"]:
        if col.get("primary_key"):
            return col["name"]
    return None


def _table_order(schema: dict) -> list[str]:
    tables = {t["name"]: t for t in schema["tables"]}
    deps = {
        name: {
            c["foreign_key"]["table"] for c in table["columns"] if c.get("foreign_key")
        }
        for name, table in tables.items()
    }
    order: list[str] = []
    while deps:
        ready = sorted(
            [name for name, need in deps.items() if need.issubset(set(order))]
        )
        if not ready:
            order.extend(list(deps.keys()))
            break
        for name in ready:
            order.append(name)
            deps.pop(name)
    return order


def _table_counts(order: list[str], base_records: int) -> dict[str, int]:
    counts: dict[str, int] = {}
    for name in order:
        lower = name.lower()
        if "product" in lower:
            counts[name] = max(8, base_records // 20)
        elif "account" in lower:
            counts[name] = max(base_records, int(base_records * 1.8))
        elif "interaction" in lower:
            counts[name] = max(base_records, int(base_records * 3))
        else:
            counts[name] = base_records
    return counts


def _default_value(fake: Faker, col: dict) -> object:
    name = col["name"].lower()
    dtype = col.get("type", "TEXT").upper()
    role = col.get("field_role", "")

    if "email" in name:
        return fake.email()
    if "phone" in name:
        return fake.phone_number()
    if "address" in name:
        return fake.address().replace("\n", ", ")
    if "first_name" in name:
        return fake.first_name()
    if "last_name" in name:
        return fake.last_name()

    if dtype == "BOOLEAN" or role == "boolean":
        return random.random() < 0.8
    if dtype == "DATE":
        return fake.date_between(start_date="-10y", end_date="today").isoformat()
    if dtype == "TIMESTAMP":
        return fake.date_time_between(start_date="-3y", end_date="now").isoformat(
            sep=" "
        )
    if dtype in {"NUMERIC", "REAL"} or role == "numerical":
        return round(random.lognormvariate(8, 0.9), 2)
    if dtype == "INTEGER" or role == "identifier":
        return random.randint(1, 1_000_000)
    if dtype == "JSON":
        return {"value": fake.word(), "source": "faker"}
    if dtype == "XML":
        return f"<meta><value>{fake.word()}</value></meta>"
    if role == "categorical":
        return random.choice(["A", "B", "C", "D"])
    return fake.word()


def _apply_nullable(col: dict, value: object) -> object:
    if col.get("nullable") and value is not None and random.random() < 0.08:
        return None
    return value


def _gen_customers(table: dict, n: int, fake: Faker, state: dict) -> list[dict]:
    rows: list[dict] = []
    for i in range(1, n + 1):
        first = fake.first_name()
        last = fake.last_name()
        segment = random.choices(
            ["Mass Market", "Affluent", "SME", "Student"],
            weights=[55, 20, 15, 10],
            k=1,
        )[0]
        onboarding = fake.date_between(start_date="-10y", end_date="today").isoformat()
        values = {
            "customer_id": i,
            "first_name": first,
            "last_name": last,
            "date_of_birth": fake.date_of_birth(
                minimum_age=18, maximum_age=85
            ).isoformat(),
            "email": f"{first.lower()}.{last.lower()}.{i}@{fake.free_email_domain()}",
            "phone_number": fake.phone_number(),
            "address": fake.address().replace("\n", ", "),
            "customer_segment": segment,
            "onboarding_date": onboarding,
            "customer_status": random.choices(
                ["Active", "Dormant", "Prospect", "Churned"],
                weights=[72, 12, 10, 6],
                k=1,
            )[0],
            "customer_preferences": {
                "preferred_channel": random.choice(
                    ["Mobile App", "Branch", "Phone", "Email"]
                ),
                "language": random.choice(["en", "es", "zh"]),
                "marketing_opt_in": random.random() < 0.65,
                "risk_appetite": random.choice(["low", "medium", "high"]),
            },
        }
        row: dict[str, object] = {}
        for col in table["columns"]:
            value = values.get(col["name"], _default_value(fake, col))
            row[col["name"]] = _apply_nullable(col, value)
        rows.append(row)

    state["customer_segment"] = {r["customer_id"]: r["customer_segment"] for r in rows}
    state["customer_onboarding"] = {
        r["customer_id"]: r["onboarding_date"] for r in rows
    }
    return rows


def _gen_products(table: dict, n: int, fake: Faker, state: dict) -> list[dict]:
    catalog = [
        ("Everyday Checking", "Checking", 0.10),
        ("Premier Checking", "Checking", 0.20),
        ("High Yield Savings", "Savings", 3.20),
        ("Youth Savings", "Savings", 1.50),
        ("Rewards Credit Card", "Credit Card", 21.90),
        ("Cashback Credit Card", "Credit Card", 19.40),
        ("Personal Loan", "Personal Loan", 10.20),
        ("Auto Loan", "Personal Loan", 7.10),
        ("Home Mortgage", "Mortgage", 5.10),
        ("Index Portfolio", "Investment", 0.00),
        ("Term Deposit 12M", "Term Deposit", 4.00),
        ("Term Deposit 24M", "Term Deposit", 4.30),
    ]
    rows: list[dict] = []
    for i in range(1, n + 1):
        base = catalog[(i - 1) % len(catalog)]
        values = {
            "product_id": i,
            "product_name": base[0],
            "product_type": base[1],
            "description": fake.sentence(nb_words=10),
            "interest_rate": base[2],
            "is_active": random.random() < 0.9,
            "features": {
                "mobile_access": True,
                "fees": random.choice(["low", "medium", "high"]),
                "digital_onboarding": random.random() < 0.85,
            },
        }
        row: dict[str, object] = {}
        for col in table["columns"]:
            value = values.get(col["name"], _default_value(fake, col))
            row[col["name"]] = _apply_nullable(col, value)
        rows.append(row)

    product_ids_by_type: dict[str, list[int]] = defaultdict(list)
    for r in rows:
        product_ids_by_type[r["product_type"]].append(r["product_id"])
    state["product_ids_by_type"] = product_ids_by_type
    return rows


def _pick_product_for_segment(
    segment: str, product_ids_by_type: dict[str, list[int]]
) -> int:
    preferences = {
        "Mass Market": ["Checking", "Savings", "Credit Card", "Personal Loan"],
        "Affluent": ["Investment", "Mortgage", "Checking", "Savings"],
        "SME": ["Checking", "Personal Loan", "Credit Card", "Savings"],
        "Student": ["Checking", "Savings", "Credit Card"],
    }
    for product_type in preferences.get(segment, []):
        options = product_ids_by_type.get(product_type)
        if options:
            return random.choice(options)
    fallback = [pid for ids in product_ids_by_type.values() for pid in ids]
    return random.choice(fallback)


def _balance_for_product_type(product_type: str) -> float:
    if product_type == "Mortgage":
        return round(random.uniform(80_000, 900_000), 2)
    if product_type == "Personal Loan":
        return round(random.uniform(1_000, 120_000), 2)
    if product_type == "Credit Card":
        return round(random.uniform(0, 30_000), 2)
    if product_type == "Investment":
        return round(random.uniform(2_000, 1_500_000), 2)
    if product_type == "Checking":
        return round(random.uniform(100, 50_000), 2)
    return round(random.uniform(200, 250_000), 2)


def _gen_accounts(table: dict, n: int, fake: Faker, state: dict) -> list[dict]:
    customer_ids = state["pk_values"]["Customers"]
    product_rows = state["rows"].get("Products", [])
    product_type_by_id = {r["product_id"]: r["product_type"] for r in product_rows}
    product_ids_by_type = state.get("product_ids_by_type", {})

    rows: list[dict] = []
    used_account_numbers: set[str] = set()
    for i in range(1, n + 1):
        customer_id = random.choice(customer_ids)
        segment = state["customer_segment"].get(customer_id, "Mass Market")
        product_id = _pick_product_for_segment(segment, product_ids_by_type)
        product_type = product_type_by_id.get(product_id, "Checking")

        onboarding = date.fromisoformat(
            state["customer_onboarding"].get(customer_id, "2020-01-01")
        )
        opening_date_obj = fake.date_between(start_date=onboarding, end_date="today")
        opening_date = opening_date_obj.isoformat()

        account_number = str(random.randint(10**11, 10**12 - 1))
        while account_number in used_account_numbers:
            account_number = str(random.randint(10**11, 10**12 - 1))
        used_account_numbers.add(account_number)

        account_status = random.choices(
            ["Active", "Dormant", "Closed", "Frozen"],
            weights=[76, 12, 9, 3],
            k=1,
        )[0]
        values = {
            "account_id": i,
            "customer_id": customer_id,
            "product_id": product_id,
            "account_number": account_number,
            "opening_date": opening_date,
            "current_balance": _balance_for_product_type(product_type),
            "account_status": account_status,
            "currency": random.choices(
                ["USD", "EUR", "GBP", "SGD"], weights=[78, 10, 8, 4], k=1
            )[0],
            "last_transaction_date": None
            if account_status == "Closed"
            else fake.date_between(
                start_date=opening_date_obj, end_date="today"
            ).isoformat(),
        }
        row: dict[str, object] = {}
        for col in table["columns"]:
            value = values.get(col["name"], _default_value(fake, col))
            row[col["name"]] = _apply_nullable(col, value)
        rows.append(row)
    return rows


def _gen_interactions(table: dict, n: int, fake: Faker, state: dict) -> list[dict]:
    customer_ids = state["pk_values"]["Customers"]
    outcomes_by_type = {
        "Call": ["Resolved", "Follow-up Needed", "Escalated"],
        "Email": ["Resolved", "Pending", "No Response"],
        "Branch Visit": ["Resolved", "Escalated"],
        "Complaint": ["Resolved", "Escalated", "Pending"],
        "Chat": ["Resolved", "Pending"],
    }
    rows: list[dict] = []
    for i in range(1, n + 1):
        interaction_type = random.choices(
            ["Call", "Email", "Branch Visit", "Complaint", "Chat"],
            weights=[35, 28, 15, 10, 12],
            k=1,
        )[0]
        outcome = random.choice(outcomes_by_type[interaction_type])
        values = {
            "interaction_id": i,
            "customer_id": random.choice(customer_ids),
            "interaction_timestamp": fake.date_time_between(
                start_date="-2y", end_date="now"
            ).isoformat(sep=" "),
            "interaction_type": interaction_type,
            "agent_id": random.randint(1000, 1999),
            "subject": f"{interaction_type} regarding account services",
            "notes": fake.sentence(nb_words=14),
            "outcome": outcome,
            "sentiment": "Positive"
            if outcome == "Resolved"
            else random.choice(["Neutral", "Negative"]),
        }
        row: dict[str, object] = {}
        for col in table["columns"]:
            value = values.get(col["name"], _default_value(fake, col))
            row[col["name"]] = _apply_nullable(col, value)
        rows.append(row)
    return rows


def _gen_generic(table: dict, n: int, fake: Faker, state: dict) -> list[dict]:
    rows: list[dict] = []
    for i in range(1, n + 1):
        row: dict[str, object] = {}
        for col in table["columns"]:
            fk = col.get("foreign_key")
            if col.get("primary_key"):
                value = i
            elif fk:
                parent_values = state["pk_values"].get(fk["table"], [])
                value = random.choice(parent_values) if parent_values else None
            else:
                value = _default_value(fake, col)
            row[col["name"]] = _apply_nullable(col, value)
        rows.append(row)
    return rows


def generate_data(schema: dict, records: int, seed: int) -> dict[str, list[dict]]:
    random.seed(seed)
    fake = Faker()
    fake.seed_instance(seed)

    tables = {t["name"]: t for t in schema["tables"]}
    order = _table_order(schema)
    counts = _table_counts(order, records)
    state = {
        "rows": {},
        "pk_values": {},
        "customer_segment": {},
        "customer_onboarding": {},
        "product_ids_by_type": {},
    }

    for name in order:
        table = tables[name]
        n = counts[name]
        lower = name.lower()
        if "customer" in lower and "interaction" not in lower:
            rows = _gen_customers(table, n, fake, state)
        elif "product" in lower:
            rows = _gen_products(table, n, fake, state)
        elif "account" in lower:
            rows = _gen_accounts(table, n, fake, state)
        elif "interaction" in lower:
            rows = _gen_interactions(table, n, fake, state)
        else:
            rows = _gen_generic(table, n, fake, state)

        state["rows"][name] = rows
        pk = _pk_column(table)
        if pk:
            state["pk_values"][name] = [r[pk] for r in rows]

    return state["rows"]


def _safe_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def write_tables(
    rows_by_table: dict[str, list[dict]], schema: dict, out_dir: Path
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = {t["name"]: t for t in schema["tables"]}
    summary: dict[str, int] = {}

    for table_name, rows in rows_by_table.items():
        table = tables[table_name]
        fieldnames = [c["name"] for c in table["columns"]]
        csv_path = out_dir / f"{_safe_name(table_name)}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                serialized = {
                    k: json.dumps(v) if isinstance(v, (dict, list)) else v
                    for k, v in row.items()
                }
                writer.writerow(serialized)

        summary[table_name] = len(rows)

    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic data from schema JSON"
    )
    parser.add_argument(
        "--schema", default="output/crm.json", help="Path to schema JSON"
    )
    parser.add_argument(
        "--out-dir",
        default="output/synthetic",
        help="Output directory for generated tables",
    )
    parser.add_argument("--records", type=int, default=500, help="Base record count")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    schema = json.loads(Path(args.schema).read_text(encoding="utf-8"))
    rows_by_table = generate_data(schema=schema, records=args.records, seed=args.seed)
    write_tables(rows_by_table=rows_by_table, schema=schema, out_dir=Path(args.out_dir))
    print(f"Generated data in {args.out_dir}")


if __name__ == "__main__":
    main()
