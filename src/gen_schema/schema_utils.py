from collections import defaultdict

from src.utils.common import tokens


def pk_column(table: dict) -> dict | None:
    for col in table["columns"]:
        if col.get("primary_key"):
            return col
    return None


def table_order(schema: dict) -> list[str]:
    tables = {t["name"]: t for t in schema["tables"]}
    deps = {
        name: {
            c["foreign_key"]["table"] for c in table["columns"] if c.get("foreign_key")
        }
        for name, table in tables.items()
    }

    order: list[str] = []
    while deps:
        ready = sorted(name for name, need in deps.items() if need.issubset(set(order)))
        if not ready:
            order.extend(sorted(deps.keys()))
            break
        for name in ready:
            order.append(name)
            deps.pop(name)
    return order


def table_counts(schema: dict, order: list[str], base_records: int) -> dict[str, int]:
    references = defaultdict(set)
    in_degree: dict[str, int] = {name: 0 for name in order}

    for table in schema["tables"]:
        name = table["name"]
        for col in table["columns"]:
            fk = col.get("foreign_key")
            if fk:
                parent = fk["table"]
                in_degree[name] += 1
                references[parent].add(name)

    counts: dict[str, int] = {}
    for name in order:
        table_tokens = tokens(name)
        out_degree = len(references[name])
        indeg = in_degree[name]

        if table_tokens & {
            "lookup",
            "reference",
            "ref",
            "type",
            "category",
            "status",
            "dimension",
            "currency",
            "product",
            "instrument",
            "asset",
        }:
            multiplier = 0.2
        elif table_tokens & {
            "transaction",
            "interaction",
            "event",
            "execution",
            "repayment",
            "payment",
            "order",
            "trade",
            "history",
            "log",
        }:
            multiplier = 2.5
        elif table_tokens & {"account", "loan", "contract", "portfolio", "position"}:
            multiplier = 1.4
        elif indeg == 0 and out_degree > 0:
            multiplier = 1.0
        elif indeg > 0 and out_degree == 0:
            multiplier = 1.6
        else:
            multiplier = 1.0

        minimum = 8 if multiplier < 1 else 1
        counts[name] = max(minimum, int(base_records * multiplier))

    return counts
