from datetime import datetime, timedelta
import random

from faker import Faker

from src.common import parse_datetime, random_datetime, safe_name, tokens
from .relationship_rules import conditioned_relationship_value


def apply_nullable(col: dict, value: object) -> object:
    if not col.get("nullable"):
        return value

    role = str(col.get("field_role", "")).lower()
    dtype = str(col.get("type", "TEXT")).upper()
    null_rate = 0.06
    if role in {"semi_structured", "text"} or dtype in {"JSON", "XML"}:
        null_rate = 0.12

    if value is not None and random.random() < null_rate:
        return None
    return value


SEMANTIC_TOKEN_GROUPS: tuple[set[str], ...] = (
    {"type"},
    {"status", "state"},
    {"segment"},
    {"risk", "rating", "tier"},
    {"currency"},
    {"country"},
    {"date", "time", "timestamp"},
)


def _tokens_semantically_compatible(
    target_tokens: set[str], candidate_tokens: set[str]
) -> bool:
    # Prevent cross-domain inheritance when names share generic tokens only.
    # Example bug we want to avoid: loan_type inheriting loan_purpose_* just
    # because both include the token "loan".
    for group in SEMANTIC_TOKEN_GROUPS:
        if target_tokens & group and not candidate_tokens & group:
            return False
    return True


def _find_temporal_anchor(row: dict, parent_profiles: list[dict]) -> datetime | None:
    anchor_tokens = {
        "open",
        "opening",
        "start",
        "created",
        "creation",
        "onboard",
        "origination",
        "issue",
        "date",
        "time",
        "timestamp",
    }

    for key, value in row.items():
        if tokens(key) & anchor_tokens:
            parsed = parse_datetime(value)
            if parsed:
                return parsed

    for profile in parent_profiles:
        for key, value in profile.items():
            if tokens(key) & anchor_tokens:
                parsed = parse_datetime(value)
                if parsed:
                    return parsed

    return None


def _categorical_options(table_name: str, col_name: str) -> tuple[list[str], list[int]]:
    name_tokens = tokens(col_name)
    table_tokens = tokens(table_name)

    if "status" in name_tokens:
        return ["Active", "Inactive", "Pending", "Closed"], [70, 10, 12, 8]
    if "segment" in name_tokens:
        return ["Mass", "Affluent", "SME", "Student"], [55, 20, 15, 10]
    if "risk" in name_tokens:
        return ["Low", "Medium", "High"], [45, 40, 15]
    if "rating" in name_tokens:
        return ["AAA", "AA", "A", "BBB", "BB", "B", "CCC"], [10, 15, 25, 20, 12, 10, 8]
    if "currency" in name_tokens:
        return ["USD", "EUR", "GBP", "JPY", "SGD"], [62, 15, 10, 8, 5]
    if "side" in name_tokens:
        return ["BUY", "SELL"], [52, 48]
    if "channel" in name_tokens:
        return ["Mobile", "Web", "Branch", "Phone", "Email"], [35, 25, 15, 15, 10]
    if "outcome" in name_tokens:
        return ["Resolved", "Pending", "Escalated", "Rejected"], [58, 22, 12, 8]
    if "sentiment" in name_tokens:
        return ["Positive", "Neutral", "Negative"], [40, 38, 22]
    if "country" in name_tokens:
        return ["US", "GB", "DE", "SG", "AU"], [50, 15, 12, 12, 11]
    if "type" in name_tokens:
        if table_tokens & {"order", "trade", "execution"}:
            return ["Market", "Limit", "Stop", "Algo"], [40, 35, 15, 10]
        return ["Standard", "Premium", "Basic", "Enterprise"], [45, 25, 20, 10]

    base = safe_name(col_name) or "category"
    return [f"{base}_a", f"{base}_b", f"{base}_c", f"{base}_d"], [45, 25, 20, 10]


def sample_parent_key(pool: list[object]) -> object | None:
    if not pool:
        return None
    if len(pool) == 1:
        return pool[0]
    if random.random() < 0.7:
        index = int((random.random() ** 2) * len(pool))
        return pool[index]
    return random.choice(pool)


def _inherit_from_parent(col_name: str, parent_profiles: list[dict]) -> object | None:
    target_tokens = tokens(col_name)
    if not target_tokens:
        return None

    best_value = None
    best_score = 0
    for profile in parent_profiles:
        for key, value in profile.items():
            if value is None:
                continue

            key_tokens = tokens(key)
            # Require semantic compatibility (type->type, status->status, etc.)
            # before considering token-overlap inheritance.
            if not _tokens_semantically_compatible(target_tokens, key_tokens):
                continue

            score = len(target_tokens & key_tokens)
            if score == 0:
                continue

            if score > best_score:
                best_score = score
                best_value = value

    if best_value is not None and best_score > 0 and random.random() < 0.65:
        return best_value
    return None


def _numeric_value(col_name: str, dtype: str) -> object:
    col_tokens = tokens(col_name)

    if col_tokens & {"score"}:
        value = max(300, min(int(random.gauss(690, 90)), 850))
        return int(value)
    if col_tokens & {"rate", "yield", "apr", "interest"}:
        value = max(0.0, min(random.gauss(5.5, 3.0), 35.0))
        return round(value, 2)
    if col_tokens & {"probability", "pd", "ratio"}:
        return round(random.random(), 4)
    if col_tokens & {
        "amount",
        "balance",
        "price",
        "value",
        "payment",
        "income",
        "notional",
        "exposure",
        "limit",
    }:
        return round(random.lognormvariate(8.0, 0.9), 2)
    if col_tokens & {"quantity", "qty", "units", "term", "tenor", "days", "count"}:
        return int(max(1, random.lognormvariate(2.0, 0.7)))

    value = round(random.lognormvariate(6.0, 0.8), 2)
    if dtype == "INTEGER":
        return int(value)
    return value


def _text_value(fake: Faker, col_name: str) -> str:
    col_tokens = tokens(col_name)

    if {"first", "name"}.issubset(col_tokens):
        return fake.first_name()
    if {"last", "name"}.issubset(col_tokens):
        return fake.last_name()
    if "email" in col_tokens:
        return fake.email()
    if "phone" in col_tokens:
        return fake.phone_number()
    if "address" in col_tokens:
        return fake.address().replace("\n", ", ")
    if "city" in col_tokens:
        return fake.city()
    if "country" in col_tokens:
        return fake.country_code()
    if "zip" in col_tokens or "postal" in col_tokens:
        return fake.postcode()
    if "name" in col_tokens:
        return fake.name()
    if "description" in col_tokens or "subject" in col_tokens:
        return fake.sentence(nb_words=10)
    if "note" in col_tokens or "comment" in col_tokens or "reason" in col_tokens:
        return fake.paragraph(nb_sentences=2)
    if "symbol" in col_tokens or "ticker" in col_tokens:
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        return "".join(random.choice(letters) for _ in range(random.randint(3, 5)))
    if "iban" in col_tokens:
        return fake.iban()
    if "account" in col_tokens and "number" in col_tokens:
        return str(random.randint(10**11, 10**12 - 1))

    return fake.word()


def _json_value(fake: Faker, col_name: str) -> dict:
    col_tokens = tokens(col_name)

    if col_tokens & {"preference", "preferences", "setting", "settings"}:
        return {
            "channel": random.choice(["mobile", "web", "branch", "phone", "email"]),
            "language": random.choice(["en", "es", "zh"]),
            "marketing_opt_in": random.random() < 0.6,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
    if col_tokens & {"risk", "model"}:
        return {
            "score": int(max(300, min(random.gauss(690, 90), 850))),
            "tier": random.choice(["low", "medium", "high"]),
            "explanations": [fake.word(), fake.word()],
        }

    return {
        "source": "synthetic_generator",
        "tags": [fake.word(), fake.word()],
        "attributes": {"flag": random.random() < 0.5, "version": random.randint(1, 5)},
    }


def _xml_value(fake: Faker, col_name: str) -> str:
    key = safe_name(col_name) or "payload"
    return (
        f"<{key}><source>synthetic_generator</source>"
        f"<value>{fake.word()}</value><score>{random.randint(1, 100)}</score></{key}>"
    )


def non_key_value(
    fake: Faker,
    table_name: str,
    col: dict,
    row: dict,
    parent_profiles: list[dict],
    state: dict,
) -> tuple[object, dict | None]:
    col_name = col["name"]
    dtype = str(col.get("type", "TEXT")).upper()
    role = str(col.get("field_role", "")).lower()

    relationship_value = conditioned_relationship_value(
        table_name=table_name,
        col=col,
        parent_profiles=parent_profiles,
        context=state.get("relationship_context", {}),
    )
    if relationship_value is not None:
        return relationship_value["value"], relationship_value

    inherited = _inherit_from_parent(col_name, parent_profiles)
    if inherited is not None and role in {"categorical", "text", "temporal"}:
        return inherited, {
            "rule": "token_overlap_copy",
            "expected_value": inherited,
        }

    if dtype == "BOOLEAN" or role == "boolean":
        return random.random() < 0.8, None

    if dtype in {"INTEGER", "NUMERIC", "REAL"} or role == "numerical":
        return _numeric_value(col_name, dtype), None

    if dtype == "DATE" or (role == "temporal" and dtype != "TIMESTAMP"):
        col_tokens = tokens(col_name)
        if col_tokens & {"birth", "dob"}:
            return fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat(), None
        anchor = _find_temporal_anchor(row, parent_profiles)
        if (
            col_tokens
            & {
                "close",
                "closed",
                "end",
                "maturity",
                "settlement",
                "repayment",
                "last",
                "update",
            }
            and anchor
        ):
            start = anchor
        else:
            start = datetime.now() - timedelta(days=3650)
        return random_datetime(start, datetime.now()).date().isoformat(), None

    if dtype == "TIMESTAMP":
        anchor = _find_temporal_anchor(row, parent_profiles)
        start = anchor or (datetime.now() - timedelta(days=1000))
        return random_datetime(start, datetime.now()).isoformat(
            sep=" ", timespec="seconds"
        ), None

    if dtype == "XML":
        return _xml_value(fake, col_name), None

    if dtype == "JSON" or role == "semi_structured":
        return _json_value(fake, col_name), None

    if role == "categorical":
        cache_key = (table_name, col_name)
        if cache_key not in state["categorical_options"]:
            state["categorical_options"][cache_key] = _categorical_options(
                table_name, col_name
            )
        options, weights = state["categorical_options"][cache_key]
        return random.choices(options, weights=weights, k=1)[0], None

    if role == "identifier":
        return str(random.randint(10**7, 10**11 - 1)), None

    return _text_value(fake, col_name), None


def pk_value(table_name: str, col: dict, index: int) -> object:
    dtype = str(col.get("type", "TEXT")).upper()
    if dtype == "INTEGER":
        return index
    if dtype in {"NUMERIC", "REAL"}:
        return float(index)
    prefix = (safe_name(table_name)[:4] or "id").upper()
    return f"{prefix}-{index:08d}"


def profile_for_parent(row: dict, columns: list[dict]) -> dict:
    keep_tokens = {
        "status",
        "type",
        "segment",
        "risk",
        "currency",
        "date",
        "time",
        "timestamp",
        "open",
        "start",
        "created",
    }

    profile: dict[str, object] = {}
    for col in columns:
        col_name = col["name"]
        role = str(col.get("field_role", "")).lower()
        dtype = str(col.get("type", "TEXT")).upper()
        if role in {"categorical", "temporal"} or dtype in {"DATE", "TIMESTAMP"}:
            profile[col_name] = row.get(col_name)
        elif tokens(col_name) & keep_tokens:
            profile[col_name] = row.get(col_name)
    return profile
