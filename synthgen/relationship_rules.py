import random

from .common import safe_name, tokens

DOMAIN_KEYWORDS = {
    "crm": {
        "crm",
        "customer",
        "account",
        "interaction",
        "retail",
        "banking",
        "onboarding",
        "support",
    },
    "trading": {
        "trading",
        "trade",
        "order",
        "execution",
        "portfolio",
        "instrument",
        "market",
        "broker",
    },
    "credit_risk": {
        "credit",
        "risk",
        "borrower",
        "loan",
        "repayment",
        "delinquent",
        "default",
        "scorecard",
    },
}

STATUS_TRANSITIONS = {
    "crm": {
        "active": [("Active", 78), ("Pending", 14), ("Inactive", 8)],
        "inactive": [("Inactive", 68), ("Closed", 22), ("Active", 10)],
        "pending": [("Pending", 70), ("Active", 20), ("Inactive", 10)],
        "closed": [("Closed", 85), ("Inactive", 15)],
    },
    "trading": {
        "new": [("New", 58), ("Open", 27), ("Cancelled", 15)],
        "open": [("Open", 62), ("Filled", 25), ("Cancelled", 13)],
        "filled": [("Filled", 82), ("Partially Filled", 18)],
        "partially_filled": [
            ("Partially Filled", 56),
            ("Filled", 34),
            ("Cancelled", 10),
        ],
        "cancelled": [("Cancelled", 86), ("Rejected", 14)],
    },
    "credit_risk": {
        "current": [("Current", 74), ("Watchlist", 18), ("Delinquent", 8)],
        "watchlist": [("Watchlist", 52), ("Current", 26), ("Delinquent", 22)],
        "delinquent": [("Delinquent", 54), ("Current", 16), ("Default", 30)],
        "default": [("Default", 83), ("Delinquent", 17)],
    },
    "generic": {
        "active": [("Active", 70), ("Pending", 20), ("Inactive", 10)],
        "inactive": [("Inactive", 70), ("Closed", 20), ("Active", 10)],
        "pending": [("Pending", 65), ("Active", 20), ("Inactive", 15)],
        "closed": [("Closed", 85), ("Inactive", 15)],
    },
}

SEGMENT_TO_RISK = {
    "crm": {
        "mass": [("Low", 60), ("Medium", 35), ("High", 5)],
        "affluent": [("Low", 48), ("Medium", 42), ("High", 10)],
        "sme": [("Medium", 58), ("Low", 20), ("High", 22)],
        "student": [("Medium", 60), ("Low", 30), ("High", 10)],
    },
    "trading": {
        "retail": [("Medium", 60), ("Low", 25), ("High", 15)],
        "institutional": [("Low", 55), ("Medium", 30), ("High", 15)],
        "hedge": [("High", 56), ("Medium", 34), ("Low", 10)],
    },
    "credit_risk": {
        "prime": [("Low", 76), ("Medium", 20), ("High", 4)],
        "near_prime": [("Medium", 62), ("Low", 20), ("High", 18)],
        "subprime": [("High", 72), ("Medium", 22), ("Low", 6)],
    },
    "generic": {
        "mass": [("Low", 55), ("Medium", 35), ("High", 10)],
        "affluent": [("Low", 45), ("Medium", 40), ("High", 15)],
    },
}

SEGMENT_TO_TYPE = {
    "crm": {
        "mass": [("Standard", 55), ("Basic", 35), ("Premium", 10)],
        "affluent": [("Premium", 62), ("Standard", 28), ("Enterprise", 10)],
        "sme": [("Enterprise", 58), ("Premium", 24), ("Standard", 18)],
        "student": [("Basic", 72), ("Standard", 28)],
    },
    "trading": {
        "retail": [("Market", 46), ("Limit", 40), ("Stop", 14)],
        "institutional": [("Algo", 44), ("Limit", 36), ("Market", 20)],
        "hedge": [("Algo", 58), ("Limit", 28), ("Stop", 14)],
    },
    "credit_risk": {
        "prime": [("Standard", 56), ("Premium", 34), ("Basic", 10)],
        "near_prime": [("Standard", 48), ("Basic", 34), ("Premium", 18)],
        "subprime": [("Basic", 62), ("Standard", 28), ("Premium", 10)],
    },
    "generic": {
        "mass": [("Standard", 60), ("Basic", 30), ("Premium", 10)],
        "affluent": [("Premium", 60), ("Standard", 30), ("Basic", 10)],
    },
}

COUNTRY_TO_CURRENCY = {
    "US": "USD",
    "GB": "GBP",
    "DE": "EUR",
    "FR": "EUR",
    "ES": "EUR",
    "IT": "EUR",
    "SG": "SGD",
    "JP": "JPY",
    "AU": "AUD",
    "NZ": "NZD",
    "CA": "CAD",
    "CH": "CHF",
}


def build_relationship_context(schema: dict) -> dict:
    return {"domain_family": _detect_domain_family(schema)}


def conditioned_relationship_value(
    *,
    table_name: str,
    col: dict,
    parent_profiles: list[dict],
    context: dict,
) -> dict | None:
    if not parent_profiles:
        return None

    col_name = col["name"]
    col_tokens = tokens(col_name)
    role = str(col.get("field_role", "")).lower()
    dtype = str(col.get("type", "TEXT")).upper()
    domain = str(context.get("domain_family") or "generic")

    if "currency" in col_tokens:
        parent_currency = _parent_value(parent_profiles, {"currency"})
        if parent_currency is not None:
            value = str(parent_currency).upper()
            return {
                "rule": "parent_currency_copy",
                "value": value,
                "expected_value": value,
            }

        parent_country = _parent_value(parent_profiles, {"country"})
        if parent_country is not None:
            mapped = COUNTRY_TO_CURRENCY.get(str(parent_country).upper())
            if mapped:
                return {
                    "rule": "country_to_currency",
                    "value": mapped,
                    "expected_value": mapped,
                }

    if "country" in col_tokens:
        parent_country = _parent_value(parent_profiles, {"country"})
        if parent_country is not None:
            value = str(parent_country).upper()
            return {
                "rule": "parent_country_copy",
                "value": value,
                "expected_value": value,
            }

    if "segment" in col_tokens:
        parent_segment = _parent_value(parent_profiles, {"segment"})
        if parent_segment is not None:
            value = str(parent_segment)
            return {
                "rule": "parent_segment_copy",
                "value": value,
                "expected_value": value,
            }

    if "status" in col_tokens or "state" in col_tokens:
        parent_status = _parent_value(parent_profiles, {"status", "state"})
        if parent_status is not None:
            value, expected = _status_transition_value(parent_status, domain)
            return {
                "rule": "status_transition",
                "value": value,
                "expected_value": expected,
            }

    if "risk" in col_tokens or "rating" in col_tokens or "tier" in col_tokens:
        parent_risk = _parent_value(parent_profiles, {"risk", "rating", "tier"})
        if parent_risk is not None:
            value = str(parent_risk)
            return {
                "rule": "parent_risk_copy",
                "value": value,
                "expected_value": value,
            }

        parent_segment = _parent_value(parent_profiles, {"segment"})
        if parent_segment is not None:
            mapped = _segment_mapped_value(parent_segment, domain, SEGMENT_TO_RISK)
            if mapped is not None:
                value, expected = mapped
                return {
                    "rule": "segment_to_risk",
                    "value": value,
                    "expected_value": expected,
                }

    table_tokens = tokens(table_name)
    if "type" in col_tokens and not (table_tokens & {"reference", "lookup"}):
        parent_type = _parent_value(parent_profiles, {"type"})
        if parent_type is not None and random.random() < 0.65:
            value = str(parent_type)
            return {
                "rule": "parent_type_copy",
                "value": value,
                "expected_value": value,
            }

        parent_segment = _parent_value(parent_profiles, {"segment"})
        if parent_segment is not None:
            mapped = _segment_mapped_value(parent_segment, domain, SEGMENT_TO_TYPE)
            if mapped is not None:
                value, expected = mapped
                return {
                    "rule": "segment_to_type",
                    "value": value,
                    "expected_value": expected,
                }

    if dtype == "BOOLEAN" or role == "boolean":
        if "active" in col_tokens:
            parent_status = _parent_value(parent_profiles, {"status", "state"})
            if parent_status is not None:
                expected = _is_active_status(parent_status)
                value = expected if random.random() < 0.88 else (not expected)
                return {
                    "rule": "status_to_active_flag",
                    "value": value,
                    "expected_value": expected,
                }

        if col_tokens & {"default", "delinquent"}:
            parent_risk = _parent_value(parent_profiles, {"risk", "rating", "tier"})
            if parent_risk is not None:
                expected = _is_high_risk(parent_risk)
                value = expected if random.random() < 0.8 else (not expected)
                return {
                    "rule": "risk_to_default_flag",
                    "value": value,
                    "expected_value": expected,
                }

    return None


def value_matches(left: object, right: object) -> bool:
    if left is None or right is None:
        return False
    if isinstance(left, bool) or isinstance(right, bool):
        return bool(left) == bool(right)
    return safe_name(str(left)) == safe_name(str(right))


def _detect_domain_family(schema: dict) -> str:
    corpus_tokens: set[str] = set()
    corpus_tokens |= tokens(str(schema.get("schema_name", "")))
    corpus_tokens |= tokens(str(schema.get("domain", "")))

    for table in schema.get("tables", []):
        corpus_tokens |= tokens(str(table.get("name", "")))

    best_domain = "generic"
    best_score = 0
    for domain, keywords in DOMAIN_KEYWORDS.items():
        score = len(corpus_tokens & keywords)
        if score > best_score:
            best_score = score
            best_domain = domain

    return best_domain


def _parent_value(
    parent_profiles: list[dict], wanted_tokens: set[str]
) -> object | None:
    best_value = None
    best_score = 0

    for profile in parent_profiles:
        for key, value in profile.items():
            if value is None:
                continue
            score = len(tokens(key) & wanted_tokens)
            if score > best_score:
                best_score = score
                best_value = value

    return best_value


def _status_transition_value(parent_status: object, domain: str) -> tuple[str, str]:
    key = safe_name(str(parent_status))
    options = STATUS_TRANSITIONS.get(domain, {}).get(key)
    if not options:
        options = STATUS_TRANSITIONS["generic"].get(key)
    if not options:
        text = str(parent_status)
        return text, text

    sampled = _weighted_choice(options)
    expected = options[0][0]
    return sampled, expected


def _segment_mapped_value(
    parent_segment: object,
    domain: str,
    mapping: dict[str, dict[str, list[tuple[str, int]]]],
) -> tuple[str, str] | None:
    key = safe_name(str(parent_segment))
    options = mapping.get(domain, {}).get(key)
    if not options:
        options = mapping.get("generic", {}).get(key)
    if not options:
        return None

    sampled = _weighted_choice(options)
    expected = options[0][0]
    return sampled, expected


def _weighted_choice(options: list[tuple[str, int]]) -> str:
    values = [item[0] for item in options]
    weights = [item[1] for item in options]
    return random.choices(values, weights=weights, k=1)[0]


def _is_active_status(status_value: object) -> bool:
    normalized = safe_name(str(status_value))
    return normalized in {
        "active",
        "open",
        "new",
        "current",
        "filled",
        "partially_filled",
        "resolved",
    }


def _is_high_risk(risk_value: object) -> bool:
    normalized = safe_name(str(risk_value))
    return normalized in {
        "high",
        "subprime",
        "delinquent",
        "default",
        "watchlist",
        "ccc",
        "cc",
        "c",
    }
