from datetime import date, datetime, timedelta
import random
import re


def tokens(text: str) -> set[str]:
    raw = {t for t in re.split(r"[^a-z0-9]+", text.lower()) if t}
    normalized = set(raw)
    for token in raw:
        if token.endswith("s") and len(token) > 3:
            normalized.add(token[:-1])
    return normalized


def safe_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    text = str(value).strip()
    if not text:
        return None

    candidates = [text, text.replace("Z", "+00:00")]
    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            pass

    try:
        d = date.fromisoformat(text[:10])
        return datetime.combine(d, datetime.min.time())
    except ValueError:
        return None


def random_datetime(start: datetime, end: datetime) -> datetime:
    if end <= start:
        end = start + timedelta(days=1)
    span_seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, span_seconds))
