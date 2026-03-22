from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .common import normalize_schema


def load_json_schema(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("JSON schema input must be a JSON object.")

    # Support passing a request JSON as --schema for convenience.
    if isinstance(payload.get("schema"), dict):
        payload = payload["schema"]

    return normalize_schema(
        payload,
        fallback_schema_name=path.stem,
        fallback_domain="Imported JSON",
    )
