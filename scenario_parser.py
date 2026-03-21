from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

NamingStyle = Literal["snake_case", "camelCase", "PascalCase", "preserve"]
OutputFormat = Literal["json", "sql", "both"]


@dataclass(slots=True)
class ScenarioConstraints:
    min_tables: int = 3
    naming_style: NamingStyle = "snake_case"
    output_format: OutputFormat = "both"


@dataclass(slots=True)
class ParsedScenarioInput:
    business_scenario: str
    constraints: ScenarioConstraints

    def to_dict(self) -> dict[str, object]:
        return {
            "business_scenario": self.business_scenario,
            "constraints": asdict(self.constraints),
        }


_KEY_VALUE_PATTERN = re.compile(r"^\s*([A-Za-z][A-Za-z0-9_\-\s]*)\s*[:=]\s*(.+?)\s*$")

_KEY_ALIASES = {
    "scenario": "business_scenario",
    "business_scenario": "business_scenario",
    "business scenario": "business_scenario",
    "domain": "business_scenario",
    "min_tables": "min_tables",
    "minimum_tables": "min_tables",
    "minimum table count": "min_tables",
    "tables": "min_tables",
    "naming_style": "naming_style",
    "naming style": "naming_style",
    "naming": "naming_style",
    "naming_convention": "naming_style",
    "output_format": "output_format",
    "output format": "output_format",
    "format": "output_format",
}

_MIN_TABLE_PATTERNS = [
    re.compile(r"\bmin(?:imum)?[_\s-]*tables?\s*[:=]?\s*(\d+)\b", re.IGNORECASE),
    re.compile(r"\bat\s+least\s+(\d+)\s+tables?\b", re.IGNORECASE),
]


def parse_scenario_input(
    raw_text: str,
    defaults: ScenarioConstraints | None = None,
) -> ParsedScenarioInput:
    """Parse free-form scenario text and optional constraints into a typed object."""
    cleaned = raw_text.strip()
    if not cleaned:
        raise ValueError("Scenario input cannot be empty.")

    fallback = defaults or ScenarioConstraints()

    known_fields, scenario_lines = _extract_known_fields(cleaned)
    business_scenario = _resolve_business_scenario(
        cleaned, known_fields, scenario_lines
    )

    constraints = ScenarioConstraints(
        min_tables=_resolve_min_tables(
            cleaned, known_fields.get("min_tables"), fallback.min_tables
        ),
        naming_style=_resolve_naming_style(
            cleaned,
            known_fields.get("naming_style"),
            fallback.naming_style,
        ),
        output_format=_resolve_output_format(
            cleaned,
            known_fields.get("output_format"),
            fallback.output_format,
        ),
    )

    return ParsedScenarioInput(
        business_scenario=business_scenario, constraints=constraints
    )


def build_requirement_1_prompts(parsed: ParsedScenarioInput) -> tuple[str, str]:
    """Build system and user prompts for Requirement 1 schema generation."""
    system_prompt = "\n".join(
        [
            "You are a senior data model architect.",
            "Generate a relational schema for a business scenario.",
            "Return JSON only with no markdown and no commentary.",
            "The schema must include multiple tables, primary keys, foreign keys,",
            "numerical fields, categorical fields, and semi-structured fields (json/xml/text).",
        ]
    )

    user_prompt = "\n".join(
        [
            "Business scenario:",
            parsed.business_scenario,
            "",
            "Constraints:",
            f"- minimum_tables: {parsed.constraints.min_tables}",
            f"- naming_style: {parsed.constraints.naming_style}",
            f"- output_format: {parsed.constraints.output_format}",
        ]
    )

    return system_prompt, user_prompt


def _extract_known_fields(raw_text: str) -> tuple[dict[str, str], list[str]]:
    known_fields: dict[str, str] = {}
    scenario_lines: list[str] = []

    for line in raw_text.splitlines():
        candidate = line.strip()
        if not candidate:
            continue

        key_match = _KEY_VALUE_PATTERN.match(candidate)
        if not key_match:
            scenario_lines.append(candidate)
            continue

        raw_key = key_match.group(1)
        value = key_match.group(2).strip()
        canonical_key = _canonical_key(raw_key)
        if canonical_key is None:
            scenario_lines.append(candidate)
            continue

        known_fields[canonical_key] = value

    return known_fields, scenario_lines


def _canonical_key(raw_key: str) -> str | None:
    lowered = raw_key.strip().lower()
    underscored = re.sub(r"[\s\-]+", "_", lowered)

    if lowered in _KEY_ALIASES:
        return _KEY_ALIASES[lowered]
    if underscored in _KEY_ALIASES:
        return _KEY_ALIASES[underscored]
    return None


def _resolve_business_scenario(
    raw_text: str,
    known_fields: dict[str, str],
    scenario_lines: list[str],
) -> str:
    if (
        "business_scenario" in known_fields
        and known_fields["business_scenario"].strip()
    ):
        return known_fields["business_scenario"].strip()

    if scenario_lines:
        return "\n".join(scenario_lines).strip()

    return raw_text.strip()


def _resolve_min_tables(raw_text: str, explicit_value: str | None, default: int) -> int:
    parsed = _extract_int(explicit_value) if explicit_value else None
    if parsed is None:
        for pattern in _MIN_TABLE_PATTERNS:
            match = pattern.search(raw_text)
            if match:
                parsed = int(match.group(1))
                break

    if parsed is None:
        parsed = default

    return max(2, parsed)


def _extract_int(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"\d+", value)
    if not match:
        return None
    return int(match.group(0))


def _resolve_naming_style(
    raw_text: str,
    explicit_value: str | None,
    default: NamingStyle,
) -> NamingStyle:
    candidate = (explicit_value or raw_text).lower()

    if "snake" in candidate:
        return "snake_case"
    if "camel" in candidate:
        return "camelCase"
    if "pascal" in candidate:
        return "PascalCase"
    if "preserve" in candidate or "original" in candidate or "as-is" in candidate:
        return "preserve"

    return default


def _resolve_output_format(
    raw_text: str,
    explicit_value: str | None,
    default: OutputFormat,
) -> OutputFormat:
    candidate = (explicit_value or raw_text).lower()
    mentions_json = "json" in candidate
    mentions_sql = "sql" in candidate or "ddl" in candidate

    if "both" in candidate or (mentions_json and mentions_sql):
        return "both"
    if mentions_json:
        return "json"
    if mentions_sql:
        return "sql"

    return default


def _resolve_raw_text(args: argparse.Namespace) -> str:
    if args.text and args.text.strip():
        return args.text.strip()

    if args.file:
        return Path(args.file).read_text(encoding="utf-8").strip()

    if not sys.stdin.isatty():
        stdin_text = sys.stdin.read().strip()
        if stdin_text:
            return stdin_text

    raise ValueError("Provide --text, --file, or pipe scenario text via stdin.")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parse scenario input for Requirement 1."
    )
    parser.add_argument("--text", help="Scenario text input.")
    parser.add_argument("--file", help="Path to a text file with scenario input.")
    parser.add_argument(
        "--print-prompts",
        action="store_true",
        help="Also print system/user prompts after parsed JSON.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    try:
        raw_text = _resolve_raw_text(args)
        parsed = parse_scenario_input(raw_text)
    except ValueError as exc:
        parser.error(str(exc))
        return

    print(json.dumps(parsed.to_dict(), indent=2))

    if args.print_prompts:
        system_prompt, user_prompt = build_requirement_1_prompts(parsed)
        print("\n--- system_prompt ---")
        print(system_prompt)
        print("\n--- user_prompt ---")
        print(user_prompt)


if __name__ == "__main__":
    main()
