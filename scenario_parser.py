from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(slots=True)
class ParsedScenarioInput:
    business_scenario: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


_KEY_VALUE_PATTERN = re.compile(r"^\s*([A-Za-z][A-Za-z0-9_\-\s]*)\s*[:=]\s*(.+?)\s*$")

_KEY_ALIASES = {
    "scenario": "business_scenario",
    "business_scenario": "business_scenario",
    "business scenario": "business_scenario",
    "domain": "business_scenario",
}


def parse_scenario_input(raw_text: str) -> ParsedScenarioInput:
    """Parse free-form scenario text into a typed object."""
    cleaned = raw_text.strip()
    if not cleaned:
        raise ValueError("Scenario input cannot be empty.")

    business_scenario = _resolve_business_scenario(cleaned)
    return ParsedScenarioInput(business_scenario=business_scenario)


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
        ]
    )

    return system_prompt, user_prompt


def _resolve_business_scenario(raw_text: str) -> str:
    scenario_values: list[str] = []
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
        if canonical_key == "business_scenario":
            if value:
                scenario_values.append(value)
            continue

        continue

    if scenario_values:
        combined = scenario_values + scenario_lines
        return "\n".join(combined).strip()

    if scenario_lines:
        return "\n".join(scenario_lines).strip()

    return raw_text.strip()


def _canonical_key(raw_key: str) -> str | None:
    lowered = raw_key.strip().lower()
    underscored = re.sub(r"[\s\-]+", "_", lowered)

    if lowered in _KEY_ALIASES:
        return _KEY_ALIASES[lowered]
    if underscored in _KEY_ALIASES:
        return _KEY_ALIASES[underscored]
    return None


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
