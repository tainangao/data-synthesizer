import argparse
import json
from pathlib import Path

from gemini_util import GeminiClient


GEN_SCH_SYS_PROMPT = """
You are a senior data model architect designing production-grade financial data models.

Task:
- Given a business data scenario, generate a relational schema that can be used for synthetic data generation.
- The schema must be realistic, normalized enough for analytics workloads, and internally consistent.

Hard requirements:
- Include multiple tables (minimum 3 unless the scenario strictly cannot support it).
- Include primary keys and valid foreign keys.
- Include numerical fields.
- Include categorical fields.
- Include semi-structured columns (JSON, XML, or free text) where realistic.

Output format:
- Return JSON only. Do not return markdown, code fences, or explanations.
- Return exactly one JSON object with this shape:
{
  "schema_name": "string",
  "domain": "string",
  "tables": [
    {
      "name": "string",
      "description": "string",
      "columns": [
        {
          "name": "string",
          "type": "TEXT|INTEGER|NUMERIC|REAL|BOOLEAN|DATE|TIMESTAMP|JSON|XML",
          "field_role": "identifier|numerical|categorical|semi_structured|temporal|text|boolean",
          "nullable": true,
          "primary_key": false,
          "foreign_key": null
        }
      ]
    }
  ]
}

Foreign key object:
- If a column is a foreign key, set "foreign_key" to:
  {"table": "referenced_table", "column": "referenced_column"}
- Otherwise set "foreign_key" to null.

Modeling quality rules:
- Every table must have exactly one primary key column.
- Every foreign key must reference an existing table and column in this same JSON output.
- Use business-meaningful table/column names.
- Include at least one transaction-like table and at least one master/reference table when applicable.
- Prefer realistic financial entities (for example: customers, accounts, transactions, loans, payments, portfolios, orders, instruments, interactions).

Before finalizing, self-check:
- Multiple tables present.
- At least one primary key and one foreign key relationship present.
- Numerical, categorical, and semi-structured fields are all present.
- JSON is valid and fully parseable.
"""


def gen_schema(user_prompt: str) -> dict:
    client = GeminiClient()
    raw = client.chat(user_prompt, GEN_SCH_SYS_PROMPT)
    return _parse_json(raw)


def _parse_json(raw: str) -> dict:
    text = raw.strip()

    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    if text.startswith("{"):
        return json.loads(text)

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return json.loads(text[start : end + 1])

    raise ValueError("Model output is not valid JSON")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate schema JSON from a scenario")
    parser.add_argument("scenario", nargs="?", help="Business data scenario")
    parser.add_argument(
        "--out",
        default="output/schema.json",
        help="Output schema json path",
    )
    args = parser.parse_args()

    scenario = (args.scenario or input("Business scenario: ")).strip()
    if not scenario:
        raise SystemExit("Business scenario is required")

    schema = gen_schema(scenario)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")

    print(f"Schema written to {out_path}")


if __name__ == "__main__":
    main()
