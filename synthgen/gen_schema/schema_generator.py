import json
import logging
from pathlib import Path
from typing import Any

from synthgen.gen_schema.gemini_client import GeminiClient
from synthgen.gen_schema.schema_validator import (
    format_validation_feedback,
    validate_schema,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_SCHEMA_ATTEMPTS = 3
MAX_RETRY_FEEDBACK_ISSUES = 12
MAX_PREVIOUS_OUTPUT_CHARS = 6000


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


class SchemaGenerationError(RuntimeError):
    def __init__(self, message: str, *, validation_report: dict[str, Any]):
        super().__init__(message)
        self.validation_report = validation_report


def gen_schema(user_prompt: str, *, max_attempts: int = MAX_SCHEMA_ATTEMPTS) -> dict:
    result = gen_schema_with_validation(user_prompt, max_attempts=max_attempts)
    return result["schema"]


def gen_schema_with_validation(
    user_prompt: str,
    *,
    max_attempts: int = MAX_SCHEMA_ATTEMPTS,
) -> dict:
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    client = GeminiClient()
    attempt_prompt = user_prompt
    attempts: list[dict[str, Any]] = []

    for attempt_number in range(1, max_attempts + 1):
        raw = client.chat(attempt_prompt, GEN_SCH_SYS_PROMPT)

        validated_schema: dict[str, Any] | None = None
        summary: dict[str, Any]
        issues: list[dict[str, str]]

        try:
            parsed_schema = _parse_json(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            issues = [
                {
                    "code": "json_parse_error",
                    "message": str(exc),
                    "path": "model_output",
                }
            ]
            summary = _empty_attempt_summary()
        else:
            validation = validate_schema(parsed_schema)
            issues = validation["issues"]
            summary = validation["summary"]
            if validation["valid"]:
                validated_schema = validation["schema"]

        attempt_info = {
            "attempt": attempt_number,
            "valid": validated_schema is not None,
            "issue_count": len(issues),
            "issues": issues,
            "summary": summary,
            "response_preview": _response_preview(raw),
        }
        attempts.append(attempt_info)

        if validated_schema is not None:
            return {
                "schema": validated_schema,
                "validation_report": _build_validation_report(
                    attempts=attempts,
                    valid=True,
                    max_attempts=max_attempts,
                ),
            }

        if attempt_number < max_attempts:
            attempt_prompt = _build_retry_prompt(
                user_prompt=user_prompt,
                issues=issues,
                previous_output=raw,
            )

    report = _build_validation_report(
        attempts=attempts,
        valid=False,
        max_attempts=max_attempts,
    )
    raise SchemaGenerationError(
        f"Failed to generate a valid schema after {max_attempts} attempts.",
        validation_report=report,
    )


def gen_schema_with_request(
    user_prompt: str,
    *,
    max_attempts: int = MAX_SCHEMA_ATTEMPTS,
    records: int = 500,
    seed: int = 42,
    out_dir: str = "output/synthetic",
    data_formats: list[str] | None = None,
) -> dict:
    generated = gen_schema_with_validation(user_prompt, max_attempts=max_attempts)
    schema = generated["schema"]
    request = {
        "kind": "data_generation_request",
        "schema": schema,
        "generation": {
            "records": records,
            "seed": seed,
            "out_dir": out_dir,
            "data_formats": data_formats,
        },
    }

    # Save the request for reference
    outdir = Path(out_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    request_path = outdir / "data_generation_request.json"
    with request_path.open("w") as f:
        json.dump(request, f, indent=2)
    logger.info(f"Saved data generation request to {request_path}")

    schema_path = outdir / "json_schema.json"
    with schema_path.open("w") as f:
        json.dump(schema, f, indent=2)
    logger.info(f"Saved generated schema to {schema_path}")

    validation_report_path = outdir / "schema_validation_report.json"
    with validation_report_path.open("w") as f:
        json.dump(generated["validation_report"], f, indent=2)
    logger.info(f"Saved validation report to {validation_report_path}")

    return {
        "schema": schema,
        "validation_report": generated["validation_report"],
        "data_generation_request": request,
    }


def _build_retry_prompt(
    *,
    user_prompt: str,
    issues: list[dict[str, str]],
    previous_output: str,
) -> str:
    clipped_output = previous_output.strip()
    if len(clipped_output) > MAX_PREVIOUS_OUTPUT_CHARS:
        clipped_output = (
            clipped_output[:MAX_PREVIOUS_OUTPUT_CHARS].rstrip() + "\n...[truncated]"
        )

    feedback = format_validation_feedback(issues, max_items=MAX_RETRY_FEEDBACK_ISSUES)
    return (
        f"Business scenario:\n{user_prompt.strip()}\n\n"
        "Your previous schema output failed validation. "
        "Fix every issue and return JSON only.\n\n"
        "Validation issues to fix:\n"
        f"{feedback}\n\n"
        "Previous invalid output:\n"
        f"{clipped_output}\n\n"
        "Return exactly one corrected JSON object with the required shape."
    )


def _build_validation_report(
    *,
    attempts: list[dict[str, Any]],
    valid: bool,
    max_attempts: int,
) -> dict[str, Any]:
    final_issue_count = attempts[-1]["issue_count"] if attempts else 0
    return {
        "valid": valid,
        "max_attempts": max_attempts,
        "attempt_count": len(attempts),
        "final_issue_count": final_issue_count,
        "attempts": attempts,
    }


def _empty_attempt_summary() -> dict[str, Any]:
    return {
        "table_count": 0,
        "column_count": 0,
        "primary_key_count": 0,
        "foreign_key_count": 0,
        "field_role_counts": {},
    }


def _response_preview(raw: str, *, max_chars: int = 240) -> str:
    compact = " ".join(raw.strip().split())
    if len(compact) <= max_chars:
        return compact
    return compact[: max_chars - 3] + "..."


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


if __name__ == "__main__":
    gen_schema_with_request(
        user_prompt="credit risk",
        max_attempts=3,
        records=10,
        seed=74,
        out_dir="output/synthetic/credit_risk_10_records",
        data_formats=["csv"],
    )
