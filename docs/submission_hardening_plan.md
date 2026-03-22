# Submission Hardening Plan

This plan focuses on the fastest path to close assignment gaps and maximize rubric score.

## Goal

Ship a submission that clearly satisfies:

- Requirement 1: robust schema generation from scenario input
- Requirement 2: schema-driven synthetic data generation with realistic quality
- Rubric: modular architecture, multi-format support, semi-structured handling, scalability evidence

## Priority 0 (Must Have Before Submission)

### 1) Add strict schema validation + retry loop for Requirement 1

Why:

- Current schema generation relies on prompt quality and JSON parsing only.
- Assignment expects required features to be present every run.

Build:

- Add canonical schema models (Pydantic) in `synthgen/schema_models.py`.
- Add validator rules in `synthgen/schema_validator.py`:
  - multiple tables
  - per-table PK presence
  - FK target table/column existence
  - coverage for numerical/categorical/semi-structured fields
  - duplicate table/column checks
- Update `synthgen/schema_generator.py` to run retry loop (max attempts = 3) with structured validation feedback to Gemini.

Done when:

- `main.py schema ...` always returns a validation report.
- Failed attempts retry automatically and stop with a clear error if still invalid.

### 2) Support multiple schema input formats for Requirement 2

Why:

- Requirement 2 states schema input may be SQLite, PostgreSQL, Parquet, or Delta Lake.
- Current `data` command expects only internal JSON schema.

Build:

- Add adapters under `synthgen/schema_adapters/`:
  - `json_adapter.py` (existing behavior)
  - `sql_adapter.py` (parse SQL DDL for SQLite/PostgreSQL-style schemas)
  - `parquet_adapter.py` (derive columns from parquet metadata)
  - `delta_adapter.py` (derive columns from Delta metadata)
- Add a unified loader `synthgen/schema_loader.py` that detects format by extension/content and normalizes to canonical schema.
- Update `main.py data` to load via this unified loader.

Done when:

- `main.py data --schema assignment/sample_schema.sql ...` works.
- JSON and SQL both work end-to-end.
- Parquet/Delta accept at least metadata-based schema ingestion.

### 3) Add explicit deliverable docs requested by assignment

Why:

- Assignment expects architecture and methodology writeups, not only runnable code.

Build:

- Add:
  - `docs/architecture.md`
  - `docs/schema_generation_approach.md`
  - `docs/synthetic_data_pipeline.md`
  - `docs/data_quality_preservation.md`
- Keep `README.md` as quickstart; place detailed rationale in docs above.

Done when:

- A reviewer can map each assignment deliverable to a concrete document.

## Priority 1 (High Value Improvements)

### 4) Improve relationship realism beyond FK validity

Build:

- Add relationship templates/config in `synthgen/relationship_rules.py`.
- Drive conditional distributions from parent attributes when available.
- Add per-domain defaults for CRM, trading, and credit-risk token patterns.

Done when:

- Quality report includes at least one measurable parent-child distribution check.

### 5) Improve semi-structured fidelity

Build:

- Expand JSON/XML template library in `synthgen/values.py` or split to `synthgen/semi_structured.py`.
- Add schema-aware nested payload generation for common finance fields (risk, preferences, model outputs, metadata).

Done when:

- Semi-structured fields are consistently non-trivial and domain-shaped.

## Priority 2 (Proof for Rubric: Scalability)

### 6) Add reproducible performance benchmark

Build:

- Add `scripts/benchmark_generation.py`.
- Benchmark at increasing scales (for example 100k, 500k, 1M base records).
- Capture runtime and output size into `docs/scalability_results.md`.

Done when:

- Submission includes concrete numbers for scalability/performance.

## Validation Checklist (Before Final Submission)

- `python main.py schema "..."` produces valid schema + request artifacts.
- `python main.py data --schema <json|sql|parquet|delta>` runs successfully.
- FK integrity checks pass in generated outputs.
- `quality_report.json` shows categorical and numerical summaries.
- All required docs exist under `docs/` and are linked from `README.md`.
