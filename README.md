# Data Synthesizer

Config-first Python tool for the take-home assignment:

- Requirement 1: generate a relational schema from a business scenario.
- Requirement 2: generate synthetic data from a schema while preserving keys, relationships, and semi-structured fields.

## Run

Edit `run_config.json`, then run:

```bash
python main.py
```

Optional (custom config path):

```bash
python main.py path/to/run_config.json
```

You can also run the benchmark convenience wrapper:

```bash
python scripts/benchmark_generation.py
```

## Single Config File

All execution is controlled by `run_config.json`.

Set `command` to one of:

- `schema`
- `data`
- `pipeline`
- `benchmark`

## Typical Workflows

### 1) Generate schema + request (Requirement 1)

Set in `run_config.json`:

```json
{
  "command": "schema",
  "scenario": "Retail banking CRM with customers, accounts, products, and interactions"
}
```

Outputs:

- `output/schema.json`
- `output/data_generation_request.json`
- `output/schema_validation_report.json`

### 2) Generate data from schema (Requirement 2)

Set in `run_config.json`:

```json
{
  "command": "data",
  "schema": "assignment/sample_schema.sql",
  "records": 500,
  "formats": ["csv", "sqlite"],
  "out_dir": "output/synthetic"
}
```

Supported schema inputs for `data`:

- JSON schema
- SQL DDL (SQLite/PostgreSQL style)
- Parquet metadata
- Delta Lake metadata

### 3) End-to-end pipeline

Set in `run_config.json`:

```json
{
  "command": "pipeline",
  "scenario": "Trading platform with portfolios, instruments, orders, and executions",
  "records": 500
}
```

### 4) Simple performance benchmark

Set in `run_config.json`:

```json
{
  "command": "benchmark",
  "request": "output/data_generation_request.json",
  "seed": 42,
  "benchmark": {
    "scales": [1000, 5000, 10000],
    "repeats": 2,
    "out_dir": "output/benchmark",
    "formats": ["csv", "sqlite"]
  }
}
```

Benchmark outputs:

- `benchmark_runs.json`
- `benchmark_summary.json`
- `benchmark_results.md`

## Data Outputs

In `out_dir` (default: `output/synthetic`):

- table CSV files (if `csv` enabled)
- `synthetic.db` (if `sqlite` enabled)
- `summary.json`
- `quality_report.json`
- `performance_report.json`

`quality_report.json` includes relationship diagnostics beyond FK validity:

- `fk_integrity` (valid/null/invalid FK rates)
- `parent_child_distribution` (children-per-parent stats for each FK)
- `parent_child_consistency` and `relationship_rule_summary` (rule-driven parent-child consistency)

## Notes

- Schema and pipeline generation retries up to 3 times with validation feedback.
- Foreign keys are generated from real parent keys to keep referential integrity.
- Semi-structured columns (JSON/XML) are generated with structured templates.
- Gemini credentials are read from environment (`GEMINI_API_KEY` or `GOOGLE_API_KEY`).

## Repo Layout

- `main.py` (single config-driven entrypoint)
- `run_config.json` (single execution config)
- `synthgen/`
  - `schema_generator.py` (Requirement 1)
  - `schema_models.py`, `schema_validator.py` (schema validation)
  - `schema_loader.py`, `schema_adapters/` (JSON/SQL/Parquet/Delta loading)
  - `engine.py`, `values.py`, `schema_utils.py` (Requirement 2 generation)
  - `writers.py` (CSV/SQLite)
  - `reporting.py` (quality report)
- `assignment/` (take-home instructions + sample schemas)
