# Data Synthesizer

Single entrypoint CLI for Requirement 1 (schema generation) and Requirement 2 (synthetic data generation).

## Entrypoint

Use `main.py` for all workflows:

```bash
python main.py --help
```

## Commands

### 1) Generate schema + data-generation request

```bash
python main.py schema "Retail banking CRM with customers, accounts, products, interactions"
```

Default outputs:

- `output/schema.json`
- `output/data_generation_request.json`
- `output/schema_validation_report.json`

### 2) Generate synthetic data from schema

```bash
python main.py data --schema output/schema.json --records 500 --formats csv,sqlite
```

### 3) Generate synthetic data from request JSON

```bash
python main.py data --request output/data_generation_request.json
```

### 4) End-to-end pipeline

```bash
python main.py pipeline "Trading platform with portfolios, instruments, orders, and executions"
```

## Outputs

In your selected output directory (default `output/synthetic`):

- table CSV files (when `csv` format enabled)
- `synthetic.db` (when `sqlite` format enabled)
- `summary.json`
- `quality_report.json`

## Notes

- Supported formats: `csv`, `sqlite`.
- `schema` and `pipeline` commands retry schema generation up to 3 attempts and emit a validation report.
- For backward compatibility, running `python main.py` without a subcommand defaults to `data` mode.
- Gemini API credentials are read from environment (`GEMINI_API_KEY` or `GOOGLE_API_KEY`).

## Repo Layout

- Root Python entrypoint: `main.py`
- Internal modules: `synthgen/`
  - `synthgen/schema_generator.py` (Requirement 1 schema + request object)
  - `synthgen/schema_models.py` (canonical Pydantic schema models)
  - `synthgen/schema_validator.py` (strict schema validation rules)
  - `synthgen/engine.py` (Requirement 2 generation loop)
  - `synthgen/values.py` (distribution/value logic)
  - `synthgen/writers.py` (CSV/SQLite writers)
  - `synthgen/schema_utils.py` (table ordering/count heuristics)
  - `synthgen/reporting.py` (quality report)
