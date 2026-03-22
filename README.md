# Data Synthesizer

Single entrypoint CLI for Requirement 1 (schema generation) and Requirement 2 (synthetic data generation).

## Entrypoint

Use `main.py` for all workflows:

```bash
python main.py --help
```

For VS Code click-to-run workflow, edit `run_config.json` and run:

```bash
python main.py
```

Minimal `run_config.json` example:

```json
{
  "command": "data",
  "request": "output/data_generation_request.json",
  "records": 10000,
  "formats": "csv,sqlite",
  "out_dir": "output/synthetic"
}
```

You can also point to another config file:

```bash
python main.py --config path/to/config.json
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

`--schema` can load JSON, SQL DDL, parquet, or delta schema inputs.

Example SQL input:

```bash
python main.py data --schema assignment/sample_schema.sql --records 500 --formats csv,sqlite
```

### 3) Generate synthetic data from request JSON

```bash
python main.py data --request output/data_generation_request.json
```

### 4) End-to-end pipeline

```bash
python main.py pipeline "Trading platform with portfolios, instruments, orders, and executions"
```

## Benchmarking Scalability

Use the benchmark runner to stress test generation at multiple scales and repeats.
It is VS Code friendly (edit config at the top of the file, then click Run):

```bash
python scripts/benchmark_generation.py
```

Main config lives in `scripts/benchmark_generation.py`:

- `REQUEST_PATH` or `SCHEMA_PATH` (pick one)
- `SCALES`, `REPEATS`, `FORMATS`, `SEED`
- `OUTPUT_ROOT`, `LABEL`, `TIMEOUT_SECONDS`, `MARKDOWN_COPY`

Benchmark outputs are written under `output/benchmarks/<timestamp>/`:

- `benchmark_results.json` (aggregated mean/p95/stdev stats by scale)
- `benchmark_runs.json` (raw per-run details)
- `scalability_results.md` (rubric-friendly summary table)

## Outputs

In your selected output directory (default `output/synthetic`):

- table CSV files (when `csv` format enabled)
- `synthetic.db` (when `sqlite` format enabled)
- `summary.json`
- `quality_report.json`
- `performance_report.json`

## Notes

- Output formats: `csv`, `sqlite`.
- Schema input formats for `data --schema`: `json`, `sql`, `parquet`, `delta`.
- Parquet schema loading uses `pyarrow` or `fastparquet` when available.
- Delta schema loading reads `_delta_log` metadata (and also supports `deltalake` if installed).
- Use `--perf-report-out` to override the default performance report path.
- `main.py` reads `run_config.json` automatically when it exists.
- `schema` and `pipeline` commands retry schema generation up to 3 attempts and emit a validation report.
- For backward compatibility, running `python main.py` without a subcommand defaults to `data` mode.
- Gemini API credentials are read from environment (`GEMINI_API_KEY` or `GOOGLE_API_KEY`).

## Repo Layout

- Run config template: `run_config.json`
- Root Python entrypoint: `main.py`
- Internal modules: `synthgen/`
  - `synthgen/schema_generator.py` (Requirement 1 schema + request object)
  - `synthgen/schema_models.py` (canonical Pydantic schema models)
  - `synthgen/schema_validator.py` (strict schema validation rules)
  - `synthgen/schema_loader.py` (multi-format schema loader for Requirement 2)
  - `synthgen/schema_adapters/` (json/sql/parquet/delta adapters)
  - `synthgen/engine.py` (Requirement 2 generation loop)
  - `synthgen/values.py` (distribution/value logic)
  - `synthgen/writers.py` (CSV/SQLite writers)
  - `synthgen/schema_utils.py` (table ordering/count heuristics)
  - `synthgen/reporting.py` (quality report)
- Benchmarking script: `scripts/benchmark_generation.py`
