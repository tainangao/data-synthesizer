# Financial Data Synthesizer (Assignment Submission)

## Project goal
This project generates synthetic financial data by:
1) building a schema from a business scenario,
2) generating realistic linked records,
3) exporting schema and data to multiple formats.

## Intentional design choice: JSON schema first
I intentionally chose JSON schema as the internal format because it is the most effective way to drive Faker-based generation:
- one clear source of truth for columns, types, keys, and field roles,
- easy validation before generation,
- simple mapping to generation rules.

Even with this design, the program still generates **PostgreSQL / Delta / Parquet** schema and data artifacts:
- PostgreSQL schema SQL (`demo_output/schema/psql/schema.sql`)
- SQLite schema SQL + DB (`demo_output/schema/sqlite/`)
- Parquet schema/data (`demo_output/schema/parquet/`, `demo_output/parquet/`)
- Delta schema/data (`demo_output/schema/delta/`, `demo_output/delta/`)

## End-to-end architecture
1. **Schema generation** from business prompt using Gemini (`src/gen_schema/schema_generator.py`).
2. **Schema validation** with strict structural + logical checks (`src/gen_schema/schema_validator.py`).
3. **Schema conversion** to sqlite/psql/parquet/delta artifacts (`src/gen_schema/schema_converter.py`).
4. **Data generation** with PK/FK-aware ordering and Faker-based value generation (`src/gen_data/engine.py`, `src/gen_data/values.py`).
5. **Relationship-aware generation** for status/type/risk/currency consistency (`src/gen_data/relationship_rules.py`).
6. **Writers** for CSV, SQLite, Parquet, Delta (`src/gen_data/writers.py`).
7. **Quality report** for integrity, distribution, and relationship checks (`src/reporting.py`).

## Assignment requirement coverage

### Requirement 1 - Schema generation
Input: business data scenario prompt.  
Output: generated relational schema (`schema.json`) with validation report.

How it is met:
- Multiple tables: e.g., `Customers`, `LoanApplications`, `LoanAccounts`, `PaymentHistory`.
- PK/FK: each table has a primary key; foreign keys are validated.
- Numerical fields: score, amount, balance, rate, fees.
- Categorical fields: status, purpose, type.
- Semi-structured fields: JSON and XML columns (for example `address_json`, `terms_and_conditions_xml`).

Reference artifacts:
- `demo_output/schema.json`
- `demo_output/schema_validation_report.json`
- **To better view the relationships, go to `demo_output/schema/psql/schema.sql`**

### Requirement 2 - Synthetic data generation
Input: schema + configurable row count + seed (`generate_data(records, seed, ...)`).  
Output: generated tables in CSV/SQLite/Parquet/Delta.

How it is met:
- Realistic distributions: weighted categorical sampling and domain-aware numeric ranges.
- Referential integrity: FK values sampled from generated parent PK pools.
- Categorical relationships: parent-child relationship rules (status transitions, segment/type, risk alignment).
- Semi-structured structures preserved: JSON objects and XML payload generation.

Reference artifacts:
- `demo_output/summary.json`
- `demo_output/data_quality_report.json` (includes `valid_rate: 1.0` for FKs)

## Expected deliverables mapping

| Expected deliverable | How this repo meets it |
| --- | --- |
| Architecture design for synthesizer tool | Clear modular structure across schema generation, validation, conversion, generation engine, writers, and reporting. |
| Schema generation approach | Prompt-to-schema flow with retry + validation feedback loop. |
| Synthetic data generation pipeline | Ordered table generation with PK/FK logic, Faker value generation, relationship-aware rules, and multi-format writers. |
| Example generated dataset | Example outputs provided in `demo_output/` (schema, quality report, CSV/SQLite/Parquet/Delta artifacts). |
| Code implementation (Python) | Entire solution is Python (`main.py`, `src/`, `tests/`). |
| Explanation of distribution/relationship preservation | Quantified in `data_quality_report.json` and `metrics.json` (categorical distributions, numeric summaries, FK integrity, relationship alignment). |

## Grading rubric mapping

| Rubric area | How it is addressed |
| --- | --- |
| Architecture Design (25%) | Modular design with clear separation of concerns; supports multiple schema/data output formats. |
| Synthetic Data Quality (25%) | Uses weighted distributions, relationship rules, and FK checks; quality reports quantify results. |
| Engineering Implementation (20%) | Clean Python modules, Pydantic validation, Faker integration, reusable utility/writer components. |
| Handling Semi-Structured Data (15%) | Supports both JSON and XML fields in schema and generated data; XML generation logic is covered in tests. |
| Scalability and Performance (15%) | Includes batching/stress-mode options and per-table throughput metrics for larger runs. |

## Quick run
1. `uv sync`
2. Add `.env` with `GEMINI_API_KEY=...` (optional `GEMINI_MODEL=...`)
3. `uv run python main.py`

Default outputs are written under `output/yolo/`.  
I will keep adding or refreshing sample generated datasets under `demo_output/` for submission demos.
