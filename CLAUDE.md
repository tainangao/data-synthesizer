# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Financial data synthesizer that generates synthetic relational data from business scenarios. Uses Gemini API for schema generation, Faker for data generation, and exports to multiple formats (CSV, SQLite, Parquet, Delta).

## Setup and Running

```bash
# Install dependencies
uv sync

# Configure API key (required for schema generation)
# Create .env file with: GEMINI_API_KEY=your_key_here
# Optional: GEMINI_MODEL=gemini-2.0-flash-exp

# Run the full pipeline
uv run python main.py

# Run tests
uv run pytest tests/
```

## Architecture

The codebase follows a two-phase pipeline:

**Phase 1: Schema Generation** (`src/gen_schema/`)
- `schema_generator.py`: Gemini-based schema generation from business prompts with retry/validation feedback loop
- `schema_validator.py`: Structural and logical validation (PK/FK integrity, type consistency)
- `schema_converter.py`: Converts JSON schema to SQLite/PostgreSQL/Parquet/Delta artifacts
- `schema_models.py`: Pydantic models for schema representation
- `schema_utils.py`: Topological sort for FK-aware table ordering

**Phase 2: Data Generation** (`src/gen_data/`)
- `data_generator.py`: Orchestrates generation with FK-aware ordering and PK pool management
- `value_generators.py`: Faker-based value generation with temporal anchoring and inheritance logic
- `relationship_rules.py`: Business logic for status transitions, segment/type alignment, currency consistency
- `data_writers.py`: Writer implementations (CSV, SQLite, Parquet, Delta) with batching support
- `metrics_collector.py`: Collects FK integrity, distribution, and relationship metrics

**Reporting** (`src/reporting.py`)
- Generates data quality reports with FK validation, categorical distributions, and relationship checks

## Key Design Patterns

**JSON Schema as Source of Truth**: Internal schema format drives all generation and conversion. Even though JSON is the internal format, the system exports to PostgreSQL, SQLite, Parquet, and Delta.

**FK-Aware Generation**: Tables are generated in topological order. Child FK values are sampled from parent PK pools to guarantee referential integrity.

**Relationship Rules**: Not pure random generation. Applies domain logic:
- Status lifecycle: child statuses follow parent status transitions
- Segment-driven: segment influences risk/type distributions
- Currency consistency: inherited from parent or mapped from country
- Temporal ordering: downstream events occur after upstream events

**Value Inheritance**: Fields marked with `inherit_from_parent` copy values from parent records via FK relationships (see `value_generators.py`).

## Configuration Options in main.py

- `RECORD_COUNT`: Number of records per table
- `SEED`: Random seed for reproducibility
- `STRESS_MODE`: Skip expensive metrics for performance testing
- `SQLITE_BATCH_SIZE`: Batch size for SQLite inserts (default 5000)
- `PARQUET_CHUNK_SIZE`: Chunk size for Parquet/Delta writes (default 50000)

## Output Structure

All outputs go to `demo_output/`:
- `schema.json`: Generated JSON schema
- `schema_validation_report.json`: Validation results
- `schema/{sqlite,psql,parquet,delta}/`: Schema artifacts
- `{csv,sqlite,parquet,delta}/`: Generated data
- `data_quality_report.json`: FK integrity and distribution metrics
- `summary.json`: Generation summary with row counts and timing
