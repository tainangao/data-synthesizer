from pathlib import Path
import logging

from src.gen_schema import generate_schema, convert_schema, table_order
from src.gen_config import translate_and_validate
from src.gen_data import generate_data
from src.gen_data.data_writers import CSVWriter, SQLiteWriter, ParquetWriter, DeltaWriter
from src.utils.reporting import build_quality_report

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OP_DIR = Path(__file__).parent / "demo_output"
OP_DIR.mkdir(parents=True, exist_ok=True)

RECORD_COUNT = 74
SEED = 42

# Performance tuning options
STRESS_MODE = (
    False  # Skip expensive metrics (fk_child_counts, relationship_checks) for speed
)
SQLITE_BATCH_SIZE = 5000  # Rows per batch for SQLite executemany
PARQUET_CHUNK_SIZE = 50000  # Rows per chunk for Parquet/Delta (for memory efficiency)


def main():
    schema = generate_schema(user_prompt="credit risk", out_dir=OP_DIR)

    convert_schema(
        schema=schema,
        out_dir=OP_DIR / "schema",
        export_formats=["sqlite", "psql", "parquet", "delta"],
    )

    # Generate config from schema
    config, errors = translate_and_validate(
        schema=schema,
        base_records=RECORD_COUNT,
        seed=SEED,
        output_path=OP_DIR / "config.json"
    )

    if errors:
        logger.error(f"Config validation errors: {errors}")
        return

    generation_order = table_order(schema)

    csv_writer = CSVWriter(OP_DIR / "csv")
    sqlite_writer = SQLiteWriter(
        OP_DIR / "sqlite" / "data.db",
        schema=schema,
        order=generation_order,
        batch_size=SQLITE_BATCH_SIZE,
    )
    parquet_writer = ParquetWriter(OP_DIR / "parquet", chunk_size=PARQUET_CHUNK_SIZE)
    delta_writer = DeltaWriter(OP_DIR / "delta", chunk_size=PARQUET_CHUNK_SIZE)
    writers = [csv_writer, sqlite_writer, parquet_writer, delta_writer]

    row_counts = generate_data(
        schema=schema,
        config=config,
        writers=writers,
        seed=SEED,
    )

    logger.info(f"\nData generation complete: {row_counts}")

    # TODO: Update reporting to work with new gen_data
    # report = build_quality_report(
    #     schema=schema,
    #     summary=summary,
    #     metrics=metrics,
    #     seed=SEED,
    #     out_dir=OP_DIR,
    # )


if __name__ == "__main__":
    main()
