from pathlib import Path
import logging

from src.gen_schema import generate_schema, convert_schema, table_order
from src.gen_data import generate_data, CSVWriter, SQLiteWriter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


OP_DIR = Path(__file__).parent / "output" / "yolo"
OP_DIR.mkdir(parents=True, exist_ok=True)

RECORD_COUNT = 74
SEED = 42


def main():
    schema = generate_schema(
        user_prompt='credit risk',
        out_dir=OP_DIR
    )
    
    convert_schema(
        schema=schema,
        out_dir=OP_DIR / "schema",
        export_formats=["sqlite", "psql", "parquet", "delta"],
    )
    
    generation_order = table_order(schema)
    
    csv_writer = CSVWriter(OP_DIR / "csv")
    sqlite_writer = SQLiteWriter(OP_DIR / "sqlite" / "data.db", 
                                 schema=schema, 
                                 order=generation_order)
    writers = [csv_writer, sqlite_writer]
    
    summary, metrics = generate_data(
        schema=schema,
        records=RECORD_COUNT,
        seed=SEED,
        writers=writers,
        order=generation_order,
    )

    logger.info(f"Data generation summary: {summary}")
    logger.info(f"\n\nData generation metrics: {metrics}")


if __name__ == "__main__":
    main()
