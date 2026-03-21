import argparse
import json
import sys
from pathlib import Path

from schema_generator import gen_schema_with_request
from synthgen.engine import generate_data
from synthgen.reporting import build_quality_report
from synthgen.schema_utils import table_order
from synthgen.writers import CSVWriter, SQLiteWriter


def _parse_formats(raw: str | list[str] | None) -> list[str]:
    if raw is None:
        return ["csv", "sqlite"]

    if isinstance(raw, list):
        tokens = [str(part).strip().lower() for part in raw]
    else:
        tokens = [part.strip().lower() for part in str(raw).split(",")]

    normalized: list[str] = []
    for token in tokens:
        if token and token not in normalized:
            normalized.append(token)
    if not normalized:
        normalized = ["csv", "sqlite"]

    unsupported = set(normalized) - {"csv", "sqlite"}
    if unsupported:
        raise SystemExit(f"Unsupported formats: {sorted(unsupported)}")
    return normalized


def _run_data_generation(
    schema: dict,
    *,
    out_dir: str,
    records: int,
    seed: int,
    formats: list[str],
    sqlite_path: str | None,
) -> None:
    output_dir = Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    order = table_order(schema)
    writers: list[object] = []
    csv_writer = None
    sqlite_writer = None

    if "csv" in formats:
        csv_writer = CSVWriter(output_dir)
        writers.append(csv_writer)

    if "sqlite" in formats:
        db_path = Path(sqlite_path) if sqlite_path else output_dir / "synthetic.db"
        sqlite_writer = SQLiteWriter(sqlite_path=db_path, schema=schema, order=order)
        writers.append(sqlite_writer)

    if not writers:
        raise SystemExit("No output writers selected. Use --formats csv and/or sqlite.")

    try:
        summary, metrics = generate_data(
            schema=schema,
            records=records,
            seed=seed,
            writers=writers,
            order=order,
        )
    finally:
        for writer in writers:
            writer.close()

    report = build_quality_report(
        schema=schema, summary=summary, metrics=metrics, seed=seed
    )
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    (output_dir / "quality_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )

    outputs = ["csv" if csv_writer else None, "sqlite" if sqlite_writer else None]
    outputs = [entry for entry in outputs if entry]
    print(f"Generated synthetic data ({', '.join(outputs)}) in {output_dir}")


def _run_request(
    request: dict,
    *,
    out_dir: str | None = None,
    records: int | None = None,
    seed: int | None = None,
    formats: str | None = None,
    sqlite_path: str | None = None,
) -> None:
    schema = request.get("schema")
    if not isinstance(schema, dict):
        raise SystemExit("Request is missing a valid 'schema' object.")

    generation = request.get("generation", {})
    resolved_out_dir = out_dir or generation.get("out_dir", "output/synthetic")
    resolved_records = (
        records if records is not None else int(generation.get("records", 500))
    )
    resolved_seed = seed if seed is not None else int(generation.get("seed", 42))
    resolved_formats = _parse_formats(
        formats if formats is not None else generation.get("formats")
    )
    resolved_sqlite_path = (
        sqlite_path if sqlite_path is not None else generation.get("sqlite_path")
    )

    _run_data_generation(
        schema,
        out_dir=resolved_out_dir,
        records=resolved_records,
        seed=resolved_seed,
        formats=resolved_formats,
        sqlite_path=resolved_sqlite_path,
    )


def _resolve_scenario(value: str | None) -> str:
    scenario = (value or input("Business scenario: ")).strip()
    if not scenario:
        raise SystemExit("Business scenario is required")
    return scenario


def _write_json(path: str, payload: dict) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Data Synthesizer repo entrypoint")
    subparsers = parser.add_subparsers(dest="command")

    schema_parser = subparsers.add_parser(
        "schema", help="Generate schema and request JSON"
    )
    schema_parser.add_argument("scenario", nargs="?", help="Business data scenario")
    schema_parser.add_argument(
        "--schema-out", default="output/schema.json", help="Schema output path"
    )
    schema_parser.add_argument(
        "--request-out",
        default="output/data_generation_request.json",
        help="Data-generation request output path",
    )
    schema_parser.add_argument(
        "--records", type=int, default=500, help="Base record count"
    )
    schema_parser.add_argument("--seed", type=int, default=42, help="Random seed")
    schema_parser.add_argument(
        "--out-dir", default="output/synthetic", help="Synthetic data output directory"
    )
    schema_parser.add_argument(
        "--formats",
        default="csv,sqlite",
        help="Comma-separated output formats for downstream generation",
    )
    schema_parser.add_argument(
        "--sqlite-path",
        default=None,
        help="Optional sqlite output path for downstream generation",
    )
    schema_parser.add_argument(
        "--run-data",
        action="store_true",
        help="Immediately run data generation using emitted request",
    )

    data_parser = subparsers.add_parser("data", help="Generate synthetic data")
    data_parser.add_argument(
        "--schema", default="output/schema.json", help="Path to schema JSON"
    )
    data_parser.add_argument(
        "--request",
        default=None,
        help="Path to data-generation request JSON from `schema` command",
    )
    data_parser.add_argument(
        "--out-dir", default=None, help="Output directory override"
    )
    data_parser.add_argument(
        "--records", type=int, default=None, help="Base record count override"
    )
    data_parser.add_argument(
        "--seed", type=int, default=None, help="Random seed override"
    )
    data_parser.add_argument(
        "--formats", default=None, help="Comma-separated output formats override"
    )
    data_parser.add_argument(
        "--sqlite-path",
        default=None,
        help="Optional sqlite output path override",
    )

    pipeline_parser = subparsers.add_parser(
        "pipeline", help="Generate schema, request JSON, and synthetic data"
    )
    pipeline_parser.add_argument("scenario", nargs="?", help="Business data scenario")
    pipeline_parser.add_argument(
        "--schema-out", default="output/schema.json", help="Schema output path"
    )
    pipeline_parser.add_argument(
        "--request-out",
        default="output/data_generation_request.json",
        help="Data-generation request output path",
    )
    pipeline_parser.add_argument(
        "--records", type=int, default=500, help="Base record count"
    )
    pipeline_parser.add_argument("--seed", type=int, default=42, help="Random seed")
    pipeline_parser.add_argument(
        "--out-dir", default="output/synthetic", help="Synthetic data output directory"
    )
    pipeline_parser.add_argument(
        "--formats",
        default="csv,sqlite",
        help="Comma-separated output formats",
    )
    pipeline_parser.add_argument(
        "--sqlite-path",
        default=None,
        help="Optional sqlite output path",
    )

    return parser


def main() -> None:
    parser = _build_parser()
    argv = sys.argv[1:]

    if argv and argv[0] in {"-h", "--help"}:
        parser.print_help()
        return

    supported_commands = {"schema", "data", "pipeline"}
    if not argv or argv[0] not in supported_commands:
        argv = ["data", *argv]

    args = parser.parse_args(argv)

    if args.command == "schema":
        scenario = _resolve_scenario(args.scenario)
        schema_path = Path(args.schema_out)
        result = gen_schema_with_request(
            scenario,
            records=args.records,
            seed=args.seed,
            out_dir=args.out_dir,
            formats=_parse_formats(args.formats),
            sqlite_path=args.sqlite_path,
            schema_path=str(schema_path),
        )

        written_schema = _write_json(args.schema_out, result["schema"])
        request = result["data_generation_request"]
        request["schema_path"] = str(written_schema)
        written_request = _write_json(args.request_out, request)

        print(f"Schema written to {written_schema}")
        print(f"Data generation request written to {written_request}")

        if args.run_data:
            _run_request(request)
        return

    if args.command == "pipeline":
        scenario = _resolve_scenario(args.scenario)
        schema_path = Path(args.schema_out)
        result = gen_schema_with_request(
            scenario,
            records=args.records,
            seed=args.seed,
            out_dir=args.out_dir,
            formats=_parse_formats(args.formats),
            sqlite_path=args.sqlite_path,
            schema_path=str(schema_path),
        )

        written_schema = _write_json(args.schema_out, result["schema"])
        request = result["data_generation_request"]
        request["schema_path"] = str(written_schema)
        written_request = _write_json(args.request_out, request)

        print(f"Schema written to {written_schema}")
        print(f"Data generation request written to {written_request}")
        _run_request(request)
        return

    if args.request:
        request = json.loads(Path(args.request).read_text(encoding="utf-8"))
        _run_request(
            request,
            out_dir=args.out_dir,
            records=args.records,
            seed=args.seed,
            formats=args.formats,
            sqlite_path=args.sqlite_path,
        )
        return

    schema = json.loads(Path(args.schema).read_text(encoding="utf-8"))
    _run_data_generation(
        schema,
        out_dir=args.out_dir or "output/synthetic",
        records=args.records if args.records is not None else 500,
        seed=args.seed if args.seed is not None else 42,
        formats=_parse_formats(args.formats),
        sqlite_path=args.sqlite_path,
    )


if __name__ == "__main__":
    main()
