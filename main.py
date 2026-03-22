import json
import sys
import time
from pathlib import Path

from synthgen.common import safe_name
from synthgen.engine import generate_data
from synthgen.reporting import build_quality_report
from synthgen.schema_generator import SchemaGenerationError, gen_schema_with_request
from synthgen.schema_utils import table_order
from synthgen.writers import CSVWriter, SQLiteWriter

HELP_TEXT = """Data Synthesizer

Commands:
  python main.py data [--schema PATH] [--request PATH] [--records N] [--seed N]
                      [--out-dir PATH] [--formats csv,sqlite]
                      [--sqlite-path PATH] [--perf-report-out PATH]

  python main.py schema "business scenario" [--records N] [--seed N]
                        [--schema-out PATH] [--request-out PATH]
                        [--validation-report-out PATH] [--out-dir PATH]
                        [--formats csv,sqlite] [--sqlite-path PATH]
                        [--perf-report-out PATH] [--run-data]

  python main.py pipeline "business scenario" [same flags as schema]

Notes:
  - Running with no command defaults to `data`.
  - If `data` gets no `--request`, it reads `output/schema.json`.
  - Optional config file: `run_config.json` (or `--config path/to/file.json`).
"""

COMMANDS = {"schema", "data", "pipeline"}
DEFAULT_CONFIG_PATH = Path("run_config.json")


def _parse_formats(raw: str | list[str] | None) -> list[str]:
    if not raw:
        return ["csv", "sqlite"]

    tokens = raw if isinstance(raw, list) else str(raw).split(",")

    normalized: list[str] = []
    for token in tokens:
        value = str(token).strip().lower()
        if value and value not in normalized:
            normalized.append(value)

    if not normalized:
        return ["csv", "sqlite"]

    unsupported = set(normalized) - {"csv", "sqlite"}
    if unsupported:
        raise SystemExit(f"Unsupported formats: {sorted(unsupported)}")
    return normalized


def _opt_str(options: dict[str, object], key: str) -> str | None:
    value = options.get(key)
    if value is None or isinstance(value, bool):
        return None
    return str(value)


def _opt_int(
    options: dict[str, object], key: str, default: int | None = None
) -> int | None:
    value = options.get(key)
    if value is None or isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(
            f"Invalid integer for --{key.replace('_', '-')}: {value}"
        ) from exc


def _opt_flag(options: dict[str, object], key: str) -> bool:
    value = options.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "y", "on"}
    return False


def _split_cli(argv: list[str]) -> tuple[str, list[str], dict[str, object]]:
    if "-h" in argv or "--help" in argv:
        print(HELP_TEXT)
        raise SystemExit(0)

    command = "data"
    rest = argv
    if argv and argv[0] in COMMANDS:
        command = argv[0]
        rest = argv[1:]

    options: dict[str, object] = {}
    positionals: list[str] = []
    i = 0
    while i < len(rest):
        token = rest[i]
        if token.startswith("--"):
            key = token[2:].replace("-", "_")
            has_value = i + 1 < len(rest) and not rest[i + 1].startswith("--")
            options[key] = rest[i + 1] if has_value else True
            i += 2 if has_value else 1
            continue
        positionals.append(token)
        i += 1

    return command, positionals, options


def _load_config(path: Path) -> dict[str, object]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit(f"Config file must contain a JSON object: {path}")

    normalized: dict[str, object] = {}
    for key, value in raw.items():
        normalized[str(key).replace("-", "_")] = value
    return normalized


def _peak_memory_mb() -> float | None:
    try:
        import resource
    except ImportError:
        return None

    max_rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if sys.platform == "darwin":
        return round(max_rss / (1024 * 1024), 4)
    return round(max_rss / 1024, 4)


def _collect_output_artifact_sizes(
    summary: dict[str, int],
    *,
    output_dir: Path,
    formats: list[str],
    sqlite_path: str | None,
) -> dict[str, int]:
    artifacts: dict[str, int] = {}

    if "csv" in formats:
        for table_name in summary:
            csv_path = output_dir / f"{safe_name(table_name)}.csv"
            if csv_path.exists():
                artifacts[str(csv_path)] = csv_path.stat().st_size

    if "sqlite" in formats:
        db_path = Path(sqlite_path) if sqlite_path else output_dir / "synthetic.db"
        if db_path.exists():
            artifacts[str(db_path)] = db_path.stat().st_size

    return artifacts


def _run_data_generation(
    schema: dict,
    *,
    out_dir: str,
    records: int,
    seed: int,
    formats: list[str],
    sqlite_path: str | None,
    perf_report_out: str | None = None,
) -> None:
    started = time.perf_counter()
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

    elapsed_seconds = time.perf_counter() - started
    artifact_sizes = _collect_output_artifact_sizes(
        summary,
        output_dir=output_dir,
        formats=formats,
        sqlite_path=sqlite_path,
    )

    total_rows = sum(summary.values())
    output_bytes = sum(artifact_sizes.values())
    output_mb = output_bytes / (1024 * 1024)
    performance_report = {
        "schema_name": schema.get("schema_name"),
        "domain": schema.get("domain"),
        "seed": seed,
        "records_input": records,
        "rows_generated_total": total_rows,
        "elapsed_seconds": round(elapsed_seconds, 4),
        "rows_per_second": round(total_rows / elapsed_seconds, 2)
        if elapsed_seconds > 0
        else None,
        "peak_memory_mb": _peak_memory_mb(),
        "output_bytes": output_bytes,
        "output_mb": round(output_mb, 4),
        "mb_per_second": round(output_mb / elapsed_seconds, 4)
        if elapsed_seconds > 0
        else None,
        "artifacts": artifact_sizes,
        "table_performance": metrics.get("table_performance", {}),
    }

    report = build_quality_report(
        schema=schema,
        summary=summary,
        metrics=metrics,
        seed=seed,
    )

    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    (output_dir / "quality_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )

    perf_report_path = (
        Path(perf_report_out)
        if perf_report_out
        else output_dir / "performance_report.json"
    )
    perf_report_path.parent.mkdir(parents=True, exist_ok=True)
    perf_report_path.write_text(
        json.dumps(performance_report, indent=2), encoding="utf-8"
    )

    outputs = ["csv" if csv_writer else None, "sqlite" if sqlite_writer else None]
    outputs = [item for item in outputs if item]
    print(f"Generated synthetic data ({', '.join(outputs)}) in {output_dir}")
    print(f"Performance report written to {perf_report_path}")


def _run_request(
    request: dict,
    *,
    out_dir: str | None = None,
    records: int | None = None,
    seed: int | None = None,
    formats: str | list[str] | None = None,
    sqlite_path: str | None = None,
    perf_report_out: str | None = None,
) -> None:
    schema = request.get("schema")
    if not isinstance(schema, dict):
        raise SystemExit("Request is missing a valid 'schema' object.")

    generation = request.get("generation", {})
    _run_data_generation(
        schema,
        out_dir=out_dir or generation.get("out_dir", "output/synthetic"),
        records=records if records is not None else int(generation.get("records", 500)),
        seed=seed if seed is not None else int(generation.get("seed", 42)),
        formats=_parse_formats(formats or generation.get("formats")),
        sqlite_path=sqlite_path
        if sqlite_path is not None
        else generation.get("sqlite_path"),
        perf_report_out=(
            perf_report_out
            if perf_report_out is not None
            else generation.get("perf_report_out")
        ),
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


def _run_schema_command(
    scenario: str,
    *,
    schema_out: str,
    request_out: str,
    validation_report_out: str,
    records: int,
    seed: int,
    out_dir: str,
    formats: list[str],
    sqlite_path: str | None,
    perf_report_out: str | None,
) -> dict:
    schema_path = Path(schema_out)

    try:
        result = gen_schema_with_request(
            scenario,
            records=records,
            seed=seed,
            out_dir=out_dir,
            formats=formats,
            sqlite_path=sqlite_path,
            perf_report_out=perf_report_out,
            schema_path=str(schema_path),
        )
    except SchemaGenerationError as exc:
        written_report = _write_json(validation_report_out, exc.validation_report)
        print(f"Schema validation report written to {written_report}")
        raise SystemExit(str(exc)) from exc

    written_schema = _write_json(schema_out, result["schema"])
    written_report = _write_json(validation_report_out, result["validation_report"])
    request = result["data_generation_request"]
    request["schema_path"] = str(written_schema)
    written_request = _write_json(request_out, request)

    print(f"Schema written to {written_schema}")
    print(f"Schema validation report written to {written_report}")
    print(f"Data generation request written to {written_request}")
    return request


def main() -> None:
    argv = sys.argv[1:]
    explicit_command = bool(argv and argv[0] in COMMANDS)

    command, positionals, cli_options = _split_cli(argv)

    config_path_raw = _opt_str(cli_options, "config")
    config_path = (
        Path(config_path_raw).expanduser() if config_path_raw else DEFAULT_CONFIG_PATH
    )
    config_options: dict[str, object] = {}
    if config_path.exists():
        config_options = _load_config(config_path)

    options = {**config_options, **cli_options}

    use_config_command = (
        not explicit_command
        and not positionals
        and set(cli_options).issubset({"config"})
    )
    if use_config_command:
        config_command = _opt_str(options, "command")
        if config_command in COMMANDS:
            command = config_command

    if command in {"schema", "pipeline"}:
        scenario = _resolve_scenario(
            positionals[0] if positionals else _opt_str(options, "scenario")
        )

        request = _run_schema_command(
            scenario,
            schema_out=_opt_str(options, "schema_out") or "output/schema.json",
            request_out=_opt_str(options, "request_out")
            or "output/data_generation_request.json",
            validation_report_out=_opt_str(options, "validation_report_out")
            or "output/schema_validation_report.json",
            records=_opt_int(options, "records", 500) or 500,
            seed=_opt_int(options, "seed", 42) or 42,
            out_dir=_opt_str(options, "out_dir") or "output/synthetic",
            formats=_parse_formats(options.get("formats") or "csv,sqlite"),
            sqlite_path=_opt_str(options, "sqlite_path"),
            perf_report_out=_opt_str(options, "perf_report_out"),
        )

        if command == "pipeline" or _opt_flag(options, "run_data"):
            _run_request(request, perf_report_out=_opt_str(options, "perf_report_out"))
        return

    request_path = _opt_str(options, "request")
    if request_path:
        request = json.loads(Path(request_path).read_text(encoding="utf-8"))
        _run_request(
            request,
            out_dir=_opt_str(options, "out_dir"),
            records=_opt_int(options, "records"),
            seed=_opt_int(options, "seed"),
            formats=options.get("formats"),
            sqlite_path=_opt_str(options, "sqlite_path"),
            perf_report_out=_opt_str(options, "perf_report_out"),
        )
        return

    schema_path = Path(_opt_str(options, "schema") or "output/schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    _run_data_generation(
        schema,
        out_dir=_opt_str(options, "out_dir") or "output/synthetic",
        records=_opt_int(options, "records", 500) or 500,
        seed=_opt_int(options, "seed", 42) or 42,
        formats=_parse_formats(options.get("formats")),
        sqlite_path=_opt_str(options, "sqlite_path"),
        perf_report_out=_opt_str(options, "perf_report_out"),
    )


if __name__ == "__main__":
    main()
