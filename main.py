import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from synthgen.common import safe_name
from synthgen.engine import generate_data
from synthgen.reporting import build_quality_report
from synthgen.gen_schema.schema_generator import SchemaGenerationError, gen_schema_with_request
from synthgen.gen_schema.schema_loader import load_schema
from synthgen.gen_schema.schema_utils import table_order
from synthgen.writers import CSVWriter, SQLiteWriter

DEFAULT_CONFIG_PATH = Path("run_config.json")
SUPPORTED_COMMANDS = {"schema", "data", "pipeline", "benchmark"}

HELP_TEXT = """Data Synthesizer (config-first)

Usage:
  python main.py
  python main.py path/to/run_config.json

Set \"command\" in your config file to one of:
  schema    -> generate schema + request files
  data      -> generate synthetic data from schema or request
  pipeline  -> schema + data in one run
  benchmark -> run simple performance benchmark
"""


def _clean_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise SystemExit(f"Invalid integer for '{name}': {value}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"Invalid integer for '{name}': {value}") from exc


def _int_or_default(value: object, name: str, default: int) -> int:
    if value is None:
        return default
    return _to_int(value, name)


def _as_bool(value: object, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _parse_formats(raw: object) -> list[str]:
    if raw is None:
        return ["csv", "sqlite"]

    if isinstance(raw, list):
        tokens = [str(item).strip().lower() for item in raw]
    else:
        tokens = [part.strip().lower() for part in str(raw).split(",")]

    formats: list[str] = []
    for token in tokens:
        if token and token not in formats:
            formats.append(token)

    if not formats:
        formats = ["csv", "sqlite"]

    unsupported = sorted(set(formats) - {"csv", "sqlite"})
    if unsupported:
        raise SystemExit(f"Unsupported formats: {unsupported}")

    return formats


def _load_json_object(path: Path, label: str) -> dict:
    if not path.exists():
        raise SystemExit(f"{label} file does not exist: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{label} file is not valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"{label} file must contain a JSON object: {path}")
    return payload


def _load_config(path: Path) -> dict:
    resolved = path.expanduser()
    return _load_json_object(resolved, "Config")


def _peak_memory_mb() -> float | None:
    try:
        import resource
    except ImportError:
        return None

    max_rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if sys.platform == "darwin":
        return round(max_rss / (1024 * 1024), 4)
    return round(max_rss / 1024, 4)


def _write_json(path: str | Path, payload: dict) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path


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
    performance_report_out: str | None = None,
    quiet: bool = False,
) -> dict:
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
        raise SystemExit("No output writers selected. Use 'csv' and/or 'sqlite'.")

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

    quality_report = build_quality_report(
        schema=schema,
        summary=summary,
        metrics=metrics,
        seed=seed,
    )
    _write_json(output_dir / "summary.json", summary)
    _write_json(output_dir / "quality_report.json", quality_report)

    elapsed_seconds = time.perf_counter() - started
    artifact_sizes = _collect_output_artifact_sizes(
        summary,
        output_dir=output_dir,
        formats=formats,
        sqlite_path=sqlite_path,
    )

    rows_generated = sum(summary.values())
    output_bytes = sum(artifact_sizes.values())
    output_mb = output_bytes / (1024 * 1024)

    performance_report = {
        "schema_name": schema.get("schema_name"),
        "domain": schema.get("domain"),
        "seed": seed,
        "records_input": records,
        "rows_generated_total": rows_generated,
        "elapsed_seconds": round(elapsed_seconds, 4),
        "rows_per_second": round(rows_generated / elapsed_seconds, 2)
        if elapsed_seconds > 0
        else None,
        "peak_memory_mb": _peak_memory_mb(),
        "output_bytes": output_bytes,
        "output_mb": round(output_mb, 4),
        "artifacts": artifact_sizes,
    }

    performance_path = (
        Path(performance_report_out)
        if performance_report_out
        else output_dir / "performance_report.json"
    )
    _write_json(performance_path, performance_report)

    if not quiet:
        selected_outputs = [
            name
            for name, writer in (("csv", csv_writer), ("sqlite", sqlite_writer))
            if writer
        ]
        print(
            f"Generated synthetic data ({', '.join(selected_outputs)}) in {output_dir}"
        )
        print(f"Performance report written to {performance_path}")

    return {
        "summary": summary,
        "quality_report": quality_report,
        "performance_report": performance_report,
        "performance_report_path": str(performance_path),
        "output_dir": str(output_dir),
    }


def _resolve_perf_report_out(
    config: dict, generation: dict | None = None
) -> str | None:
    generation = generation or {}
    return (
        _clean_string(config.get("performance_report_out"))
        or _clean_string(config.get("perf_report_out"))
        or _clean_string(generation.get("performance_report_out"))
        or _clean_string(generation.get("perf_report_out"))
    )


def _run_request_object(request: dict, config: dict, *, quiet: bool = False) -> dict:
    schema = request.get("schema")
    if not isinstance(schema, dict):
        raise SystemExit("Request is missing a valid 'schema' object.")

    generation = request.get("generation", {})
    if not isinstance(generation, dict):
        generation = {}

    records = _int_or_default(
        config.get("records"),
        "records",
        _int_or_default(generation.get("records"), "request.generation.records", 500),
    )
    seed = _int_or_default(
        config.get("seed"),
        "seed",
        _int_or_default(generation.get("seed"), "request.generation.seed", 42),
    )

    out_dir = (
        _clean_string(config.get("out_dir"))
        or _clean_string(generation.get("out_dir"))
        or "output/synthetic"
    )
    formats_raw = (
        config.get("formats")
        if config.get("formats") is not None
        else generation.get("formats")
    )
    formats = _parse_formats(formats_raw)

    sqlite_path = _clean_string(config.get("sqlite_path")) or _clean_string(
        generation.get("sqlite_path")
    )
    perf_out = _resolve_perf_report_out(config, generation)

    return _run_data_generation(
        schema,
        out_dir=out_dir,
        records=records,
        seed=seed,
        formats=formats,
        sqlite_path=sqlite_path,
        performance_report_out=perf_out,
        quiet=quiet,
    )


def _run_schema_command(config: dict) -> dict:
    scenario = _clean_string(config.get("scenario"))
    if not scenario:
        raise SystemExit("'scenario' is required for the schema/pipeline command.")

    schema_out = _clean_string(config.get("schema_out")) or "output/schema.json"
    request_out = (
        _clean_string(config.get("request_out"))
        or "output/data_generation_request.json"
    )
    validation_report_out = (
        _clean_string(config.get("validation_report_out"))
        or "output/schema_validation_report.json"
    )

    records = _int_or_default(config.get("records"), "records", 500)
    seed = _int_or_default(config.get("seed"), "seed", 42)
    out_dir = _clean_string(config.get("out_dir")) or "output/synthetic"
    formats = _parse_formats(config.get("formats"))
    sqlite_path = _clean_string(config.get("sqlite_path"))
    perf_out = _resolve_perf_report_out(config)

    try:
        result = gen_schema_with_request(
            scenario,
            records=records,
            seed=seed,
            out_dir=out_dir,
            formats=formats,
            sqlite_path=sqlite_path,
            perf_report_out=perf_out,
            schema_path=schema_out,
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


def _run_data_command(config: dict) -> None:
    request_path = _clean_string(config.get("request"))
    if request_path:
        request = _load_json_object(Path(request_path), "Request")
        _run_request_object(request, config)
        return

    schema_path = Path(_clean_string(config.get("schema")) or "output/schema.json")
    schema = load_schema(schema_path)

    records = _int_or_default(config.get("records"), "records", 500)
    seed = _int_or_default(config.get("seed"), "seed", 42)
    out_dir = _clean_string(config.get("out_dir")) or "output/synthetic"
    formats = _parse_formats(config.get("formats"))
    sqlite_path = _clean_string(config.get("sqlite_path"))
    perf_out = _resolve_perf_report_out(config)

    _run_data_generation(
        schema,
        out_dir=out_dir,
        records=records,
        seed=seed,
        formats=formats,
        sqlite_path=sqlite_path,
        performance_report_out=perf_out,
    )


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _format_number(value: float | int | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, int):
        return str(value)
    return f"{value:.{digits}f}"


def _build_benchmark_markdown(summary: dict) -> str:
    config = summary["config"]
    lines = [
        "# Benchmark Results",
        "",
        f"- Generated at (UTC): {summary['completed_at_utc']}",
        f"- Source: `{config['source']}`",
        f"- Source path: `{config['source_path']}`",
        f"- Scales: `{', '.join(str(value) for value in config['scales'])}`",
        f"- Repeats: `{config['repeats']}`",
        f"- Formats: `{config['formats']}`",
        f"- Seed: `{config['seed']}`",
        "",
        "| Records | Runs | Avg Rows | Avg Elapsed (s) | Avg Rows/s | Avg Output (MB) | Avg Peak Mem (MB) |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for row in summary["aggregates"]:
        lines.append(
            "| "
            + f"{row['records_input']} | "
            + f"{row['run_count']} | "
            + f"{_format_number(row['avg_rows_generated'], digits=0)} | "
            + f"{_format_number(row['avg_elapsed_seconds'])} | "
            + f"{_format_number(row['avg_rows_per_second'])} | "
            + f"{_format_number(row['avg_output_mb'])} | "
            + f"{_format_number(row['avg_peak_memory_mb'])} |"
        )

    lines.extend(
        [
            "",
            "## Files",
            "",
            f"- Runs: `{summary['runs_json_path']}`",
            f"- Summary: `{summary['summary_json_path']}`",
        ]
    )

    return "\n".join(lines) + "\n"


def _load_benchmark_schema(config: dict) -> tuple[str, str, dict]:
    request_path = _clean_string(config.get("request"))
    if request_path:
        request = _load_json_object(Path(request_path), "Request")
        schema = request.get("schema")
        if not isinstance(schema, dict):
            raise SystemExit("Request is missing a valid 'schema' object.")
        return "request", request_path, schema

    schema_path = _clean_string(config.get("schema")) or "output/schema.json"
    schema = load_schema(Path(schema_path))
    return "schema", schema_path, schema


def _parse_scales(value: object, *, default_records: int) -> list[int]:
    if value is None:
        return [default_records]
    if not isinstance(value, list) or not value:
        raise SystemExit("benchmark.scales must be a non-empty list of integers.")

    scales: list[int] = []
    for index, item in enumerate(value):
        records = _to_int(item, f"benchmark.scales[{index}]")
        if records < 1:
            raise SystemExit("benchmark.scales values must be >= 1.")
        scales.append(records)
    return scales


def _run_benchmark_command(config: dict) -> None:
    source_name, source_path, schema = _load_benchmark_schema(config)

    benchmark_config = config.get("benchmark", {})
    if benchmark_config is None:
        benchmark_config = {}
    if not isinstance(benchmark_config, dict):
        raise SystemExit("'benchmark' must be a JSON object.")

    default_records = _int_or_default(config.get("records"), "records", 500)
    scales = _parse_scales(
        benchmark_config.get("scales"), default_records=default_records
    )
    repeats = _int_or_default(benchmark_config.get("repeats"), "benchmark.repeats", 1)
    if repeats < 1:
        raise SystemExit("benchmark.repeats must be >= 1.")

    formats_raw = (
        benchmark_config.get("formats")
        if benchmark_config.get("formats") is not None
        else config.get("formats")
    )
    formats = _parse_formats(formats_raw)
    seed = _int_or_default(config.get("seed"), "seed", 42)

    out_root = Path(
        _clean_string(benchmark_config.get("out_dir")) or "output/benchmark"
    )
    label = _clean_string(benchmark_config.get("label")) or datetime.now(
        timezone.utc
    ).strftime("%Y%m%d_%H%M%S")
    benchmark_dir = out_root / label
    benchmark_dir.mkdir(parents=True, exist_ok=True)

    runs: list[dict] = []
    print(f"Running benchmark in {benchmark_dir}")

    for records in scales:
        for run_index in range(1, repeats + 1):
            run_dir = benchmark_dir / f"records_{records}" / f"run_{run_index}"
            run_dir.mkdir(parents=True, exist_ok=True)

            print(f"- records={records}, run={run_index}/{repeats}")
            run_result = _run_data_generation(
                schema,
                out_dir=str(run_dir),
                records=records,
                seed=seed,
                formats=formats,
                sqlite_path=None,
                performance_report_out=None,
                quiet=True,
            )

            performance = run_result["performance_report"]
            runs.append(
                {
                    "records_input": records,
                    "run_index": run_index,
                    "elapsed_seconds": performance.get("elapsed_seconds"),
                    "rows_generated_total": performance.get("rows_generated_total"),
                    "rows_per_second": performance.get("rows_per_second"),
                    "output_mb": performance.get("output_mb"),
                    "peak_memory_mb": performance.get("peak_memory_mb"),
                    "output_dir": run_result["output_dir"],
                    "performance_report_path": run_result["performance_report_path"],
                }
            )

    grouped: dict[int, list[dict]] = {scale: [] for scale in scales}
    for run in runs:
        grouped[run["records_input"]].append(run)

    aggregates: list[dict] = []
    for scale in scales:
        group = grouped[scale]

        rows_values = [
            float(value)
            for value in [item.get("rows_generated_total") for item in group]
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        elapsed_values = [
            float(value)
            for value in [item.get("elapsed_seconds") for item in group]
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        rows_per_second_values = [
            float(value)
            for value in [item.get("rows_per_second") for item in group]
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        output_mb_values = [
            float(value)
            for value in [item.get("output_mb") for item in group]
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        peak_memory_values = [
            float(value)
            for value in [item.get("peak_memory_mb") for item in group]
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]

        aggregates.append(
            {
                "records_input": scale,
                "run_count": len(group),
                "avg_rows_generated": _average(rows_values),
                "avg_elapsed_seconds": _average(elapsed_values),
                "avg_rows_per_second": _average(rows_per_second_values),
                "avg_output_mb": _average(output_mb_values),
                "avg_peak_memory_mb": _average(peak_memory_values),
            }
        )

    runs_path = benchmark_dir / "benchmark_runs.json"
    summary_path = benchmark_dir / "benchmark_summary.json"
    markdown_path = benchmark_dir / "benchmark_results.md"

    summary = {
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "runs_json_path": str(runs_path),
        "summary_json_path": str(summary_path),
        "config": {
            "source": source_name,
            "source_path": source_path,
            "scales": scales,
            "repeats": repeats,
            "formats": ",".join(formats),
            "seed": seed,
        },
        "aggregates": aggregates,
    }

    _write_json(runs_path, {"runs": runs})
    _write_json(summary_path, summary)
    markdown_path.write_text(_build_benchmark_markdown(summary), encoding="utf-8")

    print(f"Benchmark runs written to {runs_path}")
    print(f"Benchmark summary written to {summary_path}")
    print(f"Benchmark markdown written to {markdown_path}")


def main() -> None:
    argv = sys.argv[1:]

    if argv and argv[0] in {"-h", "--help"}:
        print(HELP_TEXT)
        return

    if len(argv) > 1:
        raise SystemExit("Usage: python main.py [path/to/run_config.json]")

    config_path = Path(argv[0]) if argv else DEFAULT_CONFIG_PATH
    config = _load_config(config_path)

    command = (_clean_string(config.get("command")) or "data").lower()
    if command not in SUPPORTED_COMMANDS:
        raise SystemExit(
            f"Unsupported command '{command}'. Use one of: {sorted(SUPPORTED_COMMANDS)}"
        )

    if command == "schema":
        request = _run_schema_command(config)
        if _as_bool(config.get("run_data"), default=False):
            _run_request_object(request, config)
        return

    if command == "pipeline":
        request = _run_schema_command(config)
        _run_request_object(request, config)
        return

    if command == "benchmark":
        _run_benchmark_command(config)
        return

    _run_data_command(config)


if __name__ == "__main__":
    main()
