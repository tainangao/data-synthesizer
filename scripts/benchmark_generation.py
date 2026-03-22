import json
import statistics
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# VS Code friendly config: edit values, then click Run.
REQUEST_PATH = Path("output/data_generation_request.json")
SCHEMA_PATH = None  # Example: Path("output/schema.json")

SCALES = [10_000, 50_000, 100_000]
REPEATS = 3
FORMATS = "csv,sqlite"
SEED = 42

OUTPUT_ROOT = Path("output/benchmarks")
LABEL = ""  # Empty = timestamp folder
TIMEOUT_SECONDS = None
MARKDOWN_COPY = None  # Example: Path("docs/scalability_results.md")


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100.0) * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def _stats(values: list[float]) -> dict[str, float | int] | None:
    if not values:
        return None
    return {
        "count": len(values),
        "min": round(min(values), 4),
        "max": round(max(values), 4),
        "mean": round(statistics.fmean(values), 4),
        "median": round(statistics.median(values), 4),
        "p95": round(_percentile(values, 95.0), 4),
        "stdev": round(statistics.stdev(values), 4) if len(values) > 1 else 0.0,
    }


def _fmt(stats: dict[str, float | int] | None, key: str, digits: int = 2) -> str:
    if not stats:
        return "n/a"
    value = stats.get(key)
    if value is None:
        return "n/a"
    if isinstance(value, int):
        return str(value)
    return f"{value:.{digits}f}"


def _metric(runs: list[dict], key: str) -> list[float]:
    values: list[float] = []
    for run in runs:
        value = run["performance"].get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            values.append(float(value))
    return values


def _pick_source(repo_root: Path) -> tuple[str, Path]:
    has_request = REQUEST_PATH is not None
    has_schema = SCHEMA_PATH is not None

    if has_request == has_schema:
        raise SystemExit("Set exactly one of REQUEST_PATH or SCHEMA_PATH.")

    source_flag = "--request" if has_request else "--schema"
    source_path = REQUEST_PATH if has_request else SCHEMA_PATH
    assert source_path is not None

    resolved = source_path if source_path.is_absolute() else (repo_root / source_path)
    resolved = resolved.resolve()
    if not resolved.exists():
        raise SystemExit(f"Input file does not exist: {resolved}")
    return source_flag, resolved


def _slowest_table(perf_report: dict) -> str | None:
    table_perf = perf_report.get("table_performance", {})
    if not isinstance(table_perf, dict) or not table_perf:
        return None

    name, _ = max(
        table_perf.items(),
        key=lambda item: float(item[1].get("elapsed_seconds") or 0.0),
    )
    return name


def _run_once(
    *,
    repo_root: Path,
    main_py: Path,
    source_flag: str,
    source_path: Path,
    records: int,
    run_index: int,
    run_dir: Path,
) -> dict:
    perf_path = run_dir / "performance_report.json"
    cmd = [
        sys.executable,
        str(main_py),
        "data",
        source_flag,
        str(source_path),
        "--records",
        str(records),
        "--seed",
        str(SEED),
        "--formats",
        FORMATS,
        "--out-dir",
        str(run_dir),
        "--perf-report-out",
        str(perf_path),
    ]

    started = time.perf_counter()
    completed = subprocess.run(
        cmd,
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        check=False,
    )
    wall_clock = round(time.perf_counter() - started, 4)

    if completed.returncode != 0:
        raise SystemExit(
            "Benchmark run failed.\n"
            f"records={records}, run={run_index}, returncode={completed.returncode}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

    if not perf_path.exists():
        raise SystemExit(f"Missing performance report: {perf_path}")

    perf_report = json.loads(perf_path.read_text(encoding="utf-8"))
    return {
        "records_input": records,
        "run_index": run_index,
        "run_dir": str(run_dir),
        "wall_clock_seconds": wall_clock,
        "slowest_table": _slowest_table(perf_report),
        "performance": perf_report,
    }


def _aggregate_scale(records: int, runs: list[dict]) -> dict:
    slowest = Counter(run["slowest_table"] for run in runs if run.get("slowest_table"))
    return {
        "records_input": records,
        "run_count": len(runs),
        "elapsed_seconds": _stats(_metric(runs, "elapsed_seconds")),
        "rows_per_second": _stats(_metric(runs, "rows_per_second")),
        "peak_memory_mb": _stats(_metric(runs, "peak_memory_mb")),
        "output_mb": _stats(_metric(runs, "output_mb")),
        "slowest_table_frequency": dict(slowest),
        "most_common_slowest_table": slowest.most_common(1)[0][0] if slowest else None,
    }


def _build_markdown(summary: dict) -> str:
    config = summary["config"]
    lines = [
        "# Scalability Benchmark Results",
        "",
        f"- Generated at (UTC): {summary['completed_at_utc']}",
        f"- Source: `{config['source_path']}`",
        f"- Scales: `{', '.join(str(v) for v in config['scales'])}`",
        f"- Repeats per scale: `{config['repeats']}`",
        f"- Formats: `{config['formats']}`",
        f"- Seed: `{config['seed']}`",
        "",
        "| Records | Runs | Avg Elapsed (s) | P95 Elapsed (s) | Avg Rows/s | P95 Rows/s | Avg Peak Mem (MB) | Avg Output (MB) | Slowest Table (mode) |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]

    for aggregate in summary["aggregates"]:
        lines.append(
            "| "
            + f"{aggregate['records_input']} | "
            + f"{aggregate['run_count']} | "
            + f"{_fmt(aggregate['elapsed_seconds'], 'mean')} | "
            + f"{_fmt(aggregate['elapsed_seconds'], 'p95')} | "
            + f"{_fmt(aggregate['rows_per_second'], 'mean')} | "
            + f"{_fmt(aggregate['rows_per_second'], 'p95')} | "
            + f"{_fmt(aggregate['peak_memory_mb'], 'mean')} | "
            + f"{_fmt(aggregate['output_mb'], 'mean')} | "
            + f"{aggregate.get('most_common_slowest_table') or 'n/a'} |"
        )

    lines.extend(
        [
            "",
            "## Artifacts",
            "",
            f"- JSON summary: `{summary['results_json_path']}`",
            f"- Raw runs: `{summary['runs_json_path']}`",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    main_py = repo_root / "main.py"
    source_flag, source_path = _pick_source(repo_root)

    output_root = (
        OUTPUT_ROOT if OUTPUT_ROOT.is_absolute() else (repo_root / OUTPUT_ROOT)
    )
    label = LABEL.strip() or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    benchmark_dir = output_root / label
    runs_dir = benchmark_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    print(f"[benchmark] output dir: {benchmark_dir}")

    started_at = datetime.now(timezone.utc)
    all_runs: list[dict] = []
    grouped: dict[int, list[dict]] = {scale: [] for scale in SCALES}

    for records in SCALES:
        for run_index in range(1, REPEATS + 1):
            run_dir = runs_dir / f"records_{records}" / f"run_{run_index}"
            run_dir.mkdir(parents=True, exist_ok=True)
            print(f"[benchmark] records={records} run={run_index}/{REPEATS}")

            run = _run_once(
                repo_root=repo_root,
                main_py=main_py,
                source_flag=source_flag,
                source_path=source_path,
                records=records,
                run_index=run_index,
                run_dir=run_dir,
            )
            all_runs.append(run)
            grouped[records].append(run)

    aggregates = [_aggregate_scale(scale, grouped[scale]) for scale in SCALES]

    results_path = benchmark_dir / "benchmark_results.json"
    runs_path = benchmark_dir / "benchmark_runs.json"
    markdown_path = benchmark_dir / "scalability_results.md"

    summary = {
        "started_at_utc": started_at.isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "results_json_path": str(results_path),
        "runs_json_path": str(runs_path),
        "markdown_path": str(markdown_path),
        "config": {
            "source_path": str(source_path),
            "scales": SCALES,
            "repeats": REPEATS,
            "formats": FORMATS,
            "seed": SEED,
            "timeout_seconds": TIMEOUT_SECONDS,
        },
        "aggregates": aggregates,
    }

    results_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    runs_path.write_text(json.dumps(all_runs, indent=2), encoding="utf-8")

    markdown = _build_markdown(summary)
    markdown_path.write_text(markdown, encoding="utf-8")

    if MARKDOWN_COPY is not None:
        copy_path = (
            MARKDOWN_COPY
            if MARKDOWN_COPY.is_absolute()
            else (repo_root / MARKDOWN_COPY)
        )
        copy_path.parent.mkdir(parents=True, exist_ok=True)
        copy_path.write_text(markdown, encoding="utf-8")
        print(f"[benchmark] markdown copy: {copy_path}")

    print(f"[benchmark] summary json: {results_path}")
    print(f"[benchmark] runs json: {runs_path}")
    print(f"[benchmark] markdown: {markdown_path}")


if __name__ == "__main__":
    main()
