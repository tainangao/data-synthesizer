import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from main import _load_config, _run_benchmark_command


def main() -> None:
    argv = sys.argv[1:]
    if argv and argv[0] in {"-h", "--help"}:
        print("Usage: python scripts/benchmark_generation.py [path/to/run_config.json]")
        return

    if len(argv) > 1:
        raise SystemExit("Usage: python scripts/benchmark_generation.py [config-path]")

    config_path = Path(argv[0]) if argv else REPO_ROOT / "run_config.json"
    config = _load_config(config_path)
    _run_benchmark_command(config)


if __name__ == "__main__":
    main()
