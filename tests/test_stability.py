#!/usr/bin/env python3
"""Test stability across multiple business scenarios."""
import subprocess
import sys
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

SCENARIOS = [
    "credit risk",
    "retail banking",
    "financial trading",
    'banking CRM',
]


def run_scenario(scenario: str) -> tuple[str, bool, str]:
    """Run main.py with a specific scenario."""
    try:
        env = os.environ.copy()
        env["OUTPUT_DIR"] = f"output_{scenario.replace(' ', '_')}"
        result = subprocess.run(
            [sys.executable, "main.py", scenario],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=Path(__file__).parent.parent,
            env=env,
        )
        success = result.returncode == 0
        output = result.stdout if success else result.stderr
        return scenario, success, output
    except Exception as e:
        return scenario, False, str(e)


def main():
    print(f"Testing {len(SCENARIOS)} scenarios...\n")

    results = []
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(run_scenario, s): s for s in SCENARIOS}

        for future in as_completed(futures):
            scenario, success, output = future.result()
            results.append((scenario, success))
            status = "✓" if success else "✗"
            print(f"{status} {scenario}")
            if not success:
                print(f"  Error: {output[-500:]}")

    print(f"\n{'='*50}")
    passed = sum(1 for _, s in results if s)
    print(f"Results: {passed}/{len(results)} passed")

    if passed < len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
