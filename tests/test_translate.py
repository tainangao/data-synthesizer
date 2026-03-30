"""Test script for schema to config translation."""

import json
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.gen_data.translate_config import translate_and_validate

# Test scenarios
SCENARIOS = [
    {
        "name": "credit_risk",
        "prompt": "Credit risk management system with customers, loan applications, credit scores, and repayment tracking",
        "schema_file": "demo_output/schema.json"
    },
    {
        "name": "crm",
        "prompt": "CRM system with customers, accounts, transactions, and interactions",
        "schema_file": 'tests/e2e_output/crm/schema.json'
    },
    {
        "name": "trading",
        "prompt": "Trading platform with traders, orders, executions, and settlements",
        "schema_file": None
    }
]

def test_scenario(scenario: dict):
    """Test translation for a single scenario."""
    print(f"\n{'='*60}")
    print(f"Testing: {scenario['name']}")
    print(f"{'='*60}")

    # Load or generate schema
    if scenario['schema_file']:
        schema_path = Path(__file__).parent.parent / scenario['schema_file']
        if not schema_path.exists():
            print(f"❌ Schema not found: {schema_path}")
            return False
        schema = json.loads(schema_path.read_text())
    else:
        print(f"⚠️  No schema file provided, skipping {scenario['name']}")
        return None

    # Translate
    print(f"🔄 Translating schema to config...")
    try:
        config, errors = translate_and_validate(
            schema=schema,
            base_records=1000,
            seed=42,
            output_path=Path(f"tests/e2e_output/{scenario['name']}_config.json")
        )

        if errors:
            print("❌ Validation errors:")
            for error in errors:
                print(f"  - {error}")
            return False
        else:
            print("✅ Config generated and validated successfully!")
            print(f"📊 Entities: {list(config['entities'].keys())}")
            print(f"📊 State machines: {list(config.get('state_machines', {}).keys())}")
            print(f"📊 Events: {list(config.get('events', {}).keys())}")
            print(f"💾 Saved to: tests/e2e_output/{scenario['name']}_config.json")
            return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    # Run all scenarios
    results = {}
    for scenario in SCENARIOS:
        result = test_scenario(scenario)
        if result is not None:
            results[scenario['name']] = result

    # Summary
    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")
    passed = sum(1 for r in results.values() if r)
    total = len(results)
    print(f"✅ Passed: {passed}/{total}")
    if passed < total:
        print(f"❌ Failed: {total - passed}/{total}")

