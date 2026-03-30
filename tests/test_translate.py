"""Test script for schema to config translation."""

import json
import sys
import logging 
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.gen_data.translate_config import translate_and_validate

# Test scenarios
SCENARIOS = [
    {
        "name": "credit_risk",
        "prompt": "Credit risk management system with customers, loan applications, credit scores, and repayment tracking",
        "schema_file": "tests/e2e_output/credit_risk/schema.json"
    },
    # {
    #     "name": "crm",
    #     "prompt": "CRM system with customers, accounts, transactions, and interactions",
    #     "schema_file": 'tests/e2e_output/crm/schema.json'
    # },
    {
        "name": "trading",
        "prompt": "Trading platform with traders, orders, executions, and settlements",
        "schema_file": 'tests/e2e_output/trading/schema.json'
    }
]

def test_scenario(scenario: dict):
    """Test translation for a single scenario."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Testing: {scenario['name']}")
    logger.info(f"{'='*60}")

    # Load or generate schema
    if scenario['schema_file']:
        schema_path = Path(__file__).parent.parent / scenario['schema_file']
        if not schema_path.exists():
            logger.error(f"❌ Schema not found: {schema_path}")
            return False
        schema = json.loads(schema_path.read_text())
    else:
        logger.error(f"❌ No schema file provided for {scenario['name']}")
        return None

    # Translate
    logger.info(f"🔄 Translating schema to config...")
    try:
        config, errors = translate_and_validate(
            schema=schema,
            base_records=1000,
            seed=42,
            output_path=Path(f"tests/e2e_output/{scenario['name']}/config.json")
        )

        if errors:
            logger.error("❌ Validation errors:")
            for error in errors:
                logger.error(f"  - {error}")
            return False
        else:
            logger.info("✅ Config generated and validated successfully!")
            logger.info(f"📊 Entities: {list(config['entities'].keys())}")
            logger.info(f"📊 State machines: {list(config.get('state_machines', {}).keys())}")
            logger.info(f"📊 Events: {list(config.get('events', {}).keys())}")
            logger.info(f"💾 Saved to: tests/e2e_output/{scenario['name']}_config.json")
            return True
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    # Run all scenarios
    results = {}
    for scenario in SCENARIOS:
        result = test_scenario(scenario)
        if result is not None:
            results[scenario['name']] = result

    # Summary
    logger.critical(f"\n{'='*60}")
    logger.critical("Summary")
    logger.critical(f"{'='*60}")
    passed = sum(1 for r in results.values() if r)
    total = len(results)
    logger.critical(f"✅ Passed: {passed}/{total}")
    if passed < total:
        logger.error(f"❌ Failed: {total - passed}/{total}")

