"""End-to-end test: schema generation + config translation."""

import json
import sys
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.gen_schema.schema_generator import generate_schema
from src.gen_data.translate_config import translate_and_validate

# Test scenarios
SCENARIOS = [
    {
        "name": "credit_risk",
        "prompt": "Credit risk management system with customers applying for loans, credit score tracking, loan repayments, and delinquency management"
    },
    {
        "name": "crm",
        "prompt": "CRM system with customers, accounts with status lifecycle, transactions, and customer interactions"
    },
    {
        "name": "trading",
        "prompt": "Trading platform with traders, orders with execution states, trade executions, and T+2 settlements"
    }
]

def test_scenario(scenario: dict, output_dir: Path):
    """Test schema generation and config translation for a scenario."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Testing: {scenario['name']}")
    logger.info(f"{'='*60}")

    scenario_dir = output_dir / scenario['name']
    scenario_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Generate schema
    logger.info(f"📝 Generating schema...")
    try:
        schema = generate_schema(scenario['prompt'])

        # Save schema
        schema_path = scenario_dir / "schema.json"
        schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
        logger.info(f"✅ Schema generated: {len(schema['tables'])} tables")

    except Exception as e:
        logger.error(f"❌ Schema generation failed: {e}")
        return False

    # Step 2: Translate to config
    logger.info(f"🔄 Translating to config...")
    try:
        config, errors = translate_and_validate(
            schema=schema,
            base_records=1000,
            seed=42,
            output_path=scenario_dir / "config.json"
        )
        # Save config
        config_path = scenario_dir / "config.json"
        config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")

        if errors:
            logger.error("❌ Config validation errors:")
            for error in errors:
                logger.error(f"  - {error}")
            return False

        logger.info("✅ Config generated and validated!")
        logger.info(f"📊 Entities: {list(config['entities'].keys())}")
        logger.info(f"📊 State machines: {list(config.get('state_machines', {}).keys())}")
        logger.info(f"📊 Events: {list(config.get('events', {}).keys())}")
        logger.info(f"💾 Saved to: {scenario_dir}")
        return True

    except Exception as e:
        logger.error(f"❌ Config translation failed: {e}")
        return False

if __name__ == "__main__":
    output_dir = Path(__file__).parent / "e2e_output"
    output_dir.mkdir(exist_ok=True)

    # Run all scenarios
    results = {}
    for scenario in SCENARIOS:
        results[scenario['name']] = test_scenario(scenario, output_dir)

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("Summary")
    logger.info(f"{'='*60}")
    passed = sum(1 for r in results.values() if r)
    total = len(results)
    logger.info(f"✅ Passed: {passed}/{total}")
    if passed < total:
        logger.error(f"❌ Failed: {total - passed}/{total}")
        failed = [name for name, result in results.items() if not result]
        logger.error(f"Failed scenarios: {', '.join(failed)}")
