"""Test script for schema to config translation."""

import json
from pathlib import Path
from src.gen_data.translate_config import translate_and_validate

# Load existing schema
schema_path = Path("../demo_output/schema.json")
if not schema_path.exists():
    print("❌ No schema found. Run main.py first to generate a schema.")
    exit(1)

schema = json.loads(schema_path.read_text())

print("🔄 Translating schema to config...")
config, errors = translate_and_validate(
    schema=schema,
    base_records=1000,
    seed=42,
    output_path=Path("demo_output/scenario_config.json")
)

if errors:
    print("❌ Validation errors:")
    for error in errors:
        print(f"  - {error}")
else:
    print("✅ Config generated and validated successfully!")
    print(f"📊 Entities: {list(config['entities'].keys())}")
    print(f"📊 State machines: {list(config.get('state_machines', {}).keys())}")
    print(f"📊 Events: {list(config.get('events', {}).keys())}")
    print(f"💾 Saved to: demo_output/scenario_config.json")
