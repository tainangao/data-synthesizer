# Data Generation Implementation Plan

## Overview
Generate realistic synthetic data from schema (gen_schema output) and config (gen_config output) while meeting grading rubric requirements.

## Architecture (25% - Modular Design)

### Core Components

1. **DataGenerator** (`src/gen_data/data_generator.py`)
   - Orchestrates generation pipeline
   - Manages FK-aware topological ordering
   - Coordinates state machines and event emission
   - Handles batch writing to multiple formats

2. **ValueGenerators** (`src/gen_data/value_generators.py`)
   - Field-level value generation based on field_role
   - Faker integration for realistic data
   - Distribution-aware sampling (categorical, numerical)
   - Semi-structured (JSON/XML) generation

3. **StateMachine** (`src/gen_data/state_machine.py`)
   - Lifecycle state transitions with probabilities
   - Context-aware adjustments (segment, risk, etc.)
   - Terminal state handling

4. **EventEmitter** (`src/gen_data/event_emitter.py`)
   - Poisson-distributed event generation
   - State-conditional emission
   - Temporal ordering constraints

5. **DataWriters** (`src/gen_data/data_writers.py`)
   - Multi-format output (CSV, SQLite, Parquet, Delta)
   - Batch writing for performance
   - Schema-aware type conversion

## Data Quality (25% - Realism & Relationships)

### Distribution Realism
- **Categorical fields**: Sample from config-defined distributions (e.g., segment: 60% retail, 30% corporate, 10% institutional)
- **Numerical fields**: Use realistic ranges with appropriate distributions (normal, uniform, exponential)
- **Temporal fields**: Respect ordering constraints (child events after parent events)
- **Semi-structured**: Generate nested JSON/XML with domain-appropriate structure

### Relationship Preservation
- **FK integrity**: Sample child FKs from parent PK pools (guaranteed referential integrity)
- **Topological ordering**: Generate parent tables before children
- **State alignment**: Child states follow parent state transitions (e.g., closed loan → no new payments)
- **Inheritance**: Fields marked `inherit_from_parent` copy values via FK relationships
- **Cross-table constraints**: Currency consistency, segment-driven distributions

## Engineering (20% - Code Clarity & Libraries)

### Key Libraries
- **Faker**: Realistic names, addresses, emails, dates
- **Pydantic**: Config validation and type safety
- **Polars/Pandas**: Efficient batch operations
- **PyArrow**: Parquet/Delta writing

### Code Organization
```
src/gen_data/
├── data_generator.py      # Main orchestrator
├── value_generators.py    # Field-level generation
├── state_machine.py       # State transitions
├── event_emitter.py       # Event generation
├── data_writers.py        # Multi-format output
└── __init__.py
```

## Semi-Structured Data (15% - JSON/XML)

### JSON Generation
- Profile objects: `{"preferences": {...}, "metadata": {...}}`
- Nested structures based on field semantics
- Realistic key-value pairs using Faker

### XML Generation
- Well-formed documents with proper escaping
- Domain-appropriate tags and structure
- Text content using Faker

## Scalability (15% - Large Datasets)

### Performance Optimizations
- **Batch writing**: 5K rows for SQLite, 50K for Parquet
- **Lazy evaluation**: Generate rows on-demand, don't hold all in memory
- **Stress mode**: Skip expensive metrics for large datasets
- **Efficient FK sampling**: Pre-build parent PK pools, use random.choice()

### Memory Management
- Stream rows to writers instead of accumulating
- Clear intermediate data structures after each table
- Use generators for row production

## Implementation Steps

### Phase 1: Core Generation (Priority 1)
1. **data_generator.py**: Main loop with topological ordering
2. **value_generators.py**: Field-level generation by field_role
3. **data_writers.py**: CSV, SQLite, Parquet, Delta writers

### Phase 2: Business Logic (Priority 2)
4. **state_machine.py**: State transitions with probability adjustments
5. **event_emitter.py**: Poisson-distributed events with state conditions

### Phase 3: Quality & Performance (Priority 3)
6. Add FK validation and distribution metrics
7. Optimize batch sizes and memory usage
8. Add progress reporting

## Key Algorithms

### FK-Aware Generation
```python
# Topological sort ensures parents generated first
for table in generation_order:
    for row in range(row_count):
        # Sample FK from parent PK pool
        if fk_column:
            parent_pks = state["pk_values"][parent_table]
            fk_value = random.choice(parent_pks)

        # Generate other fields
        row_data = generate_row(table, fk_value, state)

        # Store PK for child tables
        state["pk_values"][table].append(row_data[pk_column])
```

### State Machine Transitions
```python
# Get current state from parent via FK
parent_state = state["pk_profiles"][parent_table][fk_value]["status"]

# Sample next state based on probabilities + adjustments
transitions = state_machine[parent_state]
probs = apply_adjustments(transitions, context)
next_state = weighted_choice(probs)
```

### Event Emission
```python
# Check if parent state allows emission
if parent_state in emit_when_states:
    # Poisson-distributed event count
    lambda_val = apply_modifiers(lambda_base, context)
    event_count = poisson(lambda_val)

    # Generate events with temporal ordering
    for _ in range(event_count):
        event_timestamp = parent_timestamp + random_offset()
        emit_event(event_timestamp)
```

## Testing Strategy
- Unit tests for each value generator
- Integration tests for FK integrity
- E2E tests with sample schemas (CRM, credit risk)
- Performance tests with large record counts (100K+)

## Success Criteria
✓ Generates data for all field_roles (identifier, categorical, numerical, semi_structured, temporal, text, boolean)
✓ 100% FK referential integrity
✓ Realistic distributions matching config
✓ State transitions follow business logic
✓ Events emitted according to Poisson distribution
✓ Outputs to CSV, SQLite, Parquet, Delta
✓ Handles 100K+ records efficiently
✓ Clear, modular code with proper separation of concerns
