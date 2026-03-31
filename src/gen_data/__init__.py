"""Data generation module - synthetic relational data with semantic intelligence.

## Architecture Overview

This module generates synthetic data in two phases:

    Phase 1: Entity Tables          Phase 2: Event Tables
    ┌─────────────────┐            ┌──────────────────┐
    │ value_generators│◄───────────│  event_emitter   │
    │  (field-level)  │            │ (Poisson counts) │
    └────────┬────────┘            └────────┬─────────┘
             │                              │
             ▼                              ▼
    ┌─────────────────┐            ┌──────────────────┐
    │ data_generator  │            │ data_generator   │
    │ (orchestration) │            │ (event builder)  │
    └────────┬────────┘            └────────┬─────────┘
             │                              │
             ▼                              ▼
    ┌─────────────────┐            ┌──────────────────┐
    │ state_machine   │            │  data_writers    │
    │ (status logic)  │            │ (CSV/SQL/etc)    │
    └─────────────────┘            └──────────────────┘

## Key Concepts

**Semantic Field Matching**: Column names drive generation logic
  - "score" → Gaussian(650, 100)
  - "amount" → Lognormal distribution
  - "currency" → inherits from parent via FK

**FK-Aware Generation**: Tables generated in topological order
  - Child FK values sampled from parent PK pools
  - Guarantees referential integrity

**Value Inheritance**: Fields marked with semantic tokens copy parent values
  - Child "currency" inherits parent "currency" via FK join
  - Reduces redundancy, maintains consistency

**Temporal Constraints**:
  - Within-row anchors: end_date relative to start_date
  - Parent floors: transaction_date ≥ account_open_date
  - Ordering: date1 ≤ date2 ≤ date3

## Module Components

- `value_generators.py`: Field-level value generation with semantic defaults
- `data_generator.py`: Orchestrates entity/event generation pipeline
- `state_machine.py`: Probabilistic state transitions with feature adjustments
- `event_emitter.py`: Poisson-based event count generation
- `data_writers.py`: Multi-format persistence (CSV, SQLite, Parquet, Delta)
- `types.py`: Type definitions for common structures
"""

from .data_generator import generate_data

__all__ = ["generate_data"]
