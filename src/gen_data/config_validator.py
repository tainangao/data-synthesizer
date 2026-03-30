"""Validates scenario configuration files using Pydantic."""

from typing import Any, Literal
from pydantic import BaseModel, Field, field_validator


class FieldDistribution(BaseModel):
    """Field distribution configuration."""
    distribution: Literal["normal", "lognormal", "uniform", "poisson", "choice", "constant", "date_offset"]
    params: dict[str, Any] = Field(default_factory=dict)


class EntityDefinition(BaseModel):
    """Entity definition with fields."""
    count: int = Field(gt=0)
    fields: dict[str, FieldDistribution]


class TransitionAdjustment(BaseModel):
    """Feature-based transition adjustment."""
    field: str
    direction: Literal["higher_increases", "higher_decreases"]
    strength: Literal["weak", "moderate", "strong"]


class Transition(BaseModel):
    """State transition with probability and adjustments."""
    base_prob: float = Field(ge=0.0, le=1.0)
    adjustments: list[TransitionAdjustment] = Field(default_factory=list)


class StateMachine(BaseModel):
    """State machine definition for entity lifecycle."""
    state_field: str
    initial_state: str
    terminal_states: list[str]
    transitions: dict[str, dict[str, Transition]]


class LambdaModifier(BaseModel):
    """Modifier for event frequency lambda."""
    field: str
    effect: Literal["higher_increases", "higher_decreases"]


class EventFrequency(BaseModel):
    """Event emission frequency configuration."""
    distribution: Literal["poisson"]
    lambda_base: float = Field(gt=0.0)
    lambda_modifiers: list[LambdaModifier] = Field(default_factory=list)


class EventDefinition(BaseModel):
    """Event table definition."""
    emitted_by: str
    emit_when_states: list[str]
    frequency: EventFrequency
    fields: dict[str, FieldDistribution]


class Constraint(BaseModel):
    """Data constraint definition."""
    type: Literal["temporal_order", "no_events_after_terminal", "running_balance"]
    fields: list[str] | None = None
    entity: str | None = None
    event_table: str | None = None
    credit_field: str | None = None
    debit_field: str | None = None
    balance_field: str | None = None


class SimulationConfig(BaseModel):
    """Simulation time range."""
    start_date: str
    end_date: str


class ScenarioConfig(BaseModel):
    """Complete scenario configuration."""
    scenario_name: str
    seed: int
    simulation: SimulationConfig
    entities: dict[str, EntityDefinition]
    generation_order: list[str]
    table_counts: dict[str, int]
    state_machines: dict[str, StateMachine] = Field(default_factory=dict)
    events: dict[str, EventDefinition] = Field(default_factory=dict)
    constraints: list[Constraint] = Field(default_factory=list)

    @field_validator("generation_order")
    @classmethod
    def validate_generation_order(cls, v, info):
        """Ensure generation_order matches entity keys."""
        entities = info.data.get("entities", {})
        entity_names = set(entities.keys())
        order_names = set(v)
        if not order_names.issubset(entity_names):
            extra = order_names - entity_names
            raise ValueError(f"generation_order contains unknown entities: {extra}")
        return v


def validate_config(config: dict) -> list[str]:
    """Validate scenario config and return list of errors.

    Args:
        config: The scenario configuration dictionary

    Returns:
        List of error messages (empty if valid)
    """
    try:
        ScenarioConfig(**config)
        return []
    except Exception as e:
        return [str(e)]