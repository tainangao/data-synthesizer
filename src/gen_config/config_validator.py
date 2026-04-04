"""Validates scenario configuration files using Pydantic."""

from typing import Any, Literal
from pydantic import BaseModel, Field, field_validator


class FieldDistribution(BaseModel):
    """Field distribution configuration for generating realistic data patterns."""
    distribution: str  # e.g., "normal", "lognormal", "choice", "constant", "faker"
    params: dict[str, Any] = Field(default_factory=dict)


class EntityDefinition(BaseModel):
    """Entity definition with demographic/risk profile distributions."""
    fields: dict[str, FieldDistribution] = Field(default_factory=dict)


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
    effect: str  # Allow any effect description


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


class Constraint(BaseModel):
    """Data constraint definition."""
    type: str  # Allow any constraint type
    fields: list[str] | None = None


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
        """Ensure generation_order matches entity and event keys."""
        entities = info.data.get("entities", {})
        events = info.data.get("events", {})
        all_tables = set(entities.keys()) | set(events.keys())
        order_names = set(v)
        if not order_names.issubset(all_tables):
            extra = order_names - all_tables
            raise ValueError(f"generation_order contains unknown tables: {extra}")
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
