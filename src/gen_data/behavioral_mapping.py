"""Simplified config schema for LLM generation."""

from typing import Any
from pydantic import BaseModel, Field


class SimplifiedTransition(BaseModel):
    """Simplified transition definition."""
    to_state: str
    base_prob: float
    adjustments: list[dict[str, Any]] = Field(default_factory=list)


class SimplifiedStateMachine(BaseModel):
    """Simplified state machine for LLM generation."""
    entity: str
    state_field: str
    initial_state: str
    terminal_states: list[str]
    transitions: dict[str, list[SimplifiedTransition]]


class SimplifiedEvent(BaseModel):
    """Simplified event definition."""
    event_table: str
    emitted_by: str
    emit_when_states: list[str]
    lambda_base: float
    lambda_modifiers: list[dict[str, str]] = Field(default_factory=list)


class SimplifiedConstraint(BaseModel):
    """Simplified constraint definition."""
    type: str
    params: dict[str, Any]


class BehavioralMapping(BaseModel):
    """LLM-generated behavioral mapping."""
    scenario_name: str
    start_date: str
    end_date: str
    state_machines: list[SimplifiedStateMachine] = Field(default_factory=list)
    events: list[SimplifiedEvent] = Field(default_factory=list)
    constraints: list[SimplifiedConstraint] = Field(default_factory=list)
    key_distributions: dict[str, dict[str, dict[str, Any]]] = Field(default_factory=dict)
