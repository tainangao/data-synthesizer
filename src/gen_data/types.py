"""Type definitions for gen_data module.

Provides TypedDict definitions for common data structures to improve
type safety and code clarity.
"""

from typing import TypedDict, NotRequired


class ForeignKey(TypedDict):
    """Foreign key reference to another table."""
    table: str
    column: str


class ColumnDef(TypedDict):
    """Column definition in schema."""
    name: str
    type: str
    field_role: NotRequired[str]
    nullable: NotRequired[bool]
    primary_key: NotRequired[bool]
    foreign_key: NotRequired[ForeignKey]


class SchemaTable(TypedDict):
    """Table definition in schema."""
    name: str
    columns: list[ColumnDef]


class StateMachineConfig(TypedDict):
    """State machine configuration."""
    state_field: str
    initial_state: str
    transitions: dict[str, dict[str, dict]]


class EventConfig(TypedDict):
    """Event table configuration."""
    emitted_by: str
    emit_when_states: NotRequired[list[str]]
    frequency: NotRequired[dict]
