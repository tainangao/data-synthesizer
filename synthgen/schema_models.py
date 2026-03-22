from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

ColumnType = Literal[
    "TEXT",
    "INTEGER",
    "NUMERIC",
    "REAL",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "JSON",
    "XML",
]

FieldRole = Literal[
    "identifier",
    "numerical",
    "categorical",
    "semi_structured",
    "temporal",
    "text",
    "boolean",
]


class ForeignKeyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, strict=True)

    table: str = Field(min_length=1)
    column: str = Field(min_length=1)


class ColumnModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, strict=True)

    name: str = Field(min_length=1)
    type: ColumnType
    field_role: FieldRole
    nullable: bool = True
    primary_key: bool = False
    foreign_key: ForeignKeyModel | None = None

    @field_validator("type", mode="before")
    @classmethod
    def normalize_type(cls, value: object) -> str:
        return str(value).strip().upper()

    @field_validator("field_role", mode="before")
    @classmethod
    def normalize_field_role(cls, value: object) -> str:
        return str(value).strip().lower()


class TableModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, strict=True)

    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    columns: list[ColumnModel] = Field(min_length=1)


class SchemaModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, strict=True)

    schema_name: str = Field(min_length=1)
    domain: str = Field(min_length=1)
    tables: list[TableModel] = Field(min_length=1)


__all__ = ["SchemaModel"]
