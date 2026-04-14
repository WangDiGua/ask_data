from __future__ import annotations

from pydantic import BaseModel, Field


class MetricContract(BaseModel):
    metric_id: str
    name: str
    aliases: list[str] = Field(default_factory=list)
    business_definition: str | None = None
    base_table: str
    measure_expression: str = "COUNT(*)"
    default_filters: list[str] = Field(default_factory=list)
    available_dimensions: list[str] = Field(default_factory=list)
    time_field: str | None = None
    supported_time_grains: list[str] = Field(default_factory=list)
    join_path_ids: list[str] = Field(default_factory=list)
    entity_scope: str | None = None
    entity_scope_options: list[str] = Field(default_factory=list)
    requires_entity_scope: bool = False
    requires_time_scope: bool = False
    example_questions: list[str] = Field(default_factory=list)
    golden_sql: str | None = None
    score: float | None = None


class DimensionContract(BaseModel):
    dimension_id: str
    name: str
    aliases: list[str] = Field(default_factory=list)
    table: str
    column: str
    expression: str
    groupable: bool = True
    output_alias: str | None = None


class JoinPathContract(BaseModel):
    join_id: str
    left_table: str
    right_table: str
    join_type: str = "INNER"
    join_condition: str
    join_sql: str
    cardinality: str | None = None
    semantic_meaning: str | None = None
    disabled: bool = False


class TimeSemantics(BaseModel):
    semantic_id: str
    name: str
    aliases: list[str] = Field(default_factory=list)
    field: str
    supported_grains: list[str] = Field(default_factory=list)
    default_grain: str | None = None
    comparison_modes: list[str] = Field(default_factory=list)
