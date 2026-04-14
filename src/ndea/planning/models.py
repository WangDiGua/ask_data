from typing import Any

from pydantic import BaseModel, Field

from ndea.protocol import ChartPayload, TablePayload, TextPayload


class ResolvedMetricPayload(BaseModel):
    metric_id: str
    name: str
    base_table: str
    measure_expression: str = "COUNT(*)"
    default_filters: list[str] = Field(default_factory=list)
    entity_scope: str | None = None


class ResolvedDimensionPayload(BaseModel):
    dimension_id: str
    name: str
    expression: str
    output_alias: str | None = None
    table: str | None = None


class ResolvedFilterPayload(BaseModel):
    filter_id: str | None = None
    expression: str
    source: str | None = None


class ResolvedTimeScopePayload(BaseModel):
    scope_type: str
    field: str | None = None
    value: str | None = None
    label: str | None = None
    start: str | None = None
    end: str | None = None


class JoinPlanStepPayload(BaseModel):
    join_id: str
    join_sql: str
    left_table: str | None = None
    right_table: str | None = None
    join_type: str | None = None


class RankedSQLCandidatePayload(BaseModel):
    asset_id: str
    sql: str
    compatibility_score: float
    selection_reason: str
    score: float | None = None
    hybrid_score: float | None = None


class QueryPlanPayload(BaseModel):
    query_text: str
    intent_type: str
    summary: str
    degraded: bool = False
    error_code: str | None = None
    clarification_required: bool
    clarification_reason: str | None = None
    clarification_questions: list[str] = Field(default_factory=list)
    candidate_tables: list[str] = Field(default_factory=list)
    candidate_metrics: list[str] = Field(default_factory=list)
    join_hints: list[str] = Field(default_factory=list)
    selected_sql_asset_id: str | None = None
    selected_sql: str | None = None
    selected_candidate_reason: str | None = None
    metric_id: str | None = None
    dimensions: list[ResolvedDimensionPayload] = Field(default_factory=list)
    filters: list[ResolvedFilterPayload] = Field(default_factory=list)
    time_scope: ResolvedTimeScopePayload | None = None
    time_grain: str | None = None
    entity_scope: str | None = None
    join_plan: list[JoinPlanStepPayload] = Field(default_factory=list)
    chosen_strategy: str | None = None
    confidence: float | None = None
    resolved_metric: ResolvedMetricPayload | None = None
    ranked_sql_candidates: list[RankedSQLCandidatePayload] = Field(default_factory=list)


class SQLAttemptPayload(BaseModel):
    attempt_number: int
    sql: str
    source: str
    status: str
    reason: str | None = None


class QueryWorkflowPayload(BaseModel):
    trace_id: str
    request_id: str
    query_text: str
    database: str | None = None
    request_context: dict[str, Any] | None = None
    policy_context: dict[str, Any] | None = None
    degraded: bool = False
    error_code: str | None = None
    audit_id: str | None = None
    policy_summary: dict[str, Any] = Field(default_factory=dict)
    clarification_required: bool = False
    clarification_questions: list[str] = Field(default_factory=list)
    resolved_metric: dict[str, Any] | None = None
    resolved_dimensions: list[dict[str, Any]] = Field(default_factory=list)
    resolved_filters: list[dict[str, Any]] = Field(default_factory=list)
    resolved_time_scope: dict[str, Any] | None = None
    executed: bool = False
    tool_trace: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    plan: QueryPlanPayload
    generation: Any | None = None
    repair: Any | None = None
    sql_attempts: list[SQLAttemptPayload] = Field(default_factory=list)
    execution: dict[str, Any] | None = None
    response_text: TextPayload
    response_table: TablePayload | None = None
    response_chart: ChartPayload | None = None
