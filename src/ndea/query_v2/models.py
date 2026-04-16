from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from ndea.protocol import ChartPayload, TablePayload, TextPayload


class QueryOptions(BaseModel):
    dry_run: bool = False
    max_rows: int | None = None
    debug: bool = False


class QueryRequestV2(BaseModel):
    query_text: str
    database: str | None = None
    request_context: dict[str, Any] | None = None
    policy_context: dict[str, Any] | None = None
    options: QueryOptions = Field(default_factory=QueryOptions)


class InteractionResult(BaseModel):
    query_text: str
    normalized_query_text: str
    rewritten_query_text: str
    recent_user_messages: list[str] = Field(default_factory=list)
    context_summary: str | None = None
    references_resolved: bool = False
    notes: list[str] = Field(default_factory=list)


class QueryIR(BaseModel):
    intent_type: str
    entity_scope: str | None = None
    metric: str | None = None
    dimensions: list[str] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    identifiers: list[dict[str, str]] = Field(default_factory=list)
    time_scope: dict[str, str | None] | None = None
    sort: list[str] = Field(default_factory=list)
    limit: int | None = None
    answer_mode: str = "aggregate"
    ambiguities: list[str] = Field(default_factory=list)
    campus_terms: list[str] = Field(default_factory=list)
    confidence: float = 0.0


class SemanticHint(BaseModel):
    base_table: str | None = None
    measure_expression: str = "COUNT(*)"
    entity_scope: str | None = None
    dimensions: list[dict[str, str]] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    joins: list[dict[str, str]] = Field(default_factory=list)
    candidate_tables: list[str] = Field(default_factory=list)
    ambiguities: list[str] = Field(default_factory=list)
    confidence: float = 0.0
    source: str = "semantic"


class SchemaHint(BaseModel):
    base_table: str | None = None
    candidate_tables: list[str] = Field(default_factory=list)
    dimensions: list[dict[str, str]] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    joins: list[dict[str, str]] = Field(default_factory=list)
    confidence: float = 0.0
    source: str = "schema"


class PlanCandidate(BaseModel):
    candidate_id: str
    intent_type: str
    answer_mode: str
    source: Literal["semantic-first", "schema-first", "historical-case", "template-fallback"]
    base_table: str | None = None
    measure_expression: str = "COUNT(*)"
    entity_scope: str | None = None
    candidate_tables: list[str] = Field(default_factory=list)
    dimensions: list[dict[str, str]] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    joins: list[dict[str, str]] = Field(default_factory=list)
    time_scope: dict[str, str | None] | None = None
    sort: list[str] = Field(default_factory=list)
    limit: int | None = None
    ambiguities: list[str] = Field(default_factory=list)
    semantic_score: float = 0.0
    schema_score: float = 0.0
    confidence: float = 0.0
    reasoning: str | None = None
    requires_clarification: bool = False
    clarification_question: str | None = None


class SQLCandidate(BaseModel):
    candidate_id: str
    plan_candidate_id: str
    source: Literal["semantic-first", "schema-first", "historical-case", "template-fallback"]
    sql: str
    reasoning: str | None = None
    model_name: str | None = None
    score: float = 0.0


class VerificationIssue(BaseModel):
    code: str
    severity: Literal["info", "warning", "error"] = "warning"
    message: str


class VerificationReport(BaseModel):
    sql_candidate_id: str
    allowed: bool
    score: float = 0.0
    effective_sql: str | None = None
    issues: list[VerificationIssue] = Field(default_factory=list)
    referenced_tables: list[str] = Field(default_factory=list)
    referenced_columns: list[str] = Field(default_factory=list)


class RankingDecision(BaseModel):
    selected_plan_candidate_id: str | None = None
    selected_sql_candidate_id: str | None = None
    confidence: float = 0.0
    reason: str | None = None
    scoreboard: list[dict[str, Any]] = Field(default_factory=list)


class ClarificationPayload(BaseModel):
    required: bool = False
    reason: str | None = None
    question: str | None = None
    options: list[str] = Field(default_factory=list)


class LearningEvent(BaseModel):
    event_type: str
    session_id: str
    payload: dict[str, Any] = Field(default_factory=dict)


class PromotionCandidate(BaseModel):
    promotion_type: str
    session_id: str
    confidence: float
    payload: dict[str, Any] = Field(default_factory=dict)


class QueryInterpretationPayload(BaseModel):
    interaction: InteractionResult
    ir: QueryIR
    selected_plan: PlanCandidate | None = None
    selected_sql: SQLCandidate | None = None
    verification: VerificationReport | None = None
    ambiguities: list[str] = Field(default_factory=list)


class QueryResponseV2(BaseModel):
    session_id: str
    interpretation: QueryInterpretationPayload
    answer: TextPayload
    table: TablePayload | None = None
    chart: ChartPayload | None = None
    sql: str | None = None
    audit: dict[str, Any] = Field(default_factory=dict)
    confidence: float = 0.0
    clarification: ClarificationPayload = Field(default_factory=ClarificationPayload)
    learning_trace_id: str | None = None
    tool_trace: list[str] = Field(default_factory=list)
    executed: bool = False
    debug: dict[str, Any] = Field(default_factory=dict)
