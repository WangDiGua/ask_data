from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PortalTableColumnPayload(BaseModel):
    key: str
    label: str


class PortalTablePayload(BaseModel):
    columns: list[PortalTableColumnPayload] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)


class PortalQueryMetadataPayload(BaseModel):
    tool_trace: list[str] = Field(default_factory=list)
    confidence: float | None = None
    selected_sql_asset_id: str | None = None
    metric_id: str | None = None
    answer_mode: str | None = None
    resolved_tables: list[str] = Field(default_factory=list)
    resolved_entities: list[dict[str, str]] = Field(default_factory=list)
    sql_strategy: str | None = None
    clarification_reason: str | None = None


class PortalQueryPayload(BaseModel):
    text: str
    table: PortalTablePayload | None = None
    visualization: dict[str, Any] | None = None
    clarification_required: bool = False
    clarification_question: str | None = None
    executed: bool = False
    sql: str | None = None
    metadata: PortalQueryMetadataPayload = Field(default_factory=PortalQueryMetadataPayload)
