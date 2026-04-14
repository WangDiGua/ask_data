from typing import Any

from pydantic import BaseModel, Field


class SQLAdvisorExample(BaseModel):
    asset_id: str | None = None
    question: str | None = None
    sql: str
    score: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SQLAdvisoryPayload(BaseModel):
    selected_sql: str | None = None
    selected_sql_asset_id: str | None = None
    strategy: str | None = None
    confidence: float | None = None
    examples: list[SQLAdvisorExample] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
