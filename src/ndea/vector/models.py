from typing import Any

from pydantic import BaseModel, Field

from ndea.semantic import DimensionContract, JoinPathContract, MetricContract, TimeSemantics


class SemanticAssetMatch(BaseModel):
    asset_id: str
    asset_type: str
    title: str
    text: str
    score: float
    hybrid_score: float | None = None
    keyword_score: float | None = None
    source: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class VectorLocatorPayload(BaseModel):
    query_text: str
    asset_types: list[str] = Field(default_factory=list)
    limit: int
    summary: str
    total_matches: int
    matches: list[SemanticAssetMatch] = Field(default_factory=list)
    metric_contracts: list[MetricContract] = Field(default_factory=list)
    dimension_contracts: list[DimensionContract] = Field(default_factory=list)
    join_path_contracts: list[JoinPathContract] = Field(default_factory=list)
    time_semantics_catalog: list[TimeSemantics] = Field(default_factory=list)
