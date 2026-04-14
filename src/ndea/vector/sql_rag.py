from typing import Any

from pydantic import BaseModel, Field

from ndea.config import Settings
from ndea.vector.hybrid import HybridSearchScorer
from ndea.vector.qdrant_client import QdrantVectorStore, open_qdrant_client


class GoldenSQLCandidate(BaseModel):
    asset_id: str
    question: str
    sql: str
    notes: str | None = None
    score: float
    hybrid_score: float | None = None
    keyword_score: float | None = None
    source: str | None = None
    tables: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SQLRAGPayload(BaseModel):
    query_text: str
    limit: int
    summary: str
    total_candidates: int
    candidates: list[GoldenSQLCandidate] = Field(default_factory=list)


class SQLRAGService:
    def __init__(
        self,
        settings: Settings,
        store: Any | None = None,
    ) -> None:
        self._settings = settings
        self._hybrid_scorer = HybridSearchScorer(settings)
        self._store = store or QdrantVectorStore(
            client=open_qdrant_client(settings),
            collection_name=settings.qdrant_collection,
            vector_name=settings.qdrant_vector_name,
            output_fields=[
                "asset_id",
                "asset_type",
                "question",
                "sql",
                "notes",
                "source",
                "tables",
                "metadata",
            ],
        )

    def retrieve(
        self,
        query_text: str,
        query_vector: list[float],
        limit: int | None = None,
    ) -> SQLRAGPayload:
        resolved_limit = max(1, limit or self._settings.qdrant_search_limit)
        search_limit = resolved_limit
        if self._settings.qdrant_hybrid_enabled:
            search_limit = max(resolved_limit, self._settings.qdrant_hybrid_overfetch_limit)
        hits = self._store.search(
            query_vector=query_vector,
            asset_types=["golden_sql"],
            limit=search_limit,
        )
        candidates = [self._normalize_hit(hit, query_text) for hit in hits]
        candidates = sorted(
            candidates,
            key=lambda candidate: candidate.hybrid_score if candidate.hybrid_score is not None else candidate.score,
            reverse=True,
        )[:resolved_limit]
        total_candidates = len(candidates)
        summary = "No golden SQL candidates found"
        if candidates:
            summary = f"Found {total_candidates} golden SQL candidates"

        return SQLRAGPayload(
            query_text=query_text,
            limit=resolved_limit,
            summary=summary,
            total_candidates=total_candidates,
            candidates=candidates,
        )

    def _normalize_hit(self, hit: dict[str, Any], query_text: str) -> GoldenSQLCandidate:
        entity = hit.get("entity")
        if not isinstance(entity, dict):
            entity = {}

        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}

        tables = entity.get("tables")
        if not isinstance(tables, list):
            tables = []

        score_value = hit.get("distance", hit.get("score", 0.0))
        try:
            score = float(score_value)
        except (TypeError, ValueError):
            score = 0.0
        hybrid_score = None
        keyword_score = None
        if self._settings.qdrant_hybrid_enabled:
            lexical_fields = [
                str(entity.get("question") or ""),
                str(entity.get("notes") or ""),
                str(entity.get("sql") or ""),
                *[str(table) for table in tables],
                *self._hybrid_scorer.collect_strings(metadata),
            ]
            hybrid_score, keyword_score = self._hybrid_scorer.score(query_text, lexical_fields, score)

        raw_asset_id = entity.get("asset_id") or hit.get("id") or "unknown"
        raw_source = entity.get("source")
        raw_notes = entity.get("notes")
        return GoldenSQLCandidate(
            asset_id=str(raw_asset_id),
            question=str(entity.get("question") or ""),
            sql=str(entity.get("sql") or ""),
            notes=None if raw_notes in (None, "") else str(raw_notes),
            score=score,
            hybrid_score=hybrid_score,
            keyword_score=keyword_score,
            source=None if raw_source in (None, "") else str(raw_source),
            tables=[str(table) for table in tables],
            metadata=metadata,
        )

