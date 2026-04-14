from typing import Any

from ndea.config import Settings
from ndea.semantic import DimensionContract, JoinPathContract, MetricContract, TimeSemantics
from ndea.vector.hybrid import HybridSearchScorer
from ndea.vector.qdrant_client import QdrantVectorStore, open_qdrant_client
from ndea.vector.models import SemanticAssetMatch, VectorLocatorPayload


class VectorLocatorService:
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
        )

    def locate(
        self,
        query_text: str,
        query_vector: list[float],
        asset_types: list[str] | None = None,
        limit: int | None = None,
    ) -> VectorLocatorPayload:
        resolved_limit = max(1, limit or self._settings.qdrant_search_limit)
        search_limit = resolved_limit
        if self._settings.qdrant_hybrid_enabled:
            search_limit = max(resolved_limit, self._settings.qdrant_hybrid_overfetch_limit)
        hits = self._store.search(
            query_vector=query_vector,
            asset_types=asset_types,
            limit=search_limit,
        )
        matches = sorted(
            (self._normalize_hit(hit, query_text) for hit in hits),
            key=lambda match: match.hybrid_score if match.hybrid_score is not None else match.score,
            reverse=True,
        )[:resolved_limit]
        total_matches = len(matches)
        summary = "No semantic matches found"
        if matches:
            summary = f"Found {total_matches} semantic matches"

        metric_contracts = self._extract_metric_contracts(matches)
        dimension_contracts = self._extract_dimension_contracts(matches)
        join_path_contracts = self._extract_join_path_contracts(matches)
        time_semantics_catalog = self._extract_time_semantics(matches)

        return VectorLocatorPayload(
            query_text=query_text,
            asset_types=asset_types or [],
            limit=resolved_limit,
            summary=summary,
            total_matches=total_matches,
            matches=matches,
            metric_contracts=metric_contracts,
            dimension_contracts=dimension_contracts,
            join_path_contracts=join_path_contracts,
            time_semantics_catalog=time_semantics_catalog,
        )

    def _normalize_hit(self, hit: dict[str, Any], query_text: str) -> SemanticAssetMatch:
        entity = hit.get("entity")
        if not isinstance(entity, dict):
            entity = {}

        metadata = entity.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}

        raw_asset_id = entity.get("asset_id") or hit.get("id") or "unknown"
        raw_source = entity.get("source")
        score_value = hit.get("distance", hit.get("score", 0.0))
        try:
            score = float(score_value)
        except (TypeError, ValueError):
            score = 0.0
        hybrid_score = None
        keyword_score = None
        if self._settings.qdrant_hybrid_enabled:
            lexical_fields = [
                str(entity.get("title") or ""),
                str(entity.get("text") or entity.get("content") or ""),
                str(entity.get("source") or ""),
                *self._hybrid_scorer.collect_strings(metadata),
            ]
            hybrid_score, keyword_score = self._hybrid_scorer.score(query_text, lexical_fields, score)

        asset_id = str(raw_asset_id)
        return SemanticAssetMatch(
            asset_id=asset_id,
            asset_type=str(entity.get("asset_type") or "unknown"),
            title=str(entity.get("title") or entity.get("name") or asset_id),
            text=str(entity.get("text") or entity.get("content") or ""),
            score=score,
            hybrid_score=hybrid_score,
            keyword_score=keyword_score,
            source=None if raw_source in (None, "") else str(raw_source),
            metadata=metadata,
        )

    def _extract_metric_contracts(self, matches: list[SemanticAssetMatch]) -> list[MetricContract]:
        contracts: list[MetricContract] = []
        for match in matches:
            if match.asset_type != "metric_contract":
                continue
            payload = match.metadata.get("metric_contract")
            if not isinstance(payload, dict):
                continue
            contract_payload = dict(payload)
            contract_payload.setdefault("score", match.score)
            contracts.append(MetricContract.model_validate(contract_payload))
        return contracts

    def _extract_dimension_contracts(
        self,
        matches: list[SemanticAssetMatch],
    ) -> list[DimensionContract]:
        contracts: list[DimensionContract] = []
        for match in matches:
            if match.asset_type != "dimension_contract":
                continue
            payload = match.metadata.get("dimension_contract")
            if not isinstance(payload, dict):
                continue
            contracts.append(DimensionContract.model_validate(payload))
        return contracts

    def _extract_join_path_contracts(
        self,
        matches: list[SemanticAssetMatch],
    ) -> list[JoinPathContract]:
        contracts: list[JoinPathContract] = []
        for match in matches:
            if match.asset_type != "join_path":
                continue
            payload = match.metadata.get("join_path_contract")
            if not isinstance(payload, dict):
                continue
            contracts.append(JoinPathContract.model_validate(payload))
        return contracts

    def _extract_time_semantics(self, matches: list[SemanticAssetMatch]) -> list[TimeSemantics]:
        contracts: list[TimeSemantics] = []
        for match in matches:
            if match.asset_type != "time_semantics":
                continue
            payload = match.metadata.get("time_semantics")
            if not isinstance(payload, dict):
                continue
            contracts.append(TimeSemantics.model_validate(payload))
        return contracts

