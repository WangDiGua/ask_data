from typing import Any

from pydantic import BaseModel
from qdrant_client import QdrantClient, models

from ndea.config import Settings


class QdrantConnectionInfo(BaseModel):
    url: str
    api_key: str


def build_qdrant_connection_info(settings: Settings) -> QdrantConnectionInfo:
    return QdrantConnectionInfo(
        url=settings.qdrant_url,
        api_key=settings.qdrant_api_key,
    )


def open_qdrant_client(settings: Settings) -> QdrantClient:
    info = build_qdrant_connection_info(settings)
    return QdrantClient(
        url=info.url,
        api_key=info.api_key or None,
    )


class QdrantVectorStore:
    _default_output_fields = [
        "asset_id",
        "asset_type",
        "title",
        "text",
        "source",
        "metadata",
    ]

    def __init__(
        self,
        client: Any,
        collection_name: str,
        vector_name: str,
        output_fields: list[str] | None = None,
    ) -> None:
        self._client = client
        self._collection_name = collection_name
        self._vector_name = vector_name
        self._output_fields = list(output_fields or self._default_output_fields)

    def search(
        self,
        query_vector: list[float],
        asset_types: list[str] | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        response = self._client.query_points(
            collection_name=self._collection_name,
            query=query_vector,
            query_filter=self._build_filter(asset_types),
            limit=limit,
            with_payload=list(self._output_fields),
            using=self._vector_name,
        )
        points = getattr(response, "points", None)
        if not isinstance(points, list):
            return []
        hits: list[dict[str, Any]] = []
        for point in points:
            payload = getattr(point, "payload", None)
            if not isinstance(payload, dict):
                payload = {}
            hits.append(
                {
                    "id": str(getattr(point, "id", "unknown")),
                    "score": float(getattr(point, "score", 0.0)),
                    "entity": dict(payload),
                }
            )
        return hits

    def _build_filter(self, asset_types: list[str] | None):
        if not asset_types:
            return None
        return models.Filter(
            must=[
                models.FieldCondition(
                    key="asset_type",
                    match=models.MatchAny(any=list(asset_types)),
                )
            ]
        )
