from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel

from ndea.config import Settings
from ndea.runtime import configure_runtime


class MilvusConnectionInfo(BaseModel):
    uri: str
    token: str
    database: str


def build_milvus_connection_info(settings: Settings) -> MilvusConnectionInfo:
    return MilvusConnectionInfo(
        uri=settings.milvus_uri,
        token=settings.milvus_token,
        database=settings.milvus_database,
    )


def open_milvus_client(settings: Settings) -> Any:
    configure_runtime()
    from pymilvus import MilvusClient

    info = build_milvus_connection_info(settings)
    return MilvusClient(
        uri=info.uri,
        token=info.token,
        db_name=info.database,
    )


class MilvusVectorStore:
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
        response = self._client.search(
            collection_name=self._collection_name,
            data=[query_vector],
            filter=self._build_filter(asset_types),
            limit=limit,
            output_fields=list(self._output_fields),
            anns_field=self._vector_name,
        )
        rows = response[0] if isinstance(response, list) and response else []
        if not isinstance(rows, list):
            return []

        hits: list[dict[str, Any]] = []
        for row in rows:
            if isinstance(row, Mapping):
                row_payload = dict(row)
            else:
                try:
                    row_payload = dict(row)
                except Exception:
                    row_payload = None
            if not isinstance(row_payload, dict):
                continue
            entity = row_payload.get("entity")
            if not isinstance(entity, dict):
                entity = {
                    key: value
                    for key, value in row_payload.items()
                    if key not in {"id", "distance", "score", "entity"}
                }
            score_value = row_payload.get("distance", row_payload.get("score", 0.0))
            try:
                score = float(score_value)
            except (TypeError, ValueError):
                score = 0.0
            hits.append(
                {
                    "id": str(row_payload.get("id", entity.get("asset_id", "unknown"))),
                    "score": score,
                    "entity": entity,
                }
            )
        return hits

    def _build_filter(self, asset_types: list[str] | None) -> str:
        if not asset_types:
            return ""
        quoted = ", ".join(f'"{asset_type}"' for asset_type in asset_types)
        return f"asset_type in [{quoted}]"

__all__ = [
    "MilvusConnectionInfo",
    "MilvusVectorStore",
    "build_milvus_connection_info",
    "open_milvus_client",
]
