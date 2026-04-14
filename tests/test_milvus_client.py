from qdrant_client import models

from ndea.vector.qdrant_client import QdrantVectorStore


class FakeQueryResponse:
    def __init__(self, points) -> None:
        self.points = points


class FakeQdrantClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def query_points(self, **kwargs):
        self.calls.append(kwargs)
        return FakeQueryResponse(
            [
                models.ScoredPoint(
                    id="metric-1",
                    version=1,
                    score=0.92,
                    payload={
                        "asset_id": "metric-1",
                        "asset_type": "metric",
                        "title": "Student Count",
                        "text": "Count of active students",
                    },
                    vector=None,
                )
            ]
        )


def test_qdrant_vector_store_builds_expected_search_request() -> None:
    client = FakeQdrantClient()
    store = QdrantVectorStore(
        client=client,
        collection_name="semantic_assets",
        vector_name="embedding",
    )

    hits = store.search(
        query_vector=[0.1, 0.2, 0.3],
        asset_types=["metric", "schema"],
        limit=4,
    )

    assert hits[0]["id"] == "metric-1"
    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["collection_name"] == "semantic_assets"
    assert call["query"] == [0.1, 0.2, 0.3]
    assert call["limit"] == 4
    assert call["using"] == "embedding"
    assert call["query_filter"].must[0].key == "asset_type"
    assert call["query_filter"].must[0].match.any == ["metric", "schema"]
    assert call["with_payload"] == [
        "asset_id",
        "asset_type",
        "title",
        "text",
        "source",
        "metadata",
    ]


def test_qdrant_vector_store_allows_custom_output_fields() -> None:
    client = FakeQdrantClient()
    store = QdrantVectorStore(
        client=client,
        collection_name="golden_sql_assets",
        vector_name="embedding",
        output_fields=["asset_id", "question", "sql"],
    )

    store.search(
        query_vector=[0.9, 0.8],
        asset_types=["golden_sql"],
        limit=2,
    )

    call = client.calls[0]
    assert call["collection_name"] == "golden_sql_assets"
    assert call["with_payload"] == ["asset_id", "question", "sql"]
    assert call["query_filter"].must[0].match.any == ["golden_sql"]
