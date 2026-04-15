from ndea.vector.milvus_client import MilvusVectorStore


class FakeMilvusClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def search(self, **kwargs):
        self.calls.append(kwargs)
        return [
            [
                {
                    "id": "metric-1",
                    "distance": 0.92,
                    "entity": {
                        "asset_id": "metric-1",
                        "asset_type": "metric",
                        "title": "Student Count",
                        "text": "Count of active students",
                    },
                }
            ]
        ]


def test_milvus_vector_store_builds_expected_search_request() -> None:
    client = FakeMilvusClient()
    store = MilvusVectorStore(
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
    assert call["data"] == [[0.1, 0.2, 0.3]]
    assert call["limit"] == 4
    assert call["anns_field"] == "embedding"
    assert call["filter"] == 'asset_type in ["metric", "schema"]'
    assert call["output_fields"] == [
        "asset_id",
        "asset_type",
        "title",
        "text",
        "source",
        "metadata",
    ]


def test_milvus_vector_store_allows_custom_output_fields() -> None:
    client = FakeMilvusClient()
    store = MilvusVectorStore(
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
    assert call["output_fields"] == ["asset_id", "question", "sql"]
    assert call["filter"] == 'asset_type in ["golden_sql"]'
