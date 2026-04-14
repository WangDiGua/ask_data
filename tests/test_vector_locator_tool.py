from ndea.tools.vector_locator import mcp_vector_locator


class FakeVectorLocatorService:
    def locate(
        self,
        query_text: str,
        query_vector: list[float],
        asset_types: list[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, object]:
        return {
            "query_text": query_text,
            "asset_types": asset_types or [],
            "limit": limit,
            "summary": "Found 1 semantic matches",
            "total_matches": 1,
            "matches": [
                {
                    "asset_id": "metric-1",
                    "asset_type": "metric",
                    "title": "student count",
                    "text": "Count of active students",
                    "score": 0.9,
                    "source": "metric_catalog",
                    "metadata": {},
                }
            ],
        }


def test_mcp_vector_locator_uses_injected_service(monkeypatch) -> None:
    monkeypatch.setattr(
        "ndea.tools.vector_locator.get_vector_locator_service",
        lambda: FakeVectorLocatorService(),
    )
    payload = mcp_vector_locator(
        query_text="student count",
        query_vector=[0.1, 0.2],
        asset_types=["metric"],
        limit=4,
    )
    assert payload["query_text"] == "student count"
    assert payload["asset_types"] == ["metric"]
    assert payload["matches"][0]["asset_id"] == "metric-1"
