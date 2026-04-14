from ndea.tools.sql_rag import mcp_sql_rag_engine


class FakeSQLRAGService:
    def retrieve(
        self,
        query_text: str,
        query_vector: list[float],
        limit: int | None = None,
    ) -> dict[str, object]:
        return {
            "query_text": query_text,
            "limit": limit,
            "summary": "Found 1 golden SQL candidates",
            "total_candidates": 1,
            "candidates": [
                {
                    "asset_id": "sql-1",
                    "question": "How many active students are there?",
                    "sql": "SELECT COUNT(*) FROM student WHERE status = 'active'",
                    "notes": "Uses active status filter",
                    "score": 0.88,
                    "source": "golden_sql",
                    "tables": ["student"],
                    "metadata": {},
                }
            ],
        }


def test_mcp_sql_rag_engine_uses_injected_service(monkeypatch) -> None:
    monkeypatch.setattr(
        "ndea.tools.sql_rag.get_sql_rag_service",
        lambda: FakeSQLRAGService(),
    )
    payload = mcp_sql_rag_engine(
        query_text="active students",
        query_vector=[0.1, 0.2],
        limit=3,
    )
    assert payload["query_text"] == "active students"
    assert payload["candidates"][0]["asset_id"] == "sql-1"
