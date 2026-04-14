from ndea.tools.query_planner import mcp_query_planner


class FakeQueryPlannerService:
    def plan(self, query_text: str, query_vector: list[float]) -> dict[str, object]:
        return {
            "query_text": query_text,
            "intent_type": "metric",
            "summary": "Identified metric query with reusable SQL",
            "clarification_required": False,
            "clarification_reason": None,
            "candidate_tables": ["student"],
            "candidate_metrics": ["active student count"],
            "join_hints": [],
            "selected_sql_asset_id": "sql-1",
            "selected_sql": "SELECT COUNT(*) AS total FROM student WHERE status = 'active'",
        }


def test_mcp_query_planner_uses_injected_service(monkeypatch) -> None:
    monkeypatch.setattr(
        "ndea.tools.query_planner.get_query_planner_service",
        lambda: FakeQueryPlannerService(),
    )
    payload = mcp_query_planner(
        query_text="How many active students are there?",
        query_vector=[0.1, 0.2],
    )
    assert payload["intent_type"] == "metric"
    assert payload["selected_sql_asset_id"] == "sql-1"
