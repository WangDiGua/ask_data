from ndea.tools.query_workflow import mcp_query_workflow


class FakeQueryWorkflowService:
    def run(
        self,
        query_text: str,
        query_vector: list[float],
        database: str | None = None,
        execute: bool = False,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return {
            "trace_id": "trace-1",
            "request_id": "request-1",
            "query_text": query_text,
            "executed": execute,
            "database": database,
            "request_context": request_context,
            "policy_context": policy_context,
            "degraded": False,
            "error_code": None,
            "audit_id": "audit-1",
            "policy_summary": {"allowed_tables": ["student"]},
            "tool_trace": ["query_planner"],
            "notes": [],
            "plan": {
                "intent_type": "metric",
                "selected_sql_asset_id": "sql-1",
            },
            "execution": None,
        }


def test_mcp_query_workflow_uses_injected_service(monkeypatch) -> None:
    monkeypatch.setattr(
        "ndea.tools.query_workflow.get_query_workflow_service",
        lambda: FakeQueryWorkflowService(),
    )
    payload = mcp_query_workflow(
        query_text="How many active students are there?",
        query_vector=[0.1, 0.2],
        database="campus",
        execute=True,
        request_context={"trace_id": "trace-1", "request_id": "request-1", "actor_id": "user-9"},
    )
    assert payload["trace_id"] == "trace-1"
    assert payload["request_id"] == "request-1"
    assert payload["executed"] is True
    assert payload["database"] == "campus"
    assert payload["request_context"] == {"trace_id": "trace-1", "request_id": "request-1", "actor_id": "user-9"}
