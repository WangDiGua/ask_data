from ndea.tools.query_executor import execute_guarded_query


class FakeQueryService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def execute_query(
        self,
        database: str,
        sql: str,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        self.calls.append(
            {
                "database": database,
                "sql": sql,
                "request_context": request_context,
                "policy_context": policy_context,
            }
        )
        return {
            "trace_id": "trace-1",
            "request_id": "request-1",
            "database": database,
            "sql": sql,
            "effective_sql": sql,
            "allowed": True,
            "degraded": False,
            "error_code": None,
            "audit_id": "audit-1",
            "policy_summary": {"allowed_tables": ["student"]},
            "summary": {"summary": "Returned 1 rows from campus", "details": None},
            "table": {
                "columns": ["student_id"],
                "rows": [{"student_id": 1}],
                "total_rows": 1,
            },
            "permission": None,
            "audit": {
                "actor_id": None,
                "original_sql": sql,
                "effective_sql": sql,
                "applied_row_filters": [],
                "masked_columns": [],
                "blocked_columns": [],
                "truncated": False,
            },
            "guard": {
                "allowed": True,
                "reason": None,
                "rejection_code": None,
                "statement_count": 1,
                "statement_type": "select",
                "needs_explain": False,
            },
            "explain": None,
            "truncated": False,
            "reason": None,
        }


def test_execute_guarded_query_uses_injected_service(monkeypatch) -> None:
    service = FakeQueryService()
    monkeypatch.setattr(
        "ndea.tools.query_executor.get_guarded_query_service",
        lambda: service,
    )
    payload = execute_guarded_query(
        "campus",
        "SELECT 1",
        request_context={
            "trace_id": "trace-1",
            "request_id": "request-1",
            "actor_id": "user-7",
            "policy": {"allowed_tables": ["student"]},
        },
    )
    assert payload["database"] == "campus"
    assert payload["allowed"] is True
    assert payload["table"]["rows"][0]["student_id"] == 1
    assert payload["audit_id"] == "audit-1"
    assert service.calls == [
        {
            "database": "campus",
            "sql": "SELECT 1",
            "request_context": {
                "trace_id": "trace-1",
                "request_id": "request-1",
                "actor_id": "user-7",
                "policy": {"allowed_tables": ["student"]},
            },
            "policy_context": None,
        }
    ]
