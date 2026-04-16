from ndea.tools.portal_query import ask_data_query


class FakePortalQueryService:
    def query(
        self,
        query_text: str,
        database: str | None = None,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ):
        return type(
            "PortalPayload",
            (),
            {
                "model_dump": lambda self, mode="json": {
                    "text": "ok",
                    "table": None,
                    "visualization": None,
                    "clarification_required": False,
                    "clarification_question": None,
                    "executed": True,
                    "sql": "select 1",
                    "metadata": {
                        "tool_trace": ["interaction", "intent_parse", "respond"],
                        "confidence": 1.0,
                        "selected_sql_asset_id": "sql-1",
                        "metric_id": "count",
                    },
                    "query_text": query_text,
                    "database": database,
                    "request_context": request_context,
                    "policy_context": policy_context,
                }
            },
        )()


def test_ask_data_query_uses_injected_service(monkeypatch) -> None:
    monkeypatch.setattr(
        "ndea.tools.portal_query.get_portal_query_service",
        lambda: FakePortalQueryService(),
    )

    payload = ask_data_query(
        query_text="按学院统计在校学生人数",
        database="wenshu_db",
        request_context={"actor_id": "user-1"},
        policy_context={"scope": "read_only"},
    )

    assert payload["executed"] is True
    assert payload["query_text"] == "按学院统计在校学生人数"
    assert payload["database"] == "wenshu_db"
    assert payload["request_context"] == {"actor_id": "user-1"}
    assert payload["policy_context"] == {"scope": "read_only"}
