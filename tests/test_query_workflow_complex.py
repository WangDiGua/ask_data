from ndea.planning import QueryPlannerService, QueryWorkflowService
from ndea.sql_generation import SQLGeneratorService


class FakeVectorLocatorService:
    def __init__(self, payload) -> None:
        self.payload = payload

    def locate(
        self,
        query_text: str,
        query_vector: list[float],
        asset_types: list[str] | None = None,
        limit: int | None = None,
    ):
        return self.payload


class FakeSQLRAGService:
    def __init__(self, payload) -> None:
        self.payload = payload

    def retrieve(
        self,
        query_text: str,
        query_vector: list[float],
        limit: int | None = None,
    ):
        return self.payload


class FakeQueryExecutionService:
    def __init__(self, payload) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def execute_query(
        self,
        database: str,
        sql: str,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        call = {"database": database, "sql": sql}
        if request_context is not None:
            call["request_context"] = request_context
        self.calls.append(call)
        return self.payload


def test_query_workflow_returns_clarification_payload_for_ambiguous_complex_metric() -> None:
    planner = QueryPlannerService(
        vector_locator=FakeVectorLocatorService(
            {
                "matches": [],
                "metric_contracts": [
                    {
                        "metric_id": "metric-campus-population",
                        "name": "校园人数",
                        "aliases": ["学校有多少人", "校园人数", "总人数"],
                        "business_definition": "校园在册人数",
                        "base_table": "student",
                        "measure_expression": "COUNT(*)",
                        "default_filters": ["student.status = 'active'"],
                        "entity_scope_options": ["student", "faculty", "all_people"],
                        "requires_entity_scope": True,
                    }
                ],
                "dimension_contracts": [],
                "join_path_contracts": [],
                "time_semantics_catalog": [],
            }
        ),
        sql_rag=FakeSQLRAGService({"candidates": []}),
    )
    workflow = QueryWorkflowService(
        planner=planner,
        query_service=FakeQueryExecutionService({"unused": True}),
        generator=SQLGeneratorService(),
    )

    payload = workflow.run(
        query_text="我们学校有多少人",
        query_vector=[0.2, 0.3],
        database="campus",
        execute=True,
    )

    assert payload.executed is False
    assert payload.clarification_required is True
    assert payload.clarification_questions == [
        "你想查询学生、教职工，还是全体在册人员？"
    ]
    assert payload.resolved_metric is not None
    assert payload.resolved_metric["metric_id"] == "metric-campus-population"
    assert payload.resolved_time_scope is None
    assert payload.tool_trace == ["query_planner", "response_assembler"]


def test_query_workflow_executes_after_planning_context_resolves_complex_metric() -> None:
    planner = QueryPlannerService(
        vector_locator=FakeVectorLocatorService(
            {
                "matches": [],
                "metric_contracts": [
                    {
                        "metric_id": "metric-campus-population",
                        "name": "校园人数",
                        "aliases": ["学校有多少人", "校园人数", "总人数"],
                        "business_definition": "校园在册人数",
                        "base_table": "student",
                        "measure_expression": "COUNT(*)",
                        "default_filters": ["student.status = 'active'"],
                        "entity_scope_options": ["student", "faculty", "all_people"],
                        "requires_entity_scope": True,
                    }
                ],
                "dimension_contracts": [],
                "join_path_contracts": [],
                "time_semantics_catalog": [],
            }
        ),
        sql_rag=FakeSQLRAGService({"candidates": []}),
    )
    executor = FakeQueryExecutionService(
        {
            "trace_id": "trace-complex",
            "request_id": "request-complex",
            "database": "campus",
            "allowed": True,
            "sql": "SELECT COUNT(*) AS total FROM student WHERE student.status = 'active'",
            "effective_sql": "SELECT COUNT(*) AS total FROM student WHERE student.status = 'active'",
            "degraded": False,
            "error_code": None,
            "audit_id": "audit-complex",
            "policy_summary": {"allowed_tables": ["student"]},
            "summary": {"summary": "Returned 1 rows from campus", "details": None},
            "table": {"columns": ["total"], "rows": [{"total": 32100}], "total_rows": 1},
        }
    )
    workflow = QueryWorkflowService(
        planner=planner,
        query_service=executor,
        generator=SQLGeneratorService(),
    )

    payload = workflow.run(
        query_text="我们学校有多少人",
        query_vector=[0.2, 0.3],
        database="campus",
        execute=True,
        request_context={
            "trace_id": "trace-complex",
            "request_id": "request-complex",
            "planning_context": {"entity_scope": "student"},
        },
    )

    assert payload.executed is True
    assert payload.clarification_required is False
    assert payload.resolved_metric is not None
    assert payload.resolved_metric["metric_id"] == "metric-campus-population"
    assert payload.tool_trace == ["query_planner", "sql_generator", "safe_executor", "response_assembler"]
    assert executor.calls == [
        {
            "database": "campus",
            "sql": "SELECT COUNT(*) AS total FROM student WHERE student.status = 'active'",
            "request_context": {
                "trace_id": "trace-complex",
                "request_id": "request-complex",
                "planning_context": {"entity_scope": "student"},
            },
        }
    ]
