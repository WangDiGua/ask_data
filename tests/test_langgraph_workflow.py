from ndea.orchestration.langgraph_workflow import LangGraphQueryWorkflowService
from ndea.planning import QueryPlanPayload
from ndea.protocol import TablePayload, TextPayload
from ndea.response import AssembledResponsePayload
from ndea.sql_advisor import SQLAdvisoryPayload


class FakePlanner:
    def plan(
        self,
        query_text: str,
        query_vector: list[float],
        request_context: dict[str, object] | None = None,
    ) -> QueryPlanPayload:
        return QueryPlanPayload(
            query_text=query_text,
            intent_type="metric",
            summary="Planned a metric query",
            clarification_required=False,
            candidate_tables=["student"],
            candidate_metrics=["student_count"],
            join_hints=[],
            selected_sql_asset_id=None,
            selected_sql=None,
        )


class FakeAdvisor:
    def advise(self, query_text: str, plan: QueryPlanPayload) -> SQLAdvisoryPayload:
        return SQLAdvisoryPayload(
            selected_sql="SELECT COUNT(*) AS total FROM student",
            strategy="vanna_style_examples",
            confidence=0.96,
            examples=[
                {
                    "asset_id": "sql-1",
                    "question": "How many students are there?",
                    "sql": "SELECT COUNT(*) AS total FROM student",
                    "score": 0.96,
                }
            ],
            notes=["Used exemplar-guided SQL selection"],
        )


class FakeQueryService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def execute_query(
        self,
        database: str,
        sql: str,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        self.calls.append((database, sql))
        return {
            "trace_id": "trace-graph",
            "request_id": "request-graph",
            "database": database,
            "allowed": True,
            "sql": sql,
            "effective_sql": sql,
            "degraded": False,
            "error_code": None,
            "audit_id": "audit-graph",
            "policy_summary": {"allowed_tables": ["student"]},
            "summary": {"summary": "Returned 1 rows from campus", "details": None},
            "table": {
                "columns": ["total"],
                "rows": [{"total": 32000}],
                "total_rows": 1,
            },
        }


class FakeAssembler:
    def assemble(
        self,
        plan: QueryPlanPayload,
        execution_payload: dict[str, object] | None,
    ) -> AssembledResponsePayload:
        return AssembledResponsePayload(
            text=TextPayload(summary="校园总人数为 32000"),
            table=TablePayload(columns=["total"], rows=[{"total": 32000}], total_rows=1),
            chart=None,
        )


def test_langgraph_workflow_executes_and_streams_events() -> None:
    query_service = FakeQueryService()
    workflow = LangGraphQueryWorkflowService(
        planner=FakePlanner(),
        advisor=FakeAdvisor(),
        query_service=query_service,
        assembler=FakeAssembler(),
        trace_id_factory=lambda: "trace-graph",
        request_id_factory=lambda: "request-graph",
        audit_id_factory=lambda: "audit-graph",
    )

    payload = workflow.run(
        query_text="我们学校有多少人",
        query_vector=[0.1, 0.2],
        database="campus",
        execute=True,
    )

    assert payload.executed is True
    assert payload.trace_id == "trace-graph"
    assert payload.request_id == "request-graph"
    assert payload.audit_id == "audit-graph"
    assert payload.execution is not None
    assert payload.execution["allowed"] is True
    assert payload.response_text.summary == "校园总人数为 32000"
    assert payload.tool_trace == [
        "langgraph_planner",
        "sql_advisor",
        "langgraph_executor",
        "response_assembler",
    ]
    assert query_service.calls == [("campus", "SELECT COUNT(*) AS total FROM student")]

    events = list(
        workflow.stream(
            query_text="我们学校有多少人",
            query_vector=[0.1, 0.2],
            database="campus",
            execute=True,
        )
    )

    assert [event["node"] for event in events if event["type"] == "node"] == [
        "planner",
        "advisor",
        "executor",
        "assembler",
    ]
    assert events[-1]["type"] == "final"
    assert events[-1]["payload"]["executed"] is True
