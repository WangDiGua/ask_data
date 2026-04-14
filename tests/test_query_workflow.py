from ndea.planning import QueryPlanPayload, QueryWorkflowService
from ndea.sql_generation import SQLGenerationPayload, SQLRepairPayload


class FakeQueryPlannerService:
    def __init__(self, payload: QueryPlanPayload) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def plan(
        self,
        query_text: str,
        query_vector: list[float],
        request_context: dict[str, object] | None = None,
    ) -> QueryPlanPayload:
        call = {
            "query_text": query_text,
            "query_vector": query_vector,
        }
        if request_context is not None:
            call["request_context"] = request_context
        self.calls.append(call)
        return self.payload


class FakeQueryExecutionService:
    def __init__(self, payload: dict[str, object] | list[object]) -> None:
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
        if policy_context is not None:
            call["policy_context"] = policy_context
        self.calls.append(call)
        if isinstance(self.payload, list):
            result = self.payload.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        return self.payload


class FakeSQLGeneratorService:
    def __init__(self, payload: SQLGenerationPayload) -> None:
        self.payload = payload
        self.calls: list[QueryPlanPayload] = []

    def generate(self, plan: QueryPlanPayload) -> SQLGenerationPayload:
        self.calls.append(plan)
        return self.payload


class FakeSQLRepairService:
    def __init__(self, payloads: list[SQLRepairPayload]) -> None:
        self.payloads = payloads
        self.calls: list[dict[str, object]] = []

    def repair(
        self,
        plan: QueryPlanPayload,
        failed_sql: str,
        failure_reason: str,
        attempt_number: int,
    ) -> SQLRepairPayload:
        self.calls.append(
            {
                "plan": plan,
                "failed_sql": failed_sql,
                "failure_reason": failure_reason,
                "attempt_number": attempt_number,
            }
        )
        return self.payloads.pop(0)


class FakeAuditSink:
    def __init__(self, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.events: list[object] = []

    def emit(self, event) -> None:
        self.events.append(event)
        if self.should_fail:
            raise RuntimeError("audit sink unavailable")


def test_query_workflow_executes_selected_sql_when_requested() -> None:
    planner = FakeQueryPlannerService(
        QueryPlanPayload(
            query_text="How many active students are there?",
            intent_type="metric",
            summary="Identified metric query with reusable SQL",
            clarification_required=False,
            clarification_reason=None,
            candidate_tables=["student"],
            candidate_metrics=["active student count"],
            join_hints=[],
            selected_sql_asset_id="sql-1",
            selected_sql="SELECT COUNT(*) AS total FROM student WHERE status = 'active'",
        )
    )
    executor = FakeQueryExecutionService(
        {
            "trace_id": "trace-123",
            "request_id": "request-123",
            "database": "campus",
            "allowed": True,
            "sql": "SELECT COUNT(*) AS total FROM student WHERE status = 'active'",
            "effective_sql": "SELECT COUNT(*) AS total FROM student WHERE status = 'active'",
            "degraded": False,
            "error_code": None,
            "audit_id": "audit-123",
            "policy_summary": {"allowed_tables": ["student"]},
            "summary": {"summary": "Returned 1 rows from campus", "details": None},
            "table": {
                "columns": ["total"],
                "rows": [{"total": 1234}],
                "total_rows": 1,
            },
        }
    )
    workflow = QueryWorkflowService(
        planner=planner,
        query_service=executor,
        generator=FakeSQLGeneratorService(
            SQLGenerationPayload(
                generated=True,
                sql="SELECT COUNT(*) AS total FROM student WHERE status = 'active'",
                strategy="rag_candidate",
                reason=None,
            )
        ),
        trace_id_factory=lambda: "unused-trace",
        request_id_factory=lambda: "unused-request",
    )

    payload = workflow.run(
        query_text="How many active students are there?",
        query_vector=[0.1, 0.2],
        database="campus",
        execute=True,
        request_context={
            "trace_id": "trace-123",
            "request_id": "request-123",
            "actor_id": "user-1",
            "policy": {"allowed_tables": ["student"]},
        },
    )

    assert payload.trace_id == "trace-123"
    assert payload.request_id == "request-123"
    assert payload.executed is True
    assert payload.audit_id == "audit-123"
    assert payload.error_code is None
    assert payload.execution == {
        "trace_id": "trace-123",
        "request_id": "request-123",
        "database": "campus",
        "allowed": True,
        "sql": "SELECT COUNT(*) AS total FROM student WHERE status = 'active'",
        "effective_sql": "SELECT COUNT(*) AS total FROM student WHERE status = 'active'",
        "degraded": False,
        "error_code": None,
        "audit_id": "audit-123",
        "policy_summary": {"allowed_tables": ["student"]},
        "summary": {"summary": "Returned 1 rows from campus", "details": None},
        "table": {
            "columns": ["total"],
            "rows": [{"total": 1234}],
            "total_rows": 1,
        },
    }
    assert payload.response_text.summary == "Returned 1 rows from campus"
    assert payload.response_table is not None
    assert payload.response_table.columns == ["total"]
    assert payload.response_chart is None
    assert payload.tool_trace == ["query_planner", "safe_executor", "response_assembler"]
    assert executor.calls == [
        {
            "database": "campus",
            "sql": "SELECT COUNT(*) AS total FROM student WHERE status = 'active'",
            "request_context": {
                "trace_id": "trace-123",
                "request_id": "request-123",
                "actor_id": "user-1",
                "policy": {"allowed_tables": ["student"]},
            },
        }
    ]


def test_query_workflow_skips_execution_when_clarification_is_required() -> None:
    planner = FakeQueryPlannerService(
        QueryPlanPayload(
            query_text="Compare graduation rates this year and last year",
            intent_type="comparison",
            summary="Need more semantic grounding before planning SQL",
            clarification_required=True,
            clarification_reason="Need more semantic grounding before planning SQL",
            candidate_tables=[],
            candidate_metrics=[],
            join_hints=[],
            selected_sql_asset_id=None,
            selected_sql=None,
        )
    )
    executor = FakeQueryExecutionService({"unused": True})
    workflow = QueryWorkflowService(
        planner=planner,
        query_service=executor,
        generator=FakeSQLGeneratorService(
            SQLGenerationPayload(
                generated=False,
                sql=None,
                strategy=None,
                reason="Clarification required before SQL generation",
            )
        ),
        trace_id_factory=lambda: "trace-clarify",
        request_id_factory=lambda: "request-clarify",
    )

    payload = workflow.run(
        query_text="Compare graduation rates this year and last year",
        query_vector=[0.3, 0.4],
        database="campus",
        execute=True,
    )

    assert payload.trace_id == "trace-clarify"
    assert payload.request_id == "request-clarify"
    assert payload.executed is False
    assert payload.execution is None
    assert payload.error_code == "clarification_required"
    assert payload.notes == ["Clarification required before execution."]
    assert payload.response_text.summary == "Need more semantic grounding before planning SQL"
    assert payload.response_table is None
    assert payload.response_chart is None
    assert payload.tool_trace == ["query_planner", "response_assembler"]
    assert executor.calls == []


def test_query_workflow_generates_sql_when_plan_has_no_reusable_candidate() -> None:
    planner = FakeQueryPlannerService(
        QueryPlanPayload(
            query_text="How many students are there?",
            intent_type="metric",
            summary="Metric query with candidate table",
            clarification_required=False,
            clarification_reason=None,
            candidate_tables=["student"],
            candidate_metrics=["student count"],
            join_hints=[],
            selected_sql_asset_id=None,
            selected_sql=None,
        )
    )
    generator = FakeSQLGeneratorService(
        SQLGenerationPayload(
            generated=True,
            sql="SELECT COUNT(*) AS total FROM student",
            strategy="count_metric",
            reason=None,
        )
    )
    executor = FakeQueryExecutionService(
        {
            "trace_id": "trace-generated",
            "request_id": "request-generated",
            "database": "campus",
            "allowed": True,
            "sql": "SELECT COUNT(*) AS total FROM student",
            "effective_sql": "SELECT COUNT(*) AS total FROM student",
            "degraded": False,
            "error_code": None,
            "audit_id": "audit-generated",
            "policy_summary": {"allowed_tables": ["student"]},
            "summary": {"summary": "Returned 1 rows from campus", "details": None},
            "table": {
                "columns": ["total"],
                "rows": [{"total": 2500}],
                "total_rows": 1,
            },
        }
    )
    workflow = QueryWorkflowService(
        planner=planner,
        query_service=executor,
        generator=generator,
        trace_id_factory=lambda: "unused-trace",
        request_id_factory=lambda: "unused-request",
    )

    payload = workflow.run(
        query_text="How many students are there?",
        query_vector=[0.4, 0.5],
        database="campus",
        execute=True,
        request_context={
            "trace_id": "trace-generated",
            "request_id": "request-generated",
            "actor_id": "user-2",
            "policy": {"allowed_tables": ["student"]},
        },
    )

    assert payload.trace_id == "trace-generated"
    assert payload.request_id == "request-generated"
    assert payload.executed is True
    assert payload.generation is not None
    assert payload.generation.strategy == "count_metric"
    assert payload.tool_trace == ["query_planner", "sql_generator", "safe_executor", "response_assembler"]
    assert executor.calls == [
        {
            "database": "campus",
            "sql": "SELECT COUNT(*) AS total FROM student",
            "request_context": {
                "trace_id": "trace-generated",
                "request_id": "request-generated",
                "actor_id": "user-2",
                "policy": {"allowed_tables": ["student"]},
            },
        }
    ]


def test_query_workflow_repairs_after_execution_exception() -> None:
    planner = FakeQueryPlannerService(
        QueryPlanPayload(
            query_text="How many students are there?",
            intent_type="metric",
            summary="Metric query with reusable SQL",
            clarification_required=False,
            clarification_reason=None,
            candidate_tables=["student"],
            candidate_metrics=["student count"],
            join_hints=[],
            selected_sql_asset_id="sql-legacy",
            selected_sql="SELECT bad_column FROM student",
        )
    )
    executor = FakeQueryExecutionService(
        [
            RuntimeError("Unknown column 'bad_column' in 'field list'"),
            {
                "trace_id": "trace-repair-success",
                "request_id": "request-repair-success",
                "database": "campus",
                "allowed": True,
                "sql": "SELECT COUNT(*) AS total FROM student",
                "effective_sql": "SELECT COUNT(*) AS total FROM student",
                "degraded": False,
                "error_code": None,
                "audit_id": "audit-repair-success",
                "policy_summary": {"allowed_tables": ["student"]},
                "summary": {"summary": "Returned 1 rows from campus", "details": None},
                "table": {
                    "columns": ["total"],
                    "rows": [{"total": 2500}],
                    "total_rows": 1,
                },
            },
        ]
    )
    repairer = FakeSQLRepairService(
        [
            SQLRepairPayload(
                repaired=True,
                sql="SELECT COUNT(*) AS total FROM student",
                strategy="repair_unknown_column",
                trigger="unknown_column",
                reason=None,
                attempt_number=1,
            )
        ]
    )
    workflow = QueryWorkflowService(
        planner=planner,
        query_service=executor,
        generator=FakeSQLGeneratorService(
            SQLGenerationPayload(
                generated=True,
                sql="SELECT COUNT(*) AS total FROM student",
                strategy="count_metric",
                reason=None,
            )
        ),
        repairer=repairer,
        trace_id_factory=lambda: "trace-repair-success",
        request_id_factory=lambda: "request-repair-success",
    )

    payload = workflow.run(
        query_text="How many students are there?",
        query_vector=[0.9, 0.8],
        database="campus",
        execute=True,
    )

    assert payload.trace_id == "trace-repair-success"
    assert payload.request_id == "request-repair-success"
    assert payload.executed is True
    assert payload.audit_id == "audit-repair-success"
    assert payload.repair is not None
    assert payload.repair.repaired is True
    assert payload.repair.trigger == "unknown_column"
    assert [attempt.sql for attempt in payload.sql_attempts] == [
        "SELECT bad_column FROM student",
        "SELECT COUNT(*) AS total FROM student",
    ]
    assert [attempt.status for attempt in payload.sql_attempts] == ["failed", "succeeded"]
    assert payload.tool_trace == [
        "query_planner",
        "safe_executor",
        "sql_repair",
        "safe_executor",
        "response_assembler",
    ]
    assert repairer.calls[0]["failure_reason"] == "Unknown column 'bad_column' in 'field list'"
    assert executor.calls == [
        {
            "database": "campus",
            "sql": "SELECT bad_column FROM student",
        },
        {
            "database": "campus",
            "sql": "SELECT COUNT(*) AS total FROM student",
        },
    ]


def test_query_workflow_stops_when_repair_declines_failure() -> None:
    planner = FakeQueryPlannerService(
        QueryPlanPayload(
            query_text="Show department details",
            intent_type="detail",
            summary="Department detail query",
            clarification_required=False,
            clarification_reason=None,
            candidate_tables=["department"],
            candidate_metrics=[],
            join_hints=[],
            selected_sql_asset_id="sql-dept",
            selected_sql="SELECT * FROM department",
        )
    )
    executor = FakeQueryExecutionService(
        {
            "trace_id": "trace-repair-stop",
            "request_id": "request-repair-stop",
            "database": "campus",
            "allowed": False,
            "sql": "SELECT * FROM department",
            "effective_sql": "SELECT * FROM department",
            "reason": "Access to tables is not allowed: department",
            "degraded": False,
            "error_code": "policy_denied",
            "audit_id": "audit-repair-stop",
            "policy_summary": {"allowed_tables": ["student"]},
            "summary": {"summary": "Access to tables is not allowed: department", "details": None},
            "table": None,
        }
    )
    repairer = FakeSQLRepairService(
        [
            SQLRepairPayload(
                repaired=False,
                sql=None,
                strategy=None,
                trigger="permission_conflict",
                reason="Execution failure is not repairable under current policy",
                attempt_number=1,
            )
        ]
    )
    workflow = QueryWorkflowService(
        planner=planner,
        query_service=executor,
        generator=FakeSQLGeneratorService(
            SQLGenerationPayload(
                generated=True,
                sql="SELECT * FROM department",
                strategy="detail_scan",
                reason=None,
            )
        ),
        repairer=repairer,
        trace_id_factory=lambda: "trace-repair-stop",
        request_id_factory=lambda: "request-repair-stop",
    )

    payload = workflow.run(
        query_text="Show department details",
        query_vector=[0.2, 0.1],
        database="campus",
        execute=True,
    )

    assert payload.trace_id == "trace-repair-stop"
    assert payload.request_id == "request-repair-stop"
    assert payload.executed is False
    assert payload.execution is not None
    assert payload.execution["allowed"] is False
    assert payload.error_code == "policy_denied"
    assert payload.notes == ["Execution failure is not repairable under current policy"]
    assert len(payload.sql_attempts) == 1
    assert payload.sql_attempts[0].status == "failed"
    assert payload.tool_trace == [
        "query_planner",
        "safe_executor",
        "sql_repair",
        "response_assembler",
    ]
    assert executor.calls == [
        {
            "database": "campus",
            "sql": "SELECT * FROM department",
        }
    ]


def test_query_workflow_emits_audit_event_and_request_metadata() -> None:
    planner = FakeQueryPlannerService(
        QueryPlanPayload(
            query_text="How many students are there?",
            intent_type="metric",
            summary="Metric query",
            clarification_required=False,
            candidate_tables=["student"],
            candidate_metrics=["student count"],
            join_hints=[],
            selected_sql_asset_id="sql-1",
            selected_sql="SELECT COUNT(*) AS total FROM student",
        )
    )
    executor = FakeQueryExecutionService(
        {
            "trace_id": "trace-audit",
            "request_id": "request-audit",
            "database": "campus",
            "allowed": True,
            "sql": "SELECT COUNT(*) AS total FROM student",
            "effective_sql": "SELECT COUNT(*) AS total FROM student WHERE tenant_id = 1",
            "degraded": False,
            "error_code": None,
            "audit_id": "audit-audit",
            "policy_summary": {"allowed_tables": ["student"]},
            "summary": {"summary": "Returned 1 rows from campus", "details": None},
            "table": {"columns": ["total"], "rows": [{"total": 100}], "total_rows": 1},
        }
    )
    audit_sink = FakeAuditSink()
    time_values = iter([10.0, 10.25])
    workflow = QueryWorkflowService(
        planner=planner,
        query_service=executor,
        audit_sink=audit_sink,
        trace_id_factory=lambda: "unused-trace",
        request_id_factory=lambda: "unused-request",
        time_source=lambda: next(time_values),
    )

    payload = workflow.run(
        query_text="How many students are there?",
        query_vector=[0.1, 0.2],
        database="campus",
        execute=True,
        request_context={
            "trace_id": "trace-audit",
            "request_id": "request-audit",
            "actor_id": "user-11",
            "tenant_id": "tenant-11",
            "policy": {"allowed_tables": ["student"]},
        },
    )

    assert payload.trace_id == "trace-audit"
    assert payload.request_id == "request-audit"
    assert payload.audit_id == "audit-audit"
    assert len(audit_sink.events) == 1
    event = audit_sink.events[0]
    assert event.trace_id == "trace-audit"
    assert event.request_id == "request-audit"
    assert event.actor_id == "user-11"
    assert event.tenant_id == "tenant-11"
    assert event.final_status == "succeeded"
    assert event.dependency_status["mysql"] == "healthy"
    assert event.latency_ms == 250


def test_query_workflow_does_not_fail_when_audit_sink_errors() -> None:
    planner = FakeQueryPlannerService(
        QueryPlanPayload(
            query_text="How many students are there?",
            intent_type="metric",
            summary="Metric query",
            clarification_required=False,
            candidate_tables=["student"],
            candidate_metrics=["student count"],
            join_hints=[],
            selected_sql_asset_id="sql-1",
            selected_sql="SELECT COUNT(*) AS total FROM student",
        )
    )
    executor = FakeQueryExecutionService(
        {
            "trace_id": "trace-audit-fail",
            "request_id": "request-audit-fail",
            "database": "campus",
            "allowed": True,
            "sql": "SELECT COUNT(*) AS total FROM student",
            "effective_sql": "SELECT COUNT(*) AS total FROM student",
            "degraded": False,
            "error_code": None,
            "audit_id": "audit-audit-fail",
            "policy_summary": {"allowed_tables": ["student"]},
            "summary": {"summary": "Returned 1 rows from campus", "details": None},
            "table": {"columns": ["total"], "rows": [{"total": 100}], "total_rows": 1},
        }
    )
    workflow = QueryWorkflowService(
        planner=planner,
        query_service=executor,
        audit_sink=FakeAuditSink(should_fail=True),
    )

    payload = workflow.run(
        query_text="How many students are there?",
        query_vector=[0.1, 0.2],
        database="campus",
        execute=True,
    )

    assert payload.executed is True
    assert payload.response_text.summary == "Returned 1 rows from campus"


def test_query_workflow_returns_service_unavailable_without_repairing() -> None:
    planner = FakeQueryPlannerService(
        QueryPlanPayload(
            query_text="How many students are there?",
            intent_type="metric",
            summary="Metric query",
            clarification_required=False,
            candidate_tables=["student"],
            candidate_metrics=["student count"],
            join_hints=[],
            selected_sql_asset_id="sql-1",
            selected_sql="SELECT COUNT(*) AS total FROM student",
        )
    )
    executor = FakeQueryExecutionService([RuntimeError("mysql unavailable")])
    workflow = QueryWorkflowService(
        planner=planner,
        query_service=executor,
        trace_id_factory=lambda: "trace-down",
        request_id_factory=lambda: "request-down",
    )

    payload = workflow.run(
        query_text="How many students are there?",
        query_vector=[0.5, 0.6],
        database="campus",
        execute=True,
    )

    assert payload.executed is False
    assert payload.error_code == "service_unavailable"
    assert payload.degraded is True
    assert payload.tool_trace == ["query_planner", "safe_executor", "response_assembler"]
