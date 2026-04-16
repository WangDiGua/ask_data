from ndea.portal.service import PortalQueryService
from ndea.query_v2 import (
    ClarificationPayload,
    InteractionResult,
    PlanCandidate,
    QueryIR,
    QueryInterpretationPayload,
    QueryRequestV2,
    QueryResponseV2,
    SQLCandidate,
    VerificationReport,
)
from ndea.protocol import TablePayload, TextPayload


class FakeQueryService:
    def __init__(self, payload: QueryResponseV2) -> None:
        self._payload = payload
        self.calls: list[QueryRequestV2] = []

    def run(self, request: QueryRequestV2) -> QueryResponseV2:
        self.calls.append(request)
        return self._payload


def build_payload(*, clarification_required: bool = False) -> QueryResponseV2:
    plan = PlanCandidate(
        candidate_id="plan-1",
        intent_type="metric",
        answer_mode="aggregate",
        source="semantic-first",
        base_table="dcstu",
        candidate_tables=["dcstu", "dcorg"],
        dimensions=[{"column": "college_name", "name": "学院"}],
        confidence=0.88,
        clarification_question="你想查学生还是教职工？" if clarification_required else None,
        requires_clarification=clarification_required,
    )
    sql_candidate = SQLCandidate(
        candidate_id="sql-1",
        plan_candidate_id="plan-1",
        source="semantic-first",
        sql="SELECT college_name, COUNT(*) AS total FROM dcstu GROUP BY college_name",
        score=0.9,
    )
    verification = VerificationReport(
        sql_candidate_id="sql-1",
        allowed=True,
        score=0.92,
        effective_sql=sql_candidate.sql,
    )
    return QueryResponseV2(
        session_id="session-1",
        interpretation=QueryInterpretationPayload(
            interaction=InteractionResult(
                query_text="按学院统计在校学生人数",
                normalized_query_text="按学院统计在校学生人数",
                rewritten_query_text="按学院统计在校学生人数",
            ),
            ir=QueryIR(
                intent_type="metric",
                metric="count",
                entity_scope="student",
                dimensions=["college"],
                answer_mode="aggregate",
                confidence=0.82,
            ),
            selected_plan=plan,
            selected_sql=sql_candidate,
            verification=verification,
        ),
        answer=TextPayload(summary="已按学院返回在校学生人数"),
        table=TablePayload(
            columns=["college_name", "total"],
            rows=[{"college_name": "计算机学院", "total": 1200}, {"college_name": "外国语学院", "total": 800}],
            total_rows=2,
        ),
        sql=sql_candidate.sql,
        audit={"allowed": True},
        confidence=0.9,
        clarification=ClarificationPayload(
            required=clarification_required,
            question="你想查学生还是教职工？" if clarification_required else None,
            reason="entity_scope_required" if clarification_required else None,
        ),
        learning_trace_id="learn-session-1",
        tool_trace=["interaction", "intent_parse", "semantic_resolve", "respond"],
        executed=not clarification_required,
    )


def test_portal_query_service_uses_v2_request_and_maps_payload() -> None:
    workflow = FakeQueryService(build_payload())
    service = PortalQueryService(query_service=workflow)

    payload = service.query(
        query_text="按学院统计在校学生人数",
        database="campus",
        request_context={"actor_id": "u1"},
        policy_context={"allowed_tables": ["dcstu"]},
    )

    assert workflow.calls[0].query_text == "按学院统计在校学生人数"
    assert workflow.calls[0].database == "campus"
    assert payload.executed is True
    assert payload.sql == "SELECT college_name, COUNT(*) AS total FROM dcstu GROUP BY college_name"
    assert payload.metadata.metric_id == "count"
    assert payload.metadata.resolved_tables == ["dcstu", "dcorg"]
    assert payload.table is not None
    assert payload.table.columns[0].label == "学院"


def test_portal_query_service_preserves_clarification() -> None:
    service = PortalQueryService(query_service=FakeQueryService(build_payload(clarification_required=True)))

    payload = service.query(query_text="我们学校有多少人", database="campus")

    assert payload.clarification_required is True
    assert payload.clarification_question == "你想查学生还是教职工？"
    assert payload.executed is False


def test_portal_query_service_handles_runtime_errors() -> None:
    class ExplodingQueryService:
        def run(self, request: QueryRequestV2) -> QueryResponseV2:
            raise RuntimeError("downstream failed")

    service = PortalQueryService(query_service=ExplodingQueryService())
    payload = service.query(query_text="按学院统计在校学生人数", database="campus")

    assert payload.executed is False
    assert "Query workflow failed" in payload.text
