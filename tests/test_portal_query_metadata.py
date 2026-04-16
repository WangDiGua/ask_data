from ndea.portal.service import PortalQueryService
from ndea.query_v2 import (
    ClarificationPayload,
    InteractionResult,
    PlanCandidate,
    QueryIR,
    QueryInterpretationPayload,
    QueryResponseV2,
    SQLCandidate,
)
from ndea.protocol import TextPayload


class StaticQueryService:
    def __init__(self, payload: QueryResponseV2) -> None:
        self._payload = payload

    def run(self, request):
        return self._payload


def test_portal_query_metadata_maps_v2_fields() -> None:
    payload = QueryResponseV2(
        session_id="session-1",
        interpretation=QueryInterpretationPayload(
            interaction=InteractionResult(
                query_text="教师名单",
                normalized_query_text="教师名单",
                rewritten_query_text="教师名单",
            ),
            ir=QueryIR(
                intent_type="detail",
                metric=None,
                entity_scope="faculty",
                dimensions=[],
                answer_mode="detail",
                confidence=0.76,
            ),
            selected_plan=PlanCandidate(
                candidate_id="plan-1",
                intent_type="detail",
                answer_mode="detail",
                source="schema-first",
                base_table="dcemp",
                candidate_tables=["dcemp"],
                entity_scope="faculty",
                confidence=0.77,
            ),
            selected_sql=SQLCandidate(
                candidate_id="sql-1",
                plan_candidate_id="plan-1",
                source="schema-first",
                sql="SELECT * FROM dcemp LIMIT 50",
                score=0.77,
            ),
        ),
        answer=TextPayload(summary="已返回教师名单"),
        sql="SELECT * FROM dcemp LIMIT 50",
        audit={},
        confidence=0.77,
        clarification=ClarificationPayload(required=False),
        learning_trace_id="learn-session-1",
        tool_trace=["interaction", "intent_parse", "schema_resolve", "respond"],
        executed=True,
    )
    service = PortalQueryService(query_service=StaticQueryService(payload))

    result = service.query("教师名单", database="campus")

    assert result.metadata.tool_trace == ["interaction", "intent_parse", "schema_resolve", "respond"]
    assert result.metadata.confidence == 0.77
    assert result.metadata.selected_sql_asset_id == "sql-1"
    assert result.metadata.answer_mode == "detail"
    assert result.metadata.resolved_tables == ["dcemp"]
    assert result.metadata.resolved_entities == [{"type": "entity_scope", "value": "faculty"}]
    assert result.metadata.sql_strategy == "schema-first"
