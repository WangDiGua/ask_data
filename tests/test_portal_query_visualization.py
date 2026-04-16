from ndea.portal.service import PortalQueryService
from ndea.query_v2 import (
    ClarificationPayload,
    InteractionResult,
    PlanCandidate,
    QueryIR,
    QueryInterpretationPayload,
    QueryResponseV2,
)
from ndea.protocol import TablePayload, TextPayload


class StaticQueryService:
    def __init__(self, payload: QueryResponseV2) -> None:
        self._payload = payload

    def run(self, request):
        return self._payload


def build_payload(query_text: str, rows: list[dict[str, object]], intent_type: str = "metric") -> QueryResponseV2:
    return QueryResponseV2(
        session_id="session-1",
        interpretation=QueryInterpretationPayload(
            interaction=InteractionResult(
                query_text=query_text,
                normalized_query_text=query_text,
                rewritten_query_text=query_text,
            ),
            ir=QueryIR(intent_type=intent_type, metric="count", answer_mode="aggregate", confidence=0.8),
            selected_plan=PlanCandidate(
                candidate_id="plan-1",
                intent_type=intent_type,
                answer_mode="aggregate",
                source="semantic-first",
                base_table="dcstu",
                candidate_tables=["dcstu"],
                confidence=0.9,
            ),
        ),
        answer=TextPayload(summary="ok"),
        table=TablePayload(columns=list(rows[0].keys()), rows=rows, total_rows=len(rows)),
        sql="SELECT 1",
        audit={},
        confidence=0.9,
        clarification=ClarificationPayload(required=False),
        executed=True,
    )


def test_portal_query_visualization_prefers_horizontal_bar_for_ranking() -> None:
    service = PortalQueryService(
        query_service=StaticQueryService(
            build_payload(
                "按学院排名统计在校学生人数",
                [
                    {"college_name": "计算机学院", "total": 1200},
                    {"college_name": "外国语学院", "total": 800},
                    {"college_name": "法学院", "total": 760},
                    {"college_name": "管理学院", "total": 730},
                    {"college_name": "经贸学院", "total": 710},
                    {"college_name": "艺术学院", "total": 680},
                    {"college_name": "体育学院", "total": 420},
                ],
            )
        )
    )

    payload = service.query("按学院排名统计在校学生人数", database="campus")

    assert payload.visualization is not None
    assert payload.visualization["chart"]["kind"] == "bar-horizontal"


def test_portal_query_visualization_uses_heatmap_when_two_dimensions_exist() -> None:
    service = PortalQueryService(
        query_service=StaticQueryService(
            build_payload(
                "按学院和民族统计人数分布",
                [
                    {"college_name": "计算机学院", "nation_name": "汉族", "total": 120},
                    {"college_name": "计算机学院", "nation_name": "回族", "total": 110},
                    {"college_name": "外国语学院", "nation_name": "汉族", "total": 98},
                    {"college_name": "外国语学院", "nation_name": "维吾尔族", "total": 87},
                ],
            )
        )
    )

    payload = service.query("按学院和民族统计人数分布", database="campus")

    assert payload.visualization is not None
    assert payload.visualization["chart"]["kind"] == "heatmap"
