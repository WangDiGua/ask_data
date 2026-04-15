from ndea.planning.models import QueryPlanPayload, QueryWorkflowPayload
from ndea.portal.service import PortalQueryService
from ndea.protocol import TablePayload, TextPayload


class FakeWorkflowService:
    def __init__(self, payload):
        self.payload = payload

    def run(
        self,
        query_text: str,
        query_vector: list[float],
        database: str | None = None,
        execute: bool = False,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ):
        return self.payload


def build_service(payload: QueryWorkflowPayload) -> PortalQueryService:
    return PortalQueryService(
        workflow_service=FakeWorkflowService(payload),
        embedder=lambda base_url, model, texts: [[0.1, 0.2]],
    )


def test_ranking_query_prefers_horizontal_bar() -> None:
    payload = QueryWorkflowPayload(
        trace_id="trace-1",
        request_id="request-1",
        query_text="在校学生人数最多的前10个学院",
        database="wenshu_db",
        executed=True,
        plan=QueryPlanPayload(
            query_text="在校学生人数最多的前10个学院",
            intent_type="ranking",
            summary="按在校学生人数排序的学院前十名。",
            clarification_required=False,
            confidence=0.91,
        ),
        response_text=TextPayload(summary="按在校学生人数排序的学院前十名。"),
        response_table=TablePayload(
            columns=["college_name", "total"],
            rows=[
                {"college_name": "烟台研究院", "total": 3009},
                {"college_name": "信息与电气工程学院", "total": 2645},
                {"college_name": "工学院", "total": 2598},
            ],
            total_rows=3,
        ),
    )

    result = build_service(payload).query("在校学生人数最多的前10个学院", database="wenshu_db")

    assert result.visualization is not None
    assert result.visualization["chart"]["kind"] == "bar-horizontal"


def test_cross_dimension_query_uses_heatmap() -> None:
    payload = QueryWorkflowPayload(
        trace_id="trace-2",
        request_id="request-2",
        query_text="按学院和政治面貌统计在校学生人数",
        database="wenshu_db",
        executed=True,
        plan=QueryPlanPayload(
            query_text="按学院和政治面貌统计在校学生人数",
            intent_type="metric",
            summary="按学院和政治面貌统计在校学生人数。",
            clarification_required=False,
            confidence=0.93,
        ),
        response_text=TextPayload(summary="按学院和政治面貌统计在校学生人数。"),
        response_table=TablePayload(
            columns=["college_name", "political_status", "total"],
            rows=[
                {"college_name": "食品学院", "political_status": "共青团员", "total": 420},
                {"college_name": "食品学院", "political_status": "中共党员", "total": 120},
                {"college_name": "工学院", "political_status": "共青团员", "total": 510},
                {"college_name": "工学院", "political_status": "中共党员", "total": 160},
            ],
            total_rows=4,
        ),
    )

    result = build_service(payload).query("按学院和政治面貌统计在校学生人数", database="wenshu_db")

    assert result.visualization is not None
    assert result.visualization["chart"]["kind"] == "heatmap"
