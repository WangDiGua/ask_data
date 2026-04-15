from ndea.planning import QueryPlanPayload, QueryWorkflowPayload
from ndea.portal.service import PortalQueryService
from ndea.protocol import TablePayload, TextPayload


class FakeWorkflowService:
    def __init__(self, payload):
        self.payload = payload

    def run(self, **kwargs):
        return self.payload


def test_portal_query_service_exposes_answer_mode_metadata() -> None:
    payload = QueryWorkflowPayload(
        trace_id="trace-1",
        request_id="request-1",
        query_text="列出烟台研究院在岗教师名单",
        database="wenshu_db",
        executed=True,
        tool_trace=["langgraph_planner", "sql_generator", "langgraph_executor", "response_assembler"],
        plan=QueryPlanPayload(
            query_text="列出烟台研究院在岗教师名单",
            intent_type="roster",
            answer_mode="roster",
            summary="Resolved roster query",
            clarification_required=False,
            confidence=0.81,
            candidate_tables=["dcemp"],
            chosen_strategy="core_table_projection_registry",
            resolved_entities=[{"type": "table", "value": "dcemp", "label": "在岗教职工"}],
        ),
        response_text=TextPayload(summary="查询到2条教师名单", details="请查看数据表获取完整名单。"),
        response_table=TablePayload(
            columns=["staff_no", "name"],
            rows=[{"staff_no": "10001", "name": "张三"}],
            total_rows=1,
        ),
    )
    service = PortalQueryService(
        workflow_service=FakeWorkflowService(payload),
        embedder=lambda base_url, model, texts: [[0.1, 0.2]],
    )

    result = service.query("列出烟台研究院在岗教师名单")

    assert result.metadata.answer_mode == "roster"
    assert result.metadata.resolved_tables == ["dcemp"]
    assert result.metadata.resolved_entities == [{"type": "table", "value": "dcemp", "label": "在岗教职工"}]
    assert result.metadata.sql_strategy == "core_table_projection_registry"
    assert result.visualization is None
