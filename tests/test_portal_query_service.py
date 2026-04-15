from ndea.planning.models import QueryPlanPayload, QueryWorkflowPayload
from ndea.portal.service import PortalQueryService
from ndea.protocol import TablePayload, TextPayload


class FakeWorkflowService:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def run(
        self,
        query_text: str,
        query_vector: list[float],
        database: str | None = None,
        execute: bool = False,
        request_context: dict[str, object] | None = None,
        policy_context: dict[str, object] | None = None,
    ):
        self.calls.append(
            {
                "query_text": query_text,
                "query_vector": query_vector,
                "database": database,
                "execute": execute,
                "request_context": request_context,
                "policy_context": policy_context,
            }
        )
        return self.payload


def test_portal_query_service_embeds_and_returns_visualization() -> None:
    workflow_payload = QueryWorkflowPayload(
        trace_id="trace-1",
        request_id="request-1",
        query_text="按政治面貌统计在校学生人数",
        database="wenshu_db",
        executed=True,
        tool_trace=["langgraph_planner", "sql_advisor", "langgraph_executor", "response_assembler"],
        plan=QueryPlanPayload(
            query_text="按政治面貌统计在校学生人数",
            intent_type="metric",
            summary="按政治面貌统计在校学生人数",
            clarification_required=False,
            confidence=0.94,
            metric_id="active_student_count",
            selected_sql_asset_id="golden_sql:active_students_by_political_status",
        ),
        response_text=TextPayload(summary="按政治面貌统计在校学生人数如下"),
        response_table=TablePayload(
            columns=["political_status", "total"],
            rows=[
                {"political_status": "共青团员", "total": 11637},
                {"political_status": "群众", "total": 5625},
            ],
            total_rows=2,
        ),
        execution={"effective_sql": "select political_status, count(*) as total from student group by political_status"},
    )
    workflow = FakeWorkflowService(workflow_payload)
    service = PortalQueryService(
        workflow_service=workflow,
        embedder=lambda base_url, model, texts: [[0.1, 0.2]],
    )

    payload = service.query(
        query_text="按政治面貌统计在校学生人数",
        database="wenshu_db",
        request_context={"actor_id": "user-1"},
    )

    assert payload.executed is True
    assert payload.text == "按政治面貌统计在校学生人数如下"
    assert payload.table is not None
    assert payload.table.columns[0].key == "political_status"
    assert payload.visualization is not None
    assert payload.visualization["type"] == "visualization"
    assert payload.visualization["chart"]["kind"] == "bar"
    assert payload.sql == "select political_status, count(*) as total from student group by political_status"
    assert payload.metadata.metric_id == "active_student_count"
    assert workflow.calls[0]["query_vector"] == [0.1, 0.2]
    assert workflow.calls[0]["request_context"] == {"actor_id": "user-1"}


def test_portal_query_service_preserves_clarification() -> None:
    workflow_payload = QueryWorkflowPayload(
        trace_id="trace-1",
        request_id="request-1",
        query_text="工号87024的所在单位和职称",
        database="wenshu_db",
        clarification_required=True,
        clarification_questions=["请拆开查询，或明确要查基本信息还是业务记录字段。"],
        executed=False,
        tool_trace=["langgraph_planner", "response_assembler"],
        plan=QueryPlanPayload(
            query_text="工号87024的所在单位和职称",
            intent_type="attribute_lookup",
            summary="该编号涉及的属性分布在不同表里，请拆开查询，或明确要查基本信息还是业务记录字段。",
            clarification_required=True,
            clarification_questions=["请拆开查询，或明确要查基本信息还是业务记录字段。"],
        ),
        response_text=TextPayload(
            summary="该编号涉及的属性分布在不同表里，请拆开查询，或明确要查基本信息还是业务记录字段。"
        ),
    )
    service = PortalQueryService(
        workflow_service=FakeWorkflowService(workflow_payload),
        embedder=lambda base_url, model, texts: [[0.1, 0.2]],
    )

    payload = service.query(query_text="工号87024的所在单位和职称", database="wenshu_db")

    assert payload.executed is False
    assert payload.clarification_required is True
    assert payload.clarification_question == "请拆开查询，或明确要查基本信息还是业务记录字段。"
    assert payload.visualization is None


def test_portal_query_service_returns_embedding_error_payload() -> None:
    service = PortalQueryService(
        workflow_service=FakeWorkflowService({}),
        embedder=lambda base_url, model, texts: (_ for _ in ()).throw(RuntimeError("embedding down")),
    )

    payload = service.query(query_text="按学院统计在校学生人数")

    assert payload.executed is False
    assert payload.table is None
    assert payload.visualization is None
    assert "Embedding failed" in payload.text
