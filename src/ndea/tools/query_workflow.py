from ndea.context import RequestContext
from ndea.config import Settings
from ndea.orchestration import LangGraphQueryWorkflowService
from ndea.planning import QueryWorkflowService
from ndea.sql_advisor import VannaStyleSQLAdvisorService
from ndea.tools.query_executor import get_guarded_query_service
from ndea.tools.query_planner import get_query_planner_service
from ndea.tools.sql_rag import get_sql_rag_service


def get_query_workflow_service() -> QueryWorkflowService:
    settings = Settings()
    if settings.workflow_runtime.lower() == "langgraph":
        return LangGraphQueryWorkflowService(
            planner=get_query_planner_service(),
            advisor=VannaStyleSQLAdvisorService(
                settings=settings,
                sql_rag=get_sql_rag_service(),
            ),
            query_service=get_guarded_query_service(),
        )

    return QueryWorkflowService(
        planner=get_query_planner_service(),
        query_service=get_guarded_query_service(),
    )


def mcp_query_workflow(
    query_text: str,
    query_vector: list[float],
    database: str | None = None,
    execute: bool = False,
    request_context: RequestContext | dict[str, object] | None = None,
    policy_context: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = get_query_workflow_service().run(
        query_text=query_text,
        query_vector=query_vector,
        database=database,
        execute=execute,
        request_context=request_context,
        policy_context=policy_context,
    )
    if hasattr(payload, "model_dump"):
        return payload.model_dump()
    return payload
