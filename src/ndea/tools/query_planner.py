from ndea.context import RequestContext
from ndea.config import Settings
from ndea.planning import QueryPlannerService
from ndea.tools.sql_rag import get_sql_rag_service
from ndea.tools.vector_locator import get_vector_locator_service


def get_query_planner_service() -> QueryPlannerService:
    Settings()
    return QueryPlannerService(
        vector_locator=get_vector_locator_service(),
        sql_rag=get_sql_rag_service(),
    )


def mcp_query_planner(
    query_text: str,
    query_vector: list[float],
    request_context: RequestContext | dict[str, object] | None = None,
) -> dict[str, object]:
    planner = get_query_planner_service()
    try:
        if request_context is None:
            payload = planner.plan(query_text=query_text, query_vector=query_vector)
        else:
            payload = planner.plan(
                query_text=query_text,
                query_vector=query_vector,
                request_context=request_context,
            )
    except TypeError as exc:
        if "request_context" not in str(exc):
            raise
        payload = planner.plan(query_text=query_text, query_vector=query_vector)
    if hasattr(payload, "model_dump"):
        return payload.model_dump()
    return payload
