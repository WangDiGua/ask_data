from ndea.context import RequestContext
from ndea.config import Settings
from ndea.security import MySQLGuardedQueryService


def get_guarded_query_service() -> MySQLGuardedQueryService:
    settings = Settings()
    return MySQLGuardedQueryService(settings)


def execute_guarded_query(
    database: str,
    sql: str,
    request_context: RequestContext | dict[str, object] | None = None,
    policy_context: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = get_guarded_query_service().execute_query(
        database,
        sql,
        request_context=request_context,
        policy_context=policy_context,
    )
    if hasattr(payload, "model_dump"):
        return payload.model_dump()
    return payload
