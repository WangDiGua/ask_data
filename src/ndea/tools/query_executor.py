from __future__ import annotations

from ndea.context import RequestContext
from ndea.config import Settings


def get_guarded_query_service():
    from ndea.security.mysql_safe_execution import MySQLGuardedQueryService

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
