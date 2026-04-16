from __future__ import annotations

from typing import Any

from ndea.query_v2 import QueryRequestV2


def get_query_service_v2():
    from ndea.config import Settings
    from ndea.services import QueryServiceV2

    return QueryServiceV2(settings=Settings())


def mcp_query_v2(
    query_text: str,
    database: str | None = None,
    request_context: dict[str, object] | None = None,
    policy_context: dict[str, object] | None = None,
    options: dict[str, Any] | None = None,
) -> dict[str, object]:
    payload = get_query_service_v2().run(
        QueryRequestV2(
            query_text=query_text,
            database=database,
            request_context=request_context,
            policy_context=policy_context,
            options=options or {},
        )
    )
    return payload.model_dump(mode="json")
