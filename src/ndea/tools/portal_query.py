from __future__ import annotations

from ndea.portal import PortalQueryService


def get_portal_query_service() -> PortalQueryService:
    return PortalQueryService()


def ask_data_query(
    query_text: str,
    database: str | None = None,
    request_context: dict[str, object] | None = None,
    policy_context: dict[str, object] | None = None,
) -> dict[str, object]:
    """Execute a natural-language data query for portal clients.

    Use this tool for campus business data questions such as counts, grouped statistics,
    trends, ranking lists, roster lookups, and staff/student attribute queries.
    Do not guess database answers when this tool is available.
    """
    payload = get_portal_query_service().query(
        query_text=query_text,
        database=database,
        request_context=request_context,
        policy_context=policy_context,
    )
    return payload.model_dump(mode="json")
