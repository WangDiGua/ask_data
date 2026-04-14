from ndea.config import Settings
from ndea.vector import SQLRAGService


def get_sql_rag_service() -> SQLRAGService:
    settings = Settings()
    return SQLRAGService(settings)


def mcp_sql_rag_engine(
    query_text: str,
    query_vector: list[float],
    limit: int | None = None,
) -> dict[str, object]:
    payload = get_sql_rag_service().retrieve(
        query_text=query_text,
        query_vector=query_vector,
        limit=limit,
    )
    if hasattr(payload, "model_dump"):
        return payload.model_dump()
    return payload
