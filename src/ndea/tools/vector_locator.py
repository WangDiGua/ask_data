from ndea.config import Settings
from ndea.vector import VectorLocatorService


def get_vector_locator_service() -> VectorLocatorService:
    settings = Settings()
    return VectorLocatorService(settings)


def mcp_vector_locator(
    query_text: str,
    query_vector: list[float],
    asset_types: list[str] | None = None,
    limit: int | None = None,
) -> dict[str, object]:
    payload = get_vector_locator_service().locate(
        query_text=query_text,
        query_vector=query_vector,
        asset_types=asset_types,
        limit=limit,
    )
    if hasattr(payload, "model_dump"):
        return payload.model_dump()
    return payload
