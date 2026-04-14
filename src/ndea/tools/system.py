from ndea.config import Settings
from ndea.observability import get_health_service


def system_status() -> dict[str, object]:
    payload = get_health_service(Settings()).system_status()
    if hasattr(payload, "model_dump"):
        return payload.model_dump()
    return payload
