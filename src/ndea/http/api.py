from __future__ import annotations

import json
from typing import Any

from fastapi import FastAPI
from starlette.responses import StreamingResponse

from ndea.config import Settings
from ndea.observability import get_health_service
from ndea.query_v2 import QueryRequestV2
from ndea.server import create_mcp, create_portal_mcp

try:
    from sse_starlette import EventSourceResponse
except Exception:
    class EventSourceResponse(StreamingResponse):
        def __init__(self, content, *args, **kwargs):
            def encode():
                for item in content:
                    yield f"event: {item['event']}\ndata: {item['data']}\n\n".encode("utf-8")

            super().__init__(encode(), media_type="text/event-stream", *args, **kwargs)


def create_http_app(
    settings: Settings | None = None,
    query_service: Any | None = None,
    health_service: Any | None = None,
) -> FastAPI:
    resolved = settings or Settings()
    app = FastAPI(title=resolved.app_name)

    def get_query_service() -> Any:
        if query_service is not None:
            return query_service
        from ndea.services import QueryServiceV2

        return QueryServiceV2(settings=resolved)

    @app.get("/health/liveness")
    def liveness() -> dict[str, Any]:
        payload = (health_service or get_health_service(resolved)).liveness()
        if hasattr(payload, "model_dump"):
            return payload.model_dump()
        return payload

    @app.get("/health/readiness")
    def readiness() -> dict[str, Any]:
        payload = (health_service or get_health_service(resolved)).readiness()
        if hasattr(payload, "model_dump"):
            return payload.model_dump()
        return payload

    @app.post("/api/v2/query")
    def query_v2(request: QueryRequestV2) -> dict[str, Any]:
        payload = get_query_service().run(request)
        if hasattr(payload, "model_dump"):
            return payload.model_dump(mode="json")
        return payload

    @app.post("/api/v2/query/stream")
    def query_v2_stream(request: QueryRequestV2) -> EventSourceResponse:
        def event_generator():
            for event in get_query_service().stream(request):
                event_name = event.get("node") if event.get("type") == "node" else "final"
                yield {
                    "event": event_name,
                    "data": json.dumps(event, ensure_ascii=False, separators=(",", ":")),
                }

        return EventSourceResponse(event_generator())

    app.mount("/mcp", create_mcp(resolved).http_app(transport="sse"))
    app.mount("/mcp-portal", create_portal_mcp(resolved).http_app(transport="sse"))
    return app
