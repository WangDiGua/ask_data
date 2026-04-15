from __future__ import annotations

import json
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel
from sse_starlette import EventSourceResponse

from ndea.config import Settings
from ndea.observability import get_health_service
from ndea.server import create_mcp, create_portal_mcp
from ndea.tools.query_workflow import get_query_workflow_service


class QueryWorkflowRequest(BaseModel):
    query_text: str
    query_vector: list[float]
    database: str | None = None
    execute: bool = False
    request_context: dict[str, Any] | None = None
    policy_context: dict[str, Any] | None = None


def create_http_app(
    settings: Settings | None = None,
    workflow_service: Any | None = None,
    health_service: Any | None = None,
) -> FastAPI:
    resolved = settings or Settings()
    app = FastAPI(title=resolved.app_name)

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

    @app.post("/api/query-workflow")
    def query_workflow(request: QueryWorkflowRequest) -> dict[str, Any]:
        payload = (workflow_service or get_query_workflow_service()).run(
            query_text=request.query_text,
            query_vector=request.query_vector,
            database=request.database,
            execute=request.execute,
            request_context=request.request_context,
            policy_context=request.policy_context,
        )
        if hasattr(payload, "model_dump"):
            return payload.model_dump(mode="json")
        return payload

    @app.post("/api/query-workflow/stream")
    def query_workflow_stream(request: QueryWorkflowRequest) -> EventSourceResponse:
        def event_generator():
            for event in (workflow_service or get_query_workflow_service()).stream(
                query_text=request.query_text,
                query_vector=request.query_vector,
                database=request.database,
                execute=request.execute,
                request_context=request.request_context,
                policy_context=request.policy_context,
            ):
                yield {
                    "event": "workflow",
                    "data": json.dumps(event, ensure_ascii=False, separators=(",", ":")),
                }

        return EventSourceResponse(event_generator())

    app.mount("/mcp", create_mcp(resolved).http_app(transport="sse"))
    app.mount("/mcp-portal", create_portal_mcp(resolved).http_app(transport="sse"))
    return app
