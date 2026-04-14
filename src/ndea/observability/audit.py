from __future__ import annotations

import json
import logging
from typing import Any, Protocol

from pydantic import BaseModel, Field


class AuditEvent(BaseModel):
    audit_id: str
    trace_id: str
    request_id: str
    actor_id: str | None = None
    tenant_id: str | None = None
    query_text: str
    intent_type: str
    tool_trace: list[str] = Field(default_factory=list)
    selected_sql: str | None = None
    effective_sql: str | None = None
    sql_attempts: list[dict[str, Any]] = Field(default_factory=list)
    permission_actions: dict[str, Any] = Field(default_factory=dict)
    degraded: bool = False
    final_status: str = "unknown"
    error_code: str | None = None
    latency_ms: int = 0
    dependency_status: dict[str, str] = Field(default_factory=dict)


class AuditSink(Protocol):
    def emit(self, event: AuditEvent) -> None:
        ...


class StructuredLoggerAuditSink:
    def __init__(self, writer=None, logger: logging.Logger | None = None) -> None:
        self._writer = writer
        self._logger = logger or logging.getLogger("ndea.audit")

    def emit(self, event: AuditEvent) -> None:
        payload = json.dumps(event.model_dump(), separators=(",", ":"), ensure_ascii=True)
        if self._writer is not None:
            self._writer(payload)
            return
        self._logger.info(payload)
