from __future__ import annotations

from typing import Any

from ndea.config import Settings


class LangfuseTracer:
    def __init__(self, settings: Settings) -> None:
        self._enabled = bool(settings.langfuse_public_key and settings.langfuse_secret_key)
        self._client = None
        if self._enabled:
            try:
                from langfuse import Langfuse

                self._client = Langfuse(
                    public_key=settings.langfuse_public_key,
                    secret_key=settings.langfuse_secret_key,
                    host=settings.langfuse_host,
                )
            except Exception:
                self._client = None

    def start_trace(self, name: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        trace: dict[str, Any] = {"enabled": self._enabled, "name": name, "metadata": metadata or {}}
        if self._client is not None:
            try:
                trace["client_trace"] = self._client.trace(name=name, metadata=metadata or {})
            except Exception:
                trace["client_trace"] = None
        return trace

    def record_node(self, trace: dict[str, Any], node: str, payload: dict[str, Any]) -> None:
        trace.setdefault("nodes", []).append({"node": node, "payload": payload})
        client_trace = trace.get("client_trace")
        if client_trace is not None:
            try:
                if hasattr(client_trace, "event"):
                    client_trace.event(name=node, output=payload)
            except Exception:
                pass

    def finish_trace(self, trace: dict[str, Any], output: dict[str, Any]) -> dict[str, Any]:
        trace["output"] = output
        client_trace = trace.get("client_trace")
        if client_trace is not None:
            try:
                if hasattr(client_trace, "update"):
                    client_trace.update(output=output)
            except Exception:
                pass
        return trace
