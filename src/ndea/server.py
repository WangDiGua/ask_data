from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from ndea.config import Settings
from ndea.tools import register_legacy_tools, register_portal_tools, register_tools

try:
    from fastmcp import FastMCP
except Exception:
    class FastMCP:  # type: ignore[override]
        def __init__(self, name: str) -> None:
            self.name = name
            self._app = FastAPI(title=name)

        def tool(self, name: str):
            def decorator(func):
                self._app.get(f"/tools/{name}")(lambda: {"tool": name})
                return func

            return decorator

        def http_app(self, transport: str = "sse") -> FastAPI:
            return self._app


def create_mcp(settings: Settings | None = None) -> FastMCP:
    resolved = settings or Settings()
    mcp = FastMCP(resolved.app_name)
    try:
        register_tools(mcp)
    except Exception:
        pass
    if resolved.enable_legacy_tools:
        try:
            register_legacy_tools(mcp)
        except Exception:
            pass
    return mcp


def create_portal_mcp(settings: Settings | None = None) -> FastMCP:
    resolved = settings or Settings()
    mcp = FastMCP(f"{resolved.app_name} Portal")
    try:
        register_portal_tools(mcp)
    except Exception:
        pass
    return mcp
