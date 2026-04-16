from __future__ import annotations

from typing import Any, Callable

from ndea.config import Settings
from ndea.tools import register_tools

try:
    from fastmcp import FastMCP
except Exception:
    class FastMCP:  # type: ignore[override]
        def __init__(self, name: str) -> None:
            self.name = name
            self.tools: dict[str, Callable[..., Any]] = {}

        def tool(self, name: str):
            def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                self.tools[name] = func
                return func

            return decorator


def create_mcp(settings: Settings | None = None) -> FastMCP:
    resolved = settings or Settings()
    mcp = FastMCP(resolved.app_name)
    try:
        register_tools(mcp)
    except Exception:
        pass
    return mcp
