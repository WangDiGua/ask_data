from fastmcp import FastMCP

from ndea.config import Settings
from ndea.tools import register_portal_tools, register_tools


def create_mcp(settings: Settings | None = None) -> FastMCP:
    resolved = settings or Settings()
    mcp = FastMCP(resolved.app_name)
    register_tools(mcp)
    return mcp


def create_portal_mcp(settings: Settings | None = None) -> FastMCP:
    resolved = settings or Settings()
    mcp = FastMCP(f"{resolved.app_name} Portal")
    register_portal_tools(mcp)
    return mcp
