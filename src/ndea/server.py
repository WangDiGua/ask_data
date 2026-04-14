from fastmcp import FastMCP

from ndea.config import Settings
from ndea.tools import register_tools


def create_mcp(settings: Settings | None = None) -> FastMCP:
    resolved = settings or Settings()
    mcp = FastMCP(resolved.app_name)
    register_tools(mcp)
    return mcp
