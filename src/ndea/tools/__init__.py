from __future__ import annotations

from typing import Any


def register_tools(mcp: Any) -> None:
    from ndea.tools.db_inspector import inspect_table_schema
    from ndea.tools.query_executor import execute_guarded_query
    from ndea.tools.query_v2 import mcp_query_v2
    from ndea.tools.system import system_status

    mcp.tool(name="mcp_query_v2")(mcp_query_v2)
    mcp.tool(name="execute_guarded_query")(execute_guarded_query)
    mcp.tool(name="inspect_table_schema")(inspect_table_schema)
    mcp.tool(name="system_status")(system_status)
