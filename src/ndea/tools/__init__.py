from fastmcp import FastMCP

from ndea.tools.db_inspector import inspect_table_schema
from ndea.tools.portal_query import ask_data_query
from ndea.tools.query_planner import mcp_query_planner
from ndea.tools.query_executor import execute_guarded_query
from ndea.tools.query_workflow import mcp_query_workflow
from ndea.tools.sql_rag import mcp_sql_rag_engine
from ndea.tools.system import system_status
from ndea.tools.vector_locator import mcp_vector_locator


def register_tools(mcp: FastMCP) -> None:
    mcp.tool(name="execute_guarded_query")(execute_guarded_query)
    mcp.tool(name="inspect_table_schema")(inspect_table_schema)
    mcp.tool(name="mcp_query_planner")(mcp_query_planner)
    mcp.tool(name="mcp_query_workflow")(mcp_query_workflow)
    mcp.tool(name="mcp_sql_rag_engine")(mcp_sql_rag_engine)
    mcp.tool(name="mcp_vector_locator")(mcp_vector_locator)
    mcp.tool(name="system_status")(system_status)


def register_portal_tools(mcp: FastMCP) -> None:
    mcp.tool(name="ask_data_query")(ask_data_query)
    mcp.tool(name="system_status")(system_status)
