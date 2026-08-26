"""
Callsheet MCP Client module.
Configures and initializes Google ADK McpToolset connections to Grafana Cloud MCP.
"""

from typing import Any, Optional
import os
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset, StreamableHTTPConnectionParams


def get_grafana_mcp_connection_params(
    mcp_server_url: Optional[str] = None,
    grafana_url: Optional[str] = None,
    service_account_token: Optional[str] = None,
    server_auth_token: Optional[str] = None,
    timeout: float = 30.0,
) -> StreamableHTTPConnectionParams:
    """
    Builds StreamableHTTPConnectionParams for connecting ADK McpToolset to Grafana MCP server.
    """
    url = mcp_server_url or os.getenv("GRAFANA_MCP_SERVER_URL", "http://127.0.0.1:8000/mcp")
    g_url = grafana_url or os.getenv("GRAFANA_URL", "")
    sa_token = service_account_token or os.getenv("GRAFANA_SERVICE_ACCOUNT_TOKEN", "")
    s_token = server_auth_token or os.getenv("MCP_GRAFANA_SERVER_TOKEN", "")

    headers: dict[str, str] = {}
    if g_url:
        headers["X-Grafana-URL"] = g_url
    if sa_token:
        headers["Authorization"] = f"Bearer {sa_token}"
    elif s_token:
        headers["Authorization"] = f"Bearer {s_token}"

    return StreamableHTTPConnectionParams(
        url=url,
        headers=headers,
        timeout=timeout,
    )


def create_grafana_mcp_toolset(
    connection_params: Optional[StreamableHTTPConnectionParams] = None,
    tool_filter: Optional[list[str]] = None,
) -> McpToolset:
    """
    Creates an initialized ADK McpToolset instance connected to Grafana Cloud MCP.
    """
    params = connection_params or get_grafana_mcp_connection_params()
    return McpToolset(
        connection_params=params,
        tool_filter=tool_filter,
    )
