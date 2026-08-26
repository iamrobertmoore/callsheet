"""
Tests for Callsheet MCP Client and connection to Grafana MCP server.
"""

import os
import subprocess
import time
import pytest
from callsheet.mcp.client import (
    create_grafana_mcp_toolset,
    get_grafana_mcp_connection_params,
)


def test_connection_params_defaults():
    """Verify default connection parameters."""
    params = get_grafana_mcp_connection_params()
    assert params.url == "http://127.0.0.1:8000/mcp"
    assert params.timeout == 30.0


def test_connection_params_with_env(monkeypatch):
    """Verify connection parameters respect environment variables."""
    monkeypatch.setenv("GRAFANA_MCP_SERVER_URL", "http://localhost:9090/mcp")
    monkeypatch.setenv("GRAFANA_URL", "https://example.grafana.net")
    monkeypatch.setenv("GRAFANA_SERVICE_ACCOUNT_TOKEN", "glsa_mock_token_123")

    params = get_grafana_mcp_connection_params()
    assert params.url == "http://localhost:9090/mcp"
    assert params.headers["X-Grafana-URL"] == "https://example.grafana.net"
    assert params.headers["Authorization"] == "Bearer glsa_mock_token_123"


def test_mcp_toolset_instantiation():
    """Verify McpToolset instantiates cleanly with connection params."""
    params = get_grafana_mcp_connection_params()
    toolset = create_grafana_mcp_toolset(params)
    assert toolset is not None
    assert toolset.connection_params.url == "http://127.0.0.1:8000/mcp"


@pytest.mark.asyncio
async def test_mcp_server_live_handshake_and_tool_discovery():
    """
    Spawns mcp-grafana binary in streamable-http mode,
    connects ADK McpToolset, and verifies tool discovery.
    """
    mcp_bin = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "bin", "mcp-grafana"))
    if not os.path.exists(mcp_bin):
        pytest.skip(f"mcp-grafana binary not found at {mcp_bin}")

    port = 8123
    server_url = f"http://127.0.0.1:{port}/mcp"
    env = os.environ.copy()
    env["GRAFANA_URL"] = "http://localhost:3000"
    env["GRAFANA_SERVICE_ACCOUNT_TOKEN"] = "mock_test_token"

    cmd = [
        mcp_bin,
        "-t", "streamable-http",
        "-address", f"127.0.0.1:{port}",
        "-endpoint-path", "/mcp",
        "-log-level", "debug",
    ]

    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Wait up to 5 seconds for server to start
        time.sleep(1.5)
        assert proc.poll() is None, f"mcp-grafana process exited early: {proc.stderr.read()}"

        params = get_grafana_mcp_connection_params(
            mcp_server_url=server_url,
            grafana_url="http://localhost:3000",
            service_account_token="mock_test_token",
        )
        toolset = create_grafana_mcp_toolset(params)

        # Query discovered tools from the MCP server
        tools = await toolset.get_tools()
        assert len(tools) > 0, "Expected tools to be discovered from mcp-grafana server"

        tool_names = [t.name for t in tools]
        print(f"\nDiscovered {len(tools)} MCP tools: {tool_names[:10]}...")

        # Verify key observability tools are present in the server's tool catalog
        expected_tool_subnames = ["prometheus", "loki", "alert", "dashboard"]
        found_signals = {}
        for expected in expected_tool_subnames:
            matches = [name for name in tool_names if expected in name.lower()]
            found_signals[expected] = matches

        print("Found observability signals:", found_signals)
        assert any("prometheus" in name.lower() for name in tool_names), "Prometheus metrics tool missing"
        assert any("loki" in name.lower() for name in tool_names), "Loki logs tool missing"

    finally:
        proc.terminate()
        proc.wait(timeout=5)
