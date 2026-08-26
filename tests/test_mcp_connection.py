"""
Tests for Callsheet MCP Client and connection to Grafana MCP server.
Includes live round-trip tests against Grafana Cloud when credentials are configured.
"""

import os
import subprocess
import time
import pytest
from dotenv import load_dotenv
from callsheet.mcp.client import (
    create_grafana_mcp_toolset,
    get_grafana_mcp_connection_params,
)

load_dotenv()


def test_connection_params_defaults():
    """Verify default connection parameters."""
    params = get_grafana_mcp_connection_params()
    assert params.url == "http://127.0.0.1:8000/mcp"
    assert params.timeout == 30.0


def test_connection_params_with_env(monkeypatch):
    """Verify connection parameters respect environment variables."""
    monkeypatch.setenv("GRAFANA_MCP_SERVER_URL", "http://localhost:9090/mcp")
    monkeypatch.setenv("GRAFANA_URL", "https://example.grafana.net")
    monkeypatch.setenv("GRAFANA_SERVICE_ACCOUNT_TOKEN", "glsa_test_token")

    params = get_grafana_mcp_connection_params()
    assert params.url == "http://localhost:9090/mcp"
    assert params.headers["X-Grafana-URL"] == "https://example.grafana.net"
    assert params.headers["Authorization"] == "Bearer glsa_test_token"


def test_mcp_toolset_instantiation():
    """Verify McpToolset instantiates cleanly with connection params."""
    params = get_grafana_mcp_connection_params()
    toolset = create_grafana_mcp_toolset(params)
    assert toolset is not None
    assert toolset.connection_params.url == "http://127.0.0.1:8000/mcp"


@pytest.mark.asyncio
async def test_mcp_server_live_roundtrip_against_grafana_cloud():
    """
    Spawns mcp-grafana binary in streamable-http mode pointing at real Grafana Cloud,
    connects ADK McpToolset, and performs real round-trip queries for Prometheus, Loki,
    Alerts, and Datasource health.
    """
    grafana_url = os.getenv("GRAFANA_URL")
    sa_token = os.getenv("GRAFANA_SERVICE_ACCOUNT_TOKEN")

    if not grafana_url or not sa_token:
        pytest.skip("GRAFANA_URL or GRAFANA_SERVICE_ACCOUNT_TOKEN missing from environment")

    mcp_bin = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "bin", "mcp-grafana"))
    if not os.path.exists(mcp_bin):
        pytest.skip(f"mcp-grafana binary not found at {mcp_bin}")

    port = 8124
    server_url = f"http://127.0.0.1:{port}/mcp"
    env = os.environ.copy()
    env["GRAFANA_URL"] = grafana_url
    env["GRAFANA_SERVICE_ACCOUNT_TOKEN"] = sa_token

    cmd = [
        mcp_bin,
        "-t", "streamable-http",
        "-address", f"127.0.0.1:{port}",
        "-endpoint-path", "/mcp",
        "-log-level", "info",
    ]

    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        time.sleep(2)
        assert proc.poll() is None, f"mcp-grafana process exited early: {proc.stderr.read()}"

        params = get_grafana_mcp_connection_params(
            mcp_server_url=server_url,
            grafana_url=grafana_url,
            service_account_token=sa_token,
        )
        toolset = create_grafana_mcp_toolset(params)

        # 1. Discover tools
        tools = await toolset.get_tools()
        assert len(tools) > 0, "No MCP tools discovered from mcp-grafana"
        tool_names = [t.name for t in tools]
        assert "query_prometheus" in tool_names
        assert "query_loki_logs" in tool_names
        assert "list_alert_groups" in tool_names

        # 2. Real round trip: Datasource Health Check
        res_ds = await toolset._execute_with_session(
            lambda s: s.call_tool("check_datasources_health", {}),
            "Call check_datasources_health"
        )
        assert res_ds.isError is False
        assert len(res_ds.content) > 0

        # 3. Real round trip: Prometheus Metrics (instant query)
        res_prom = await toolset._execute_with_session(
            lambda s: s.call_tool(
                "query_prometheus",
                {
                    "datasourceUid": "grafanacloud-prom",
                    "expr": "up",
                    "queryType": "instant",
                    "endTime": "now",
                }
            ),
            "Call query_prometheus"
        )
        assert res_prom.isError is False
        assert len(res_prom.content) > 0

        # 4. Real round trip: Loki Logs
        res_loki = await toolset._execute_with_session(
            lambda s: s.call_tool(
                "query_loki_logs",
                {
                    "datasourceUid": "grafanacloud-logs",
                    "logql": '{job=~".+"}',
                    "startRfc3339": "now-1h",
                    "endRfc3339": "now",
                    "limit": 10,
                }
            ),
            "Call query_loki_logs"
        )
        assert res_loki.isError is False
        assert len(res_loki.content) > 0

        # 5. Real round trip: Alerting Groups
        res_alerts = await toolset._execute_with_session(
            lambda s: s.call_tool("list_alert_groups", {}),
            "Call list_alert_groups"
        )
        assert res_alerts.isError is False
        assert len(res_alerts.content) > 0

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()
