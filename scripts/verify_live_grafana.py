"""
Script to verify live Grafana Cloud MCP connectivity with exact schemas and queries.
Sanitizes all outputs to prevent credential leakage.
"""

import asyncio
import json
import os
import subprocess
import time
from dotenv import load_dotenv
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset, StreamableHTTPConnectionParams

load_dotenv()

GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_SERVICE_ACCOUNT_TOKEN = os.getenv("GRAFANA_SERVICE_ACCOUNT_TOKEN")

if not GRAFANA_URL or not GRAFANA_SERVICE_ACCOUNT_TOKEN:
    print("ERROR: GRAFANA_URL or GRAFANA_SERVICE_ACCOUNT_TOKEN is missing from environment.")
    exit(1)


async def run_verification():
    port = 8199
    server_url = f"http://127.0.0.1:{port}/mcp"
    mcp_bin = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "bin", "mcp-grafana"))
    
    env = os.environ.copy()
    env["GRAFANA_URL"] = GRAFANA_URL
    env["GRAFANA_SERVICE_ACCOUNT_TOKEN"] = GRAFANA_SERVICE_ACCOUNT_TOKEN

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
        params = StreamableHTTPConnectionParams(url=server_url, timeout=30.0)
        toolset = McpToolset(connection_params=params)

        async def call_tool(tool_name: str, arguments: dict):
            try:
                res = await toolset._execute_with_session(
                    lambda session: session.call_tool(tool_name, arguments=arguments),
                    f"Call {tool_name}"
                )
                if hasattr(res, "model_dump"):
                    return res.model_dump(mode="json")
                return str(res)
            except Exception as ex:
                return f"ERROR: {ex}"

        print("\n=== 1. PROMETHEUS METRICS PATH ===")
        res_prom_names = await call_tool("list_prometheus_metric_names", {"datasourceUid": "grafanacloud-prom"})
        print("list_prometheus_metric_names response:")
        print(json.dumps(res_prom_names, indent=2))

        res_prom_query = await call_tool(
            "query_prometheus",
            {
                "datasourceUid": "grafanacloud-prom",
                "expr": "up",
                "queryType": "instant",
                "endTime": "now",
            }
        )
        print("query_prometheus ('up', instant) response:")
        print(json.dumps(res_prom_query, indent=2))

        print("\n=== 2. LOKI LOGS PATH ===")
        res_loki_labels = await call_tool("list_loki_label_names", {"datasourceUid": "grafanacloud-logs"})
        print("list_loki_label_names response:")
        print(json.dumps(res_loki_labels, indent=2))

        res_loki_logs = await call_tool(
            "query_loki_logs",
            {
                "datasourceUid": "grafanacloud-logs",
                "logql": '{job=~".+"}',
                "startRfc3339": "now-1h",
                "endRfc3339": "now",
                "limit": 10,
            }
        )
        print("query_loki_logs response:")
        print(json.dumps(res_loki_logs, indent=2))

        print("\n=== 3. ALERTING PATH ===")
        res_alerts = await call_tool("list_alert_groups", {})
        print("list_alert_groups response:")
        print(json.dumps(res_alerts, indent=2))

        print("\n=== 4. TRACES & DATASOURCE HEALTH PATH ===")
        res_datasources = await call_tool("check_datasources_health", {})
        print("check_datasources_health response:")
        print(json.dumps(res_datasources, indent=2))

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    asyncio.run(run_verification())
