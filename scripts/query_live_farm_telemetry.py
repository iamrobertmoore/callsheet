"""
Script to query newly emitted farm metrics from Grafana Cloud via MCP.
"""

import asyncio
import json
import os
import subprocess
import time
from dotenv import load_dotenv
from callsheet.mcp.client import create_grafana_mcp_toolset, get_grafana_mcp_connection_params

load_dotenv()


async def query_live_farm_telemetry():
    port = 8199
    server_url = f"http://127.0.0.1:{port}/mcp"
    mcp_bin = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "bin", "mcp-grafana"))
    
    env = os.environ.copy()
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
        params = get_grafana_mcp_connection_params(mcp_server_url=server_url)
        toolset = create_grafana_mcp_toolset(params)

        async def call_tool(tool_name: str, arguments: dict):
            res = await toolset._execute_with_session(
                lambda session: session.call_tool(tool_name, arguments=arguments),
                f"Call {tool_name}"
            )
            return res.model_dump(mode="json")

        print("=== 1. PROMETHEUS TEMPERATURE METRIC VALUES ===")
        res_temp = await call_tool(
            "query_prometheus",
            {
                "datasourceUid": "grafanacloud-prom",
                "expr": "render_farm_node_temperature_celsius",
                "queryType": "instant",
                "endTime": "now",
            }
        )
        print(json.dumps(res_temp, indent=2))

        print("\n=== 2. LOKI LOG ENTRIES FOR RENDER FARM ===")
        res_logs = await call_tool(
            "query_loki_logs",
            {
                "datasourceUid": "grafanacloud-logs",
                "logql": '{service_name="render-farm"}',
                "startRfc3339": "now-15m",
                "endRfc3339": "now",
                "limit": 5,
            }
        )
        print(json.dumps(res_logs, indent=2))

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    asyncio.run(query_live_farm_telemetry())
