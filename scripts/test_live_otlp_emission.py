"""
Test script emitting live OTLP telemetry (metrics, logs, traces) to Grafana Cloud,
then querying Grafana Cloud via MCP to verify the data arrived and is queryable.
"""

import asyncio
import json
import os
import subprocess
import time
from dotenv import load_dotenv

from callsheet.farm.emitter import FarmTelemetryEmitter
from callsheet.farm.models import ScenarioType
from callsheet.farm.simulator import RenderFarmSimulator
from callsheet.mcp.client import create_grafana_mcp_toolset, get_grafana_mcp_connection_params

load_dotenv()


async def run_live_otlp_test():
    print("=== 1. INITIALIZING SIMULATOR & OTLP EMITTER ===")
    sim = RenderFarmSimulator()
    emitter = FarmTelemetryEmitter(sim)

    print("Emitting baseline metrics tick...")
    emitter.emit_metrics_tick()

    print("Injecting thermal throttling scenario on node-07...")
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    emitter.emit_metrics_tick()

    print("Emitting logs and traces for frame events...")
    events = sim.tick(delta_seconds=30.0)
    emitter.process_events(events)

    # Emit an explicit log record
    emitter.emit_log(
        "CRITICAL: Thermal junction temperature on node-07 reached 94.5C. Render task for shot 118 throttled.",
        level="WARN",
        node_id="node-07",
        shot_code="118",
        show_id="show-dune",
        frame_number=1025,
    )

    # Force flush providers
    print("Flushing OTLP exporters to Grafana Cloud...")
    emitter.metric_reader.force_flush()
    emitter.logger_provider.force_flush()
    emitter.tracer_provider.force_flush()
    print("Flushed successfully.")

    print("\nWaiting 10 seconds for Grafana Cloud ingestion indexing...")
    time.sleep(10)

    print("\n=== 2. QUERYING GRAFANA CLOUD VIA MCP ===")
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

        # 1. Query metric names
        print("\n--- A. PROMETHEUS METRIC NAMES IN GRAFANA CLOUD ---")
        res_names = await call_tool(
            "list_prometheus_metric_names",
            {"datasourceUid": "grafanacloud-prom", "regex": ".*render_farm.*"}
        )
        print(json.dumps(res_names, indent=2))

        # 2. Query metric values
        print("\n--- B. QUERY RENDER NODE TEMPERATURE METRIC ---")
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

        # 3. Query Loki labels
        print("\n--- C. LOKI LOG LABELS IN GRAFANA CLOUD ---")
        res_loki_labels = await call_tool(
            "list_loki_label_names",
            {"datasourceUid": "grafanacloud-logs"}
        )
        print(json.dumps(res_loki_labels, indent=2))

        # 4. Query Loki logs
        print("\n--- D. QUERY LOKI LOGS FOR NODE-07 ---")
        res_loki_query = await call_tool(
            "query_loki_logs",
            {
                "datasourceUid": "grafanacloud-logs",
                "logql": '{service_name="render-farm"}',
                "startRfc3339": "now-15m",
                "endRfc3339": "now",
                "limit": 10,
            }
        )
        print(json.dumps(res_loki_query, indent=2))

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    asyncio.run(run_live_otlp_test())
