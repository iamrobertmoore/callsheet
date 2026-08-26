"""
Verification script: Emits both a healthy frame (20s) and a throttled frame (120s),
then queries Grafana Cloud Tempo back via MCP and prints both span breakdowns side by side.
"""

import asyncio
import json
import os
import subprocess
import time
from dotenv import load_dotenv

from callsheet.farm.emitter import FarmTelemetryEmitter
from callsheet.farm.simulator import RenderFarmSimulator
from callsheet.mcp.client import create_grafana_mcp_toolset, get_grafana_mcp_connection_params

load_dotenv()


async def verify_healthy_vs_throttled_traces():
    print("=== 1. EMITTING HEALTHY (20s) & THROTTLED (120s) TRACES TO TEMPO ===")
    sim = RenderFarmSimulator()
    emitter = FarmTelemetryEmitter(sim)

    # 1. Healthy frame on node-06 (20s)
    emitter.emit_frame_trace(
        node_id="node-06",
        shot_code="212",
        show_name="Solar Flare: Redux",
        frame_number=1012,
        duration_seconds=20.0,
        is_throttled=False,
    )

    # 2. Throttled frame on node-07 (120s)
    emitter.emit_frame_trace(
        node_id="node-07",
        shot_code="118",
        show_name="Chronicles of Aethelgard: Episode 6",
        frame_number=1026,
        duration_seconds=120.0,
        is_throttled=True,
    )

    # Flush spans to Grafana Cloud OTLP endpoint
    emitter.tracer_provider.force_flush()
    print("Flushed traces to Grafana Cloud. Waiting 8s for Tempo indexing...")
    time.sleep(8)

    print("\n=== 2. QUERYING TEMPO VIA MCP ===")
    port = 8175
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

        now_epoch = int(time.time())
        start_epoch = now_epoch - 3600

        # Search traces in the window
        endpoint = f"/api/datasources/proxy/uid/grafanacloud-traces/api/search?start={start_epoch}&end={now_epoch}&limit=10"
        search_res = await toolset._execute_with_session(
            lambda s: s.call_tool("grafana_api_request", arguments={"endpoint": endpoint}),
            "grafana_api_request"
        )
        data = json.loads(search_res.content[0].text).get("data", {})
        traces = data.get("traces", [])
        print(f"Found {len(traces)} traces in Tempo.")

        # Find healthy trace and throttled trace
        for tr in traces:
            trace_id = tr["traceID"]
            root_name = tr["rootTraceName"]
            dur_ms = tr["durationMs"]
            
            # Fetch trace detail
            detail_endpoint = f"/api/datasources/proxy/uid/grafanacloud-traces/api/traces/{trace_id}"
            det_res = await toolset._execute_with_session(
                lambda s: s.call_tool("grafana_api_request", arguments={"endpoint": detail_endpoint}),
                "grafana_api_request"
            )
            det_data = json.loads(det_res.content[0].text).get("data", {})
            
            # Parse spans
            spans = []
            for b in det_data.get("batches", []):
                for scope in b.get("scopeSpans", []):
                    for s in scope.get("spans", []):
                        s_name = s.get("name")
                        s_start = int(s.get("startTimeUnixNano", 0))
                        s_end = int(s.get("endTimeUnixNano", 0))
                        dur_s = (s_end - s_start) / 1e9 if s_end > s_start else 0.0
                        attrs = {a["key"]: a.get("value", {}) for a in s.get("attributes", [])}
                        node = attrs.get("node_id", {}).get("stringValue") or attrs.get("node.id", {}).get("stringValue")
                        spans.append({"name": s_name, "duration_s": dur_s, "node": node})

            print(f"\nTrace: {root_name} (Total: {dur_ms}ms, TraceID: {trace_id})")
            for sp in spans:
                print(f"   -> Span: {sp['name']:<30} | Duration: {sp['duration_s']:>6.1f}s | Node: {sp['node']}")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    asyncio.run(verify_healthy_vs_throttled_traces())
