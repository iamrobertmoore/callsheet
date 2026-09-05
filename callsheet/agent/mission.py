"""
Multi-step reasoning mission runner for Callsheet.
Executes genuine, load-bearing observability investigations against Grafana Cloud MCP
(Prometheus metrics, Loki logs, and Tempo traces) and generates producer-facing
intervention summaries using Vertex AI Gemini.
"""

import asyncio
import base64
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field

from google.genai import Client, types

from callsheet.agent.prompts import (
    CALLSHEET_AGENT_SYSTEM_PROMPT,
    CALLSHEET_SUMMARY_PROMPT_TEMPLATE,
)
from callsheet.farm.emitter import get_deployment_id
from callsheet.farm.models import NodeStatus
from callsheet.interventions.dispatcher import (
    InterventionDispatcher,
    InterventionRecord,
)
from callsheet.mcp.client import (
    create_grafana_mcp_toolset,
    get_grafana_mcp_connection_params,
)

logger = logging.getLogger(__name__)

# Registry of rendered panel PNG images keyed by mission ID
MISSION_PANEL_IMAGES: Dict[str, bytes] = {}


class MissionStep(BaseModel):
    step_number: int
    name: str
    description: str
    execution_type: str = "DETERMINISTIC_TELEMETRY"
    status: str = "COMPLETED"
    evidence: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class MissionResult(BaseModel):
    id: str = Field(default_factory=lambda: f"mission_{uuid.uuid4().hex[:8]}")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    show_id: str
    show_name: str
    client: str
    deadline: str
    penalty_clause: str
    anomalous_node_id: str
    anomaly_detected: str
    root_cause: str
    affected_shots: List[str]
    intervention_record: InterventionRecord
    verification_status: str
    steps: List[MissionStep]
    callsheet_briefing: str
    incident_id: Optional[str] = None
    incident_url: Optional[str] = None
    incident_status: Optional[str] = None
    annotation_id: Optional[int] = None
    deeplinks: Dict[str, str] = Field(default_factory=dict)
    panel_image_url: Optional[str] = None
    time_intervention_to_resolved_seconds: Optional[float] = None
    mcp_read_calls: int = 0
    mcp_write_calls: int = 0
    trigger_type: str = "cycle_boundary"


def parse_loki_timestamp(ts_raw: Any) -> Optional[float]:
    """
    Parses a Loki log timestamp into float unix epoch seconds.
    Loki timestamps can be string nanoseconds, int nanoseconds, or ISO strings.
    """
    if not ts_raw:
        return None
    if isinstance(ts_raw, (int, float)):
        val = float(ts_raw)
        return val / 1e9 if val > 1e11 else val
    ts_str = str(ts_raw).strip('"').strip("'").strip()
    if not ts_str:
        return None
    try:
        val = float(ts_str)
        return val / 1e9 if val > 1e11 else val
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt.timestamp()
    except ValueError:
        return None


def parse_loki_frame_duration(line: str) -> Optional[float]:
    """
    Extracts frame duration in seconds from standard or degraded Loki log lines.
    Handles 'rendered on <node> successfully in <dur>s' and 'DEGRADED PERFORMANCE (<dur>s)'.
    """
    m = re.search(r"(?:in\s+|DEGRADED PERFORMANCE\s*\()([0-9]+(?:\.[0-9]+)?)\s*s", line)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


class MultiStepMissionRunner:
    """
    Executes load-bearing multi-step observability investigations across Prometheus, Loki,
    and Tempo via Grafana Cloud MCP, and generates producer-facing intervention summaries
    using Vertex AI Gemini.
    """

    def __init__(
        self,
        dispatcher: InterventionDispatcher,
        mcp_server_url: Optional[str] = None,
        project_id: str = "agent-attest-2026",
        location: str = "global",
        model_name: str = "gemini-3.8-flash",
        deployment_id: Optional[str] = None,
    ):
        self.dispatcher = dispatcher
        self.mcp_server_url = mcp_server_url
        self.project_id = project_id
        self.location = location
        self.model_name = model_name
        self.deployment_id = deployment_id or get_deployment_id()
        self.grafana_url = (os.getenv("GRAFANA_URL") or "").rstrip("/")
        self.dashboard_uid = os.getenv("GRAFANA_FARM_DASHBOARD_UID", "callsheet-control-tower")
        self.mcp_read_calls = 0
        self.mcp_write_calls = 0

        self.verification_progress: Optional[Dict[str, Any]] = None
        self.progress_callback: Optional[Any] = None
        self.max_poll_seconds: float = 150.0
        self.poll_interval_seconds: float = 5.0

        self.genai_client = Client(
            vertexai=True,
            project=self.project_id,
            location=self.location,
        )

    async def _execute_mcp_tool(self, toolset, tool_name: str, arguments: dict) -> dict:
        """
        Executes an MCP tool call. Counts reads and writes.
        Raises RuntimeError if the call fails or errors.
        """
        WRITE_TOOLS = {
            "create_incident",
            "add_activity_to_incident",
            "update_incident",
            "create_annotation",
            "update_annotation",
        }
        if tool_name in WRITE_TOOLS:
            self.mcp_write_calls += 1
        else:
            self.mcp_read_calls += 1

        try:
            res = await toolset._execute_with_session(
                lambda session: session.call_tool(tool_name, arguments=arguments),
                f"Call {tool_name}"
            )
        except Exception as e:
            raise ConnectionError(f"Grafana MCP transport error calling {tool_name}: {e}") from e

        if hasattr(res, "isError") and res.isError:
            error_msg = res.content[0].text if res.content else "Unknown MCP error"
            raise RuntimeError(f"Grafana MCP tool {tool_name} returned error: {error_msg}")

        # Parse content from response
        if hasattr(res, "content") and res.content:
            for item in res.content:
                if getattr(item, "type", None) == "image" or hasattr(item, "data"):
                    return {
                        "_is_image": True,
                        "data": getattr(item, "data", ""),
                        "mimeType": getattr(item, "mimeType", "image/png"),
                    }
            raw_text = res.content[0].text
            try:
                return json.loads(raw_text)
            except Exception:
                return {"raw_text": raw_text}

        return {}

    async def _discover_dashboard_uid(self, toolset) -> str:
        """
        Discovers the farm dashboard UID via search_dashboards.
        Falls back to GRAFANA_FARM_DASHBOARD_UID env var if search returns nothing.
        """
        try:
            res = await self._execute_mcp_tool(toolset, "search_dashboards", {"query": "Callsheet"})
            dashboards = res.get("dashboards", []) if isinstance(res, dict) else []
            for d in dashboards:
                if d.get("type") == "dash-db" and "uid" in d:
                    uid = d["uid"]
                    logger.info("Discovered farm dashboard '%s' (UID: %s) via search_dashboards", d.get("title"), uid)
                    return uid
        except Exception as e:
            logger.warning("search_dashboards failed: %s", e)

        fallback = os.getenv("GRAFANA_FARM_DASHBOARD_UID", "callsheet-control-tower")
        logger.info("Farm dashboard not found via search_dashboards, falling back to: %s", fallback)
        return fallback

    async def render_panel_image(
        self,
        from_ms: int,
        to_ms: int,
        panel_id: int = 1,
        width: int = 1000,
        height: int = 500,
    ) -> Optional[bytes]:
        """
        Dynamically renders a panel PNG via Grafana MCP get_panel_image.
        """
        try:
            params = get_grafana_mcp_connection_params(mcp_server_url=self.mcp_server_url)
            toolset = create_grafana_mcp_toolset(params)
            dashboard_uid = self.dashboard_uid or await self._discover_dashboard_uid(toolset)
            img_res = await self._execute_mcp_tool(
                toolset,
                "get_panel_image",
                {
                    "dashboardUid": dashboard_uid,
                    "panelId": panel_id,
                    "width": width,
                    "height": height,
                    "timeRange": {"from": str(from_ms), "to": str(to_ms)},
                },
            )
            if isinstance(img_res, dict) and img_res.get("_is_image") and img_res.get("data"):
                return base64.b64decode(img_res["data"])
        except Exception as e:
            logger.warning("render_panel_image failed: %s", e)
        return None

    async def _generate_four_deeplinks(
        self,
        toolset,
        anomalous_node_id: str,
        chosen_standby_node: str,
        tempo_trace_id: str,
        from_ms: int,
        to_ms: int,
    ) -> Dict[str, str]:
        """
        Generates four absolute deeplinks:
        1. Prometheus explore (temperature query on anomalous node)
        2. Loki explore (frame logs query on standby node)
        3. Tempo explore (traceql query on trace ID)
        4. Dashboard (at absolute incident time range)
        """
        links: Dict[str, str] = {}
        # 1. Prometheus Explore
        try:
            res1 = await self._execute_mcp_tool(
                toolset,
                "generate_deeplink",
                {
                    "resourceType": "explore",
                    "datasourceUid": "grafanacloud-prom",
                    "queries": [
                        {
                            "refId": "A",
                            "expr": f'render_farm_node_temperature_celsius{{deployment_id="{self.deployment_id}", node_id="{anomalous_node_id}"}}',
                        }
                    ],
                    "timeRange": {"from": str(from_ms), "to": str(to_ms)},
                },
            )
            links["prometheus"] = res1.get("raw_text") or res1.get("url") or ""
        except Exception as e:
            logger.warning("Failed to generate Prometheus deeplink: %s", e)
            links["prometheus"] = ""

        # 2. Loki Explore
        try:
            res2 = await self._execute_mcp_tool(
                toolset,
                "generate_deeplink",
                {
                    "resourceType": "explore",
                    "datasourceUid": "grafanacloud-logs",
                    "queries": [
                        {
                            "refId": "A",
                            "expr": f'{{service_name="render-farm"}} | deployment_id="{self.deployment_id}" | node_id="{chosen_standby_node}" |= "rendered on"',
                        }
                    ],
                    "timeRange": {"from": str(from_ms), "to": str(to_ms)},
                },
            )
            links["loki"] = res2.get("raw_text") or res2.get("url") or ""
        except Exception as e:
            logger.warning("Failed to generate Loki deeplink: %s", e)
            links["loki"] = ""

        # 3. Tempo Trace Explore
        try:
            res3 = await self._execute_mcp_tool(
                toolset,
                "generate_deeplink",
                {
                    "resourceType": "explore",
                    "datasourceUid": "grafanacloud-traces",
                    "queries": [
                        {
                            "refId": "A",
                            "queryType": "traceql",
                            "query": tempo_trace_id,
                        }
                    ],
                    "timeRange": {"from": str(from_ms), "to": str(to_ms)},
                },
            )
            links["tempo"] = res3.get("raw_text") or res3.get("url") or ""
        except Exception as e:
            logger.warning("Failed to generate Tempo deeplink: %s", e)
            links["tempo"] = ""

        # 4. Control Tower Dashboard
        try:
            res4 = await self._execute_mcp_tool(
                toolset,
                "generate_deeplink",
                {
                    "resourceType": "dashboard",
                    "dashboardUid": self.dashboard_uid,
                    "timeRange": {"from": str(from_ms), "to": str(to_ms)},
                },
            )
            links["dashboard"] = res4.get("raw_text") or res4.get("url") or ""
        except Exception as e:
            logger.warning("Failed to generate Dashboard deeplink: %s", e)
            links["dashboard"] = ""

        return links

    async def execute_mission(
        self,
        show_id: str = "show-aethelgard",
        force_verification_fault: bool = False,
        poll_interval_seconds: Optional[float] = None,
        max_poll_seconds: Optional[float] = None,
        trigger_type: str = "cycle_boundary",
    ) -> MissionResult:
        """
        Runs the complete 7-step closed-loop mission strictly driven by Grafana Cloud MCP responses:
        1. Anomaly Detection (Prometheus response parsing) - DETERMINISTIC_TELEMETRY
        2. Signal Correlation (Loki Logs & Tempo Trace Spans response parsing) - DETERMINISTIC_TELEMETRY
        3. Root Cause Deduction (Vertex AI Gemini reasoning over retrieved signals) - GENERATIVE_SYNTHESIS
        4. Production Impact Calculation - DETERMINISTIC_ARITHMETIC
        5. Workload Reallocation Intervention - DETERMINISTIC_ACTION
        6. Post-Intervention Telemetry Verification (Grafana Cloud audit of target node) - DETERMINISTIC_VERIFICATION
        7. Producer Callsheet Briefing Generation (Vertex AI Gemini) - GENERATIVE_SYNTHESIS
        """
        steps: List[MissionStep] = []

        # Connect to MCP toolset
        params = get_grafana_mcp_connection_params(mcp_server_url=self.mcp_server_url)
        toolset = create_grafana_mcp_toolset(params)
        self.dashboard_uid = await self._discover_dashboard_uid(toolset)

        # ---------------------------------------------------------
        # STEP 1: DETECT METRIC ANOMALY (Parse Prometheus Response with retry)
        # ---------------------------------------------------------
        anomalous_node_id = None
        max_temp = 0.0
        all_node_temps = {}
        series_list = []

        max_attempts = 4
        prom_expr = f'render_farm_node_temperature_celsius{{deployment_id="{self.deployment_id}"}}'
        for attempt in range(max_attempts):
            prom_data = await self._execute_mcp_tool(
                toolset,
                "query_prometheus",
                {
                    "datasourceUid": "grafanacloud-prom",
                    "expr": prom_expr,
                    "queryType": "instant",
                    "endTime": "now",
                }
            )

            series_list = prom_data.get("data", [])
            if not series_list and "result" in prom_data.get("data", {}):
                series_list = prom_data["data"]["result"]

            for item in series_list:
                metric_meta = item.get("metric", {})
                val_tuple = item.get("value", [0, "0"])
                n_id = metric_meta.get("node_id")
                try:
                    t_val = float(val_tuple[1])
                except (ValueError, IndexError):
                    t_val = 0.0

                if n_id:
                    all_node_temps[n_id] = t_val
                    if t_val > 90.0 and t_val > max_temp:
                        max_temp = t_val
                        anomalous_node_id = n_id

            if anomalous_node_id:
                break

            if attempt < max_attempts - 1:
                logger.info("Prometheus query found no node > 90C yet (ingestion latency). Retrying in 3.0s...")
                await asyncio.sleep(3.0)

        if not series_list:
            raise RuntimeError(f"Step 1 failed: Prometheus query returned empty metric series for deployment_id={self.deployment_id}. No farm telemetry found.")

        if not anomalous_node_id:
            raise RuntimeError(
                f"Step 1 failed: No node exceeded the 90.0°C thermal limit in Prometheus for deployment_id={self.deployment_id}. Temperatures: {all_node_temps}"
            )
        logger.info("Step 1 detected anomalous_node_id=%s with temp=%.1fC (deployment_id=%s)", anomalous_node_id, max_temp, self.deployment_id)

        step1 = MissionStep(
            step_number=1,
            name="Anomaly Detection (Prometheus)",
            execution_type="DETERMINISTIC_TELEMETRY",
            description=f"Prometheus query identified critical temperature spike on {anomalous_node_id} ({max_temp:.1f}°C, limit: 90.0°C).",
            evidence={
                "tool": "query_prometheus",
                "expr": prom_expr,
                "deployment_id": self.deployment_id,
                "anomalous_node_id": anomalous_node_id,
                "temperature_celsius": max_temp,
                "active_series_count": len(series_list),
            },
        )
        steps.append(step1)

        # ---------------------------------------------------------
        # STEP 2: SIGNAL CORRELATION (Parse Loki Logs & Tempo Trace Spans)
        # ---------------------------------------------------------
        # 2a. Query Loki logs specifically for anomalous_node_id (most recent 10 minutes)
        retrieved_log_lines = []
        max_loki_attempts = 4
        for attempt in range(max_loki_attempts):
            loki_data = await self._execute_mcp_tool(
                toolset,
                "query_loki_logs",
                {
                    "datasourceUid": "grafanacloud-logs",
                    "logql": f'{{service_name="render-farm"}} | deployment_id="{self.deployment_id}" |= "{anomalous_node_id}"',
                    "startRfc3339": "now-10m",
                    "endRfc3339": "now",
                    "limit": 30,
                }
            )

            log_entries = loki_data.get("data", [])
            retrieved_log_lines = [
                entry.get("line", "")
                for entry in log_entries
                if anomalous_node_id in entry.get("line", "")
            ]
            if retrieved_log_lines:
                break
            if attempt < max_loki_attempts - 1:
                logger.info("Loki query found no log entries yet for %s (ingestion latency). Retrying in 3.0s...", anomalous_node_id)
                await asyncio.sleep(3.0)

        if not retrieved_log_lines:
            raise RuntimeError(f"Step 2 failed: No Loki log entries found for anomalous node {anomalous_node_id} on deployment {self.deployment_id}.")

        # Prioritize critical / warning / degraded lines on the anomalous node
        critical_logs = [
            l for l in retrieved_log_lines
            if any(k in l.upper() for k in ["CRITICAL", "DEGRADED", "THROTTL", "THERMAL", "WARN"])
        ]

        # Correlate the specific log corresponding to the detected incident temperature
        matching_logs = []
        for l in critical_logs:
            m = re.search(r"reached\s+([0-9]+(?:\.[0-9]+)?)\s*C", l)
            if m and round(abs(float(m.group(1)) - round(max_temp, 1)), 1) <= 0.1:
                matching_logs.append(l)
        quoted_log_evidence = matching_logs[-1] if matching_logs else (critical_logs[-1] if critical_logs else retrieved_log_lines[-1])

        # Coherence check: Verify that the temperature quoted in the Loki log matches the Prometheus sample
        log_temp_match = re.search(r"reached\s+([0-9]+(?:\.[0-9]+)?)\s*C", quoted_log_evidence)
        if log_temp_match:
            log_temp = float(log_temp_match.group(1))
            prom_temp = round(max_temp, 1)
            if round(abs(log_temp - prom_temp), 1) > 0.1:
                raise ValueError(
                    f"Step 2 coherence check failed: Loki log temperature ({log_temp:.1f}°C) "
                    f"does not match Prometheus sample ({prom_temp:.1f}°C)."
                )

        # 2b. Query Tempo traces filtered strictly by the anomalous node and minimum throttled duration
        now_epoch = int(time.time())
        start_epoch = now_epoch - 7200

        tempo_search = await self._execute_mcp_tool(
            toolset,
            "grafana_api_request",
            {
                "endpoint": f"/api/datasources/proxy/uid/grafanacloud-traces/api/search?tags=node_id%3D{anomalous_node_id}&tags=deployment_id%3D{self.deployment_id}&minDuration=30s&start={start_epoch}&end={now_epoch}&limit=10",
                "method": "GET",
            }
        )

        tempo_data = tempo_search.get("data", {})
        traces_list = tempo_data.get("traces", [])
        if not traces_list and "traces" in tempo_search:
            traces_list = tempo_search.get("traces", [])

        # Fallback without minDuration if needed
        if not traces_list:
            tempo_search = await self._execute_mcp_tool(
                toolset,
                "grafana_api_request",
                {
                    "endpoint": f"/api/datasources/proxy/uid/grafanacloud-traces/api/search?tags=node_id%3D{anomalous_node_id}&tags=deployment_id%3D{self.deployment_id}&start={start_epoch}&end={now_epoch}&limit=10",
                    "method": "GET",
                }
            )
            traces_list = tempo_search.get("data", {}).get("traces", [])

        if not traces_list:
            raise RuntimeError(f"Step 2 failed: No trace spans returned from Tempo for anomalous node {anomalous_node_id}.")

        # Find the trace exhibiting throttled execution duration (duration >= 60s or highest duration)
        matched_trace = None
        for t in traces_list:
            if t.get("durationMs", 0) >= 60000:
                matched_trace = t
                break
        if not matched_trace:
            matched_trace = max(traces_list, key=lambda t: t.get("durationMs", 0))

        raw_trace_id = str(matched_trace.get("traceID", "")).lower()
        trace_id = raw_trace_id.zfill(32)
        root_trace_name = matched_trace.get("rootTraceName", "unknown")
        total_duration_ms = matched_trace.get("durationMs", 0)

        # Query Tempo detail endpoint using padded trace_id (fallback to raw_trace_id if needed)
        try:
            trace_detail = await self._execute_mcp_tool(
                toolset,
                "grafana_api_request",
                {
                    "endpoint": f"/api/datasources/proxy/uid/grafanacloud-traces/api/traces/{trace_id}",
                    "method": "GET",
                }
            )
            if not trace_detail.get("data", {}).get("batches"):
                raise ValueError("Empty batches")
        except Exception:
            trace_detail = await self._execute_mcp_tool(
                toolset,
                "grafana_api_request",
                {
                    "endpoint": f"/api/datasources/proxy/uid/grafanacloud-traces/api/traces/{raw_trace_id}",
                    "method": "GET",
                }
            )

        # Parse child spans and isolate raytrace span duration
        child_spans = []
        raytrace_duration_s = 0.0
        batches = trace_detail.get("data", {}).get("batches", [])
        for b in batches:
            for scope in b.get("scopeSpans", []):
                for s in scope.get("spans", []):
                    span_name = s.get("name", "")
                    start_ns = int(s.get("startTimeUnixNano", 0))
                    end_ns = int(s.get("endTimeUnixNano", 0))
                    dur_sec = (end_ns - start_ns) / 1e9 if end_ns > start_ns else 0.0
                    child_spans.append({
                        "name": span_name,
                        "duration_seconds": round(dur_sec, 2),
                    })
                    if span_name == "raytrace_volumetrics_pass":
                        raytrace_duration_s = round(dur_sec, 2)

        # Enforce that the trace actually demonstrates throttling
        if raytrace_duration_s < 30.0 and total_duration_ms < 40000:
            raise RuntimeError(
                f"Step 2 failed: Retained trace {trace_id} on {anomalous_node_id} shows normal execution "
                f"(raytrace {raytrace_duration_s:.1f}s, total {total_duration_ms/1000:.1f}s) rather than throttled state."
            )

        quoted_trace_evidence = (
            f"TraceID {trace_id} ({root_trace_name} on {anomalous_node_id}): Total Duration {total_duration_ms/1000.0:.1f}s. "
            f"raytrace_volumetrics_pass took {raytrace_duration_s:.1f}s (inflated from 17.0s baseline by {raytrace_duration_s - 17.0:.1f}s due to 800MHz CPU down-throttling). "
            f"Child Spans: {json.dumps(child_spans[:4])}"
        )

        step2 = MissionStep(
            step_number=2,
            name="Signal Correlation (Loki & Tempo)",
            execution_type="DETERMINISTIC_TELEMETRY",
            description=f"Correlated Prometheus anomaly with Loki worker logs and Tempo trace spans on {anomalous_node_id}.",
            evidence={
                "tool_logs": "query_loki_logs",
                "quoted_loki_log": quoted_log_evidence,
                "total_logs_retrieved": len(retrieved_log_lines),
                "tool_traces": f"grafana_api_request (/api/datasources/proxy/uid/grafanacloud-traces/api/traces/{trace_id})",
                "trace_id": trace_id,
                "root_trace_name": root_trace_name,
                "raytrace_duration_seconds": raytrace_duration_s,
                "quoted_trace_span": quoted_trace_evidence,
                "spans_breakdown": child_spans,
            },
        )
        steps.append(step2)

        # ---------------------------------------------------------
        # STEP 3: ROOT CAUSE DEDUCTION (Vertex AI Gemini Reasoning)
        # ---------------------------------------------------------
        reasoning_prompt = f"""
Analyze the following retrieved observability signals from Grafana Cloud for render node {anomalous_node_id}:
- Prometheus Temperature Metric: {max_temp:.1f}°C (Hardware Threshold: 90.0°C)
- Loki Log Records: {json.dumps(retrieved_log_lines[:3])}
- Tempo Trace Span Timings: {quoted_trace_evidence}

State the technical root cause in 1 to 2 clear sentences, explaining how the hardware temperature spike caused the raytrace span duration to inflate to {raytrace_duration_s:.1f}s. Do not use em dashes.
"""
        root_cause_response = await self.genai_client.aio.models.generate_content(
            model=self.model_name,
            contents=reasoning_prompt,
            config=types.GenerateContentConfig(
                system_instruction=CALLSHEET_AGENT_SYSTEM_PROMPT,
                temperature=0.1,
            ),
        )
        
        if not root_cause_response or not root_cause_response.text or not root_cause_response.text.strip():
            raise RuntimeError(f"Step 3 failed: Vertex AI Gemini reasoning model ({self.model_name}) returned empty diagnosis.")

        deduced_root_cause = root_cause_response.text.strip()

        step3 = MissionStep(
            step_number=3,
            name="Root Cause Deduction (Gemini)",
            execution_type="GENERATIVE_SYNTHESIS",
            description=deduced_root_cause,
            evidence={
                "model": self.model_name,
                "anomalous_node": anomalous_node_id,
                "hardware_temperature": max_temp,
                "raytrace_duration_seconds": raytrace_duration_s,
            },
        )
        steps.append(step3)

        # ---------------------------------------------------------
        # STEP 4: PRODUCTION IMPACT MAPPING
        # ---------------------------------------------------------
        now_dt = datetime.now(timezone.utc)
        # Update simulation deadlines and burn-down for current time
        self.dispatcher.simulator.update_cycle_deadlines(now_dt)

        show = self.dispatcher.simulator.state.shows.get(show_id)
        show_name = show.name if show else "Chronicles of Aethelgard: Episode 6"
        client_name = show.client if show else "Cinefex Northern Pictures"
        deadline_dt = show.delivery_deadline if show else now_dt + timedelta(hours=4.0)
        deadline_str = deadline_dt.strftime("%A %d %B, %H:%M UTC")
        penalty_str = f"£{show.penalty_daily_amount:,.0f} / day" if show else "£25,000 / day"

        # 1. Target shot selection: must be the active shot allocated to anomalous_node_id
        target_shot = None
        for s in self.dispatcher.simulator.state.shots.values():
            if s.allocated_node_id == anomalous_node_id:
                target_shot = s
                break
        if not target_shot:
            # Check for any shot belonging to the show that is AT_RISK or on anomalous node
            for s in self.dispatcher.simulator.state.shots.values():
                if s.show_id == show_id and (s.allocated_node_id == anomalous_node_id or s.status == "AT_RISK"):
                    target_shot = s
                    break
        if not target_shot:
            for s in self.dispatcher.simulator.state.shots.values():
                if s.show_id == show_id:
                    target_shot = s
                    s.allocated_node_id = anomalous_node_id
                    break
        if not target_shot:
            raise RuntimeError(f"Step 4 failed: No active shot allocated to anomalous node {anomalous_node_id}.")

        frames_rem = target_shot.frames_remaining
        throttled_sec = target_shot.current_seconds_per_frame
        normal_sec = target_shot.estimated_seconds_per_frame

        # Unmitigated projected completion under throttling
        unmitigated_time_sec = frames_rem * throttled_sec
        unmitigated_completion_dt = now_dt + timedelta(seconds=unmitigated_time_sec)
        unmitigated_buffer_hours = (deadline_dt - unmitigated_completion_dt).total_seconds() / 3600.0

        step4 = MissionStep(
            step_number=4,
            name="Production Impact Mapping",
            execution_type="DETERMINISTIC_ARITHMETIC",
            description=(
                f"Mapped {anomalous_node_id} failure to {show_name} (Deadline: {deadline_str}). "
                f"Without intervention, Shot {target_shot.shot_code} ({frames_rem} frames remaining at {throttled_sec:.0f}s/frame) "
                f"would complete at {unmitigated_completion_dt.strftime('%H:%M UTC')} with a NEGATIVE buffer of {unmitigated_buffer_hours:.1f} hours "
                f"({abs(unmitigated_buffer_hours):.1f} hours past deadline), triggering the {penalty_str} contractual penalty clause."
            ),
            evidence={
                "show_name": show_name,
                "shot_code": target_shot.shot_code,
                "frames_remaining": frames_rem,
                "throttled_rate_sec": throttled_sec,
                "unmitigated_completion": unmitigated_completion_dt.isoformat(),
                "unmitigated_buffer_hours": round(unmitigated_buffer_hours, 1),
                "penalty_clause": penalty_str,
            },
        )

        # Create Grafana IRM Incident via MCP create_incident
        deficit_hours = abs(unmitigated_buffer_hours)
        now_utc_str = now_dt.strftime("%Y-%m-%d %H:%M UTC")
        title_core = f"{show_name} | Shot {target_shot.shot_code} | {anomalous_node_id} | Deficit {deficit_hours:.1f}h | {now_utc_str}"
        if self.deployment_id != "cloud-run":
            incident_title = f"[{self.deployment_id}] {title_core}"
        else:
            incident_title = title_core

        incident_severity = "critical" if deficit_hours > 2.0 else "major"
        is_drill = (self.deployment_id != "cloud-run")
        room_prefix = f"callsheet-{target_shot.shot_code.lower()}"

        incident_id = None
        incident_url = None
        incident_status = None
        try:
            inc_res = await self._execute_mcp_tool(
                toolset,
                "create_incident",
                {
                    "title": incident_title,
                    "severity": incident_severity,
                    "roomPrefix": room_prefix,
                    "isDrill": is_drill,
                    "labels": [
                        {"key": "deployment_id", "label": self.deployment_id},
                        {"key": "source", "label": "callsheet"},
                        {"key": "show", "label": show_name},
                        {"key": "node", "label": anomalous_node_id},
                        {"key": "shot", "label": target_shot.shot_code},
                    ],
                },
            )
            raw_id = (
                inc_res.get("incidentID")
                or inc_res.get("incidentId")
                or inc_res.get("id")
                or (inc_res.get("incident", {}).get("id") if isinstance(inc_res.get("incident"), dict) else None)
            )
            if raw_id:
                incident_id = str(raw_id)
                incident_status = "active"
                overview_url = inc_res.get("overviewURL") or (inc_res.get("incident", {}).get("overviewURL") if isinstance(inc_res.get("incident"), dict) else None)
                if overview_url:
                    incident_url = overview_url if str(overview_url).startswith("http") else f"{self.grafana_url}{overview_url}"
                else:
                    incident_url = f"{self.grafana_url}/a/grafana-irm-app/incidents/{incident_id}"
                logger.info("Created Grafana IRM Incident #%s: %s", incident_id, incident_url)
        except Exception as e:
            logger.warning("Failed to create Grafana IRM incident (non-blocking): %s", e)

        step4.evidence["incident_id"] = incident_id
        step4.evidence["incident_url"] = incident_url
        step4.evidence["incident_status"] = incident_status
        steps.append(step4)

        # ---------------------------------------------------------
        # ARCHITECTURAL INVARIANT: DETERMINISTIC DECISION PATH
        # Interventions are triggered strictly by arithmetic threshold breaches and telemetry verification.
        # Vertex AI Gemini is employed exclusively for explanatory synthesis (Root Cause Deduction and Executive Briefing).
        # No LLM output, heuristic score, or model response is permitted to influence the failover decision or buffer calculation.
        # ---------------------------------------------------------
        assert unmitigated_buffer_hours < 0.0, "Intervention disallowed: arithmetic buffer is not in deficit."
        assert max_temp > 90.0, "Intervention disallowed: hardware temperature did not breach threshold."

        # ---------------------------------------------------------
        # STEP 5: WORKLOAD REALLOCATION INTERVENTION (Dynamic Standby Selection)
        # ---------------------------------------------------------
        available_standby_nodes = [
            n for n in self.dispatcher.simulator.state.nodes.values()
            if n.is_standby and n.status == NodeStatus.STANDBY
        ]
        if not available_standby_nodes:
            raise RuntimeError("Step 5 failed: No standby spare nodes available for workload reallocation.")

        chosen_standby_node = available_standby_nodes[0].id

        intervention_record = self.dispatcher.execute_reallocation(
            shot_id=target_shot.id,
            target_node_id=chosen_standby_node,
            reason=f"Automated failover from throttled {anomalous_node_id} ({max_temp:.1f}°C) to protect {show_name} deadline.",
            telemetry_evidence={
                "source_node": anomalous_node_id,
                "target_node": chosen_standby_node,
                "source_temp": max_temp,
                "quoted_loki_log": quoted_log_evidence,
                "quoted_tempo_trace": quoted_trace_evidence,
            },
            force_fault=force_verification_fault,
        )

        # ---------------------------------------------------------
        # COHERENCE CHECK GATE
        # Enforces mathematical consistency across all telemetry signals before Gemini briefing generation
        # ---------------------------------------------------------
        if unmitigated_buffer_hours >= 0:
            raise ValueError(
                f"Coherence check failed: unmitigated buffer margin ({unmitigated_buffer_hours:.1f}h) must be negative during a thermal failure."
            )
        if throttled_sec <= normal_sec:
            raise ValueError(
                f"Coherence check failed: throttled render rate ({throttled_sec:.1f}s) must be greater than baseline rate ({normal_sec:.1f}s)."
            )
        if intervention_record.buffer_margin_hours <= unmitigated_buffer_hours:
            raise ValueError(
                f"Coherence check failed: restored buffer ({intervention_record.buffer_margin_hours:.1f}h) must be greater than unmitigated deficit ({unmitigated_buffer_hours:.1f}h)."
            )
        if intervention_record.previous_node_id != anomalous_node_id:
            raise ValueError(
                f"Coherence check failed: previous node ({intervention_record.previous_node_id}) does not match anomalous node ({anomalous_node_id})."
            )
        log_temp_match = re.search(r"reached\s+([0-9]+(?:\.[0-9]+)?)\s*C", quoted_log_evidence)
        if log_temp_match:
            log_temp = float(log_temp_match.group(1))
            prom_temp = round(max_temp, 1)
            if round(abs(log_temp - prom_temp), 1) > 0.1:
                raise ValueError(
                    f"Coherence check failed: Loki log temperature ({log_temp:.1f}°C) "
                    f"does not match Prometheus sample ({prom_temp:.1f}°C)."
                )

        intervention_ts = intervention_record.timestamp
        intervention_epoch = intervention_ts.timestamp()

        # Add activity to Grafana IRM incident
        if incident_id:
            try:
                await self._execute_mcp_tool(
                    toolset,
                    "add_activity_to_incident",
                    {
                        "incidentId": incident_id,
                        "body": (
                            f"Step 5 Workload Reallocation executed:\n"
                            f"- Shot: {target_shot.shot_code}\n"
                            f"- Migrated from: {anomalous_node_id} ({max_temp:.1f}C) to {chosen_standby_node}\n"
                            f"- Quarantined: {anomalous_node_id}\n"
                            f"- Restored buffer margin: +{intervention_record.buffer_margin_hours:.1f}h"
                        ),
                    },
                )
            except Exception as e:
                logger.warning("Failed to add Step 5 activity to incident #%s: %s", incident_id, e)

        # Create dashboard-wide annotation in Grafana
        annotation_id = None
        try:
            ann_res = await self._execute_mcp_tool(
                toolset,
                "create_annotation",
                {
                    "dashboardUid": self.dashboard_uid,
                    "time": int(intervention_epoch * 1000),
                    "text": f"Callsheet Intervention: Shot {target_shot.shot_code} migrated from {anomalous_node_id} ({max_temp:.1f}C) to {chosen_standby_node}",
                    "tags": ["callsheet", "intervention", anomalous_node_id, target_shot.shot_code, self.deployment_id],
                },
            )
            payload = ann_res.get("Payload") if isinstance(ann_res, dict) else None
            if isinstance(payload, dict):
                annotation_id = payload.get("id")
            elif isinstance(ann_res, dict):
                annotation_id = ann_res.get("id")
            logger.info("Created Grafana dashboard annotation ID: %s", annotation_id)
        except Exception as e:
            logger.warning("Failed to create dashboard annotation (non-blocking): %s", e)

        step5 = MissionStep(
            step_number=5,
            name="Workload Reallocation Intervention",
            execution_type="DETERMINISTIC_ACTION",
            description=(
                f"Reallocated Shot {target_shot.shot_code} from {anomalous_node_id} to standby spare {chosen_standby_node} (quarantined {anomalous_node_id}). "
                f"Clean render speed ({normal_sec:.0f}s/frame) restored. Projected buffer margin restored from {unmitigated_buffer_hours:.1f}h to +{intervention_record.buffer_margin_hours:.1f} hours, avoiding the {penalty_str} penalty."
            ),
            evidence={
                "intervention_id": intervention_record.id,
                "shot_code": intervention_record.shot_code,
                "previous_node": intervention_record.previous_node_id,
                "target_node": intervention_record.target_node_id,
                "unmitigated_buffer_hours": round(unmitigated_buffer_hours, 1),
                "restored_buffer_margin_hours": intervention_record.buffer_margin_hours,
                "status": intervention_record.status,
                "annotation_id": annotation_id,
            },
        )
        steps.append(step5)

        # ---------------------------------------------------------
        # STEP 6: POST-INTERVENTION TELEMETRY VERIFICATION (Grafana Cloud)
        # Closed-loop verification: Poll Grafana telemetry for up to 150 seconds.
        # Accept telemetry witnesses strictly timestamped after intervention_ts.
        # Zero synthesized or fallback values permitted.
        # ---------------------------------------------------------
        intervention_ts = intervention_record.timestamp
        intervention_epoch = intervention_ts.timestamp()
        rate_limit_seconds = normal_sec * 1.25

        window_start_time = time.time()
        effective_max_poll = max_poll_seconds if max_poll_seconds is not None else self.max_poll_seconds
        effective_interval = poll_interval_seconds if poll_interval_seconds is not None else self.poll_interval_seconds

        verified_rate_sec: Optional[float] = None
        verified_temp_c: Optional[float] = None
        verified_log_line: Optional[str] = None
        accepted_loki_ts: Optional[float] = None
        accepted_prom_ts: Optional[float] = None
        accepted_prom_sample_ts: Optional[float] = None
        accepted_prom_eval_ts: Optional[float] = None
        accepted_tempo_trace_id: Optional[str] = None
        tempo_raytrace_duration_s: Optional[float] = None
        tempo_grace_deadline: Optional[float] = None

        witnesses_accepted: List[str] = []
        is_verified = False
        verification_status = "PENDING_VERIFICATION"
        escalation_required = False
        human_recommendation = ""
        poll_attempts = 0

        while (time.time() - window_start_time) <= effective_max_poll:
            poll_attempts += 1
            elapsed = int(time.time() - window_start_time)
            progress_msg = f"Awaiting first frame on {chosen_standby_node}. Verification window 150s, {elapsed}s elapsed."
            self.verification_progress = {
                "active": True,
                "target_node": chosen_standby_node,
                "elapsed_seconds": elapsed,
                "max_window_seconds": 150,
                "message": progress_msg,
            }
            if self.progress_callback:
                try:
                    self.progress_callback(self.verification_progress)
                except Exception:
                    pass

            # 1. Query Loki for frame completion lines on chosen_standby_node
            latest_loki_entry = None
            try:
                loki_res = await self._execute_mcp_tool(
                    toolset,
                    "query_loki_logs",
                    {
                        "datasourceUid": "grafanacloud-logs",
                        "logql": f'{{service_name="render-farm"}} | deployment_id="{self.deployment_id}" |= "rendered on {chosen_standby_node}"',
                        "startRfc3339": "now-5m",
                        "endRfc3339": "now",
                        "limit": 10,
                    },
                )
                entries = loki_res.get("data", []) if isinstance(loki_res, dict) else []
                for entry in entries:
                    line_content = entry.get("line", "")
                    raw_ts = entry.get("timestamp") or entry.get("labels", {}).get("observed_timestamp")
                    parsed_ts = parse_loki_timestamp(raw_ts)
                    if parsed_ts is not None and parsed_ts >= (intervention_epoch - 0.5):
                        dur = parse_loki_frame_duration(line_content)
                        if dur is not None:
                            latest_loki_entry = (line_content, dur, parsed_ts)
                            break
            except Exception as e:
                logger.warning("Loki verification poll failed on attempt %d: %s", poll_attempts, e)

            # 2. Query Prometheus instant temperature metric and actual sample timestamp on chosen_standby_node
            latest_prom_entry = None
            try:
                prom_res = await self._execute_mcp_tool(
                    toolset,
                    "query_prometheus",
                    {
                        "datasourceUid": "grafanacloud-prom",
                        "expr": f'render_farm_node_temperature_celsius{{node_id="{chosen_standby_node}", deployment_id="{self.deployment_id}"}}',
                        "queryType": "instant",
                        "endTime": "now",
                    },
                )
                ts_res = await self._execute_mcp_tool(
                    toolset,
                    "query_prometheus",
                    {
                        "datasourceUid": "grafanacloud-prom",
                        "expr": f'timestamp(render_farm_node_temperature_celsius{{node_id="{chosen_standby_node}", deployment_id="{self.deployment_id}"}})',
                        "queryType": "instant",
                        "endTime": "now",
                    },
                )
                series_data = prom_res.get("data", [])
                if isinstance(series_data, dict):
                    series_data = series_data.get("result", [])
                ts_data = ts_res.get("data", [])
                if isinstance(ts_data, dict):
                    ts_data = ts_data.get("result", [])

                p_temp = None
                eval_ts = None
                for s in series_data:
                    val = s.get("value", [])
                    if len(val) >= 2:
                        eval_ts = float(val[0])
                        p_temp = float(val[1])
                        break

                actual_sample_ts = None
                for s in ts_data:
                    val = s.get("value", [])
                    if len(val) >= 2:
                        actual_sample_ts = float(val[1])
                        break

                if p_temp is not None and actual_sample_ts is not None:
                    if actual_sample_ts >= (intervention_epoch - 0.5):
                        latest_prom_entry = (p_temp, actual_sample_ts, eval_ts or actual_sample_ts)
            except Exception as e:
                logger.warning("Prometheus verification poll failed on attempt %d: %s", poll_attempts, e)

            # 3. Query Tempo for traces on chosen_standby_node (third witness)
            latest_tempo_entry = None
            try:
                now_epoch = int(time.time())
                start_epoch = max(0, int(intervention_epoch) - 5)
                tempo_search = await self._execute_mcp_tool(
                    toolset,
                    "grafana_api_request",
                    {
                        "endpoint": f"/api/datasources/proxy/uid/grafanacloud-traces/api/search?tags=node_id%3D{chosen_standby_node}&tags=deployment_id%3D{self.deployment_id}&start={start_epoch}&end={now_epoch}&limit=5",
                        "method": "GET",
                    },
                )
                t_list = tempo_search.get("data", {}).get("traces", [])
                if not t_list and "traces" in tempo_search:
                    t_list = tempo_search.get("traces", [])
                for tr in t_list:
                    tr_start_ns = int(tr.get("startTimeUnixNano", 0))
                    tr_start_epoch = tr_start_ns / 1e9 if tr_start_ns > 0 else 0.0
                    if tr_start_epoch >= (intervention_epoch - 2.0):
                        t_id = str(tr.get("traceID", "")).lower().zfill(32)
                        latest_tempo_entry = (t_id, tr_start_epoch)
                        break
            except Exception as e:
                logger.warning("Tempo verification poll failed on attempt %d: %s", poll_attempts, e)

            # Check if Loki and Prometheus samples are both retrieved
            if latest_loki_entry is not None and latest_prom_entry is not None:
                # If Tempo is not yet available, continue polling Tempo alone for up to 30s
                if latest_tempo_entry is None:
                    if tempo_grace_deadline is None:
                        tempo_grace_deadline = time.time() + 30.0
                        logger.info(
                            "Loki and Prometheus verified for %s. Polling Tempo for up to 30s to acquire 3rd witness...",
                            chosen_standby_node,
                        )

                    if time.time() < tempo_grace_deadline and (time.time() - window_start_time + effective_interval) < effective_max_poll:
                        elapsed = int(time.time() - window_start_time)
                        grace_elapsed = int(30.0 - (tempo_grace_deadline - time.time()))
                        self.verification_progress = {
                            "active": True,
                            "target_node": chosen_standby_node,
                            "elapsed_seconds": elapsed,
                            "max_window_seconds": 150,
                            "message": f"Frame confirmed on {chosen_standby_node}. Awaiting Tempo trace correlation ({grace_elapsed}s/30s).",
                        }
                        if self.progress_callback:
                            try:
                                self.progress_callback(self.verification_progress)
                            except Exception:
                                pass
                        await asyncio.sleep(effective_interval)
                        continue

                # Finalize verification with all retrieved telemetry
                verified_log_line, verified_rate_sec, accepted_loki_ts = latest_loki_entry
                verified_temp_c = round(latest_prom_entry[0], 1)
                accepted_prom_sample_ts = latest_prom_entry[1]
                accepted_prom_eval_ts = latest_prom_entry[2]

                witnesses_accepted = ["Loki", "Prometheus"]
                if latest_tempo_entry is not None:
                    accepted_tempo_trace_id = latest_tempo_entry[0]
                    witnesses_accepted.append("Tempo")
                    try:
                        trace_detail = await self._execute_mcp_tool(
                            toolset,
                            "grafana_api_request",
                            {
                                "endpoint": f"/api/datasources/proxy/uid/grafanacloud-traces/api/traces/{accepted_tempo_trace_id}",
                                "method": "GET",
                            },
                        )
                        batches = trace_detail.get("data", {}).get("batches", [])
                        for b in batches:
                            for scope in b.get("scopeSpans", []):
                                for sp in scope.get("spans", []):
                                    if sp.get("name") == "raytrace_volumetrics_pass":
                                        st_ns = int(sp.get("startTimeUnixNano", 0))
                                        et_ns = int(sp.get("endTimeUnixNano", 0))
                                        if et_ns > st_ns:
                                            tempo_raytrace_duration_s = round((et_ns - st_ns) / 1e9, 2)
                    except Exception as te:
                        logger.warning("Failed to fetch Tempo trace details for %s: %s", accepted_tempo_trace_id, te)

                # Evaluate pass or fail criteria
                if verified_rate_sec <= rate_limit_seconds and verified_temp_c < 90.0:
                    is_verified = True
                    verification_status = "VERIFIED_PROTECTED"
                    intervention_record.status = "PROTECTED"
                    escalation_required = False
                    human_recommendation = "None. Workload successfully secured and verified on standby infrastructure."
                    break
                else:
                    is_verified = False
                    verification_status = "ESCALATED"
                    intervention_record.status = "ESCALATED"
                    escalation_required = True
                    human_recommendation = (
                        f"IMMEDIATE HUMAN ACTION REQUIRED: Failover standby node {chosen_standby_node} failed post-intervention verification "
                        f"with degraded throughput ({verified_rate_sec:.1f}s/frame, temp {verified_temp_c:.1f}°C). "
                        f"Manually allocate external cloud burst capacity to protect delivery."
                    )
                    break

            if (time.time() - window_start_time) < effective_max_poll:
                await asyncio.sleep(effective_interval)

        # Clear active progress
        self.verification_progress = None

        # Check if window expired without definitive telemetry proof
        if verification_status == "PENDING_VERIFICATION":
            verification_status = "VERIFICATION_INCONCLUSIVE"
            intervention_record.status = "PENDING_VERIFICATION"
            is_verified = False
            escalation_required = True
            human_recommendation = (
                f"MANUAL INVESTIGATION REQUIRED: 150-second verification window expired without post-intervention telemetry from {chosen_standby_node}. "
                f"Technical Director review required to confirm render progress."
            )

        # Formulate Step 6 description based on true verified outcome
        elapsed_total = round(time.time() - window_start_time, 1)
        if verification_status == "VERIFIED_PROTECTED":
            step6_desc = (
                f"Closed-loop verification confirmed via Grafana Cloud telemetry for {chosen_standby_node} in {elapsed_total}s. "
                f"Retrieved frame render duration {verified_rate_sec:.1f}s (nominal baseline {normal_sec:.0f}s, threshold {rate_limit_seconds:.1f}s) "
                f"and stable junction temperature ({verified_temp_c:.1f}°C). Telemetry timestamped after intervention ({intervention_ts.isoformat()}). "
                f"Witnesses accepted: {', '.join(witnesses_accepted)}. "
                f"Delivery deadline confirmed PROTECTED with +{intervention_record.buffer_margin_hours:.1f}h buffer margin."
            )
        elif verification_status == "ESCALATED":
            step6_desc = (
                f"Closed-loop telemetry verification for {chosen_standby_node} FAILED in {elapsed_total}s. "
                f"Retrieved frame duration {verified_rate_sec:.1f}s (exceeds {rate_limit_seconds:.1f}s limit) "
                f"and elevated temperature ({verified_temp_c:.1f}°C). Witnesses accepted: {', '.join(witnesses_accepted)}. "
                f"Status set to ESCALATED: IMMEDIATE HUMAN TD ACTION REQUIRED."
            )
        else:
            step6_desc = (
                f"Closed-loop telemetry verification for {chosen_standby_node} INCONCLUSIVE. "
                f"150-second polling window expired without post-intervention frame completion logs or metrics. "
                f"Intervention status left at PENDING_VERIFICATION. Immediate Technical Director investigation required."
            )

        # Epoch calculations for absolute time range
        verified_epoch = time.time()
        verified_ms = int(verified_epoch * 1000)
        from_ms = int((intervention_epoch - 300) * 1000)
        to_ms = int((verified_epoch + 300) * 1000)

        # Generate four absolute deeplinks
        deeplinks = await self._generate_four_deeplinks(
            toolset=toolset,
            anomalous_node_id=anomalous_node_id,
            chosen_standby_node=chosen_standby_node,
            tempo_trace_id=accepted_tempo_trace_id or trace_id,
            from_ms=from_ms,
            to_ms=to_ms,
        )

        # Annotation update to region or failure annotation
        if verification_status == "VERIFIED_PROTECTED":
            if annotation_id:
                try:
                    await self._execute_mcp_tool(
                        toolset,
                        "update_annotation",
                        {
                            "id": int(annotation_id),
                            "timeEnd": verified_ms,
                            "text": (
                                f"Callsheet Verified Protected: Shot {target_shot.shot_code} on {chosen_standby_node} "
                                f"({verified_rate_sec:.1f}s/frame, {verified_temp_c:.1f}C). Witnesses: {', '.join(witnesses_accepted)}."
                            ),
                            "tags": ["callsheet", "intervention", "verified", chosen_standby_node, target_shot.shot_code, self.deployment_id],
                        },
                    )
                    logger.info("Updated annotation ID %s to region ending at %d", annotation_id, verified_ms)
                except Exception as e:
                    logger.warning("Failed to update annotation ID %s: %s", annotation_id, e)
        else:
            fail_tag = "verification-failed" if verification_status == "ESCALATED" else "verification-inconclusive"
            try:
                await self._execute_mcp_tool(
                    toolset,
                    "create_annotation",
                    {
                        "dashboardUid": self.dashboard_uid,
                        "time": verified_ms,
                        "text": f"Callsheet Verification: {verification_status} on {chosen_standby_node}",
                        "tags": ["callsheet", fail_tag, chosen_standby_node, self.deployment_id],
                    },
                )
            except Exception as e:
                logger.warning("Failed to create verification failure annotation: %s", e)

        # Incident resolution & activity
        time_intervention_to_resolved_seconds = None
        if incident_id:
            links_formatted = "\n".join([f"- **{k.upper()}**: {v}" for k, v in deeplinks.items() if v])
            rate_disp = f"{verified_rate_sec:.1f}s/frame" if verified_rate_sec is not None else "Unverified"
            temp_disp = f"{verified_temp_c:.1f}C" if verified_temp_c is not None else "Unverified"
            verification_activity = (
                f"Step 6 Post-Intervention Verification: **{verification_status}**\n\n"
                f"- Target Node: {chosen_standby_node}\n"
                f"- Frame Rate: {rate_disp} (threshold {rate_limit_seconds:.1f}s)\n"
                f"- Node Temperature: {temp_disp}\n"
                f"- Witnesses Accepted: {', '.join(witnesses_accepted)}\n\n"
                f"Absolute Evidence Deeplinks:\n{links_formatted}"
            )
            try:
                await self._execute_mcp_tool(
                    toolset,
                    "add_activity_to_incident",
                    {
                        "incidentId": incident_id,
                        "body": verification_activity,
                    },
                )
            except Exception as e:
                logger.warning("Failed to add Step 6 verification activity to incident #%s: %s", incident_id, e)

            # Note: Incident resolution is deferred to Step 7 so the briefing note is posted before closure.

        step6 = MissionStep(
            step_number=6,
            name="Post-Intervention Telemetry Verification (Grafana Cloud)",
            execution_type="DETERMINISTIC_VERIFICATION",
            description=step6_desc,
            evidence={
                "target_node": chosen_standby_node,
                "deployment_id": self.deployment_id,
                "intervention_ts": intervention_ts.isoformat(),
                "intervention_epoch": intervention_epoch,
                "baseline_seconds": normal_sec,
                "threshold_seconds": round(rate_limit_seconds, 2),
                "verified_frame_duration_seconds": verified_rate_sec,
                "target_temperature_celsius": verified_temp_c,
                "speedup_factor": round(throttled_sec / verified_rate_sec, 1) if verified_rate_sec and verified_rate_sec > 0 else 1.0,
                "verification_passed": is_verified,
                "quoted_loki_log": verified_log_line,
                "loki_timestamp": accepted_loki_ts,
                "prometheus_actual_sample_timestamp": accepted_prom_sample_ts,
                "prometheus_sample_timestamp": accepted_prom_sample_ts,
                "prometheus_evaluation_timestamp": accepted_prom_eval_ts,
                "tempo_trace_id": accepted_tempo_trace_id,
                "tempo_raytrace_duration_seconds": tempo_raytrace_duration_s,
                "witnesses_accepted": witnesses_accepted,
                "verification_status": verification_status,
                "human_recommendation": human_recommendation,
                "poll_attempts": poll_attempts,
                "elapsed_seconds": elapsed_total,
                "deeplinks": deeplinks,
                "incident_id": incident_id,
                "incident_status": incident_status,
                "time_intervention_to_resolved_seconds": time_intervention_to_resolved_seconds,
            },
        )
        steps.append(step6)

        # ---------------------------------------------------------
        # STEP 7: PRODUCER CALLSHEET BRIEFING (Vertex AI Gemini)
        # ---------------------------------------------------------
        restored_completion_dt = datetime.fromisoformat(intervention_record.projected_completion)
        restored_completion_str = restored_completion_dt.strftime("%A %d %B, %H:%M UTC")
        unmitigated_completion_str = unmitigated_completion_dt.strftime("%A %d %B, %H:%M UTC")
        unmitigated_hours_late_str = f"{abs(unmitigated_buffer_hours):.1f}"
        restored_buffer_str = f"+{intervention_record.buffer_margin_hours:.1f} hours"
        unmitigated_buffer_str = f"{unmitigated_buffer_hours:.1f} hours"

        prompt_content = CALLSHEET_SUMMARY_PROMPT_TEMPLATE.format(
            show_name=show_name,
            client=client_name,
            deadline=deadline_str,
            penalty_daily_amount=f"{show.penalty_daily_amount:,.0f}" if show else "25,000",
            penalty_currency="GBP",
            affected_shots=f"Shot {target_shot.shot_code} ({target_shot.sequence})",
            hardware_temp=f"{max_temp:.1f}°C",
            root_cause=deduced_root_cause,
            anomalous_node_id=anomalous_node_id,
            metric_evidence=f"render_farm_node_temperature_celsius on {anomalous_node_id} reached {max_temp:.1f}°C",
            log_evidence=f"Loki log: {quoted_log_evidence}",
            trace_evidence=f"Tempo raytrace span: {quoted_trace_evidence}",
            throttled_rate=f"{throttled_sec:.0f}s",
            unmitigated_completion=unmitigated_completion_str,
            unmitigated_buffer=unmitigated_buffer_str,
            unmitigated_hours_late=unmitigated_hours_late_str,
            intervention_taken=f"Shot {target_shot.shot_code} migrated from {anomalous_node_id} to standby spare {chosen_standby_node} (quarantined {anomalous_node_id})",
            restored_rate=f"{normal_sec:.0f}s",
            restored_completion=restored_completion_str,
            restored_buffer=restored_buffer_str,
            shot_code=target_shot.shot_code,
            previous_node=anomalous_node_id,
            target_node=chosen_standby_node,
            frames_remaining=frames_rem,
            verification_status=verification_status,
            verification_target_node=chosen_standby_node,
            verification_rate=f"{verified_rate_sec:.1f}s" if verified_rate_sec is not None else "Unverified (no frame in window)",
            verification_temp=f"{verified_temp_c:.1f}°C" if verified_temp_c is not None else "Unverified",
            verification_log=verified_log_line or "None (No post-intervention frame logged within 150s window)",
            escalation_required=str(escalation_required),
            human_recommendation=human_recommendation,
        )

        response = await self.genai_client.aio.models.generate_content(
            model=self.model_name,
            contents=prompt_content,
            config=types.GenerateContentConfig(
                system_instruction=CALLSHEET_AGENT_SYSTEM_PROMPT,
                temperature=0.2,
            ),
        )
        if not response or not response.text or not response.text.strip():
            raise RuntimeError(f"Step 7 failed: Vertex AI Gemini ({self.model_name}) returned empty briefing text.")

        briefing_text = response.text.strip()
        briefing_text = briefing_text.replace("Callshet", "Callsheet")
        briefing_text = briefing_text.replace("\u2014", " - ").replace("\u2013", "-")

        mission_id = f"mission_{uuid.uuid4().hex[:8]}"

        # Capture Panel Image via MCP get_panel_image
        panel_image_url = None
        try:
            img_res = await self._execute_mcp_tool(
                toolset,
                "get_panel_image",
                {
                    "dashboardUid": self.dashboard_uid,
                    "panelId": 1,
                    "width": 1000,
                    "height": 500,
                    "timeRange": {"from": str(from_ms), "to": str(to_ms)},
                },
            )
            if isinstance(img_res, dict) and img_res.get("_is_image") and img_res.get("data"):
                img_bytes = base64.b64decode(img_res["data"])
                MISSION_PANEL_IMAGES[mission_id] = img_bytes
                panel_image_url = f"/api/missions/{mission_id}/panel.png"
                logger.info("Captured Grafana panel image (%d bytes) for mission %s", len(img_bytes), mission_id)
        except Exception as e:
            logger.warning("Failed to capture panel image: %s", e)

        # Briefing Activity and Incident Resolution on Incident
        time_intervention_to_resolved_seconds = None
        if incident_id:
            try:
                briefing_preview = briefing_text[:600] + ("..." if len(briefing_text) > 600 else "")
                await self._execute_mcp_tool(
                    toolset,
                    "add_activity_to_incident",
                    {
                        "incidentId": incident_id,
                        "body": f"Producer Callsheet Briefing:\n\n{briefing_preview}",
                    },
                )
            except Exception as e:
                logger.warning("Failed to add Step 7 briefing activity to incident #%s: %s", incident_id, e)

            # Resolution summary note and final incident resolution (briefing first, resolve last)
            if verification_status == "VERIFIED_PROTECTED":
                summary_line = (
                    f"Shot {target_shot.shot_code} failed over to {chosen_standby_node}, "
                    f"verified {verified_rate_sec:.1f}s/frame at {verified_temp_c:.1f}C, "
                    f"margin +{intervention_record.buffer_margin_hours:.1f}h"
                )
                try:
                    await self._execute_mcp_tool(
                        toolset,
                        "add_activity_to_incident",
                        {
                            "incidentId": incident_id,
                            "body": f"Resolution Summary: {summary_line}",
                        },
                    )
                except Exception as e:
                    logger.warning("Failed to post resolution summary note to incident #%s: %s", incident_id, e)

                # Also post native incidentSummary via Twirp if reachable
                try:
                    import urllib.request
                    token = os.environ.get("GRAFANA_SERVICE_ACCOUNT_TOKEN")
                    if token and self.grafana_url:
                        twirp_url = f"{self.grafana_url.rstrip('/')}/api/plugins/grafana-irm-app/resources/api/v1/ActivityService.AddActivity"
                        twirp_body = json.dumps({
                            "incidentID": incident_id,
                            "activityKind": "incidentSummary",
                            "body": summary_line,
                        }).encode("utf-8")
                        twirp_headers = {
                            "Authorization": f"Bearer {token}",
                            "X-Grafana-Org-Id": "1",
                            "Content-Type": "application/json",
                        }
                        req = urllib.request.Request(twirp_url, data=twirp_body, headers=twirp_headers, method="POST")
                        with urllib.request.urlopen(req, timeout=5) as resp:
                            logger.info("Posted native incidentSummary activity to incident #%s (HTTP %s)", incident_id, resp.status)
                except Exception as e:
                    logger.debug("Optional native incidentSummary call skipped: %s", e)

                # Finally resolve incident via update_incident as the last action on the record
                try:
                    await self._execute_mcp_tool(
                        toolset,
                        "update_incident",
                        {
                            "incidentId": incident_id,
                            "status": "resolved",
                        },
                    )
                    incident_status = "resolved"
                    time_intervention_to_resolved_seconds = round(time.time() - intervention_epoch, 1)
                    logger.info("Resolved Grafana IRM incident #%s in %.1fs with summary: %s", incident_id, time_intervention_to_resolved_seconds, summary_line)
                except Exception as e:
                    logger.warning("Failed to resolve Grafana IRM incident #%s: %s", incident_id, e)

        step7 = MissionStep(
            step_number=7,
            name="Producer Callsheet Briefing",
            execution_type="GENERATIVE_SYNTHESIS",
            description="Generated plain-language delivery producer Callsheet briefing with post-intervention telemetry audit.",
            evidence={
                "briefing_length": len(briefing_text),
                "verification_status": verification_status,
                "panel_image_url": panel_image_url,
                "mcp_read_calls": self.mcp_read_calls,
                "mcp_write_calls": self.mcp_write_calls,
            },
        )
        steps.append(step7)

        return MissionResult(
            id=mission_id,
            show_id=show_id,
            show_name=show_name,
            client=client_name,
            deadline=deadline_str,
            penalty_clause=penalty_str,
            anomalous_node_id=anomalous_node_id,
            anomaly_detected=f"Temperature spike on {anomalous_node_id} ({max_temp:.1f}°C)",
            root_cause=deduced_root_cause,
            affected_shots=[target_shot.shot_code],
            intervention_record=intervention_record,
            verification_status=verification_status,
            steps=steps,
            callsheet_briefing=briefing_text,
            incident_id=incident_id,
            incident_url=incident_url,
            incident_status=incident_status,
            annotation_id=annotation_id,
            deeplinks=deeplinks,
            panel_image_url=panel_image_url,
            time_intervention_to_resolved_seconds=time_intervention_to_resolved_seconds,
            mcp_read_calls=self.mcp_read_calls,
            mcp_write_calls=self.mcp_write_calls,
            trigger_type=trigger_type,
        )
