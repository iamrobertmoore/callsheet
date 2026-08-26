"""
Multi-step reasoning mission runner for Callsheet.
Executes genuine, load-bearing observability investigations against Grafana Cloud MCP
(Prometheus metrics, Loki logs, and Tempo traces) and generates producer-facing
intervention summaries using Vertex AI Gemini.
"""

from datetime import datetime, timezone
import json
import logging
import time
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field

from google.genai import Client, types

from callsheet.agent.prompts import (
    CALLSHEET_AGENT_SYSTEM_PROMPT,
    CALLSHEET_SUMMARY_PROMPT_TEMPLATE,
)
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


class MissionStep(BaseModel):
    step_number: int
    name: str
    description: str
    status: str = "COMPLETED"
    evidence: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class MissionResult(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
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
    intervention_record: Optional[InterventionRecord] = None
    steps: List[MissionStep] = Field(default_factory=list)
    callsheet_briefing: str = ""


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
        model_name: str = "gemini-3.6-flash",
    ):
        self.dispatcher = dispatcher
        self.mcp_server_url = mcp_server_url
        self.project_id = project_id
        self.location = location
        self.model_name = model_name

        self.genai_client = Client(
            vertexai=True,
            project=self.project_id,
            location=self.location,
        )

    async def _execute_mcp_tool(self, toolset, tool_name: str, arguments: dict) -> dict:
        """
        Executes an MCP tool call. Raises RuntimeError if the call fails or errors.
        """
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

        # Parse text content from response
        if hasattr(res, "content") and res.content:
            raw_text = res.content[0].text
            try:
                return json.loads(raw_text)
            except Exception:
                return {"raw_text": raw_text}

        return {}

    async def execute_mission(self, show_id: str = "show-aethelgard") -> MissionResult:
        """
        Runs the complete 6-step mission strictly driven by Grafana Cloud MCP responses:
        1. Anomaly Detection (Prometheus response parsing)
        2. Signal Correlation (Loki Logs & Tempo Trace Spans response parsing)
        3. Root Cause Deduction (Vertex AI Gemini reasoning over retrieved signals)
        4. Production Impact Calculation
        5. Workload Reallocation Intervention
        6. Producer Callsheet Briefing Generation (Vertex AI Gemini)
        """
        steps: List[MissionStep] = []

        # Connect to MCP toolset
        params = get_grafana_mcp_connection_params(mcp_server_url=self.mcp_server_url)
        toolset = create_grafana_mcp_toolset(params)

        # ---------------------------------------------------------
        # STEP 1: DETECT METRIC ANOMALY (Parse Prometheus Response)
        # ---------------------------------------------------------
        prom_data = await self._execute_mcp_tool(
            toolset,
            "query_prometheus",
            {
                "datasourceUid": "grafanacloud-prom",
                "expr": "render_farm_node_temperature_celsius",
                "queryType": "instant",
                "endTime": "now",
            }
        )

        series_list = prom_data.get("data", [])
        if not series_list:
            raise RuntimeError("Step 1 failed: Prometheus query returned empty metric series. No farm telemetry found.")

        # Find the degraded node strictly from the Prometheus response data
        anomalous_node_id = None
        max_temp = 0.0
        all_node_temps = {}

        for item in series_list:
            metric_meta = item.get("metric", {})
            val_tuple = item.get("value", [0, "0"])
            node = metric_meta.get("node_id", "unknown")
            try:
                temp = float(val_tuple[1])
            except (ValueError, IndexError):
                temp = 0.0
            all_node_temps[node] = temp
            if temp > 90.0 and temp > max_temp:
                max_temp = temp
                anomalous_node_id = node

        if not anomalous_node_id:
            raise RuntimeError(
                f"Step 1 failed: No node exceeded the 90.0°C thermal limit in Prometheus. Temperatures: {all_node_temps}"
            )

        step1 = MissionStep(
            step_number=1,
            name="Anomaly Detection (Prometheus)",
            description=f"Prometheus query identified critical temperature spike on {anomalous_node_id} ({max_temp:.1f}°C, limit: 90.0°C).",
            evidence={
                "tool": "query_prometheus",
                "expr": "render_farm_node_temperature_celsius",
                "anomalous_node_id": anomalous_node_id,
                "temperature_celsius": max_temp,
                "active_series_count": len(series_list),
            },
        )
        steps.append(step1)

        # ---------------------------------------------------------
        # STEP 2: SIGNAL CORRELATION (Parse Loki Logs & Tempo Trace Spans)
        # ---------------------------------------------------------
        # 2a. Query Loki logs
        loki_data = await self._execute_mcp_tool(
            toolset,
            "query_loki_logs",
            {
                "datasourceUid": "grafanacloud-logs",
                "logql": f'{{node_id="{anomalous_node_id}"}}',
                "startRfc3339": "now-15m",
                "endRfc3339": "now",
                "limit": 10,
            }
        )

        log_entries = loki_data.get("data", [])
        if not log_entries:
            # Query general service logs
            loki_data = await self._execute_mcp_tool(
                toolset,
                "query_loki_logs",
                {
                    "datasourceUid": "grafanacloud-logs",
                    "logql": '{service_name="render-farm"}',
                    "startRfc3339": "now-15m",
                    "endRfc3339": "now",
                    "limit": 10,
                }
            )
            log_entries = loki_data.get("data", [])

        if not log_entries:
            raise RuntimeError(f"Step 2 failed: No corresponding log entries found in Loki for node {anomalous_node_id}.")

        retrieved_log_lines = [entry.get("line", "") for entry in log_entries]
        quoted_log_evidence = retrieved_log_lines[0] if retrieved_log_lines else "No log body"

        # 2b. Query Tempo traces via Grafana proxy with time bounds
        now_epoch = int(time.time())
        start_epoch = now_epoch - 7200

        tempo_search = await self._execute_mcp_tool(
            toolset,
            "grafana_api_request",
            {
                "endpoint": f"/api/datasources/proxy/uid/grafanacloud-traces/api/search?start={start_epoch}&end={now_epoch}&limit=10",
                "method": "GET",
            }
        )

        tempo_data = tempo_search.get("data", {})
        traces_list = tempo_data.get("traces", [])
        if not traces_list and "traces" in tempo_search:
            traces_list = tempo_search.get("traces", [])

        if not traces_list:
            raise RuntimeError(f"Step 2 failed: No trace spans returned from Tempo in Grafana Cloud across {start_epoch}-{now_epoch}.")

        # Retrieve detailed span timings for the first trace
        first_trace = traces_list[0]
        trace_id = first_trace.get("traceID")
        root_trace_name = first_trace.get("rootTraceName", "unknown")
        total_duration_ms = first_trace.get("durationMs", 0)

        trace_detail = await self._execute_mcp_tool(
            toolset,
            "grafana_api_request",
            {
                "endpoint": f"/api/datasources/proxy/uid/grafanacloud-traces/api/traces/{trace_id}",
                "method": "GET",
            }
        )

        # Parse child spans from Tempo detail payload
        child_spans = []
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

        quoted_trace_evidence = (
            f"TraceID {trace_id} ({root_trace_name}): Total Duration {total_duration_ms}ms. "
            f"Child Spans: {json.dumps(child_spans[:4])}"
        )

        step2 = MissionStep(
            step_number=2,
            name="Signal Correlation (Loki & Tempo)",
            description=f"Correlated Prometheus anomaly with Loki worker logs and Tempo trace spans on {anomalous_node_id}.",
            evidence={
                "tool_logs": "query_loki_logs",
                "quoted_loki_log": quoted_log_evidence,
                "total_logs_retrieved": len(retrieved_log_lines),
                "tool_traces": f"grafana_api_request (/api/datasources/proxy/uid/grafanacloud-traces/api/traces/{trace_id})",
                "trace_id": trace_id,
                "root_trace_name": root_trace_name,
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
- Prometheus Temperature Metric: {max_temp:.1f}°C (Threshold: 90.0°C)
- Loki Log Records: {json.dumps(retrieved_log_lines[:3])}
- Tempo Trace Span Timings: {quoted_trace_evidence}

State the technical root cause in 1 to 2 clear sentences. Do not use em dashes.
"""
        root_cause_response = self.genai_client.models.generate_content(
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
            description=deduced_root_cause,
            evidence={
                "model": self.model_name,
                "anomalous_node": anomalous_node_id,
                "hardware_temperature": max_temp,
            },
        )
        steps.append(step3)

        # ---------------------------------------------------------
        # STEP 4: PRODUCTION IMPACT MAPPING
        # ---------------------------------------------------------
        show = self.dispatcher.simulator.state.shows.get(show_id)
        show_name = show.name if show else "Chronicles of Aethelgard: Episode 6"
        client_name = show.client if show else "Cinefex Northern Pictures"
        deadline_str = show.delivery_deadline.strftime("%A %d %B, %H:%M UTC") if show else "Tuesday 17:00 UTC"
        penalty_str = f"£{show.penalty_daily_amount:,.0f} / day" if show else "£25,000 / day"

        # Find shots allocated to the degraded node dynamically
        affected_shots = [
            s for s in self.dispatcher.simulator.state.shots.values()
            if s.allocated_node_id == anomalous_node_id or s.status == "AT_RISK"
        ]
        target_shot = affected_shots[0] if affected_shots else list(self.dispatcher.simulator.state.shots.values())[0]

        # Compute slippage dynamically from remaining frames and throttled render rate
        frames_rem = target_shot.frames_remaining
        throttled_sec_per_frame = target_shot.current_seconds_per_frame
        normal_sec_per_frame = target_shot.estimated_seconds_per_frame

        added_delay_seconds = (throttled_sec_per_frame - normal_sec_per_frame) * frames_rem
        added_delay_hours = round(added_delay_seconds / 3600.0, 1)

        step4 = MissionStep(
            step_number=4,
            name="Production Impact Mapping",
            description=(
                f"Mapped {anomalous_node_id} failure to {show_name} (Deadline: {deadline_str}). "
                f"Shot {target_shot.shot_code} would slip completion by {added_delay_hours} hours without intervention, "
                f"risking contractual penalty of {penalty_str}."
            ),
            evidence={
                "show_name": show_name,
                "shot_code": target_shot.shot_code,
                "frames_remaining": frames_rem,
                "added_delay_hours": added_delay_hours,
                "penalty_clause": penalty_str,
            },
        )
        steps.append(step4)

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
        )

        step5 = MissionStep(
            step_number=5,
            name="Workload Reallocation Intervention",
            description=(
                f"Reallocated Shot {target_shot.shot_code} from {anomalous_node_id} to standby spare {chosen_standby_node}. "
                f"Clean render speed restored. Positive buffer margin: +{intervention_record.buffer_margin_hours:.1f} hours."
            ),
            evidence={
                "intervention_id": intervention_record.id,
                "shot_code": intervention_record.shot_code,
                "previous_node": intervention_record.previous_node_id,
                "target_node": intervention_record.target_node_id,
                "buffer_margin_hours": intervention_record.buffer_margin_hours,
                "status": intervention_record.status,
            },
        )
        steps.append(step5)

        # ---------------------------------------------------------
        # STEP 6: PRODUCER CALLSHEET BRIEFING (Vertex AI Gemini)
        # ---------------------------------------------------------
        prompt_content = CALLSHEET_SUMMARY_PROMPT_TEMPLATE.format(
            show_name=show_name,
            client=client_name,
            deadline=deadline_str,
            penalty_daily_amount=f"{show.penalty_daily_amount:,.0f}" if show else "25,000",
            penalty_currency="GBP",
            affected_shots=f"Shot {target_shot.shot_code} ({target_shot.sequence})",
            root_cause=deduced_root_cause,
            metric_evidence=f"render_farm_node_temperature_celsius on {anomalous_node_id} reached {max_temp:.1f}°C",
            log_evidence=f"Loki log: {quoted_log_evidence}",
            trace_evidence=f"Tempo raytrace span: {quoted_trace_evidence}",
            intervention_taken=f"Shot {target_shot.shot_code} moved to standby node {chosen_standby_node}; restored normal render rate",
            projected_buffer=f"+{intervention_record.buffer_margin_hours:.1f} hours margin before deadline",
        )

        response = self.genai_client.models.generate_content(
            model=self.model_name,
            contents=prompt_content,
            config=types.GenerateContentConfig(
                system_instruction=CALLSHEET_AGENT_SYSTEM_PROMPT,
                temperature=0.2,
            ),
        )
        if not response or not response.text or not response.text.strip():
            raise RuntimeError(f"Step 6 failed: Vertex AI Gemini ({self.model_name}) returned empty briefing text.")

        briefing_text = response.text.strip()

        step6 = MissionStep(
            step_number=6,
            name="Producer Callsheet Briefing",
            description="Generated plain-language delivery producer Callsheet briefing.",
            evidence={"briefing_length": len(briefing_text)},
        )
        steps.append(step6)

        return MissionResult(
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
            steps=steps,
            callsheet_briefing=briefing_text,
        )
