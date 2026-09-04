"""
Unit tests for Callsheet Step 6 Closed-Loop Telemetry Verification.
Verifies failure modes, timestamp filtering, criteria enforcement, and absence of hardcoded values.
"""

import ast
from datetime import datetime, timezone
import pytest
from unittest.mock import patch

from callsheet.agent.mission import (
    MultiStepMissionRunner,
    parse_loki_frame_duration,
    parse_loki_timestamp,
)
from callsheet.farm.models import ScenarioType
from callsheet.farm.simulator import RenderFarmSimulator
from callsheet.interventions.dispatcher import InterventionDispatcher


def test_parse_loki_timestamp_formats():
    """Verifies that nanosecond strings, ints, floats, and ISO strings parse correctly."""
    assert parse_loki_timestamp("\"1788539686823045632\"") == pytest.approx(1788539686.823, abs=0.001)
    assert parse_loki_timestamp(1788539686823045632) == pytest.approx(1788539686.823, abs=0.001)
    assert parse_loki_timestamp(1788539686.823) == pytest.approx(1788539686.823, abs=0.001)
    iso_ts = "2026-09-04T16:34:46.823Z"
    assert parse_loki_timestamp(iso_ts) is not None
    assert parse_loki_timestamp("") is None
    assert parse_loki_timestamp(None) is None


def test_parse_loki_frame_duration():
    """Verifies extraction of durations from standard and degraded Loki log lines."""
    line_normal = "Frame 1061 rendered on node-11 successfully in 20.0s."
    assert parse_loki_frame_duration(line_normal) == 20.0

    line_degraded = "Frame 1061 rendered on node-11 with DEGRADED PERFORMANCE (40.0s). Thermal throttle active."
    assert parse_loki_frame_duration(line_degraded) == 40.0

    line_invalid = "Frame 1061 rendered on node-11 with unknown error."
    assert parse_loki_frame_duration(line_invalid) is None


@pytest.mark.asyncio
async def test_verification_fails_closed_when_loki_returns_nothing():
    """
    Verification must fail closed when Loki returns no frame completion lines:
    Status must be VERIFICATION_INCONCLUSIVE and intervention status left at PENDING_VERIFICATION.
    """
    sim = RenderFarmSimulator()
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    dispatcher = InterventionDispatcher(sim)

    runner = MultiStepMissionRunner(
        dispatcher=dispatcher,
        project_id="test-project",
        location="global",
        model_name="gemini-3.6-flash",
    )

    async def mock_mcp(toolset, tool_name, arguments):
        if tool_name == "query_prometheus":
            if "node-11" in arguments.get("expr", "") or "node-12" in arguments.get("expr", ""):
                return {"data": [{"metric": {"node_id": "node-11"}, "value": [datetime.now(timezone.utc).timestamp() + 5.0, "62.5"]}]}
            return {"data": [{"metric": {"node_id": "node-07"}, "value": [datetime.now(timezone.utc).timestamp(), "94.8"]}]}
        if tool_name == "query_loki_logs":
            if "node-07" in arguments.get("logql", ""):
                return {"data": [{"line": "CRITICAL: Thermal junction temperature on node-07 reached 94.8C. Hardware clock down-throttled to 800MHz."}]}
            return {"data": []}
        if tool_name == "grafana_api_request":
            ep = arguments.get("endpoint", "")
            if "/api/search" in ep:
                return {"data": {"traces": [{"traceID": "abc123def456", "durationMs": 65000, "startTimeUnixNano": 1000000}]}}
            if "/api/traces/" in ep:
                return {"data": {"batches": [{"scopeSpans": [{"spans": [{"name": "raytrace_volumetrics_pass", "startTimeUnixNano": 0, "endTimeUnixNano": 65000000000}]}]}]}}
            return {}

    with patch.object(runner, "_execute_mcp_tool", side_effect=mock_mcp), \
         patch.object(runner.genai_client.models, "generate_content") as mock_gemini:
        mock_gemini.return_value.text = "Briefing: Root cause deduced and verification inconclusive."

        result = await runner.execute_mission(
            show_id="show-aethelgard",
            max_poll_seconds=0.1,
            poll_interval_seconds=0.05,
        )

        assert result.verification_status == "VERIFICATION_INCONCLUSIVE"
        assert result.intervention_record.status == "PENDING_VERIFICATION"
        step6_evidence = result.steps[5].evidence
        assert step6_evidence["verification_passed"] is False
        assert step6_evidence["verified_frame_duration_seconds"] is None
        assert step6_evidence["target_temperature_celsius"] is None


@pytest.mark.asyncio
async def test_verification_rejects_line_timestamped_before_intervention():
    """
    Verification must reject log lines timestamped before the intervention timestamp.
    """
    sim = RenderFarmSimulator()
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    dispatcher = InterventionDispatcher(sim)

    runner = MultiStepMissionRunner(
        dispatcher=dispatcher,
        project_id="test-project",
        location="global",
        model_name="gemini-3.6-flash",
    )

    async def mock_mcp(toolset, tool_name, arguments):
        if tool_name == "query_prometheus":
            if "node-11" in arguments.get("expr", "") or "node-12" in arguments.get("expr", ""):
                return {"data": [{"metric": {"node_id": "node-11"}, "value": [datetime.now(timezone.utc).timestamp() + 5.0, "61.0"]}]}
            return {"data": [{"metric": {"node_id": "node-07"}, "value": [datetime.now(timezone.utc).timestamp(), "94.8"]}]}
        if tool_name == "query_loki_logs":
            if "node-07" in arguments.get("logql", ""):
                return {"data": [{"line": "CRITICAL: Thermal junction temperature on node-07 reached 94.8C. Hardware clock down-throttled to 800MHz."}]}
            old_ts = int((datetime.now(timezone.utc).timestamp() - 3600) * 1e9)
            return {"data": [{
                "line": "Frame 1060 rendered on node-11 successfully in 20.0s.",
                "timestamp": str(old_ts),
            }]}
        if tool_name == "grafana_api_request":
            ep = arguments.get("endpoint", "")
            if "/api/search" in ep:
                return {"data": {"traces": [{"traceID": "abc123def456", "durationMs": 65000, "startTimeUnixNano": 1000000}]}}
            if "/api/traces/" in ep:
                return {"data": {"batches": [{"scopeSpans": [{"spans": [{"name": "raytrace_volumetrics_pass", "startTimeUnixNano": 0, "endTimeUnixNano": 65000000000}]}]}]}}
            return {}

    with patch.object(runner, "_execute_mcp_tool", side_effect=mock_mcp), \
         patch.object(runner.genai_client.models, "generate_content") as mock_gemini:
        mock_gemini.return_value.text = "Briefing: Old line rejected."

        result = await runner.execute_mission(
            show_id="show-aethelgard",
            max_poll_seconds=0.1,
            poll_interval_seconds=0.05,
        )

        assert result.verification_status == "VERIFICATION_INCONCLUSIVE"
        assert result.intervention_record.status == "PENDING_VERIFICATION"


@pytest.mark.asyncio
async def test_verification_passes_only_when_duration_and_temp_meet_criteria():
    """
    Verification passes (VERIFIED_PROTECTED) only when frame duration <= baseline * 1.25
    and temperature < 90.0. Escalates when exceeded.
    """
    sim = RenderFarmSimulator()
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    dispatcher = InterventionDispatcher(sim)

    runner = MultiStepMissionRunner(
        dispatcher=dispatcher,
        project_id="test-project",
        location="global",
        model_name="gemini-3.6-flash",
    )

    now_epoch = datetime.now(timezone.utc).timestamp()
    future_epoch = now_epoch + 10.0
    future_ns = int(future_epoch * 1e9)

    async def mock_mcp_nominal(toolset, tool_name, arguments):
        if tool_name == "query_prometheus":
            if "node-11" in arguments.get("expr", "") or "node-12" in arguments.get("expr", ""):
                return {"data": [{"metric": {"node_id": "node-11"}, "value": [future_epoch, "62.4"]}]}
            return {"data": [{"metric": {"node_id": "node-07"}, "value": [now_epoch, "94.8"]}]}
        if tool_name == "query_loki_logs":
            if "node-07" in arguments.get("logql", ""):
                return {"data": [{"line": "CRITICAL: Thermal junction temperature on node-07 reached 94.8C. Hardware clock down-throttled to 800MHz."}]}
            return {"data": [{
                "line": "Frame 1061 rendered on node-11 successfully in 20.0s.",
                "timestamp": str(future_ns),
            }]}
        if tool_name == "grafana_api_request":
            ep = arguments.get("endpoint", "")
            if "node-07" in ep and "/api/search" in ep:
                return {"data": {"traces": [{"traceID": "abc123def456", "durationMs": 65000, "startTimeUnixNano": 1000000}]}}
            if "/api/search" in ep:
                return {"data": {"traces": [{"traceID": "trace999", "durationMs": 20000, "startTimeUnixNano": future_ns}]}}
            if "trace999" in ep:
                return {"data": {"batches": [{"scopeSpans": [{"spans": [{"name": "raytrace_volumetrics_pass", "startTimeUnixNano": 0, "endTimeUnixNano": 17000000000}]}]}]}}
            if "/api/traces/" in ep:
                return {"data": {"batches": [{"scopeSpans": [{"spans": [{"name": "raytrace_volumetrics_pass", "startTimeUnixNano": 0, "endTimeUnixNano": 65000000000}]}]}]}}
            return {}

    with patch.object(runner, "_execute_mcp_tool", side_effect=mock_mcp_nominal), \
         patch.object(runner.genai_client.models, "generate_content") as mock_gemini:
        mock_gemini.return_value.text = "Briefing: All good."
        res_pass = await runner.execute_mission(
            show_id="show-aethelgard",
            max_poll_seconds=1.0,
            poll_interval_seconds=0.1,
        )
        assert res_pass.verification_status == "VERIFIED_PROTECTED"
        assert res_pass.intervention_record.status == "PROTECTED"
        assert res_pass.steps[5].evidence["verification_passed"] is True
        assert res_pass.steps[5].evidence["verified_frame_duration_seconds"] == 20.0
        assert res_pass.steps[5].evidence["target_temperature_celsius"] == 62.4

    # Case B: Degraded duration (40.0s > 25.0s threshold) triggers escalation
    sim_b = RenderFarmSimulator()
    sim_b.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    dispatcher_b = InterventionDispatcher(sim_b)
    runner_b = MultiStepMissionRunner(
        dispatcher=dispatcher_b,
        project_id="test-project",
        location="global",
        model_name="gemini-3.6-flash",
    )

    async def mock_mcp_degraded(toolset, tool_name, arguments):
        if tool_name == "query_prometheus":
            if "node-11" in arguments.get("expr", "") or "node-12" in arguments.get("expr", ""):
                return {"data": [{"metric": {"node_id": "node-11"}, "value": [future_epoch, "63.0"]}]}
            return {"data": [{"metric": {"node_id": "node-07"}, "value": [now_epoch, "94.8"]}]}
        if tool_name == "query_loki_logs":
            if "node-07" in arguments.get("logql", ""):
                return {"data": [{"line": "CRITICAL: Thermal junction temperature on node-07 reached 94.8C. Hardware clock down-throttled to 800MHz."}]}
            return {"data": [{
                "line": "Frame 1061 rendered on node-11 with DEGRADED PERFORMANCE (40.0s). Thermal throttle active.",
                "timestamp": str(future_ns),
            }]}
        if tool_name == "grafana_api_request":
            ep = arguments.get("endpoint", "")
            if "node-07" in ep and "/api/search" in ep:
                return {"data": {"traces": [{"traceID": "abc123def456", "durationMs": 65000, "startTimeUnixNano": 1000000}]}}
            if "/api/traces/" in ep:
                return {"data": {"batches": [{"scopeSpans": [{"spans": [{"name": "raytrace_volumetrics_pass", "startTimeUnixNano": 0, "endTimeUnixNano": 65000000000}]}]}]}}
            return {"data": {"traces": []}}
        return {}

    with patch.object(runner_b, "_execute_mcp_tool", side_effect=mock_mcp_degraded), \
         patch.object(runner_b.genai_client.models, "generate_content") as mock_gemini:
        mock_gemini.return_value.text = "Briefing: Escalated."
        res_fail = await runner_b.execute_mission(
            show_id="show-aethelgard",
            max_poll_seconds=1.0,
            poll_interval_seconds=0.1,
        )
        assert res_fail.verification_status == "ESCALATED"
        assert res_fail.intervention_record.status == "ESCALATED"
        assert res_fail.steps[5].evidence["verification_passed"] is False
        assert res_fail.steps[5].evidence["verified_frame_duration_seconds"] == 40.0


def test_no_code_path_assigns_unparsed_frame_duration():
    """
    AST inspection test:
    Asserts that mission.py never assigns a hardcoded or synthetic constant to verified_rate_sec.
    """
    import inspect
    from callsheet.agent import mission

    source = inspect.getsource(mission)
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "verified_rate_sec":
                    if isinstance(node.value, ast.Constant):
                        assert node.value.value is None, (
                            f"Illegal hardcoded assignment to verified_rate_sec: {node.value.value}"
                        )
                    elif isinstance(node.value, ast.Name):
                        assert node.value.id not in ("normal_sec", "throttled_sec", "20.0"), (
                            f"Illegal synthetic variable assigned to verified_rate_sec: {node.value.id}"
                        )
