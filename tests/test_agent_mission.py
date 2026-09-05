"""
Tests for Callsheet Multi-Step Mission Runner, Grafana Cloud MCP data-driven reasoning,
and failure modes when Grafana is unreachable.
"""

import asyncio
import os
import subprocess
import time
import pytest
from dotenv import load_dotenv

from callsheet.agent.mission import MultiStepMissionRunner
from callsheet.farm.emitter import FarmTelemetryEmitter
from callsheet.farm.models import ScenarioType
from callsheet.farm.simulator import RenderFarmSimulator
from callsheet.interventions.dispatcher import InterventionDispatcher
from callsheet.mcp.client import (
    create_grafana_mcp_toolset,
    get_grafana_mcp_connection_params,
)

load_dotenv()


@pytest.mark.asyncio
async def test_full_six_step_mission_against_live_grafana():
    """
    1. Injects thermal throttling on simulator and emits live telemetry to Grafana Cloud via OTLP.
    2. Spawns mcp-grafana server locally.
    3. Runs MultiStepMissionRunner which strictly parses the Prometheus response,
       queries Loki logs for that node, has Gemini 2.5 deduce the root cause,
       reallocates the shot to standby node-12, and synthesizes the producer briefing.
    4. Asserts all 6 steps completed based on real retrieved data.
    """
    sim = RenderFarmSimulator()
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    emitter = FarmTelemetryEmitter(sim)

    # Emit telemetry into Grafana Cloud
    events = sim.tick(delta_seconds=30.0)
    emitter.emit_metrics_tick()
    node7_temp = sim.state.nodes["node-07"].temperature_celsius
    emitter.emit_log(
        f"CRITICAL: Thermal junction temperature on node-07 reached {node7_temp:.1f}C (threshold: 90.0C). Hardware clock down-throttled to 800MHz.",
        level="WARN",
        node_id="node-07",
        shot_code="118",
        show_id="show-aethelgard",
        frame_number=1025,
    )
    emitter.emit_frame_trace(
        node_id="node-07",
        shot_code="118",
        show_name="Chronicles of Aethelgard: Episode 6",
        frame_number=1025,
        duration_seconds=120.0,
        is_throttled=True,
    )
    emitter.metric_reader.force_flush()
    emitter.logger_provider.force_flush()
    emitter.tracer_provider.force_flush()

    # Wait briefly for Grafana Cloud indexing
    time.sleep(8)

    dispatcher = InterventionDispatcher(sim)

    mcp_bin = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "bin", "mcp-grafana"))
    if not os.path.exists(mcp_bin):
        pytest.skip(f"mcp-grafana binary not found at {mcp_bin}")

    port = 8139
    server_url = f"http://127.0.0.1:{port}/mcp"
    env = os.environ.copy()
    env["GRAFANA_ORG_ID"] = "1"

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

    result = None
    try:
        time.sleep(2)
        assert proc.poll() is None, "mcp-grafana failed to start"

        runner = MultiStepMissionRunner(
            dispatcher=dispatcher,
            mcp_server_url=server_url,
            project_id="agent-attest-2026",
            location="global",
            model_name="gemini-3.8-flash",
        )

        async def bg_ticker():
            try:
                while True:
                    await asyncio.sleep(3.0)
                    events = sim.tick(delta_seconds=3.0)
                    emitter.emit_metrics_tick()
                    if events:
                        emitter.process_events(events)
                    emitter.metric_reader.force_flush()
                    emitter.logger_provider.force_flush()
                    emitter.tracer_provider.force_flush()
            except asyncio.CancelledError:
                pass

        tick_task = asyncio.create_task(bg_ticker())
        try:
            result = await runner.execute_mission(show_id="show-aethelgard")
        finally:
            tick_task.cancel()
            await asyncio.gather(tick_task, return_exceptions=True)

        # Verify mission structure and genuine data extraction
        assert result.show_id == "show-aethelgard"
        assert result.anomalous_node_id == "node-07"
        assert len(result.steps) in (7, 8)
        assert result.intervention_record is not None
        assert result.intervention_record.previous_node_id == "node-07"
        assert result.intervention_record.target_node_id.startswith("node-")
        assert result.intervention_record.status == "PROTECTED"
        assert result.verification_status == "VERIFIED_PROTECTED"

        # Verify Step 0 if present
        step0 = next((s for s in result.steps if s.step_number == 0), None)
        if step0:
            assert step0.execution_type == "DETERMINISTIC_ALERT"
            assert "node-07" in step0.description

        # Verify Step 6 (Post-Intervention Telemetry Verification)
        step6 = next(s for s in result.steps if s.step_number == 6)
        assert step6.name == "Post-Intervention Telemetry Verification (Grafana Cloud)"
        assert step6.execution_type == "DETERMINISTIC_VERIFICATION"
        assert step6.evidence["verification_passed"] is True
        assert step6.evidence["target_node"].startswith("node-")

        # Verify Step 7 (Producer Callsheet Briefing)
        step7 = next(s for s in result.steps if s.step_number == 7)
        assert step7.name == "Producer Callsheet Briefing"
        assert len(result.callsheet_briefing) > 50

        # Verify Section 2 Write-back artifacts and metrics
        assert result.incident_id is not None
        assert result.incident_status == "resolved"
        assert result.deeplinks.get("prometheus")
        assert result.deeplinks.get("loki")
        assert result.deeplinks.get("tempo")
        assert result.deeplinks.get("dashboard")
        assert result.mcp_read_calls > 0
        assert result.mcp_write_calls > 0

        # Assert no em dashes in briefing
        assert "—" not in result.callsheet_briefing, "Briefing must not contain em dashes"

        print("\n=== LIVE GENERATED CALLSHEET BRIEFING (7-STEP VERIFIED) ===")
        print(result.callsheet_briefing)

    finally:
        # Teardown: clean up created annotation to keep Grafana stack clean
        if result and result.annotation_id:
            try:
                cleanup_params = get_grafana_mcp_connection_params(mcp_server_url=server_url)
                cleanup_toolset = create_grafana_mcp_toolset(cleanup_params)
                await cleanup_toolset._execute_with_session(
                    lambda session: session.call_tool("grafana_api_request", {
                        "endpoint": f"/api/annotations/{result.annotation_id}",
                        "method": "DELETE",
                    }),
                    "Delete test annotation",
                )
            except Exception as ex:
                print("Note: annotation cleanup error:", ex)

        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()


@pytest.mark.asyncio
async def test_post_intervention_verification_escalation_path():
    """
    Verifies the testable failure path:
    When the failover standby node fails post-intervention telemetry verification,
    the mission must NOT claim success. It must set status ESCALATED, record the
    failure evidence, and provide explicit human TD escalation recommendations.
    """
    sim = RenderFarmSimulator()
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    emitter = FarmTelemetryEmitter(sim)

    # Emit telemetry into Grafana Cloud
    sim.tick(delta_seconds=30.0)
    emitter.emit_metrics_tick()
    node7_temp = sim.state.nodes["node-07"].temperature_celsius
    emitter.emit_log(
        f"CRITICAL: Thermal junction temperature on node-07 reached {node7_temp:.1f}C (threshold: 90.0C). Hardware clock down-throttled to 800MHz.",
        level="WARN",
        node_id="node-07",
        shot_code="118",
        show_id="show-aethelgard",
        frame_number=1025,
    )
    emitter.emit_frame_trace(
        node_id="node-07",
        shot_code="118",
        show_name="Chronicles of Aethelgard: Episode 6",
        frame_number=1025,
        duration_seconds=120.0,
        is_throttled=True,
    )
    emitter.metric_reader.force_flush()
    emitter.logger_provider.force_flush()
    emitter.tracer_provider.force_flush()

    time.sleep(8)

    dispatcher = InterventionDispatcher(sim)

    mcp_bin = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "bin", "mcp-grafana"))
    if not os.path.exists(mcp_bin):
        pytest.skip(f"mcp-grafana binary not found at {mcp_bin}")

    port = 8140
    server_url = f"http://127.0.0.1:{port}/mcp"
    env = os.environ.copy()
    env["GRAFANA_ORG_ID"] = "1"

    cmd = [
        mcp_bin,
        "-t", "streamable-http",
        "-address", f"127.0.0.1:{port}",
        "-endpoint-path", "/mcp",
        "-log-level", "info",
    ]

    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    result = None
    try:
        time.sleep(2)
        assert proc.poll() is None, "mcp-grafana failed to start"

        runner = MultiStepMissionRunner(
            dispatcher=dispatcher,
            mcp_server_url=server_url,
            project_id="agent-attest-2026",
            location="global",
            model_name="gemini-3.8-flash",
        )

        # Force verification fault
        async def bg_ticker():
            try:
                while True:
                    await asyncio.sleep(3.0)
                    events = sim.tick(delta_seconds=6.0)
                    emitter.emit_metrics_tick()
                    if events:
                        emitter.process_events(events)
                    emitter.metric_reader.force_flush()
                    emitter.logger_provider.force_flush()
                    emitter.tracer_provider.force_flush()
            except asyncio.CancelledError:
                pass

        tick_task = asyncio.create_task(bg_ticker())
        try:
            result = await runner.execute_mission(
                show_id="show-aethelgard",
                force_verification_fault=True,
                max_poll_seconds=60.0,
            )
        finally:
            tick_task.cancel()
            await asyncio.gather(tick_task, return_exceptions=True)

        assert len(result.steps) in (7, 8)
        assert result.verification_status == "ESCALATED"
        assert result.intervention_record.status == "ESCALATED"
        step6 = next(s for s in result.steps if s.step_number == 6)
        assert step6.evidence["verification_passed"] is False
        assert "HUMAN" in step6.evidence["human_recommendation"].upper()
        assert "—" not in result.callsheet_briefing

        # Verify incident creation for escalation path
        assert result.incident_id is not None
        assert result.incident_status == "active"

        print("\n=== ESCALATED BRIEFING (VERIFICATION FAILURE) ===")
        print(result.callsheet_briefing)

    finally:
        # Teardown: resolve incident and clean annotations so stack remains clean
        if result:
            cleanup_params = get_grafana_mcp_connection_params(mcp_server_url=server_url)
            cleanup_toolset = create_grafana_mcp_toolset(cleanup_params)
            if result.incident_id:
                try:
                    await cleanup_toolset._execute_with_session(
                        lambda session: session.call_tool("update_incident", {
                            "incidentId": str(result.incident_id),
                            "status": "resolved",
                        }),
                        "Resolve escalation test incident",
                    )
                except Exception as ex:
                    print("Note: incident resolve error:", ex)
            if result.annotation_id:
                try:
                    await cleanup_toolset._execute_with_session(
                        lambda session: session.call_tool("grafana_api_request", {
                            "endpoint": f"/api/annotations/{result.annotation_id}",
                            "method": "DELETE",
                        }),
                        "Delete test annotation",
                    )
                except Exception as ex:
                    print("Note: annotation cleanup error:", ex)

        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()


@pytest.mark.asyncio
async def test_mission_fails_when_grafana_unreachable():
    """
    THE RIP-OUT TEST:
    Points the mission runner at a non-existent / dead Grafana endpoint.
    Asserts that the mission fails with an exception and does NOT complete.
    """
    sim = RenderFarmSimulator()
    dispatcher = InterventionDispatcher(sim)

    # Point at unreachable endpoint
    dead_server_url = "http://127.0.0.1:9999/mcp"

    runner = MultiStepMissionRunner(
        dispatcher=dispatcher,
        mcp_server_url=dead_server_url,
        project_id="agent-attest-2026",
        location="global",
        model_name="gemini-3.8-flash",
    )

    with pytest.raises((ConnectionError, RuntimeError)) as excinfo:
        await runner.execute_mission(show_id="show-aethelgard")

    assert "Grafana MCP transport error" in str(excinfo.value) or "error" in str(excinfo.value).lower()
    print(f"\nSuccessfully verified mission failure on dead Grafana: {excinfo.value}")
