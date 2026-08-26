"""
Tests for Callsheet Multi-Step Mission Runner and Vertex AI Gemini synthesis.
"""

import os
import subprocess
import time
import pytest
from dotenv import load_dotenv

from callsheet.agent.mission import MultiStepMissionRunner
from callsheet.farm.models import ScenarioType
from callsheet.farm.simulator import RenderFarmSimulator
from callsheet.interventions.dispatcher import InterventionDispatcher

load_dotenv()


@pytest.mark.asyncio
async def test_full_six_step_mission_execution():
    """
    Executes a complete 6-step mission:
    1. Injects thermal throttling on simulator.
    2. Spawns mcp-grafana server.
    3. Runs MultiStepMissionRunner (MCP telemetry queries + Vertex AI Gemini summary).
    4. Asserts all 6 steps completed and producer briefing generated.
    """
    # 1. Setup simulator and dispatcher
    sim = RenderFarmSimulator()
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    dispatcher = InterventionDispatcher(sim)

    # 2. Spawn mcp-grafana server locally
    mcp_bin = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "bin", "mcp-grafana"))
    if not os.path.exists(mcp_bin):
        pytest.skip(f"mcp-grafana binary not found at {mcp_bin}")

    port = 8129
    server_url = f"http://127.0.0.1:{port}/mcp"
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
        assert proc.poll() is None, "mcp-grafana failed to start"

        runner = MultiStepMissionRunner(
            simulator=sim,
            dispatcher=dispatcher,
            mcp_server_url=server_url,
            project_id="agent-attest-2026",
            location="us-central1",
            model_name="gemini-2.5-flash",
        )

        result = await runner.execute_mission(show_id="show-dune")

        # Verify mission structure
        assert result.show_id == "show-dune"
        assert len(result.steps) == 6
        assert result.intervention_record is not None
        assert result.intervention_record.target_node_id == "node-12"
        assert result.intervention_record.status == "PROTECTED"
        assert len(result.callsheet_briefing) > 50

        print("\n=== GENERATED CALLSHEET BRIEFING ===")
        print(result.callsheet_briefing)

        # Assert no em dashes in briefing text
        assert "—" not in result.callsheet_briefing, "Briefing must not contain em dashes"

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()
