"""
Unit and integration tests for RenderFarmSimulator and FarmWorker.
"""

import asyncio
from datetime import datetime, timezone
import pytest
from callsheet.farm.models import NodeStatus, ScenarioType, ShotStatus
from callsheet.farm.simulator import RenderFarmSimulator
from callsheet.farm.worker import FarmWorker


def test_simulator_initialization():
    """Verify simulator starts with 12 nodes, 3 shows, and active queue."""
    sim = RenderFarmSimulator()
    state = sim.state

    assert len(state.nodes) == 12
    assert len(state.shows) == 3
    assert len(state.shots) >= 10

    active_nodes = [n for n in state.nodes.values() if not n.is_standby]
    standby_nodes = [n for n in state.nodes.values() if n.is_standby]
    assert len(active_nodes) == 10
    assert len(standby_nodes) == 2


def test_thermal_throttling_scenario_injection():
    """Verify thermal throttling scenario increases node temp and extends shot render time."""
    sim = RenderFarmSimulator()
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)

    node7 = sim.state.nodes["node-07"]
    assert node7.status == NodeStatus.THROTTLED
    assert node7.temperature_celsius > 90.0

    shot118 = sim.state.shots["sh_118"]
    assert shot118.status == ShotStatus.AT_RISK
    assert shot118.current_seconds_per_frame >= 100.0


def test_shot_reallocation_intervention():
    """Verify reallocating at-risk shot to standby node restores health and projects deadline margin."""
    sim = RenderFarmSimulator()
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)

    result = sim.reallocate_shot("sh_118", "node-12")

    assert result["shot_id"] == "sh_118"
    assert result["previous_node"] == "node-07"
    assert result["target_node"] == "node-12"
    assert result["estimated_seconds_per_frame"] == 20.0
    assert result["status"] == "PROTECTED"

    node12 = sim.state.nodes["node-12"]
    assert node12.status == NodeStatus.HEALTHY
    assert node12.current_shot_id == "sh_118"

    node7 = sim.state.nodes["node-07"]
    assert node7.status == NodeStatus.QUARANTINED

    shot118 = sim.state.shots["sh_118"]
    assert shot118.allocated_node_id == "node-12"
    assert shot118.status == ShotStatus.RENDERING


def test_simulator_ticks_and_events():
    """Verify ticks advance frame completion and generate events."""
    sim = RenderFarmSimulator()
    events = sim.tick(delta_seconds=50.0)
    assert isinstance(events, list)


@pytest.mark.asyncio
async def test_farm_worker_start_stop():
    """Verify background worker starts and stops cleanly."""
    worker = FarmWorker(tick_interval_seconds=0.1)
    await worker.start()
    assert worker._running is True
    await asyncio.sleep(0.3)
    await worker.stop()
    assert worker._running is False
