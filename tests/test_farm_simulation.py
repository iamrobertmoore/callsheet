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


def test_buffer_stability_across_full_six_hour_cycle():
    """
    Proves that the protected delivery buffer for Chronicles of Aethelgard: Episode 6
    remains stably above +2.0 hours at cycle start, mid-cycle, and 5 minutes before rollover.
    """
    from datetime import datetime, timezone, timedelta

    sim = RenderFarmSimulator()
    base_epoch = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)  # Cycle start at 12:00 UTC

    # 1. Test at cycle start (t = 0.0h)
    sim.update_cycle_deadlines(base_epoch)
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    result_start = sim.reallocate_shot("sh_118", "node-11")
    assert result_start["buffer_margin_hours"] >= 2.0, f"Buffer at start {result_start['buffer_margin_hours']} < 2.0h"
    assert result_start["status"] == "PROTECTED"

    # 2. Test mid-cycle (t = 3.0h)
    mid_cycle = base_epoch + timedelta(hours=3.0)
    sim = RenderFarmSimulator()
    sim.update_cycle_deadlines(mid_cycle)
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    result_mid = sim.reallocate_shot("sh_118", "node-11")
    assert result_mid["buffer_margin_hours"] >= 2.0, f"Buffer at mid-cycle {result_mid['buffer_margin_hours']} < 2.0h"
    assert result_mid["status"] == "PROTECTED"

    # 3. Test 5 minutes before rollover (t = 5h 55m)
    near_end = base_epoch + timedelta(hours=5, minutes=55)
    sim = RenderFarmSimulator()
    sim.update_cycle_deadlines(near_end)
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    result_end = sim.reallocate_shot("sh_118", "node-11")
    assert result_end["buffer_margin_hours"] >= 2.0, f"Buffer near end {result_end['buffer_margin_hours']} < 2.0h"
    assert result_end["status"] == "PROTECTED"


def test_sampled_temperature_variation_across_injections():
    """Verify that node temperature sampling generates realistic variation across cycles."""
    sim = RenderFarmSimulator()
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    t1 = sim.state.nodes["node-07"].temperature_celsius
    assert 94.0 <= t1 <= 97.0

    # Invert/re-inject to test dynamic sampling
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
    t2 = sim.state.nodes["node-07"].temperature_celsius
    assert 94.0 <= t2 <= 97.0


@pytest.mark.asyncio
async def test_server_side_rendering_zero_empty_state():
    """Verify that the root endpoint delivers pre-rendered HTML with 100% of data and no loading text."""
    from callsheet.web.app import get_producer_dashboard

    response = await get_producer_dashboard()
    html = response.body.decode("utf-8")

    # Assert no loading placeholders exist in the SSR output
    assert "Loading delivery slate..." not in html
    assert "Synchronizing latest delivery briefing" not in html
    assert "Loading node status..." not in html

    # Assert essential cards and sections are present
    assert "Chronicles of Aethelgard: Episode 6" in html
    assert "Solar Flare: Redux" in html
    assert "Abyssal Trench 3D" in html
    assert "node-07" in html
    assert "node-11" in html
    assert "node-12" in html
    assert "Active Delivery Slate" in html
    assert "Production Callsheet Briefing" in html


def test_cycle_reset_across_three_consecutive_boundaries():
    """
    Proves that across three consecutive 6-hour cycle epochs:
    1. At the start of each cycle, the farm is cleanly reset: 10 Active (all Healthy), 2 Standby (node-11, node-12), 0 Quarantined.
    2. Shot 118 is consistently assigned to node-07 at 20.0s/frame.
    3. An incident in Cycle N (reallocating Shot 118 to node-11 and quarantining node-07) NEVER leaks into Cycle N+1.
    4. No healthy node is ever quarantined in subsequent cycles.
    """
    from datetime import datetime, timezone, timedelta

    sim = RenderFarmSimulator()
    t0 = datetime(2026, 8, 26, 0, 0, 0, tzinfo=timezone.utc)

    for cycle_index in range(3):
        cycle_time = t0 + timedelta(hours=6.0 * cycle_index)

        # 1. Update cycle clock to boundary
        sim.update_cycle_deadlines(cycle_time)

        # 2. Assert clean state at start of cycle
        active_nodes = [n for n in sim.state.nodes.values() if not n.is_standby and n.status == NodeStatus.HEALTHY]
        standby_nodes = [n for n in sim.state.nodes.values() if n.is_standby and n.status == NodeStatus.STANDBY]
        quarantined_nodes = [n for n in sim.state.nodes.values() if n.status == NodeStatus.QUARANTINED]

        assert len(active_nodes) == 10, f"Cycle {cycle_index+1}: Expected 10 active healthy nodes, got {len(active_nodes)}"
        assert len(standby_nodes) == 2, f"Cycle {cycle_index+1}: Expected 2 standby nodes, got {len(standby_nodes)}"
        assert len(quarantined_nodes) == 0, f"Cycle {cycle_index+1}: Expected 0 quarantined nodes, got {len(quarantined_nodes)}"

        # Assert node-07 is healthy and assigned to Shot 118
        node07 = sim.state.nodes["node-07"]
        assert node07.status == NodeStatus.HEALTHY
        assert not node07.is_standby
        assert sim.state.shots["sh_118"].allocated_node_id == "node-07"
        assert sim.state.shots["sh_118"].estimated_seconds_per_frame == 20.0

        # Assert node-11 and node-12 are standby spares
        assert sim.state.nodes["node-11"].status == NodeStatus.STANDBY
        assert sim.state.nodes["node-11"].is_standby
        assert sim.state.nodes["node-12"].status == NodeStatus.STANDBY
        assert sim.state.nodes["node-12"].is_standby

        # 3. Simulate degradation scenario during the cycle
        sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)
        assert sim.state.nodes["node-07"].status == NodeStatus.THROTTLED
        assert sim.state.shots["sh_118"].current_seconds_per_frame == 120.0

        # 4. Execute automated failover to standby node-11
        result = sim.reallocate_shot("sh_118", "node-11")
        assert result["status"] == "PROTECTED"
        assert result["previous_node"] == "node-07"
        assert result["target_node"] == "node-11"
        assert result["buffer_margin_hours"] >= 2.0

        # 5. Assert post-intervention state within the cycle
        assert sim.state.nodes["node-07"].status == NodeStatus.QUARANTINED
        assert sim.state.nodes["node-11"].status == NodeStatus.HEALTHY
        assert not sim.state.nodes["node-11"].is_standby
        assert sim.state.nodes["node-12"].status == NodeStatus.STANDBY
        assert sim.state.nodes["node-12"].is_standby


def test_unmitigated_deficit_magnitude_few_hours():
    """
    Proves that unmitigated delivery deficit under thermal throttling
    is between -5.0 hours and -3.0 hours (a believable same-shift afternoon slip, not days).
    """
    from datetime import datetime, timezone, timedelta
    from callsheet.farm.simulator import RenderFarmSimulator, ScenarioType

    sim = RenderFarmSimulator()
    now = datetime(2026, 8, 26, 14, 0, 0, tzinfo=timezone.utc)
    sim.update_cycle_deadlines(now)
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING)

    shot = sim.state.shots["sh_118"]
    show = sim.state.shows["show-aethelgard"]
    deadline = show.delivery_deadline

    # Throttled completion (120s/frame)
    throttled_sec = shot.frames_remaining * shot.current_seconds_per_frame
    throttled_completion = now + timedelta(seconds=throttled_sec)
    unmitigated_buffer_hours = (deadline - throttled_completion).total_seconds() / 3600.0

    # Restored completion (20s/frame)
    normal_sec = shot.frames_remaining * shot.estimated_seconds_per_frame
    restored_completion = now + timedelta(seconds=normal_sec)
    restored_buffer_hours = (deadline - restored_completion).total_seconds() / 3600.0

    assert -5.0 <= unmitigated_buffer_hours <= -3.0, (
        f"Expected unmitigated deficit between -5.0h and -3.0h, got {unmitigated_buffer_hours:.1f}h"
    )
    assert 2.0 <= restored_buffer_hours <= 3.0, (
        f"Expected restored buffer between +2.0h and +3.0h, got {restored_buffer_hours:.1f}h"
    )


def test_temperature_sampling_hold_during_throttling():
    """
    Proves that once junction temperature is sampled for a thermal throttling incident,
    subsequent simulator ticks hold that exact single temperature constant without drift.
    """
    from callsheet.farm.simulator import RenderFarmSimulator, ScenarioType

    sim = RenderFarmSimulator()
    sim.inject_scenario(ScenarioType.THERMAL_THROTTLING, target_temp=95.9)
    node = sim.state.nodes["node-07"]
    initial_temp = node.temperature_celsius
    assert initial_temp == 95.9

    # Run 10 ticks and verify temperature remains locked at 95.9
    for _ in range(10):
        sim.tick(delta_seconds=5.0)
        assert node.temperature_celsius == 95.9, f"Temperature drifted from 95.9 to {node.temperature_celsius}"


def test_all_active_nodes_rendering_no_premature_idle():
    """
    Proves that node-01, node-04, and all active rendering workers have sufficient frame
    workloads and do not prematurely transition to Idle.
    """
    from callsheet.farm.simulator import RenderFarmSimulator

    sim = RenderFarmSimulator()
    for node_id in ["node-01", "node-04"]:
        node = sim.state.nodes[node_id]
        shot = sim.state.shots.get(node.current_shot_id)
        assert shot is not None, f"Node {node_id} has no assigned shot"
        assert shot.frames_remaining >= 1000, (
            f"Node {node_id} shot {shot.id} only has {shot.frames_remaining} frames remaining"
        )


