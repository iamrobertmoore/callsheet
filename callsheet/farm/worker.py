"""
Background continuous worker that drives the farm simulation and OTLP telemetry emitter.
Runs periodic ticks and handles scenario injections.
"""

import asyncio
import logging
from typing import Optional

from datetime import datetime, timezone
import os
from typing import Any, Dict, Optional

from callsheet.farm.emitter import FarmTelemetryEmitter
from callsheet.farm.models import NodeStatus, ScenarioType
from callsheet.farm.simulator import RenderFarmSimulator

logger = logging.getLogger(__name__)


def get_default_healthy_briefing() -> Dict[str, Any]:
    """Returns a structured baseline briefing when all nodes and shows are nominal."""
    return {
        "id": "baseline-slate",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "show_id": "show-aethelgard",
        "show_name": "Chronicles of Aethelgard: Episode 6",
        "client": "Cinefex Northern Pictures",
        "deadline": "Thursday 27 August, 07:35 UTC",
        "penalty_clause": "£25,000 / day",
        "anomalous_node_id": "none",
        "anomaly_detected": "None (All 12 nodes operating within nominal thermal parameters)",
        "root_cause": "Nominal operations across render fleet. Node temperatures stable between 42.0°C and 65.0°C.",
        "affected_shots": [],
        "intervention_record": None,
        "steps": [
            {
                "step_number": 1,
                "name": "Prometheus Fleet Monitoring",
                "description": "Continuous telemetry sweep across render_farm_node_temperature_celsius. Peak fleet temperature 65.1°C (nominal limit: 90.0°C).",
                "status": "COMPLETED",
                "evidence": {"peak_temp_celsius": 65.1, "threshold_celsius": 90.0, "active_nodes": 10, "standby_nodes": 2},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            {
                "step_number": 2,
                "name": "Loki & Tempo Verification",
                "description": "Verified healthy frame completions across active worker nodes. Frame raytrace times nominal at 17.0s.",
                "status": "COMPLETED",
                "evidence": {"raytrace_duration_seconds": 17.0, "error_logs_detected": 0},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            {
                "step_number": 3,
                "name": "Delivery Buffer Margin Audit",
                "description": "All 3 client shows on schedule. Chronicles of Aethelgard: Episode 6 buffer margin: +17.5 hours before £25,000/day penalty.",
                "status": "COMPLETED",
                "evidence": {"buffer_margin_hours": 17.5, "status": "ON SCHEDULE"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        ],
        "callsheet_briefing": """### 1. STATUS HEADLINE
Thursday delivery for Chronicles of Aethelgard: Episode 6 is on schedule with all render nodes operating nominally.

### 2. EXECUTIVE SUMMARY
The autonomous operations agent is actively monitoring the render farm across Prometheus metrics, Loki logs, and Tempo traces. All 10 active render nodes are operating within normal thermal thresholds (peak 65.1°C against the 90.0°C limit) with raytrace frame durations holding steady at 17.0s. All 3 shows remain on schedule. Chronicles of Aethelgard: Episode 6 maintains a 17.5 hour safety cushion before the client delivery deadline, and both standby spares (node-11 and node-12) are armed and ready for failover.

### 3. SHOT BREAKDOWN TABLE

| Show Name | Shot Code | Node | Frames Remaining | Projected Delivery | Buffer Margin |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Chronicles of Aethelgard: Ep 6 | SQ_SIEGE / sh118 | node-07 | 95 | Wed 26 Aug, 14:05 UTC | +17.5 hours |
| Solar Flare: Redux | SQ_CORONA / sh204 | node-02 | 120 | Wed 26 Aug, 18:30 UTC | +24.0 hours |
| Abyssal Trench 3D | SQ_TRENCH / sh310 | node-04 | 80 | Thu 27 Aug, 02:15 UTC | +32.0 hours |

### 4. TELEMETRY AUDIT TRAIL
* Prometheus Metric: render_farm_node_temperature_celsius average 58.5°C across active nodes.
* Loki Log Message: Continuous healthy frame completion events streamed without errors.
* Tempo Trace Span: raytrace_volumetrics_pass durations stable at 17.0s baseline.""",
    }


class FarmWorker:
    """
    Continuous background loop that advances farm state, emits telemetry every tick,
    and runs the autonomous watchdog to trigger missions automatically on anomalies.
    """

    def __init__(
        self,
        simulator: Optional[RenderFarmSimulator] = None,
        emitter: Optional[FarmTelemetryEmitter] = None,
        mission_runner: Optional[Any] = None,
        tick_interval_seconds: float = 5.0,
    ):
        self.simulator = simulator or RenderFarmSimulator()
        self.emitter = emitter or FarmTelemetryEmitter(self.simulator)
        self.mission_runner = mission_runner
        self.tick_interval_seconds = tick_interval_seconds
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.latest_mission: Dict[str, Any] = get_default_healthy_briefing()
        self.is_investigating = False
        self._last_investigated_node: Optional[str] = None

    async def start(self) -> None:
        """Starts the continuous emission and autonomous watchdog loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Farm continuous worker started (interval: %.1fs)", self.tick_interval_seconds)

    async def stop(self) -> None:
        """Stops the continuous emission loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Farm continuous worker stopped.")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                # 1. Advance simulation state
                events = self.simulator.tick(delta_seconds=self.tick_interval_seconds)

                # 2. Transmit metrics tick
                self.emitter.emit_metrics_tick()

                # 3. Transmit frame events (logs and traces)
                if events:
                    self.emitter.process_events(events)

                # 4. Autonomous Agent Watchdog: Detect degradation and trigger intervention
                await self._check_and_trigger_autonomous_mission()

            except Exception as e:
                logger.error("Error during farm worker tick: %s", e, exc_info=True)

            await asyncio.sleep(self.tick_interval_seconds)

    async def _check_and_trigger_autonomous_mission(self) -> None:
        """
        Watches for degraded nodes crossing thermal or failure limits.
        When detected, automatically triggers the 6-step mission without user intervention.
        """
        if not self.mission_runner or self.is_investigating:
            return

        # Check if any node is degraded / overheated
        overheated_nodes = [
            n for n in self.simulator.state.nodes.values()
            if n.temperature_celsius > n.thermal_limit_celsius or n.status == NodeStatus.THROTTLED
        ]

        if overheated_nodes:
            target_node = overheated_nodes[0]
            if self._last_investigated_node != target_node.id:
                logger.info(
                    "Autonomous watchdog detected anomaly on %s (%.1f°C). Running 6-step mission...",
                    target_node.id,
                    target_node.temperature_celsius,
                )
                self.is_investigating = True
                try:
                    # Allow 2 seconds for telemetry to be indexed in Grafana Cloud
                    await asyncio.sleep(2.0)
                    res = await self.mission_runner.execute_mission(show_id="show-aethelgard")
                    self.latest_mission = res.model_dump(mode="json")
                    self._last_investigated_node = target_node.id
                    logger.info("Autonomous mission completed successfully for %s", target_node.id)
                except Exception as ex:
                    logger.error("Autonomous mission execution failed: %s", ex, exc_info=True)
                finally:
                    self.is_investigating = False

    def inject_scenario(self, scenario: ScenarioType) -> None:
        """Injects a scenario into the running simulator."""
        self.simulator.inject_scenario(scenario)
        if scenario == ScenarioType.BASELINE:
            self._last_investigated_node = None
            self.is_investigating = False
            self.latest_mission = get_default_healthy_briefing()
        logger.info("Injected scenario: %s", scenario.value)

    def reallocate_shot(self, shot_id: str, target_node_id: str) -> dict:
        """Executes a shot reallocation on the running simulator."""
        result = self.simulator.reallocate_shot(shot_id, target_node_id)
        # Emit an intervention log record immediately
        self.emitter.emit_log(
            f"INTERVENTION APPLIED: Shot {result['shot_code']} reallocated from {result['previous_node']} to {result['target_node']}. Status: {result['status']}.",
            level="INFO",
            node_id=result["target_node"],
            shot_code=result["shot_code"],
        )
        return result
