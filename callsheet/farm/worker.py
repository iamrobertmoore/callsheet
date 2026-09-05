"""
Background continuous worker that drives the farm simulation and OTLP telemetry emitter.
Runs periodic ticks and handles scenario injections.
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from callsheet.agent.alerting import ensure_alert_rule, get_alert_rule, PRODUCTION_ALERT_RULE_UID
from callsheet.farm.emitter import FarmTelemetryEmitter, get_deployment_id
from callsheet.farm.models import NodeStatus, ScenarioType, PENDING_APPROVALS
from callsheet.farm.simulator import RenderFarmSimulator
from callsheet.mcp.client import create_grafana_mcp_toolset, get_grafana_mcp_connection_params

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
                "description": "All 3 client shows on schedule. Chronicles of Aethelgard: Episode 6 buffer margin: +2.9 hours before £25,000/day penalty.",
                "status": "COMPLETED",
                "evidence": {"buffer_margin_hours": 2.9, "status": "ON SCHEDULE"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        ],
        "callsheet_briefing": """### 1. STATUS HEADLINE
Delivery for Chronicles of Aethelgard: Episode 6 is on schedule with all render nodes operating nominally.

### 2. EXECUTIVE SUMMARY
The autonomous operations agent is actively monitoring the render farm across Prometheus metrics, Loki logs, and Tempo traces. All 10 active render nodes are operating within normal thermal thresholds (peak 65.1°C against the 90.0°C limit) with raytrace frame durations holding steady at 17.0s baseline. All 3 shows remain on schedule. Chronicles of Aethelgard: Episode 6 maintains a +2.9 hour safety cushion before the contractual delivery deadline, and both standby spares (node-11 and node-12) are armed and ready for failover.

### 3. SHOT BREAKDOWN TABLE

| Show Name | Shot Code | Node | Frames Remaining | Projected Delivery | Buffer Margin |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Chronicles of Aethelgard: Ep 6 | SQ_SIEGE / sh118 | node-07 | 200 | Nominal Rate (20.0s/frame) | +2.9 hours |
| Solar Flare: Redux | SQ_CORONA / sh204 | node-02 | 3,400 | Nominal Rate (19.0s/frame) | +5.5 hours |
| Abyssal Trench 3D | SQ_TRENCH / sh310 | node-08 | 7,400 | Nominal Rate (18.8s/frame) | +9.4 hours |

### 4. TELEMETRY AUDIT TRAIL
* Prometheus Metric: render_farm_node_temperature_celsius average 58.5°C across active nodes.
* Loki Log Message: Continuous healthy frame completion events streamed without errors.
* Tempo Trace Span: raytrace_volumetrics_pass durations stable at 17.0s baseline.""",
    }


class FarmWorker:
    """
    Continuous background loop that advances farm state, emits telemetry every tick,
    manages rolling 6-hour cycle epochs, and runs the autonomous watchdog to trigger
    missions automatically on anomalies.
    """

    def __init__(
        self,
        simulator: Optional[RenderFarmSimulator] = None,
        emitter: Optional[FarmTelemetryEmitter] = None,
        mission_runner: Optional[Any] = None,
        tick_interval_seconds: float = 5.0,
        cycle_interval_seconds: float = 6.0 * 3600.0,
        mcp_server_url: Optional[str] = None,
    ):
        self.simulator = simulator or RenderFarmSimulator()
        self.emitter = emitter or FarmTelemetryEmitter(self.simulator)
        self.mission_runner = mission_runner
        self.tick_interval_seconds = tick_interval_seconds
        self.cycle_interval_seconds = cycle_interval_seconds
        self.mcp_server_url = mcp_server_url
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._mission_task: Optional[asyncio.Task] = None
        self.latest_mission: Dict[str, Any] = get_default_healthy_briefing()
        self.is_investigating = False
        self._last_investigated_node: Optional[str] = None
        self._last_cycle_epoch: Optional[int] = None
        self.instance_started_at: str = datetime.now(timezone.utc).isoformat()
        self.missions_this_instance: int = 0
        self.tick_cadence_history: List[Dict[str, Any]] = []
        self._last_tick_start: Optional[float] = None

        # Section 3 & 6: Alert-Driven Autonomous Loop and telemetry metadata
        self.deployment_id: str = get_deployment_id()
        self.alert_rule_uid: Optional[str] = (
            PRODUCTION_ALERT_RULE_UID if self.deployment_id == "cloud-run" else None
        )
        self.alert_rule_status: str = "initializing"
        self.alert_rule_state: str = "Normal"
        self.alert_rule_error: Optional[str] = None
        self.scenario_primed_by: str = "instance_start"
        self.scenario_primed_at: str = datetime.now(timezone.utc).isoformat()
        self.fault_injected_at: Optional[float] = time.time()
        self.alert_firing_observed_at: Optional[float] = None
        self.mission_started_at: Optional[float] = None

        # Section 6 metadata tracking
        self.last_mission_at: Optional[str] = None
        self.last_mission_trigger: Optional[str] = None
        self.last_verification_status: Optional[str] = None

    @property
    def current_cycle_epoch(self) -> int:
        now_ts = int(time.time())
        return int(now_ts // self.cycle_interval_seconds)

    @property
    def next_reset_at_utc(self) -> str:
        next_ts = (self.current_cycle_epoch + 1) * self.cycle_interval_seconds
        return datetime.fromtimestamp(next_ts, tz=timezone.utc).isoformat()

    @property
    def alert_pending(self) -> bool:
        """
        Returns True when a node fault exists (temperature > 90C with active shot allocated)
        but the mission investigation has not yet started.
        """
        if self.is_investigating:
            return False
        for node in self.simulator.state.nodes.values():
            if (node.status == NodeStatus.THROTTLED or node.temperature_celsius > 90.0) and node.current_shot_id is not None:
                if self._last_investigated_node != node.id:
                    return True
        return False

    @property
    def verification_progress(self) -> Optional[Dict[str, Any]]:
        """Surfaces live verification progress from mission runner if active."""
        if self.mission_runner:
            return getattr(self.mission_runner, "verification_progress", None)
        return None

    @property
    def tick_cadence_stats(self) -> Dict[str, Any]:
        """Returns statistics on simulation tick cadence and execution latency."""
        if not self.tick_cadence_history:
            return {"status": "no_ticks_recorded"}
        durations = [h["duration_seconds"] for h in self.tick_cadence_history]
        intervals = [h["interval_seconds"] for h in self.tick_cadence_history if h.get("interval_seconds") is not None]
        return {
            "total_recorded_ticks": len(self.tick_cadence_history),
            "last_interval_seconds": intervals[-1] if intervals else None,
            "average_interval_seconds": round(sum(intervals) / len(intervals), 2) if intervals else None,
            "last_duration_seconds": durations[-1] if durations else None,
            "average_duration_seconds": round(sum(durations) / len(durations), 4) if durations else None,
            "is_investigating": self.is_investigating,
            "recent_ticks": self.tick_cadence_history[-5:],
        }

    async def _init_alert_rule(self) -> None:
        """Initializes or binds the Grafana alert rule for autonomous watchdog monitoring."""
        try:
            params = get_grafana_mcp_connection_params(mcp_server_url=self.mcp_server_url)
            toolset = create_grafana_mcp_toolset(params)
            rule = await ensure_alert_rule(toolset, self.deployment_id)
            if rule and "uid" in rule:
                self.alert_rule_uid = rule["uid"]
                self.alert_rule_status = "ok"
                self.alert_rule_error = None
                logger.info("Watchdog bound to Grafana alert rule %s (%s)", self.alert_rule_uid, self.deployment_id)
            else:
                self.alert_rule_status = "error"
                self.alert_rule_error = "ensure_alert_rule returned empty rule"
        except Exception as ex:
            self.alert_rule_status = "error"
            self.alert_rule_error = str(ex)
            logger.warning("Failed to initialize Grafana alert rule at startup: %s (failing closed)", ex)

    async def start(self) -> None:
        """Starts the continuous emission and autonomous watchdog loop."""
        if self._running:
            return
        self._running = True
        await self._init_alert_rule()
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            "Farm continuous worker started (interval: %.1fs, cycle: %.1fh, alert_rule: %s)",
            self.tick_interval_seconds,
            self.cycle_interval_seconds / 3600.0,
            self.alert_rule_uid,
        )

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
        if self._mission_task and not self._mission_task.done():
            self._mission_task.cancel()
            try:
                await self._mission_task
            except asyncio.CancelledError:
                pass
            self._mission_task = None
        self.emitter.shutdown()
        logger.info("Farm continuous worker stopped.")

    async def _run_loop(self) -> None:
        while self._running:
            t_start = time.time()
            interval = (t_start - self._last_tick_start) if self._last_tick_start is not None else None
            self._last_tick_start = t_start

            try:
                now_ts = int(time.time())
                current_epoch = int(now_ts // self.cycle_interval_seconds)

                # Update deadlines to align with the 6-hour cycle
                self.simulator.update_cycle_deadlines()

                # Handle cycle rollover
                if self._last_cycle_epoch is None or current_epoch != self._last_cycle_epoch:
                    trigger = "instance_start" if self._last_cycle_epoch is None else "cycle_boundary"
                    logger.info("Cycle epoch change (epoch: %d, trigger: %s). Performing full farm reset and priming scenario...", current_epoch, trigger)
                    self._last_cycle_epoch = current_epoch
                    self._last_investigated_node = None
                    self.scenario_primed_by = trigger
                    self.scenario_primed_at = datetime.now(timezone.utc).isoformat()
                    self.fault_injected_at = time.time()
                    self.alert_firing_observed_at = None
                    self.mission_started_at = None
                    self.simulator.reset_cycle()
                    self.simulator.inject_scenario(ScenarioType.THERMAL_THROTTLING)
                    PENDING_APPROVALS.clear()

                # 1. Advance simulation state tracking wall time (capped at 30 seconds)
                delta_sec = min(30.0, max(0.1, interval)) if interval is not None else self.tick_interval_seconds
                events = self.simulator.tick(delta_seconds=delta_sec)

                # 2. Transmit metrics tick
                self.emitter.emit_metrics_tick()

                # 3. Transmit frame events (logs and traces)
                if events:
                    self.emitter.process_events(events)

                # 4. Autonomous Agent Watchdog: Check Grafana alert state and trigger intervention
                await self._check_and_trigger_autonomous_mission()

            except Exception as e:
                logger.error("Error during farm worker tick: %s", e, exc_info=True)

            t_end = time.time()
            duration = t_end - t_start
            cadence_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "interval_seconds": round(interval, 2) if interval is not None else None,
                "duration_seconds": round(duration, 4),
                "is_investigating": self.is_investigating,
            }
            self.tick_cadence_history.append(cadence_entry)
            if len(self.tick_cadence_history) > 50:
                self.tick_cadence_history.pop(0)

            logger.info(
                "Worker tick completed in %.4fs (interval: %s, investigating: %s)",
                duration,
                f"{interval:.2f}s" if interval is not None else "initial",
                self.is_investigating,
            )

            sleep_time = max(0.0, self.tick_interval_seconds - duration)
            await asyncio.sleep(sleep_time)

    async def _check_and_trigger_autonomous_mission(self) -> None:
        """
        Watches for degraded nodes strictly driven by Grafana Cloud Alerting.
        When Grafana reports the alert rule is firing, automatically triggers the mission.
        Fails closed: if the rule cannot be read, logs error and does not trigger.
        """
        if not self.mission_runner or self.is_investigating:
            return

        if not self.alert_rule_uid:
            await self._init_alert_rule()
            if not self.alert_rule_uid:
                return

        params = get_grafana_mcp_connection_params(mcp_server_url=self.mcp_server_url)
        toolset = create_grafana_mcp_toolset(params)

        try:
            rule_data = await get_alert_rule(toolset, self.alert_rule_uid)
            self.alert_rule_status = "ok"
            self.alert_rule_state = rule_data.get("state", "Normal")
            self.alert_rule_error = None
        except Exception as ex:
            self.alert_rule_status = "error"
            self.alert_rule_state = "unknown"
            self.alert_rule_error = str(ex)
            logger.warning("Failed to poll Grafana alert rule %s: %s (failing closed)", self.alert_rule_uid, ex)
            return

        rule_state = str(rule_data.get("state", "")).lower()
        if rule_state == "firing":
            alerts = [a for a in rule_data.get("alerts", []) if a.get("state") == "Alerting"]
            if not alerts and rule_data.get("alerts"):
                alerts = rule_data.get("alerts")

            firing_alert = None
            for a in alerts:
                active_at_raw = a.get("activeAt", "")
                # Watchdog hygiene: ignore any alert instance whose activeAt is earlier than scenario_primed_at (allowing 65s scheduler floor tolerance)
                if self.scenario_primed_at and active_at_raw:
                    try:
                        dt_active = datetime.fromisoformat(str(active_at_raw).replace("Z", "+00:00"))
                        dt_primed = datetime.fromisoformat(str(self.scenario_primed_at).replace("Z", "+00:00"))
                        if dt_active < (dt_primed - timedelta(seconds=65)):
                            logger.info(
                                "Watchdog ignoring stale alert instance (activeAt: %s < primed_at: %s)",
                                active_at_raw,
                                self.scenario_primed_at,
                            )
                            continue
                    except Exception as parse_ex:
                        logger.warning("Error parsing alert activeAt or primed_at: %s", parse_ex)

                firing_alert = a
                break

            if firing_alert:
                target_node = firing_alert.get("labels", {}).get("node_id", "node-07")
                if self._last_investigated_node != target_node:
                    self.alert_firing_observed_at = time.time()
                    logger.info(
                        "Watchdog observed Grafana alert %s FIRING for %s (activeAt: %s). Running mission...",
                        self.alert_rule_uid,
                        target_node,
                        firing_alert.get("activeAt", ""),
                    )
                    self.is_investigating = True
                    self._last_investigated_node = target_node
                    alert_evidence = {
                        "rule_uid": self.alert_rule_uid,
                        "labels": firing_alert.get("labels", {}),
                        "activeAt": firing_alert.get("activeAt", ""),
                        "state": "Alerting",
                    }
                    self._mission_task = asyncio.create_task(
                        self._run_autonomous_mission(target_node, alert_evidence)
                    )

    async def _run_autonomous_mission(self, target_node_id: str, alert_evidence: Dict[str, Any]) -> None:
        try:
            self.mission_started_at = time.time()
            fault_to_firing = (
                self.alert_firing_observed_at - self.fault_injected_at
                if (self.alert_firing_observed_at and self.fault_injected_at)
                else None
            )
            firing_to_start = (
                self.mission_started_at - self.alert_firing_observed_at
                if (self.mission_started_at and self.alert_firing_observed_at)
                else None
            )
            logger.info(
                "Autonomous mission starting for %s: fault_to_firing=%s, firing_to_start=%s",
                target_node_id,
                f"{fault_to_firing:.2f}s" if fault_to_firing is not None else "N/A",
                f"{firing_to_start:.2f}s" if firing_to_start is not None else "N/A",
            )
            res = await self.mission_runner.execute_mission(
                show_id="show-aethelgard",
                trigger_type="grafana_alert",
                scenario_primed_by=self.scenario_primed_by,
                scenario_primed_at=self.scenario_primed_at,
                alert_evidence=alert_evidence,
            )
            self.missions_this_instance += 1
            self.latest_mission = res.model_dump(mode="json")
            self.last_mission_at = res.timestamp.isoformat() if hasattr(res.timestamp, "isoformat") else str(res.timestamp)
            self.last_mission_trigger = "grafana_alert"
            self.last_verification_status = getattr(res, "verification_status", None)
            logger.info("Autonomous mission completed successfully for %s (trigger: grafana_alert)", target_node_id)
        except Exception as ex:
            logger.error("Autonomous mission execution failed: %s", ex, exc_info=True)
        finally:
            self.is_investigating = False
            self._mission_task = None

    def inject_scenario(self, scenario: ScenarioType) -> None:
        """Injects a scenario into the running simulator (used by /demo or filming)."""
        self.simulator.inject_scenario(scenario)
        self._last_investigated_node = None
        self.is_investigating = False
        self.scenario_primed_by = "demo"
        self.scenario_primed_at = datetime.now(timezone.utc).isoformat()
        self.fault_injected_at = time.time()
        self.alert_firing_observed_at = None
        self.mission_started_at = None
        logger.info("Injected scenario: %s (watchdog re-armed, primed_by: demo)", scenario.value)

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

    async def trigger_mission(
        self,
        show_id: str = "show-aethelgard",
        force_verification_fault: bool = False,
        trigger_type: str = "api",
    ) -> Dict[str, Any]:
        """Manually or programmatically triggers a mission run (e.g. from /demo or tests)."""
        if not self.mission_runner:
            raise RuntimeError("Mission runner not configured on worker.")
        self.is_investigating = True
        try:
            res = await self.mission_runner.execute_mission(
                show_id=show_id,
                force_verification_fault=force_verification_fault,
                trigger_type=trigger_type,
                scenario_primed_by=self.scenario_primed_by,
                scenario_primed_at=self.scenario_primed_at,
            )
            self.missions_this_instance += 1
            self.latest_mission = res.model_dump(mode="json")
            self.last_mission_at = res.timestamp.isoformat() if hasattr(res.timestamp, "isoformat") else str(res.timestamp)
            self.last_mission_trigger = trigger_type
            self.last_verification_status = getattr(res, "verification_status", None)
            return self.latest_mission
        finally:
            self.is_investigating = False

