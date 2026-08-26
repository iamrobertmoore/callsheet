"""
Background continuous worker that drives the farm simulation and OTLP telemetry emitter.
Runs periodic ticks and handles scenario injections.
"""

import asyncio
import logging
from typing import Optional

from callsheet.farm.emitter import FarmTelemetryEmitter
from callsheet.farm.models import ScenarioType
from callsheet.farm.simulator import RenderFarmSimulator

logger = logging.getLogger(__name__)


class FarmWorker:
    """
    Continuous background loop that advances farm state and emits telemetry every tick.
    """

    def __init__(
        self,
        simulator: Optional[RenderFarmSimulator] = None,
        emitter: Optional[FarmTelemetryEmitter] = None,
        tick_interval_seconds: float = 5.0,
    ):
        self.simulator = simulator or RenderFarmSimulator()
        self.emitter = emitter or FarmTelemetryEmitter(self.simulator)
        self.tick_interval_seconds = tick_interval_seconds
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Starts the continuous emission loop."""
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

            except Exception as e:
                logger.error("Error during farm worker tick: %s", e, exc_info=True)

            await asyncio.sleep(self.tick_interval_seconds)

    def inject_scenario(self, scenario: ScenarioType) -> None:
        """Injects a scenario into the running simulator."""
        self.simulator.inject_scenario(scenario)
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
