"""
Intervention dispatcher for executing and recording workload reallocations.
"""

from datetime import datetime, timezone
from typing import List, Optional
import uuid
from pydantic import BaseModel, Field

from callsheet.farm.simulator import RenderFarmSimulator


class InterventionRecord(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    shot_id: str
    shot_code: str
    show_id: str
    previous_node_id: str
    target_node_id: str
    reason: str
    frames_remaining: int
    projected_completion: str
    deadline: str
    buffer_margin_hours: float
    status: str
    telemetry_evidence: dict = Field(default_factory=dict)


class InterventionDispatcher:
    """
    Manages automated interventions on the render farm and maintains an audit log.
    """

    def __init__(self, simulator: RenderFarmSimulator):
        self.simulator = simulator
        self.history: List[InterventionRecord] = []

    def execute_reallocation(
        self,
        shot_id: str,
        target_node_id: str,
        reason: str,
        telemetry_evidence: Optional[dict] = None,
    ) -> InterventionRecord:
        """
        Executes a shot reallocation from a failing node to a standby node.
        """
        raw_result = self.simulator.reallocate_shot(shot_id, target_node_id)
        shot = self.simulator.state.shots[shot_id]

        record = InterventionRecord(
            shot_id=shot_id,
            shot_code=raw_result["shot_code"],
            show_id=shot.show_id,
            previous_node_id=raw_result["previous_node"],
            target_node_id=raw_result["target_node"],
            reason=reason,
            frames_remaining=raw_result["frames_remaining"],
            projected_completion=raw_result["projected_completion"],
            deadline=raw_result["deadline"],
            buffer_margin_hours=raw_result["buffer_margin_hours"],
            status=raw_result["status"],
            telemetry_evidence=telemetry_evidence or {},
        )
        self.history.append(record)
        return record

    def list_history(self) -> List[InterventionRecord]:
        return list(reversed(self.history))
