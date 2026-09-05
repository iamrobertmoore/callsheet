"""
Data models for post-production studio shows, shots, render nodes, and farm state.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class NodeStatus(str, Enum):
    HEALTHY = "HEALTHY"
    THROTTLED = "THROTTLED"
    OOM_CRITICAL = "OOM_CRITICAL"
    OFFLINE = "OFFLINE"
    STANDBY = "STANDBY"
    QUARANTINED = "QUARANTINED"


class ShotStatus(str, Enum):
    QUEUED = "QUEUED"
    RENDERING = "RENDERING"
    COMPLETED = "COMPLETED"
    AT_RISK = "AT_RISK"
    FAILED = "FAILED"


class ScenarioType(str, Enum):
    BASELINE = "BASELINE"
    THERMAL_THROTTLING = "THERMAL_THROTTLING"
    MEMORY_LEAK_OOM = "MEMORY_LEAK_OOM"
    STORAGE_BOTTLENECK = "STORAGE_BOTTLENECK"
    DOUBLE_FAULT = "DOUBLE_FAULT"


class ApprovalRecord(BaseModel):
    id: str
    mission_id: Optional[str] = None
    incident_id: Optional[str] = None
    incident_url: Optional[str] = None
    incident_status: Optional[str] = None
    tier: int = 2
    tier_reason: str
    action_title: str
    target_node_id: str
    source_node_id: str
    shot_id: str
    preempted_shot_id: Optional[str] = None
    preempted_show_name: Optional[str] = None
    plan_summary: str
    buffer_loss_rate: str
    cost_of_waiting: str
    deadline_impact: str
    status: str = "PENDING"  # PENDING, APPROVED, DECLINED
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    resolved_at: Optional[str] = None
    decision_reason: Optional[str] = None


# Central in-memory registry of pending human producer approvals
PENDING_APPROVALS: dict[str, ApprovalRecord] = {}


class Show(BaseModel):
    id: str
    name: str
    client: str
    delivery_deadline: datetime
    penalty_daily_amount: float = 15000.0
    penalty_currency: str = "GBP"
    critical_path: bool = True


class Shot(BaseModel):
    id: str
    show_id: str
    sequence: str
    shot_code: str
    total_frames: int = 100
    completed_frames: int = 0
    frame_range_start: int = 1001
    frame_range_end: int = 1100
    estimated_seconds_per_frame: float = 20.0
    current_seconds_per_frame: float = 20.0
    allocated_node_id: Optional[str] = None
    status: ShotStatus = ShotStatus.QUEUED
    priority: int = 5
    assigned_at: Optional[datetime] = None
    render_progress_seconds: float = 0.0
    delivered_at: Optional[datetime] = None
    delivery_margin_hours_achieved: Optional[float] = None

    @property
    def frames_remaining(self) -> int:
        return max(0, self.total_frames - self.completed_frames)

    @property
    def progress_ratio(self) -> float:
        if self.total_frames == 0:
            return 1.0
        return min(1.0, self.completed_frames / self.total_frames)

    def estimated_time_remaining_seconds(self) -> float:
        return self.frames_remaining * self.current_seconds_per_frame


class RenderNode(BaseModel):
    id: str
    name: str
    gpu_type: str = "NVIDIA RTX A6000"
    vram_gb: int = 48
    cpu_cores: int = 32
    cpu_utilization: float = 25.0
    memory_bytes_used: int = 16 * 1024 * 1024 * 1024
    memory_bytes_total: int = 64 * 1024 * 1024 * 1024
    temperature_celsius: float = 58.0
    thermal_limit_celsius: float = 90.0
    gpu_utilization: float = 40.0
    status: NodeStatus = NodeStatus.HEALTHY
    current_shot_id: Optional[str] = None
    current_frame: Optional[int] = None
    is_standby: bool = False
    frames_completed_total: int = 0
    frames_failed_total: int = 0


class FarmState(BaseModel):
    shows: dict[str, Show] = Field(default_factory=dict)
    shots: dict[str, Shot] = Field(default_factory=dict)
    nodes: dict[str, RenderNode] = Field(default_factory=dict)
    active_scenario: ScenarioType = ScenarioType.BASELINE
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
