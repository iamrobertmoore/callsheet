"""
Synthetic render farm simulation engine for post-production studio operations.
Manages nodes, shows, shot scheduling, telemetry state, and scenario injections.
"""

from datetime import datetime, timedelta, timezone
import random
from typing import Optional
from callsheet.farm.models import (
    FarmState,
    NodeStatus,
    RenderNode,
    ScenarioType,
    Shot,
    ShotStatus,
    Show,
)


def round_to_quarter_hour(dt: datetime) -> datetime:
    """Rounds a datetime to the nearest 15 minutes (quarter hour) with 0 seconds."""
    ts = dt.timestamp()
    rounded_ts = round(ts / 900.0) * 900
    return datetime.fromtimestamp(rounded_ts, tz=timezone.utc)


# Stated baseline render rate parameter for the farm simulator (seconds per frame)
BASELINE_FRAME_SECONDS: float = 20.0


class RenderFarmSimulator:
    """
    Simulates a 12-node post-production render farm across multiple client shows.
    Provides deterministic scenario degradation and dynamic workload reallocations.
    """

    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.state = FarmState()
        self._current_cycle_epoch: Optional[int] = None
        self.reset_cycle()

    def reset_cycle(self, now: Optional[datetime] = None) -> None:
        """
        Completely resets the render farm to a pristine, nominal baseline at the start of a 6-hour cycle:
        - All 12 nodes restored (10 active healthy workers at nominal ~58-68C, 2 standby spares node-11 & node-12 at 42C).
        - Quarantines cleared (0 quarantined nodes).
        - All shots restored to initial baseline node assignments (Shot 118 on node-07).
        - Deadlines and frame completion counts cleanly initialized for the cycle epoch.
        """
        now = now or datetime.now(timezone.utc)
        self.state.last_updated = now
        epoch_seconds = int(now.timestamp())
        cycle_length_sec = 6 * 3600  # 6 hours
        cycle_start_ts = epoch_seconds - (epoch_seconds % cycle_length_sec)
        cycle_start = datetime.fromtimestamp(cycle_start_ts, tz=timezone.utc)
        self._current_cycle_epoch = int(epoch_seconds // cycle_length_sec)

        # 1. Shows with contractual delivery deadlines & daily penalties
        # Aethelgard delivery milestone for active sequence due 3.83h from dispatch (1.33h render + 2.5h buffer)
        self.state.shows = {
            "show-aethelgard": Show(
                id="show-aethelgard",
                name="Chronicles of Aethelgard: Episode 6",
                client="Cinefex Northern Pictures",
                delivery_deadline=round_to_quarter_hour(now + timedelta(hours=3.8333)),
                penalty_daily_amount=25000.0,
                penalty_currency="GBP",
                critical_path=True,
            ),
            "show-solarflare": Show(
                id="show-solarflare",
                name="Solar Flare: Redux",
                client="Solaris Media Works",
                delivery_deadline=cycle_start + timedelta(hours=24.0),
                penalty_daily_amount=15000.0,
                penalty_currency="GBP",
                critical_path=False,
            ),
            "show-abyssal": Show(
                id="show-abyssal",
                name="Abyssal Trench 3D",
                client="Submarine Post London",
                delivery_deadline=cycle_start + timedelta(hours=48.0),
                penalty_daily_amount=10000.0,
                penalty_currency="GBP",
                critical_path=False,
            ),
        }

        # 2. 12 Render Nodes (10 active workers, 2 standby spares)
        self.state.nodes = {}
        for i in range(1, 13):
            node_id = f"node-{i:02d}"
            is_standby = i >= 11
            status = NodeStatus.STANDBY if is_standby else NodeStatus.HEALTHY
            self.state.nodes[node_id] = RenderNode(
                id=node_id,
                name=f"Farm-Worker-{i:02d}",
                gpu_type="NVIDIA RTX A6000" if i <= 6 else "NVIDIA RTX 4090",
                vram_gb=48 if i <= 6 else 24,
                cpu_cores=32 if i <= 6 else 64,
                cpu_utilization=15.0 if is_standby else round(random.uniform(75.0, 92.0), 1),
                memory_bytes_used=(8 if is_standby else random.randint(22, 38)) * 1024 * 1024 * 1024,
                memory_bytes_total=64 * 1024 * 1024 * 1024,
                temperature_celsius=42.0 if is_standby else round(random.uniform(56.0, 68.0), 1),
                thermal_limit_celsius=90.0,
                gpu_utilization=0.0 if is_standby else round(random.uniform(65.0, 88.0), 1),
                status=status,
                is_standby=is_standby,
            )

        # 3. Shots in flight with realistic studio workloads
        shots_data = [
            # Aethelgard Critical Delivery (240 frames remaining = 1.33h render baseline, +2.5h buffer, unmitigated throttle yields -4.2h deficit)
            ("sh_118", "show-aethelgard", "SQ_SIEGE", "118", 300, 60, BASELINE_FRAME_SECONDS, "node-07", ShotStatus.RENDERING, 10),
            ("sh_142", "show-aethelgard", "SQ_DRAGON", "142", 3400, 250, 18.0, "node-04", ShotStatus.RENDERING, 9),
            ("sh_150", "show-aethelgard", "SQ_DRAGON", "150", 3200, 200, 22.0, "node-01", ShotStatus.RENDERING, 8),
            ("sh_155", "show-aethelgard", "SQ_THRONE", "155", 3000, 100, 25.0, None, ShotStatus.QUEUED, 8),
            # Solarflare Show (Realistic ~18.5h workload across 4 active nodes -> +5.5h buffer)
            ("sh_201", "show-solarflare", "SQ_ORBIT", "201", 3800, 300, 19.0, "node-02", ShotStatus.RENDERING, 6),
            ("sh_204", "show-solarflare", "SQ_FLARE", "204", 3600, 200, 19.0, "node-03", ShotStatus.RENDERING, 6),
            ("sh_208", "show-solarflare", "SQ_EVAC", "208", 3500, 100, 20.0, "node-05", ShotStatus.RENDERING, 5),
            ("sh_212", "show-solarflare", "SQ_BASE", "212", 3600, 150, 18.5, "node-06", ShotStatus.RENDERING, 5),
            # Abyssal Trench Show (Realistic ~38.6h workload across 3 active nodes -> +9.4h buffer)
            ("sh_301", "show-abyssal", "SQ_DIVE", "301", 7800, 400, 18.8, "node-08", ShotStatus.RENDERING, 4),
            ("sh_302", "show-abyssal", "SQ_CREATURE", "302", 7500, 300, 19.0, "node-09", ShotStatus.RENDERING, 4),
            ("sh_303", "show-abyssal", "SQ_SURFACE", "303", 7600, 200, 18.5, "node-10", ShotStatus.RENDERING, 3),
        ]

        self.state.shots = {}
        for s_id, show_id, seq, code, total_f, comp_f, sec_f, node_id, status, prio in shots_data:
            self.state.shots[s_id] = Shot(
                id=s_id,
                show_id=show_id,
                sequence=seq,
                shot_code=code,
                total_frames=total_f,
                completed_frames=comp_f,
                estimated_seconds_per_frame=sec_f,
                current_seconds_per_frame=sec_f,
                allocated_node_id=node_id,
                status=status,
                priority=prio,
            )
            if node_id and node_id in self.state.nodes:
                self.state.nodes[node_id].current_shot_id = s_id
                self.state.nodes[node_id].current_frame = 1000 + comp_f + 1

        self.state.active_scenario = ScenarioType.BASELINE

    def update_cycle_deadlines(self, now: Optional[datetime] = None) -> None:
        """
        Synchronizes show deadlines and in-flight progress with the current 6-hour cycle epoch.
        If a new cycle epoch has begun, resets the farm state cleanly.
        """
        now = now or datetime.now(timezone.utc)
        self.state.last_updated = now
        epoch_seconds = int(now.timestamp())
        cycle_length_sec = 6 * 3600  # 6 hours
        current_epoch = int(epoch_seconds // cycle_length_sec)

        # If we crossed into a new 6-hour cycle epoch, execute full clean reset
        if self._current_cycle_epoch is None or self._current_cycle_epoch != current_epoch:
            self.reset_cycle(now)
            return

        # Within the current cycle epoch, the delivery deadline is held strictly fixed.
        if "sh_118" in self.state.shots:
            sh = self.state.shots["sh_118"]
            # Active shot batch in flight maintains 240 frames remaining
            sh.total_frames = 300
            sh.completed_frames = 60

    def inject_scenario(self, scenario: ScenarioType, target_temp: Optional[float] = None) -> None:
        """Injects a specific degradation scenario or restores baseline."""
        self.state.active_scenario = scenario
        
        if scenario == ScenarioType.THERMAL_THROTTLING:
            # Degrade node-07 (rendering Shot 118 for Aethelgard delivery)
            node = self.state.nodes["node-07"]
            node.status = NodeStatus.THROTTLED
            node.temperature_celsius = round(target_temp, 1) if target_temp is not None else 95.9  # Exceeds 90C limit
            node.cpu_utilization = 99.0

            # Re-arm standby nodes to ensure spare capacity is available for scenario demo
            for n_id, n in self.state.nodes.items():
                if n.is_standby or n_id in ["node-11", "node-12"]:
                    n.is_standby = True
                    n.status = NodeStatus.STANDBY
                    n.temperature_celsius = 42.0
                    n.current_shot_id = None
            
            # Shot 118 re-allocated to node-07 and render time jumps from 20s to 120s
            shot = self.state.shots.get("sh_118")
            if shot:
                shot.allocated_node_id = "node-07"
                shot.current_seconds_per_frame = 120.0
                shot.status = ShotStatus.AT_RISK

        elif scenario == ScenarioType.MEMORY_LEAK_OOM:
            node = self.state.nodes["node-04"]
            node.status = NodeStatus.OOM_CRITICAL
            node.memory_bytes_used = int(63.2 * 1024 * 1024 * 1024)
            shot = self.state.shots.get("sh_142")
            if shot:
                shot.status = ShotStatus.AT_RISK

        elif scenario == ScenarioType.BASELINE:
            self.reset_cycle()

    def reallocate_shot(
        self,
        shot_id: str,
        target_node_id: str,
        source_temp: Optional[float] = None,
        force_fault: bool = False,
    ) -> dict:
        """
        Intervention action: moves a shot from a degraded node to a target node (e.g. standby node-11).
        Quarantines the degraded node and restores clean render frame rate on the standby node.
        """
        if shot_id not in self.state.shots:
            raise ValueError(f"Shot {shot_id} not found in farm state.")
        if target_node_id not in self.state.nodes:
            raise ValueError(f"Node {target_node_id} not found in farm state.")

        shot = self.state.shots[shot_id]
        prev_node_id = shot.allocated_node_id
        target_node = self.state.nodes[target_node_id]

        # Release previous node and quarantine it
        if prev_node_id and prev_node_id in self.state.nodes:
            prev_node = self.state.nodes[prev_node_id]
            prev_node.current_shot_id = None
            prev_node.current_frame = None
            prev_node.status = NodeStatus.QUARANTINED
            prev_node.is_standby = False
            if source_temp is not None:
                prev_node.temperature_celsius = round(source_temp, 1)

        # Assign to target node
        shot.allocated_node_id = target_node_id
        shot.status = ShotStatus.RENDERING
        shot.render_progress_seconds = 0.0
        target_node.is_standby = False
        target_node.current_shot_id = shot_id
        target_node.current_frame = 1000 + shot.completed_frames + 1
        target_node.cpu_utilization = round(random.uniform(82.0, 91.0), 1)

        if force_fault:
            target_node.status = NodeStatus.THROTTLED
            target_node.temperature_celsius = 94.8
            shot.current_seconds_per_frame = 40.0  # Degraded render rate
        else:
            target_node.status = NodeStatus.HEALTHY
            target_node.temperature_celsius = round(random.uniform(60.0, 66.0), 1)
            shot.current_seconds_per_frame = shot.estimated_seconds_per_frame  # Normal speed restored

        # Calculate time saved relative to simulation clock
        sim_now = self.state.last_updated or datetime.now(timezone.utc)
        est_sec_remaining = shot.estimated_time_remaining_seconds()
        est_completion_time = sim_now + timedelta(seconds=est_sec_remaining)
        
        show = self.state.shows.get(shot.show_id)
        deadline = show.delivery_deadline if show else sim_now
        margin_hours = (deadline - est_completion_time).total_seconds() / 3600.0

        return {
            "shot_id": shot_id,
            "shot_code": shot.shot_code,
            "previous_node": prev_node_id,
            "target_node": target_node_id,
            "frames_remaining": shot.frames_remaining,
            "estimated_seconds_per_frame": shot.current_seconds_per_frame,
            "projected_completion": est_completion_time.isoformat(),
            "deadline": deadline.isoformat(),
            "buffer_margin_hours": round(margin_hours, 1),
            "status": "PROTECTED" if margin_hours > 0 else "SLIPPING",
        }

    def tick(self, delta_seconds: float = 5.0) -> list[dict]:
        """
        Advances the simulation clock by delta_seconds.
        Updates frame completion progress, emits completed frame events, and modulates metrics.
        """
        events = []
        now = datetime.now(timezone.utc)
        self.state.last_updated = now

        for node_id, node in self.state.nodes.items():
            if node.is_standby or not node.current_shot_id:
                continue

            shot = self.state.shots.get(node.current_shot_id)
            if not shot:
                continue

            # Check thermal variation (Hold temperature strictly constant during throttling incidents)
            if node.status == NodeStatus.HEALTHY:
                node.temperature_celsius = max(52.0, min(72.0, node.temperature_celsius + random.uniform(-0.5, 0.5)))

            # Advance frame render based on seconds_per_frame using progress accumulation
            shot.render_progress_seconds += delta_seconds
            rate = max(1.0, shot.current_seconds_per_frame)
            frames_to_complete = int(shot.render_progress_seconds // rate)
            if frames_to_complete > 0 and shot.completed_frames < shot.total_frames:
                actual_completed = min(frames_to_complete, shot.total_frames - shot.completed_frames)
                shot.render_progress_seconds -= (actual_completed * rate)
                shot.completed_frames += actual_completed
                node.frames_completed_total += actual_completed
                current_frame = 1000 + shot.completed_frames
                node.current_frame = current_frame

                events.append({
                    "type": "FRAME_COMPLETED",
                    "node_id": node_id,
                    "shot_id": shot.id,
                    "shot_code": shot.shot_code,
                    "show_id": shot.show_id,
                    "frame_number": current_frame,
                    "duration_seconds": shot.current_seconds_per_frame,
                    "timestamp": now.isoformat(),
                })

                if shot.completed_frames >= shot.total_frames:
                    shot.status = ShotStatus.COMPLETED
                    node.current_shot_id = None
                    node.current_frame = None
                    events.append({
                        "type": "SHOT_COMPLETED",
                        "shot_id": shot.id,
                        "shot_code": shot.shot_code,
                        "show_id": shot.show_id,
                        "node_id": node_id,
                        "timestamp": now.isoformat(),
                    })

        return events
