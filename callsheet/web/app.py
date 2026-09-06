"""
FastAPI application for Callsheet web interface and API endpoints.
Provides live status, scenario injection, mission execution, and telemetry audit feeds.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import os
import time
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import urllib.parse

logger = logging.getLogger(__name__)

from callsheet.agent.mission import MultiStepMissionRunner, MISSION_PANEL_IMAGES
from callsheet.farm.models import ScenarioType, ShotStatus, PENDING_APPROVALS, ApprovalRecord
from callsheet.farm.simulator import RenderFarmSimulator
from callsheet.farm.worker import FarmWorker
from callsheet.interventions.dispatcher import InterventionDispatcher
from callsheet.mcp.client import create_grafana_mcp_toolset, get_grafana_mcp_connection_params

# Global singleton instances
simulator = RenderFarmSimulator()
dispatcher = InterventionDispatcher(simulator=simulator)
mission_runner = MultiStepMissionRunner(
    dispatcher=dispatcher,
    project_id=os.getenv("GOOGLE_CLOUD_PROJECT", "agent-attest-2026"),
    location=os.getenv("VERTEX_AI_LOCATION", "global"),
    model_name="gemini-3.8-flash",
)
worker = FarmWorker(
    simulator=simulator,
    mission_runner=mission_runner,
    tick_interval_seconds=5.0,
)


_mcp_health_cache = {
    "timestamp": 0.0,
    "reachable": True,
    "error": None,
}


async def _background_health_probe_loop():
    """Background task continuously refreshing the MCP list_datasources probe every 10 seconds."""
    while True:
        mcp_reachable = True
        mcp_error = None
        try:
            params = get_grafana_mcp_connection_params()
            toolset = create_grafana_mcp_toolset(params)
            res = await toolset._execute_with_session(
                lambda session: session.call_tool("list_datasources", {}),
                "List datasources health check"
            )
            if getattr(res, "isError", False):
                mcp_reachable = False
                mcp_error = "list_datasources returned error response"
        except Exception as ex:
            mcp_reachable = False
            mcp_error = str(ex)
        _mcp_health_cache["timestamp"] = time.monotonic()
        _mcp_health_cache["reachable"] = mcp_reachable
        _mcp_health_cache["error"] = mcp_error
        try:
            await asyncio.sleep(10.0)
        except asyncio.CancelledError:
            break


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start continuous background farm emitter and autonomous watchdog
    await worker.start()
    probe_task = asyncio.create_task(_background_health_probe_loop())
    yield
    # Stop background tasks on shutdown
    probe_task.cancel()
    try:
        await probe_task
    except asyncio.CancelledError:
        pass
    await worker.stop()


app = FastAPI(
    title="Callsheet",
    description="Autonomous render pipeline agent for post-production delivery producers",
    version="0.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ScenarioRequest(BaseModel):
    scenario: str  # BASELINE, THERMAL_THROTTLING, MEMORY_LEAK_OOM


class MissionRequest(BaseModel):
    show_id: Optional[str] = "show-aethelgard"
    force_verification_fault: Optional[bool] = False
    trigger_type: Optional[str] = "api"
    scenario_primed_by: Optional[str] = None


class ApprovalDecisionRequest(BaseModel):
    decision: str
    reason: Optional[str] = None


@app.get("/api/health")
async def health_check():
    # Read live MCP reachability from asynchronous background probe (refreshed every 10s)
    mcp_reachable = _mcp_health_cache["reachable"]
    mcp_error = _mcp_health_cache["error"]

    grafana_url = os.getenv("GRAFANA_URL", "https://bigforest2172.grafana.net")
    parsed_host = urllib.parse.urlparse(grafana_url).netloc
    grafana_stack = parsed_host.split(".")[0] if parsed_host else "bigforest2172"

    is_healthy = (worker.alert_rule_status != "error") and mcp_reachable
    pending_list = [a.model_dump(mode="json") for a in PENDING_APPROVALS.values() if a.status == "PENDING"]

    data = {
        "status": "healthy" if is_healthy else "degraded",
        "service": "callsheet",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agent_runtime": "live",
        "replay_mode": False,
        "mcp_server": "grafana/mcp-grafana v1.2.0, self-hosted sidecar",
        "mcp_transport": "streamable-http",
        "mcp_reachable": mcp_reachable,
        "grafana_stack": grafana_stack,
        "alert_rule_uid": worker.alert_rule_uid or "cfxbt56wwbocge",
        "alert_rule_state": worker.alert_rule_state,
        "alert_rule_interval_configured": "10s",
        "alert_rule_interval_observed": "60s",
        "model": mission_runner.model_name,
        "model_location": mission_runner.location,
        "last_mission_at": worker.last_mission_at,
        "last_mission_trigger": worker.last_mission_trigger,
        "last_verification_status": worker.last_verification_status,
        "cycle_epoch": worker.current_cycle_epoch,
        "next_reset_at_utc": worker.next_reset_at_utc,
        "pending_approvals": pending_list,
        # Baseline / backward-compatible health fields
        "worker_running": worker._running,
        "instance_started_at": worker.instance_started_at,
        "missions_this_instance": worker.missions_this_instance,
        "deployment_id": worker.deployment_id,
        "alert_rule_status": worker.alert_rule_status,
        "alert_rule_error": worker.alert_rule_error,
        "scenario_primed_by": worker.scenario_primed_by,
        "tick_cadence": worker.tick_cadence_stats,
        "watchdog_stalled": worker.watchdog_stalled,
        "watchdog_stalled_since": worker.watchdog_stalled_since,
    }
    if mcp_error:
        data["mcp_error"] = mcp_error
    return data


def serialize_approvals() -> List[Dict[str, Any]]:
    """
    Serializes all recorded approvals, dynamically computing real-time deadline_impact
    and cost_of_waiting from simulator.calculate_buffer_margin_hours('show-solarflare').
    """
    result = []
    margin_sf = simulator.calculate_buffer_margin_hours("show-solarflare")
    mins_sf = int(round(margin_sf * 60))
    for a in PENDING_APPROVALS.values():
        d = a.model_dump(mode="json")
        if a.status == "PENDING" and a.shot_id in ("sh_204", "204"):
            sec_show_name = "Solar Flare: Redux"
            if mins_sf > 0:
                d["deadline_impact"] = f"{sec_show_name} delivery buffer breaches in {mins_sf} minutes if unapproved."
                d["cost_of_waiting"] = (
                    f"Every minute of delay costs 1.0 min of contractual buffer. "
                    f"{sec_show_name} margin slips by 1.0 hr every 60 min of delay. "
                    f"Contractual breach in {mins_sf} minutes without approval."
                )
            else:
                d["deadline_impact"] = f"{sec_show_name} delivery buffer has breached contractual deadline."
                d["cost_of_waiting"] = "Buffer margin exhausted (0 minutes remaining). Immediate action required."
        result.append(d)
    return result


@app.get("/api/approvals")
async def list_approvals():
    """Returns all recorded producer approvals and pending requests."""
    return serialize_approvals()


@app.post("/api/approvals/{approval_id}")
async def decide_approval(approval_id: str, req: ApprovalDecisionRequest):
    """Processes a producer's approval or decline decision for a Tier 2 action."""
    if approval_id not in PENDING_APPROVALS:
        raise HTTPException(status_code=404, detail=f"Approval request {approval_id} not found.")

    res = await mission_runner.handle_approval_decision(
        approval_id=approval_id,
        decision=req.decision,
        reason=req.reason,
    )
    if req.decision.lower() == "approve":
        worker.last_verification_status = "VERIFIED_PROTECTED"
        if worker.latest_mission:
            worker.latest_mission["verification_status"] = "VERIFIED_PROTECTED"
            worker.latest_mission["incident_status"] = "resolved"
            if res.get("briefing"):
                worker.latest_mission["callsheet_briefing"] = res["briefing"]
    else:
        worker.last_verification_status = "ESCALATED"
        if worker.latest_mission:
            worker.latest_mission["verification_status"] = "ESCALATED"
            worker.latest_mission["incident_status"] = "active"
            if res.get("briefing_addendum"):
                worker.latest_mission["callsheet_briefing"] = (worker.latest_mission.get("callsheet_briefing") or "") + res["briefing_addendum"]

    return res


@app.get("/api/state")
async def get_farm_state():
    """Returns the current state of shows, nodes, active shots, and latest mission."""
    state = simulator.state
    content = {
        "active_scenario": state.active_scenario.value,
        "last_updated": state.last_updated.isoformat(),
        "shows": {
            k: {
                **v.model_dump(mode="json"),
                "status_label": simulator.get_show_status(k)[0],
                "buffer_margin_hours": simulator.get_show_status(k)[1],
                "buffer_margin_display": simulator.get_show_status(k)[2],
            } for k, v in state.shows.items()
        },
        "nodes": {k: v.model_dump(mode="json") for k, v in state.nodes.items()},
        "shots": {k: v.model_dump(mode="json") for k, v in state.shots.items()},
        "latest_mission": worker.latest_mission,
        "is_investigating": worker.is_investigating,
        "alert_pending": worker.alert_pending,
        "interventions_count": len(dispatcher.history),
        "verification_progress": worker.verification_progress,
        "scenario_primed_by": worker.scenario_primed_by,
        "alert_rule_status": worker.alert_rule_status,
        "alert_rule_uid": worker.alert_rule_uid,
        "tick_cadence": worker.tick_cadence_stats,
        "pending_approvals": serialize_approvals(),
        "cycle_epoch": worker.current_cycle_epoch,
        "next_reset_at_utc": worker.next_reset_at_utc,
        "watchdog_stalled": worker.watchdog_stalled,
        "watchdog_stalled_since": worker.watchdog_stalled_since,
    }
    return JSONResponse(
        content=content,
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.post("/api/scenario")
async def inject_scenario(req: ScenarioRequest):
    """Triggers a scenario on the synthetic farm."""
    try:
        scenario_enum = ScenarioType(req.scenario.upper())
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid scenario: {req.scenario}")

    worker.inject_scenario(scenario_enum)
    return {
        "status": "success",
        "scenario": scenario_enum.value,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/mission")
async def run_mission(req: MissionRequest):
    """Executes the 7-step observability, intervention, and post-verification mission."""
    try:
        trigger_type = req.trigger_type or "api"
        scenario_primed_by = req.scenario_primed_by or worker.scenario_primed_by
        result = await mission_runner.execute_mission(
            show_id=req.show_id or "show-aethelgard",
            force_verification_fault=bool(req.force_verification_fault),
            trigger_type=trigger_type,
            scenario_primed_by=scenario_primed_by,
            scenario_primed_at=worker.scenario_primed_at,
        )
        worker.missions_this_instance += 1
        worker.latest_mission = result.model_dump(mode="json")
        worker.last_verification_status = result.verification_status
        worker.last_mission_at = datetime.now(timezone.utc).isoformat()
        worker.last_mission_trigger = trigger_type
        return JSONResponse(
            content=worker.latest_mission,
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@app.get("/api/interventions")
async def get_interventions():
    """Returns the history of interventions executed by Callsheet."""
    return [r.model_dump(mode="json") for r in dispatcher.list_history()]


@app.get("/api/missions/{mission_id}/panel.png")
async def get_mission_panel_png(mission_id: str):
    """Serves the rendered Grafana panel PNG captured during mission execution, or dynamically re-renders on cache miss."""
    image_bytes = MISSION_PANEL_IMAGES.get(mission_id)
    if not image_bytes:
        logger.info("Panel image cache miss for mission %s. Attempting dynamic re-render via Grafana MCP...", mission_id)
        from_ms = int((datetime.now(timezone.utc).timestamp() - 1800) * 1000)
        to_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if worker.latest_mission and worker.latest_mission.get("mission_id") == mission_id:
            deeplinks = worker.latest_mission.get("deeplinks", {})
            if "dashboard" in deeplinks:
                try:
                    import urllib.parse
                    parsed = urllib.parse.urlparse(deeplinks["dashboard"])
                    qs = urllib.parse.parse_qs(parsed.query)
                    if "from" in qs and "to" in qs:
                        from_ms = int(qs["from"][0])
                        to_ms = int(qs["to"][0])
                except Exception:
                    pass
        try:
            image_bytes = await mission_runner.render_panel_image(from_ms=from_ms, to_ms=to_ms)
            if image_bytes:
                MISSION_PANEL_IMAGES[mission_id] = image_bytes
        except Exception as ex:
            logger.warning("Dynamic panel render failed for mission %s: %s", mission_id, ex)

    if not image_bytes:
        raise HTTPException(status_code=404, detail="Panel image not found for mission")
    return Response(content=image_bytes, media_type="image/png")



# ---------------------------------------------------------------------------
# Server-Side Rendering (SSR) Helpers for Instant Zero-Empty-State First Paint
# ---------------------------------------------------------------------------

def format_markdown_to_html(text: str) -> str:
    if not text:
        return ""
    import re
    html = re.sub(r'^### (.*$)', r'<h3>\1</h3>', text, flags=re.MULTILINE)
    html = re.sub(r'^## (.*$)', r'<h3>\1</h3>', html, flags=re.MULTILINE)
    html = re.sub(r'^# (.*$)', r'<h3>\1</h3>', html, flags=re.MULTILINE)
    html = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', html)
    html = re.sub(r'\*(.*?)\*', r'<em>\1</em>', html)

    if '|' in html:
        lines = html.split('\n')
        in_table = False
        formatted_lines = []
        for line in lines:
            trimmed = line.strip()
            if trimmed.startswith('|') and trimmed.endswith('|'):
                if ':---' in trimmed or '---' in trimmed:
                    continue
                cells = [c.strip() for c in trimmed.split('|')[1:-1]]
                if not in_table:
                    in_table = True
                    row = ''.join(f'<th>{c}</th>' for c in cells)
                    formatted_lines.append(f'<table><thead><tr>{row}</tr></thead><tbody>')
                else:
                    row = ''.join(f'<td>{c}</td>' for c in cells)
                    formatted_lines.append(f'<tr>{row}</tr>')
            else:
                if in_table:
                    in_table = False
                    formatted_lines.append('</tbody></table>')
                if trimmed.startswith('* '):
                    formatted_lines.append(f'<li>{trimmed[2:]}</li>')
                elif trimmed:
                    formatted_lines.append(f'<p>{trimmed}</p>')
                else:
                    formatted_lines.append('')
        if in_table:
            formatted_lines.append('</tbody></table>')
        return '\n'.join(formatted_lines)
    else:
        lines = html.split('\n')
        formatted = []
        in_list = False
        for line in lines:
            trimmed = line.strip()
            if trimmed.startswith('* '):
                if not in_list:
                    in_list = True
                    formatted.append('<ul>')
                formatted.append(f'<li>{trimmed[2:]}</li>')
            else:
                if in_list:
                    in_list = False
                    formatted.append('</ul>')
                if trimmed.startswith('<h3>'):
                    formatted.append(trimmed)
                elif trimmed:
                    formatted.append(f'<p>{trimmed}</p>')
        if in_list:
            formatted.append('</ul>')
        return '\n'.join(formatted)


def render_ssr_slate(shows: dict, mission: Optional[dict]) -> str:
    show_list = list(shows.values())
    if not show_list:
        return '<div class="slate-card">No active shows</div>'

    is_intervened = mission and mission.get("intervention_record")
    cards = []
    for s in show_list:
        show_id = s.id if hasattr(s, "id") else s.get("id")
        show_name = s.name if hasattr(s, "name") else s.get("name")
        show_client = s.client if hasattr(s, "client") else s.get("client")
        deadline = s.delivery_deadline if hasattr(s, "delivery_deadline") else s.get("delivery_deadline")
        critical = s.critical_path if hasattr(s, "critical_path") else s.get("critical_path")
        penalty = s.penalty_daily_amount if hasattr(s, "penalty_daily_amount") else s.get("penalty_daily_amount", 25000.0)

        if isinstance(deadline, datetime):
            deadline_str = deadline.strftime("%a, %d %b %Y %H:%M UTC")
        else:
            try:
                d_obj = datetime.fromisoformat(str(deadline).replace("Z", "+00:00"))
                deadline_str = d_obj.strftime("%a, %d %b %Y %H:%M UTC")
            except Exception:
                deadline_str = str(deadline)

        status_label, margin_hrs, buffer_margin = simulator.get_show_status(show_id)
        has_pending_hold = any(a.status == "PENDING" for a in PENDING_APPROVALS.values())
        if has_pending_hold and show_id in ('show-solarflare', 'show-solar'):
            status_label = "AT RISK"

        margin_class = "metric-healthy"

        if show_id == 'show-aethelgard' and is_intervened:
            rec = mission.get("intervention_record", {})
            m_val = rec.get("buffer_margin_hours", 2.8) if isinstance(rec, dict) else getattr(rec, "buffer_margin_hours", 2.8)
            buffer_margin = f'+{m_val:.1f} hours'
            status_tag = '<span class="state-tag tag-protected">PROTECTED</span>'
        elif status_label == "AT RISK":
            status_tag = '<span class="state-tag tag-critical">AT RISK</span>'
            margin_class = "metric-fault"
        elif status_label == "PROTECTED" or status_label == "DELIVERED":
            status_tag = f'<span class="state-tag tag-protected">{status_label}</span>'
        else:
            status_tag = '<span class="state-tag tag-scheduled">ON SCHEDULE</span>'

        crit_class = ' critical' if critical else ''
        cards.append(f"""
            <div class="slate-card{crit_class}" id="slate-card-{show_id}">
                <div class="slate-header">
                    <div>
                        <div class="show-title">{show_name}</div>
                        <div class="show-client">{show_client}</div>
                    </div>
                    <div class="slate-header-tag">{status_tag}</div>
                </div>
                <div class="slate-metrics">
                    <div class="metric-row">
                        <span class="metric-label">DEADLINE</span>
                        <span class="metric-val deadline-val">{deadline_str}</span>
                    </div>
                    <div class="metric-row">
                        <span class="metric-label">BUFFER MARGIN</span>
                        <span class="metric-val {margin_class} buffer-margin-val">{buffer_margin}</span>
                    </div>
                    <div class="metric-row">
                        <span class="metric-label">DAILY PENALTY</span>
                        <span class="metric-val">£{penalty:,.0f} / day</span>
                    </div>
                    <div class="metric-row">
                        <span class="metric-label">PRIORITY TIER</span>
                        <span class="metric-val">{'CRITICAL PATH' if critical else 'STANDARD'}</span>
                    </div>
                </div>
            </div>
        """)
    return ''.join(cards)


def render_ssr_delivery_banner(shot_118: Optional[Any]) -> str:
    if not shot_118:
        return '<div id="delivery-banner" class="delivery-banner" style="display: none;"></div>'
    status = getattr(shot_118, "status", None) or (shot_118.get("status") if isinstance(shot_118, dict) else None)
    delivered_at = getattr(shot_118, "delivered_at", None) or (shot_118.get("delivered_at") if isinstance(shot_118, dict) else None)
    margin = getattr(shot_118, "delivery_margin_hours_achieved", None) or (shot_118.get("delivery_margin_hours_achieved") if isinstance(shot_118, dict) else None)

    if (status == ShotStatus.COMPLETED or str(status).lower() in ("completed", "shotstatus.completed")) and delivered_at:
        try:
            if isinstance(delivered_at, str):
                dt = datetime.fromisoformat(delivered_at.replace("Z", "+00:00"))
            else:
                dt = delivered_at
            time_str = dt.strftime("%H:%M UTC")
        except Exception:
            time_str = str(delivered_at)[:16]
        margin_str = f"{margin:.1f}" if margin is not None else "2.5"
        return f'<div id="delivery-banner" class="delivery-banner">Shot 118 delivered {time_str}, {margin_str} hours ahead of deadline</div>'
    return '<div id="delivery-banner" class="delivery-banner" style="display: none;"></div>'


def render_ssr_trail(steps: list) -> str:
    if not steps:
        return '<div style="font-size: 12px; color: var(--text-dim);">No steps recorded.</div>'
    import json
    entries = []
    for step in steps:
        step_num = step.get("step_number") if isinstance(step, dict) else getattr(step, "step_number", 1)
        name = step.get("name") if isinstance(step, dict) else getattr(step, "name", "")
        desc = step.get("description") if isinstance(step, dict) else getattr(step, "description", "")
        exec_type = step.get("execution_type") if isinstance(step, dict) else getattr(step, "execution_type", "DETERMINISTIC_TELEMETRY")
        evidence = step.get("evidence") if isinstance(step, dict) else getattr(step, "evidence", {})
        evidence_str = json.dumps(evidence, indent=2) if evidence else ""

        if step_num == 0:
            badge_html = '<span class="badge-det">GRAFANA ALERT</span>'
        elif step_num in (3, 7) or exec_type == "GENERATIVE_SYNTHESIS":
            badge_html = '<span class="badge-gen">GENERATIVE AI</span>'
        else:
            badge_html = '<span class="badge-det">DETERMINISTIC</span>'

        entries.append(f"""
            <div class="step-entry">
                <div class="step-title">
                    <span>STEP {step_num}: {name.upper()}</span>
                    {badge_html}
                </div>
                <div class="step-desc">{desc}</div>
                {f'<pre class="step-evidence">{evidence_str}</pre>' if evidence_str and evidence_str != '{}' else ''}
            </div>
        """)
    return ''.join(entries)


def render_ssr_fleet(nodes: dict, mission: Optional[dict] = None) -> tuple[str, str]:
    node_list = list(nodes.values())
    if not node_list:
        return '<div>Loading node status...</div>', '10 ACTIVE / 2 STANDBY'

    active_count = 0
    quarantined_count = 0
    standby_count = 0
    maintenance_count = 0
    tiles = []

    for n in node_list:
        node_id = n.id if hasattr(n, "id") else n.get("id")
        status = n.status if hasattr(n, "status") else n.get("status")
        status_val = status.value if hasattr(status, "value") else str(status)
        is_standby = n.is_standby if hasattr(n, "is_standby") else n.get("is_standby", False)
        temp = n.temperature_celsius if hasattr(n, "temperature_celsius") else n.get("temperature_celsius", 55.0)
        shot_id = n.current_shot_id if hasattr(n, "current_shot_id") else n.get("current_shot_id")
        name = n.name if hasattr(n, "name") else n.get("name", "")

        is_maintenance = (status_val in ("OFFLINE", "MAINTENANCE")) or ("Maintenance" in name) or (node_id == "node-12" and not is_standby and not shot_id)

        if node_id == "node-07" and (status_val in ("QUARANTINED", "THROTTLED") or temp > 90.0):
            if mission and mission.get("intervention_record"):
                rec_ev = mission.get("intervention_record", {}).get("telemetry_evidence", {})
                if "source_temp" in rec_ev:
                    try:
                        temp = float(rec_ev["source_temp"])
                    except (ValueError, TypeError):
                        pass

        if status_val == "QUARANTINED":
            quarantined_count += 1
            node_class = "node-tile node-quarantined"
            temp_color = "var(--heat-fault)"
            shot_label = "QUARANTINED (FAULT)"
        elif is_maintenance:
            maintenance_count += 1
            node_class = "node-tile node-maintenance"
            temp_color = "var(--text-dim)"
            shot_label = "MAINTENANCE"
        elif is_standby:
            standby_count += 1
            node_class = "node-tile node-standby"
            temp_color = "var(--text-dim)"
            shot_label = "STANDBY SPARE"
        else:
            active_count += 1
            if status_val == "THROTTLED" or temp >= 90.0:
                node_class = "node-tile node-fault"
                temp_color = "var(--heat-fault)"
                shot_label = f"SHOT {shot_id.replace('sh_', '')}" if shot_id else "DEGRADED (FAULT)"
            elif temp >= 78.0:
                node_class = "node-tile node-hot"
                temp_color = "var(--heat-hot)"
                shot_label = f"SHOT {shot_id.replace('sh_', '')}" if shot_id else "HIGH LOAD"
            elif temp >= 68.0:
                node_class = "node-tile node-warm"
                temp_color = "var(--heat-warm)"
                shot_label = f"SHOT {shot_id.replace('sh_', '')}" if shot_id else "WARM ACTIVE"
            elif temp >= 58.0:
                node_class = "node-tile node-nominal"
                temp_color = "var(--state-healthy)"
                shot_label = f"SHOT {shot_id.replace('sh_', '')}" if shot_id else "NOMINAL"
            else:
                node_class = "node-tile node-cool"
                temp_color = "var(--state-healthy)"
                shot_label = f"SHOT {shot_id.replace('sh_', '')}" if shot_id else "COOL ACTIVE"

        tiles.append(f"""
            <div class="{node_class}" id="tile-{node_id}" data-node="{node_id}">
                <div class="node-tile-header">
                    <span class="node-id">{node_id.upper()}</span>
                    <span class="node-pip"></span>
                </div>
                <div class="node-temp" style="color: {temp_color};">{temp:.1f}°C</div>
                <div class="node-shot">{shot_label}</div>
            </div>
        """)

    summary_parts = [f"{active_count} ACTIVE"]
    if quarantined_count > 0:
        summary_parts.append(f"{quarantined_count} QUARANTINED")
    summary_parts.append(f"{standby_count} STANDBY")
    if maintenance_count > 0:
        summary_parts.append(f"{maintenance_count} MAINTENANCE")
    summary_str = " / ".join(summary_parts)

    return ''.join(tiles), summary_str


def render_ssr_briefing(mission: Optional[dict]) -> str:
    if not mission:
        return ""
    briefing_text = mission.get("callsheet_briefing", "")
    md_html = format_markdown_to_html(briefing_text)
    incident_id = mission.get("incident_id")
    incident_url = mission.get("incident_url")
    incident_status = mission.get("incident_status")
    time_res = mission.get("time_intervention_to_resolved_seconds")
    deeplinks = mission.get("deeplinks", {})
    panel_img = mission.get("panel_image_url")
    reads = mission.get("mcp_read_calls", 0)
    writes = mission.get("mcp_write_calls", 0)

    if not (incident_id or incident_url or deeplinks or panel_img):
        return md_html

    is_resolved = (incident_status == "resolved")
    badge_class = "badge-resolved" if is_resolved else "badge-active"
    status_label = f"RESOLVED IN {time_res:.1f}s" if (is_resolved and time_res) else ("RESOLVED" if is_resolved else "ACTIVE")
    cleared_sec = mission.get("quarantine_to_alert_cleared_seconds")
    cleared_badge_html = f'<span class="incident-badge badge-resolved">ALERT CLEARED IN {cleared_sec:.1f}s</span>' if cleared_sec else ""
    inc_url = incident_url or "#"

    cards = [f"""
        <div class="grafana-writeback-card">
            <div class="grafana-incident-row">
                <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
                    <span class="incident-badge {badge_class}">{status_label}</span>
                    {cleared_badge_html}
                    <span style="font-family: var(--font-condensed); font-weight: 700; font-size: 12px; color: var(--text-primary);">
                        GRAFANA IRM INCIDENT #{incident_id or ''}
                    </span>
                </div>
                <a href="{inc_url}" target="_blank" rel="noopener noreferrer" class="deeplink-btn" style="color: var(--accent);">
                    OPEN INCIDENT IN GRAFANA &rarr;
                </a>
            </div>
    """]

    if deeplinks:
        cards.append("""
            <div style="font-family: var(--font-condensed); font-size: 10px; color: var(--text-dim); text-transform: uppercase; margin-bottom: 4px;">
                PERSISTENT TELEMETRY EVIDENCE DEEPLINKS (PINNED ABSOLUTE TIME RANGE)
            </div>
            <div class="deeplinks-grid">
        """)
        if deeplinks.get("prometheus"):
            cards.append(f'<a href="{deeplinks["prometheus"]}" target="_blank" rel="noopener noreferrer" class="deeplink-btn">Prometheus Explore &nearr;</a>')
        if deeplinks.get("loki"):
            cards.append(f'<a href="{deeplinks["loki"]}" target="_blank" rel="noopener noreferrer" class="deeplink-btn">Loki Logs Explore &nearr;</a>')
        if deeplinks.get("tempo"):
            cards.append(f'<a href="{deeplinks["tempo"]}" target="_blank" rel="noopener noreferrer" class="deeplink-btn">Tempo Trace Explore &nearr;</a>')
        if deeplinks.get("dashboard"):
            cards.append(f'<a href="{deeplinks["dashboard"]}" target="_blank" rel="noopener noreferrer" class="deeplink-btn">Control Tower Dashboard &nearr;</a>')
        cards.append("</div>")

    if panel_img:
        cards.append(f"""
            <div class="panel-snapshot-box">
                <img src="{panel_img}" alt="Grafana Control Tower Panel Snapshot" />
                <div class="panel-snapshot-meta">
                    <span>CONTROL TOWER PANEL SNAPSHOT (get_panel_image)</span>
                    <span>MISSION MCP TOOL CALLS: {reads} READS / {writes} WRITES</span>
                </div>
            </div>
        """)

    cards.append("</div>")
    return "".join(cards) + md_html


PRODUCER_UI_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Callsheet: Autonomous Operations Agent for Post-Production Delivery Producers</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@500;600;700&family=Barlow:wght@400;500;600&family=JetBrains+Mono:wght@400;500;600&family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;1,6..72,400&display=swap" rel="stylesheet">
    <style>
        :root {
            /* Finishing Suite Neutral Charcoal Ground & Graphite Surfaces */
            --bg: #121211;
            --surface: #191817;
            --surface-raised: #21201f;
            --surface-inset: #0c0c0b;

            /* Hairlines in Two Weights (Paperwork Rules) */
            --rule: rgba(235, 230, 220, 0.12);
            --rule-strong: rgba(235, 230, 220, 0.24);

            /* Lifted Text Tints: High Contrast bone, warm stone, and legible grey (all >= 4.5:1, prose >= 7:1) */
            --text-primary: #f5f2ec;
            --text-secondary: #d0cbc2;
            --text-dim: #b0aaa0;

            /* Sparing Neutral Accent: Studio Titanium / Bone */
            --accent: #ded8cb;

            /* Semantic Healthy / Protected: Low Saturation Sage */
            --state-healthy: #76c79b;
            --state-healthy-wash: rgba(118, 199, 155, 0.09);
            --state-healthy-border: rgba(118, 199, 155, 0.35);

            /* Thermal Scale (Strictly Reserved for Heat on the Fleet Grid) */
            --heat-warm: #f59e0b;
            --heat-hot: #fb923c;
            --heat-fault: #f87171;

            /* Typography */
            --font-condensed: 'Barlow Condensed', 'Archivo Narrow', -apple-system, sans-serif;
            --font-serif: 'Newsreader', Georgia, 'Times New Roman', serif;
            --font-sans: 'Barlow', 'Inter', -apple-system, sans-serif;
            --font-mono: 'JetBrains Mono', ui-monospace, SFMono-Regular, monospace;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            background-color: var(--bg);
            color: var(--text-secondary);
            font-family: var(--font-sans);
            font-size: 17px;
            line-height: 1.6;
            padding: 28px 24px;
            -webkit-font-smoothing: antialiased;
            min-height: 100vh;
        }

        .container { max-width: 1400px; margin: 0 auto; }

        /* Call Sheet Masthead Block */
        .masthead {
            margin-bottom: 24px;
        }

        .masthead-main {
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
            padding-bottom: 14px;
        }

        .masthead-title {
            font-family: var(--font-condensed);
            font-size: 36px;
            line-height: 1.2;
            font-weight: 700;
            letter-spacing: 0.06em;
            color: var(--text-primary);
        }

        .masthead-tagline {
            font-family: var(--font-condensed);
            font-size: 12px;
            line-height: 1.3;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--text-dim);
            margin-top: 5px;
            display: block;
        }

        .masthead-link {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 12px;
            font-size: 12px;
            line-height: 1.3;
            font-weight: 700;
            font-family: var(--font-condensed);
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--text-primary);
            background: var(--surface);
            border: 1px solid var(--rule-strong);
            border-radius: 2px;
            text-decoration: none;
            transition: background 0.15s ease, border-color 0.15s ease;
        }

        .masthead-link:hover {
            background: var(--surface-raised);
            border-color: var(--text-primary);
            text-decoration: none;
        }

        /* Ruled Masthead Grid */
        .masthead-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            border-top: 1px solid var(--rule-strong);
            border-bottom: 1px solid var(--rule-strong);
            margin-top: 8px;
        }

        @media (max-width: 900px) {
            .masthead-grid { grid-template-columns: repeat(2, 1fr); }
        }

        .masthead-cell {
            padding: 10px 14px;
            border-right: 1px solid var(--rule);
            display: flex;
            flex-direction: column;
            gap: 4px;
        }

        .masthead-cell:last-child {
            border-right: none;
        }

        .cell-label {
            font-family: var(--font-condensed);
            font-size: 12px;
            line-height: 1.3;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--text-dim);
        }

        .cell-value {
            font-family: var(--font-condensed);
            font-size: 16px;
            line-height: 1.3;
            font-weight: 600;
            letter-spacing: 0.04em;
            color: var(--text-primary);
            text-transform: uppercase;
        }

        .cell-value.mono {
            font-family: var(--font-mono);
            font-size: 16px;
            line-height: 1.3;
            font-variant-numeric: tabular-nums;
        }

        .status-cell-val {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            color: var(--state-healthy);
        }

        .status-pip {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: currentColor;
        }

        /* Orienting Mission Band */
        .orienting-band {
            border-top: 1px solid var(--rule-strong);
            border-bottom: 1px solid var(--rule);
            padding: 16px 0;
            margin-bottom: 28px;
        }

        .orienting-text {
            font-family: var(--font-sans);
            font-size: 17px;
            line-height: 1.6;
            color: var(--text-secondary);
            max-width: 72ch;
            text-align: left;
            text-wrap: pretty;
        }

        /* Ruled Section Headers */
        .section-header {
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            padding-bottom: 8px;
            border-bottom: 1px solid var(--rule-strong);
            margin-bottom: 16px;
        }

        .section-title {
            font-family: var(--font-condensed);
            font-size: 20px;
            line-height: 1.2;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: var(--text-primary);
        }

        .section-sub {
            font-family: var(--font-condensed);
            font-size: 12px;
            line-height: 1.3;
            font-weight: 600;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-dim);
        }

        /* Active Delivery Slate (Ruled columns) */
        .slate-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            border-top: 1px solid var(--rule-strong);
            border-bottom: 1px solid var(--rule-strong);
            margin-bottom: 32px;
        }

        @media (max-width: 960px) {
            .slate-grid { grid-template-columns: 1fr; }
        }

        .slate-card {
            padding: 18px 20px;
            border-right: 1px solid var(--rule);
            background: transparent;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }

        .slate-card:last-child {
            border-right: none;
        }

        .slate-card.critical {
            background: rgba(255, 255, 255, 0.02);
            border-top: 2px solid var(--text-primary);
            margin-top: -1px;
        }

        .slate-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 12px;
            margin-bottom: 16px;
        }

        .show-title {
            font-family: var(--font-condensed);
            font-size: 24px;
            line-height: 1.2;
            font-weight: 700;
            letter-spacing: 0.02em;
            color: var(--text-primary);
            text-transform: uppercase;
        }

        .show-client {
            font-size: 12px;
            line-height: 1.3;
            color: var(--text-dim);
            margin-top: 3px;
        }

        .state-tag {
            font-family: var(--font-condensed);
            font-size: 12px;
            line-height: 1.3;
            font-weight: 700;
            letter-spacing: 0.08em;
            padding: 3px 8px;
            border-radius: 2px;
            text-transform: uppercase;
            white-space: nowrap;
        }

        .tag-scheduled {
            color: var(--text-secondary);
            border: 1px solid var(--rule-strong);
            background: transparent;
        }

        .tag-protected {
            color: var(--state-healthy);
            border: 1px solid var(--state-healthy-border);
            background: var(--state-healthy-wash);
        }

        .tag-slipping {
            color: var(--heat-warm);
            border: 1px solid rgba(245, 158, 11, 0.35);
            background: rgba(245, 158, 11, 0.09);
        }

        .tag-critical {
            color: var(--heat-fault);
            border: 1px solid rgba(248, 113, 113, 0.35);
            background: rgba(248, 113, 113, 0.09);
        }

        .delivery-banner {
            display: flex;
            align-items: center;
            justify-content: flex-start;
            padding: 12px 18px;
            margin-bottom: 24px;
            background: var(--state-healthy-wash);
            border: 1px solid var(--state-healthy-border);
            border-left: 4px solid var(--state-healthy);
            border-radius: 4px;
            font-family: var(--font-condensed);
            font-size: 13px;
            font-weight: 700;
            letter-spacing: 0.05em;
            color: var(--state-healthy);
            text-transform: uppercase;
        }

        .slate-metrics {
            display: flex;
            flex-direction: column;
            border-top: 1px solid var(--rule);
        }

        .metric-row {
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            padding: 8px 0;
            border-bottom: 1px solid var(--rule);
        }

        .metric-row:last-child {
            border-bottom: none;
        }

        .metric-label {
            font-family: var(--font-condensed);
            font-size: 12px;
            line-height: 1.3;
            font-weight: 600;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-dim);
        }

        .metric-val {
            font-family: var(--font-mono);
            font-size: 16px;
            line-height: 1.3;
            font-variant-numeric: tabular-nums;
            color: var(--text-primary);
        }

        .metric-val.metric-healthy {
            color: var(--state-healthy);
        }

        .metric-val.metric-fault {
            color: var(--heat-fault);
        }

        .metric-val.metric-warning {
            color: var(--heat-warm);
        }

        /* Main Layout: Ruled Columns */
        .main-layout {
            display: grid;
            grid-template-columns: 1.55fr 1fr;
            gap: 32px;
        }

        @media (max-width: 1024px) {
            .main-layout { grid-template-columns: 1fr; }
        }

        /* Left: Producer Briefing in Serif */
        .briefing-block {
            padding-bottom: 24px;
        }

        .briefing-content {
            font-family: var(--font-serif);
            font-size: 17px;
            line-height: 1.6;
            color: var(--text-primary);
            max-width: 70ch;
        }

        .briefing-content h3 {
            font-family: var(--font-condensed);
            font-size: 20px;
            line-height: 1.2;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: var(--text-primary);
            margin: 24px 0 10px 0;
        }

        .briefing-content p {
            font-size: 17px;
            line-height: 1.6;
            margin-bottom: 16px;
            color: var(--text-primary);
        }

        .briefing-content p strong {
            color: #ffffff;
            font-weight: 600;
        }

        .briefing-content table {
            width: 100%;
            border-collapse: collapse;
            margin: 18px 0;
            font-family: var(--font-mono);
            font-size: 16px;
            line-height: 1.4;
            font-variant-numeric: tabular-nums;
            border-top: 1px solid var(--rule-strong);
            border-bottom: 1px solid var(--rule-strong);
        }

        .briefing-content th, .briefing-content td {
            padding: 8px 10px;
            text-align: left;
            border-bottom: 1px solid var(--rule);
        }

        .briefing-content th {
            font-family: var(--font-condensed);
            font-size: 12px;
            line-height: 1.3;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-dim);
            background: transparent;
        }

        .briefing-content td {
            font-size: 16px;
            line-height: 1.4;
            color: var(--text-primary);
        }

        .briefing-content tr:last-child td {
            border-bottom: none;
        }

        .briefing-content ul {
            padding-left: 20px;
            margin-bottom: 16px;
        }

        .briefing-content li {
            font-size: 17px;
            line-height: 1.6;
            margin-bottom: 8px;
            color: var(--text-primary);
        }

        /* Evidence Chain Accordion */
        details.trail-accordion {
            border-top: 1px solid var(--rule-strong);
            padding-top: 16px;
            margin-top: 24px;
        }

        details.trail-accordion summary {
            font-family: var(--font-condensed);
            font-size: 20px;
            line-height: 1.2;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: var(--text-primary);
            cursor: pointer;
            user-select: none;
        }

        .step-timeline {
            margin-top: 16px;
        }

        .step-entry {
            padding: 14px 0;
            border-bottom: 1px solid var(--rule);
        }

        .step-title {
            font-family: var(--font-condensed);
            font-size: 16px;
            line-height: 1.3;
            font-weight: 700;
            letter-spacing: 0.04em;
            color: var(--text-primary);
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 10px;
        }

        .badge-det {
            font-family: var(--font-condensed);
            font-size: 12px;
            line-height: 1.3;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            padding: 3px 8px;
            border: 1px solid var(--rule-strong);
            color: var(--text-secondary);
            background: transparent;
            border-radius: 2px;
            white-space: nowrap;
        }

        .badge-gen {
            font-family: var(--font-condensed);
            font-size: 12px;
            line-height: 1.3;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            padding: 3px 8px;
            border: 1px solid rgba(222, 216, 203, 0.4);
            color: var(--text-primary);
            background: rgba(222, 216, 203, 0.08);
            border-radius: 2px;
            white-space: nowrap;
        }

        .step-desc {
            font-size: 17px;
            line-height: 1.6;
            color: var(--text-secondary);
            margin-top: 6px;
        }

        .step-evidence {
            font-family: var(--font-mono);
            font-size: 16px;
            line-height: 1.4;
            font-variant-numeric: tabular-nums;
            background: var(--surface-inset);
            border: 1px solid var(--rule);
            padding: 10px 12px;
            border-radius: 2px;
            margin-top: 10px;
            color: var(--text-secondary);
            white-space: pre-wrap;
            word-break: break-word;
        }

        /* Fleet Telemetry Grid */
        .fleet-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 8px;
            position: relative;
        }

        @media (max-width: 1200px) {
            .fleet-grid { grid-template-columns: repeat(3, 1fr); }
        }

        @media (max-width: 768px) {
            .fleet-grid { grid-template-columns: repeat(2, 1fr); }
        }

        .node-tile {
            background: var(--surface);
            border: 1px solid var(--rule);
            padding: 10px 10px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            min-height: 80px;
            position: relative;
            border-radius: 2px;
        }

        .node-tile-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .node-id {
            font-family: var(--font-condensed);
            font-size: 12px;
            line-height: 1.3;
            font-weight: 700;
            letter-spacing: 0.04em;
            color: var(--text-secondary);
        }

        .node-pip {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: currentColor;
        }

        .node-temp {
            font-family: var(--font-mono);
            font-size: 16px;
            line-height: 1.3;
            font-weight: 600;
            font-variant-numeric: tabular-nums;
            margin: 4px 0 2px 0;
        }

        .node-shot {
            font-family: var(--font-condensed);
            font-size: 12px;
            line-height: 1.3;
            letter-spacing: 0.04em;
            color: var(--text-dim);
            text-transform: uppercase;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        /* Thermal Scales (Amber through red carries thermal scale) */
        .node-tile.node-cool {
            border-color: var(--rule);
        }
        .node-tile.node-cool .node-pip { color: var(--state-healthy); }

        .node-tile.node-nominal {
            border-color: var(--rule);
        }
        .node-tile.node-nominal .node-pip { color: var(--state-healthy); }

        .node-tile.node-warm {
            border-color: rgba(245, 158, 11, 0.40);
            background: rgba(245, 158, 11, 0.06);
        }
        .node-tile.node-warm .node-pip { color: var(--heat-warm); }

        .node-tile.node-hot {
            border-color: rgba(251, 146, 60, 0.50);
            background: rgba(251, 146, 60, 0.08);
        }
        .node-tile.node-hot .node-pip { color: var(--heat-hot); }

        .node-tile.node-fault {
            border-color: rgba(248, 113, 113, 0.55);
            background: rgba(248, 113, 113, 0.10);
        }
        .node-tile.node-fault .node-pip { color: var(--heat-fault); }

        .node-tile.node-quarantined {
            border: 1px solid var(--heat-fault);
            outline: 1px dashed var(--heat-fault);
            outline-offset: 2px;
            background: rgba(248, 113, 113, 0.10);
        }
        .node-tile.node-quarantined .node-pip { color: var(--heat-fault); }

        .node-tile.node-standby {
            border: 1px dashed var(--rule-strong);
            background: transparent;
            opacity: 0.65;
        }
        .node-tile.node-standby .node-pip { color: var(--text-dim); }

        .node-tile.node-maintenance {
            border: 1px dashed var(--rule-strong);
            background: rgba(255, 255, 255, 0.03);
            opacity: 0.55;
        }
        .node-tile.node-maintenance .node-pip { color: var(--text-dim); }

        /* Failover Takeover Arrival Pulse */
        .node-takeover-pulse {
            animation: takeoverPulse 1.2s ease-out;
        }

        @keyframes takeoverPulse {
            0% { box-shadow: 0 0 0 0 rgba(237, 233, 227, 0.5); }
            70% { box-shadow: 0 0 0 8px rgba(237, 233, 227, 0); }
            100% { box-shadow: 0 0 0 0 rgba(237, 233, 227, 0); }
        }

        /* MCP Grafana Write-back and Deeplinks Styles */
        .grafana-writeback-card {
            background: var(--surface-inset);
            border: 1px solid var(--rule);
            border-radius: 2px;
            padding: 12px;
            margin-bottom: 12px;
        }
        .grafana-incident-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 8px;
            margin-bottom: 10px;
            padding-bottom: 8px;
            border-bottom: 1px solid var(--rule);
        }
        .incident-badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            font-family: var(--font-condensed);
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            padding: 3px 8px;
            border-radius: 2px;
        }
        .badge-active {
            background: rgba(239, 68, 68, 0.15);
            color: var(--state-critical);
            border: 1px solid rgba(239, 68, 68, 0.4);
        }
        .badge-resolved {
            background: rgba(16, 185, 129, 0.15);
            color: var(--state-healthy);
            border: 1px solid rgba(16, 185, 129, 0.4);
        }
        .deeplinks-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 8px;
            margin-top: 8px;
        }
        .deeplink-btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
            background: var(--surface);
            border: 1px solid var(--rule);
            color: var(--text-primary);
            font-family: var(--font-condensed);
            font-size: 11px;
            font-weight: 600;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            padding: 6px 10px;
            border-radius: 2px;
            text-decoration: none;
            transition: all 0.15s ease;
        }
        .deeplink-btn:hover {
            background: var(--surface-hover);
            border-color: var(--accent);
            color: var(--accent);
        }
        .panel-snapshot-box {
            margin-top: 10px;
            border: 1px solid var(--rule);
            border-radius: 2px;
            overflow: hidden;
            background: #0b0c0e;
            width: 100%;
            aspect-ratio: 2 / 1;
            display: flex;
            flex-direction: column;
        }
        .panel-snapshot-box img {
            width: 100%;
            flex: 1 1 auto;
            min-height: 0;
            object-fit: contain;
            display: block;
        }
        .panel-snapshot-meta {
            padding: 6px 10px;
            font-family: var(--font-mono);
            font-size: 11px;
            color: var(--text-dim);
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-top: 1px solid var(--rule);
            background: #0b0c0e;
            flex-shrink: 0;
        }

        /* Footer Disclosure */
        .footer-note {
            text-align: center;
            font-size: 12px;
            line-height: 1.6;
            font-family: var(--font-mono);
            color: var(--text-dim);
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid var(--rule);
            max-width: 820px;
            margin-left: auto;
            margin-right: auto;
        }

        /* Authority & Blast Radius Policy Panel */
        .authority-section {
            margin-bottom: 24px;
        }
        .authority-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 12px;
        }
        .authority-card {
            background: var(--surface);
            border: 1px solid var(--rule);
            padding: 14px 16px;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        .authority-badge {
            font-size: 10px;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }
        .tier-1-badge { color: var(--accent); }
        .tier-2-badge { color: var(--heat-warm); }
        .tier-3-badge { color: var(--state-critical); }
        .authority-text {
            font-size: 12px;
            line-height: 1.5;
            color: var(--text-dim);
            margin: 0;
        }

        /* Tier 2 Approval Card */
        .approval-banner {
            background: rgba(210, 153, 34, 0.12);
            border: 1px solid var(--state-warning);
            padding: 16px 20px;
            margin-bottom: 24px;
            animation: fadeIn 0.3s ease-out;
        }
        .approval-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }
        .approval-badge {
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.08em;
            color: #e3b341;
        }
        .approval-title {
            font-size: 15px;
            font-weight: 700;
            color: var(--text-bright);
            margin-bottom: 8px;
        }
        .approval-plan {
            font-size: 13px;
            color: var(--text-light);
            line-height: 1.5;
            margin-bottom: 12px;
        }
        .approval-arithmetic {
            background: var(--surface-inset, #0c0c0b);
            border: 1px solid var(--rule, rgba(235, 230, 220, 0.15));
            padding: 12px 14px;
            font-family: var(--font-mono);
            font-size: 12px;
            color: var(--text-secondary, #d0cbc2);
            line-height: 1.6;
            margin-bottom: 14px;
        }
        .approval-arithmetic strong {
            color: var(--text-primary, #f5f2ec);
            font-weight: 600;
        }
        .approval-controls {
            display: flex;
            gap: 12px;
        }
        .btn-approve {
            background: #238636;
            color: #ffffff;
            border: 1px solid #2ea043;
            padding: 8px 18px;
            font-size: 12px;
            font-weight: 700;
            letter-spacing: 0.04em;
            cursor: pointer;
            transition: all 0.15s ease;
        }
        .btn-approve:hover { background: #2ea043; }
        .btn-decline {
            background: #21262d;
            color: #f85149;
            border: 1px solid rgba(248, 81, 73, 0.4);
            padding: 8px 18px;
            font-size: 12px;
            font-weight: 700;
            letter-spacing: 0.04em;
            cursor: pointer;
            transition: all 0.15s ease;
        }
        .btn-decline:hover { background: rgba(248, 81, 73, 0.25); }

        @media (prefers-reduced-motion: reduce) {
            * {
                animation-duration: 0.01ms !important;
                animation-iteration-count: 1 !important;
                transition-duration: 0.01ms !important;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- Production Masthead Block -->
        <header class="masthead">
            <div class="masthead-main">
                <div class="masthead-brand">
                    <h1 class="masthead-title">CALLSHEET</h1>
                    <span class="masthead-tagline">AUTONOMOUS VFX & POST-PRODUCTION RENDER OPERATIONS</span>
                </div>
                <div class="masthead-meta-actions">
                    <a href="https://bigforest2172.grafana.net/public-dashboards/a9028daf791643b8899a10531f6b31dd" target="_blank" rel="noopener noreferrer" class="masthead-link">
                        GRAFANA CONTROL TOWER ↗
                    </a>
                </div>
            </div>

            <div class="masthead-grid">
                <div class="masthead-cell">
                    <span class="cell-label">SYSTEM / UNIT</span>
                    <span class="cell-value">POST-PRODUCTION // VFX PIPELINE</span>
                </div>
                <div class="masthead-cell">
                    <span class="cell-label">CALL TIME (CLOCK)</span>
                    <span class="cell-value mono" id="utc-clock">--:--:-- UTC</span>
                </div>
                <div class="masthead-cell">
                    <span class="cell-label">INCIDENT AUDIT</span>
                    <span class="cell-value mono" id="briefing-timestamp"><!-- SSR_BRIEFING_TIME --></span>
                </div>
                <div class="masthead-cell">
                    <span class="cell-label">SUPERVISOR WATCH</span>
                    <span class="cell-value status-cell-val" id="agent-status-text"><!-- SSR_STATUS_TEXT --></span>
                </div>
            </div>
        </header>

        <!-- Orienting Mission Band -->
        <section class="orienting-band">
            <p class="orienting-text">
                Callsheet watches a post-production render farm and protects contractual delivery dates. When a node degrades, it works out which shots are at risk, moves them to spare capacity, verifies the fix in the telemetry, and reports what it did in plain language a producer can forward to a client.
            </p>
        </section>

<!-- SSR_DELIVERY_BANNER -->

        <!-- Pending Approval Notice Container -->
        <div id="approval-card-container">
<!-- SSR_APPROVAL_CARD -->
        </div>

        <!-- Active Delivery Slate -->
        <section class="slate-section">
            <div class="section-header">
                <span class="section-title">Active Delivery Slate</span>
                <span class="section-sub">CONTRACTUAL SCHEDULE & DEADLINE MARGINS</span>
            </div>
            <div id="slate-container" class="slate-grid">
<!-- SSR_SLATE -->
            </div>
        </section>

        <!-- Authority & Blast Radius Policy Panel -->
        <section class="authority-section">
            <div class="section-header">
                <span class="section-title">Authority & Blast Radius Policy</span>
                <span class="section-sub">AUTONOMOUS DISPATCH TIERS & OPERATIONAL BOUNDARIES</span>
            </div>
            <div class="authority-grid">
                <div class="authority-card">
                    <div class="authority-badge tier-1-badge">Tier 1 // Autonomous Execution</div>
                    <p class="authority-text">Moving a shot onto an idle standby node; quarantining a node that has breached its own thermal limit. Reversible, touches no other show, no producer-visible change to any deadline.</p>
                </div>
                <div class="authority-card">
                    <div class="authority-badge tier-2-badge">Tier 2 // Producer Approval Required</div>
                    <p class="authority-text">Pre-empting a node currently rendering another show's shot; anything touching more than one show; anything changing a delivery commitment. Holds execution with buffer loss arithmetic.</p>
                </div>
                <div class="authority-card">
                    <div class="authority-badge tier-3-badge">Tier 3 // Prohibited</div>
                    <p class="authority-text">Anything outside the farm. No external cloud capacity, no vendor escalation calls, no financial spend.</p>
                </div>
            </div>
        </section>

        <!-- Main Workspace -->
        <div class="main-layout">
            <!-- Left: Callsheet Briefing & Audit -->
            <div class="briefing-column">
                <div class="section-header">
                    <span class="section-title">Production Callsheet Briefing</span>
                    <span class="section-sub">CLIENT CORRESPONDENCE DISPATCH</span>
                </div>

                <div id="briefing-container" class="briefing-content">
<!-- SSR_BRIEFING -->
                </div>

                <details class="trail-accordion" open>
                    <summary>GRAFANA CLOUD MCP EVIDENCE TRAIL // AUDIT CHAIN</summary>
                    <div id="trail-container" class="step-timeline">
<!-- SSR_TRAIL -->
                    </div>
                </details>
            </div>

            <!-- Right: Fleet Hardware Grid -->
            <div class="fleet-column">
                <div class="section-header">
                    <span class="section-title">Render Fleet Telemetry</span>
                    <span id="fleet-summary" class="section-sub"><!-- SSR_FLEET_SUMMARY --></span>
                </div>
                <div id="fleet-container" class="fleet-grid">
<!-- SSR_FLEET -->
                </div>
            </div>
        </div>

        <div class="footer-note">
            Callsheet Autonomous Post-Production Agent. The render farm is a simulator emitting genuine OpenTelemetry metrics, logs and traces to Grafana Cloud, and the agent reads that telemetry back through the Grafana MCP server exactly as it would read a real farm.
        </div>
    </div>

    <script>
        let lastRenderedMissionId = "<!-- SSR_MISSION_ID -->";
        let lastRenderedVerificationStatus = "<!-- SSR_VERIFICATION_STATUS -->";
        let lastRenderedIncidentStatus = "<!-- SSR_INCIDENT_STATUS -->";
        let lastRenderedStepsLength = <!-- SSR_STEPS_LENGTH -->;
        let lastRenderedVerificationActive = false;
        let lastRenderedVerificationElapsed = null;

        function updateClock() {
            const now = new Date();
            document.getElementById('utc-clock').innerText = now.toUTCString().split(' ')[4] + ' UTC';
        }
        setInterval(updateClock, 1000);
        updateClock();

        function formatMarkdown(text) {
            if (!text) return '';
            let html = text
                .replace(/^### (.*$)/gim, '<h3>$1</h3>')
                .replace(/^## (.*$)/gim, '<h3>$1</h3>')
                .replace(/^# (.*$)/gim, '<h3>$1</h3>')
                .replace(/\\*\\*(.*?)\\*\\*/gim, '<strong>$1</strong>')
                .replace(/\\*(.*?)\\*/gim, '<em>$1</em>');
            
            if (html.includes('|')) {
                const lines = html.split('\\n');
                let inTable = false;
                let tableHtml = '<table>';
                let formattedLines = [];

                for (let i = 0; i < lines.length; i++) {
                    const line = lines[i].trim();
                    if (line.startsWith('|') && line.endsWith('|')) {
                        if (line.includes(':---') || line.includes('---')) continue;
                        if (!inTable) {
                            inTable = true;
                            tableHtml = '<table><thead><tr>' + line.split('|').filter(c => c.trim().length > 0).map(c => `<th>${c.trim()}</th>`).join('') + '</tr></thead><tbody>';
                        } else {
                            tableHtml += '<tr>' + line.split('|').filter(c => c.trim().length > 0).map(c => `<td>${c.trim()}</td>`).join('') + '</tr>';
                        }
                    } else {
                        if (inTable) {
                            tableHtml += '</tbody></table>';
                            formattedLines.push(tableHtml);
                            inTable = false;
                        }
                        formattedLines.push(line);
                    }
                }
                if (inTable) {
                    tableHtml += '</tbody></table>';
                    formattedLines.push(tableHtml);
                }
                html = formattedLines.join('<br>');
            }
            return html;
        }

        function triggerFailoverMotion(sourceId, targetId) {
            if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
            const source = document.getElementById('tile-' + sourceId);
            const target = document.getElementById('tile-' + targetId);
            const grid = document.getElementById('fleet-container');
            if (!source || !target || !grid) return;

            const gridRect = grid.getBoundingClientRect();
            const sRect = source.getBoundingClientRect();
            const tRect = target.getBoundingClientRect();

            const startX = sRect.left - gridRect.left + sRect.width / 2;
            const startY = sRect.top - gridRect.top + sRect.height / 2;
            const endX = tRect.left - gridRect.left + tRect.width / 2;
            const endY = tRect.top - gridRect.top + tRect.height / 2;

            const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
            svg.setAttribute('class', 'failover-svg-overlay');
            svg.style.position = 'absolute';
            svg.style.top = '0';
            svg.style.left = '0';
            svg.style.width = '100%';
            svg.style.height = '100%';
            svg.style.pointerEvents = 'none';
            svg.style.zIndex = '10';

            const dx = endX - startX;
            const dy = endY - startY;
            const cx = startX + dx * 0.5 - (dy * 0.15);
            const cy = startY + dy * 0.5 - Math.abs(dx * 0.15) - 25;
            const pathD = `M ${startX} ${startY} Q ${cx} ${cy} ${endX} ${endY}`;

            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path.setAttribute('d', pathD);
            path.setAttribute('fill', 'none');
            path.setAttribute('stroke', 'rgba(237, 233, 227, 0.35)');
            path.setAttribute('stroke-width', '1.5');
            path.setAttribute('stroke-dasharray', '4 4');

            const bead = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            bead.setAttribute('r', '4.5');
            bead.setAttribute('fill', '#ede9e3');

            svg.appendChild(path);
            svg.appendChild(bead);
            grid.appendChild(svg);

            const startTime = performance.now();
            const duration = 1200;
            const pathLen = path.getTotalLength();

            function step(now) {
                const elapsed = now - startTime;
                const p = Math.min(elapsed / duration, 1.0);
                const ease = 1 - Math.pow(1 - p, 3);
                const pt = path.getPointAtLength(ease * pathLen);
                bead.setAttribute('cx', pt.x);
                bead.setAttribute('cy', pt.y);

                if (p < 1.0) {
                    requestAnimationFrame(step);
                } else {
                    target.classList.add('node-takeover-pulse');
                    setTimeout(() => {
                        svg.style.transition = 'opacity 0.6s ease';
                        svg.style.opacity = '0';
                        setTimeout(() => svg.remove(), 600);
                    }, 800);
                }
            }
            requestAnimationFrame(step);
        }

        async function handleProducerDecision(approvalId, decision) {
            try {
                const btn = event ? event.target : null;
                if (btn) btn.disabled = true;
                await fetch(`/api/approvals/${approvalId}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ decision: decision })
                });
                await fetchState();
            } catch (e) {
                console.error("Error sending approval decision:", e);
            }
        }

        function renderApprovalCards(pendingApprovals) {
            const container = document.getElementById('approval-card-container');
            if (!container) return;
            const pending = (pendingApprovals || []).filter(a => a.status === 'PENDING');
            if (pending.length === 0) {
                container.innerHTML = '';
                return;
            }
            container.innerHTML = pending.map(a => `
                <div class="approval-banner">
                    <div class="approval-header">
                        <span class="approval-badge">TIER 2 APPROVAL REQUIRED // HOLD ACTIVE</span>
                        <span class="approval-id mono">${a.id}</span>
                    </div>
                    <div class="approval-title">${a.action_title}</div>
                    <div class="approval-plan">${a.plan_summary}</div>
                    <div class="approval-arithmetic mono">
                        <div><strong>Buffer Loss Rate:</strong> ${a.buffer_loss_rate}</div>
                        <div><strong>Cost of Waiting:</strong> ${a.cost_of_waiting}</div>
                        <div><strong>Deadline Impact:</strong> ${a.deadline_impact}</div>
                    </div>
                    <div class="approval-controls">
                        <button class="btn-approve" onclick="handleProducerDecision('${a.id}', 'approve')">Approve Reallocation</button>
                        <button class="btn-decline" onclick="handleProducerDecision('${a.id}', 'decline')">Decline & Escalate</button>
                    </div>
                </div>
            `).join('');
        }

        async function fetchState() {
            try {
                const res = await fetch('/api/state');
                const data = await res.json();

                const statusText = document.getElementById('agent-status-text');
                const hasPendingHold = data.pending_approvals && data.pending_approvals.some(a => a.status === 'PENDING');
                if (data.watchdog_stalled) {
                    statusText.innerHTML = '<span class="status-pip" style="color: var(--state-critical);"></span> Agent stalled, check health';
                } else if (hasPendingHold) {
                    statusText.innerHTML = '<span class="status-pip" style="color: var(--heat-warm);"></span> Tier 2 hold: awaiting producer approval';
                } else if (data.verification_progress && data.verification_progress.active) {
                    statusText.innerHTML = '<span class="status-pip" style="color: var(--heat-warm);"></span> ' + data.verification_progress.message;
                } else if (data.is_investigating) {
                    statusText.innerHTML = '<span class="status-pip" style="color: var(--heat-warm);"></span> INVESTIGATING ANOMALY (MCP)';
                } else if (data.alert_pending) {
                    statusText.innerHTML = '<div style="display: flex; flex-direction: column;"><div style="display: flex; align-items: center; gap: 6px;"><span class="status-pip" style="color: var(--state-critical);"></span> Grafana alert pending</div><div style="font-size: 10px; color: var(--text-muted); font-weight: normal; text-transform: none; letter-spacing: normal; margin-top: 2px;">Grafana evaluates every minute</div></div>';
                } else {
                    statusText.innerHTML = '<span class="status-pip"></span> AUTONOMOUS WATCH ACTIVE';
                }

                renderApprovalCards(data.pending_approvals);

                renderSlate(data.shows, data.latest_mission, data.verification_progress, data.pending_approvals);

                const sh118 = data.shots && data.shots.sh_118;
                const banner = document.getElementById('delivery-banner');
                if (banner) {
                    if (sh118 && (sh118.status === 'COMPLETED' || sh118.status === 'completed') && sh118.delivered_at) {
                        const d = new Date(sh118.delivered_at);
                        const hr = String(d.getUTCHours()).padStart(2, '0');
                        const mn = String(d.getUTCMinutes()).padStart(2, '0');
                        const margin = (sh118.delivery_margin_hours_achieved !== undefined && sh118.delivery_margin_hours_achieved !== null)
                            ? Number(sh118.delivery_margin_hours_achieved).toFixed(1)
                            : '2.5';
                        const bannerText = `Shot 118 delivered ${hr}:${mn} UTC, ${margin} hours ahead of deadline`;
                        if (banner.textContent !== bannerText) banner.textContent = bannerText;
                        if (banner.style.display !== 'flex') banner.style.display = 'flex';
                    } else {
                        if (banner.style.display !== 'none') banner.style.display = 'none';
                    }
                }

                const mission = data.latest_mission;
                const vProgress = data.verification_progress;
                const vActive = vProgress ? vProgress.active : false;
                const vElapsed = vProgress ? vProgress.elapsed_seconds : null;
                const missionId = mission ? (mission.id || mission.mission_id || null) : null;
                const vStatus = mission ? mission.verification_status : null;
                const incStatus = mission ? mission.incident_status : null;
                const stepsLen = (mission && mission.steps) ? mission.steps.length : 0;

                const shouldRerenderBriefing = (
                    mission && (
                        missionId !== lastRenderedMissionId ||
                        vStatus !== lastRenderedVerificationStatus ||
                        incStatus !== lastRenderedIncidentStatus
                    )
                );

                const shouldRerenderTrail = (
                    mission && (
                        missionId !== lastRenderedMissionId ||
                        vStatus !== lastRenderedVerificationStatus ||
                        vActive !== lastRenderedVerificationActive ||
                        (vActive && vElapsed !== lastRenderedVerificationElapsed) ||
                        stepsLen !== lastRenderedStepsLength
                    )
                );

                if (shouldRerenderBriefing) {
                    renderBriefing(mission);
                    lastRenderedMissionId = missionId;
                    lastRenderedVerificationStatus = vStatus;
                    lastRenderedIncidentStatus = incStatus;
                }

                if (shouldRerenderTrail) {
                    renderTrail(mission.steps);
                    lastRenderedVerificationActive = vActive;
                    lastRenderedVerificationElapsed = vElapsed;
                    lastRenderedStepsLength = stepsLen;
                }

                renderFleet(data.nodes, data.latest_mission);

            } catch (err) {
                console.error("Failed to fetch farm state:", err);
            }
        }

        function renderSlate(shows, latestMission, verificationProgress, pendingApprovals) {
            const container = document.getElementById('slate-container');
            const showList = Object.values(shows);
            if (!showList.length) return;

            const isIntervened = latestMission && latestMission.intervention_record;
            const hasPendingHold = pendingApprovals && pendingApprovals.some(a => a.status === 'PENDING');

            showList.forEach(s => {
                let statusLabel = s.status_label || 'ON SCHEDULE';
                let bufferMargin = s.buffer_margin_display || (s.buffer_margin_hours !== undefined ? (s.buffer_margin_hours < 1.0 && s.buffer_margin_hours > 0 ? `+${s.buffer_margin_hours.toFixed(1)}h / ${Math.round(s.buffer_margin_hours * 60)}m` : `+${s.buffer_margin_hours.toFixed(1)} hours`) : '+4.0 hours');
                let marginClass = 'metric-healthy';

                if (hasPendingHold && (s.id === 'show-solarflare' || s.id === 'show-solar')) {
                    statusLabel = 'AT RISK';
                }

                if (s.id === 'show-aethelgard') {
                    if (verificationProgress && verificationProgress.active) {
                        statusLabel = 'VERIFYING (' + verificationProgress.elapsed_seconds + 's)';
                        bufferMargin = 'Verifying';
                    } else if (isIntervened) {
                        if (latestMission.verification_status === 'ESCALATED') {
                            statusLabel = 'ESCALATED';
                        } else if (latestMission.verification_status === 'VERIFICATION_INCONCLUSIVE') {
                            statusLabel = 'INCONCLUSIVE';
                        } else {
                            statusLabel = 'VERIFIED PROTECTED';
                        }
                        bufferMargin = '+' + (latestMission.intervention_record.buffer_margin_hours || 2.8).toFixed(1) + ' hours';
                    }
                }

                let statusTagHtml = '';
                if (statusLabel === 'AT RISK' || statusLabel === 'ESCALATED') {
                    statusTagHtml = `<span class="state-tag tag-critical">${statusLabel}</span>`;
                    marginClass = 'metric-fault';
                } else if (statusLabel.startsWith('VERIFYING') || statusLabel === 'INCONCLUSIVE' || statusLabel === 'PENDING APPROVAL') {
                    statusTagHtml = `<span class="state-tag tag-slipping">${statusLabel}</span>`;
                    if (statusLabel === 'PENDING APPROVAL') marginClass = 'metric-warning';
                } else if (statusLabel === 'PROTECTED' || statusLabel === 'VERIFIED PROTECTED' || statusLabel === 'DELIVERED') {
                    statusTagHtml = `<span class="state-tag tag-protected">${statusLabel}</span>`;
                    marginClass = 'metric-healthy';
                } else {
                    statusTagHtml = `<span class="state-tag tag-scheduled">${statusLabel}</span>`;
                    marginClass = 'metric-healthy';
                }

                const dObj = new Date(s.delivery_deadline);
                const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
                const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                const dayStr = dayNames[dObj.getUTCDay()];
                const dateStr = String(dObj.getUTCDate()).padStart(2, '0');
                const monStr = monthNames[dObj.getUTCMonth()];
                const yrStr = dObj.getUTCFullYear();
                const hrStr = String(dObj.getUTCHours()).padStart(2, '0');
                const minStr = String(dObj.getUTCMinutes()).padStart(2, '0');
                const deadlineFormatted = `${dayStr}, ${dateStr} ${monStr} ${yrStr} ${hrStr}:${minStr} UTC`;

                const cardEl = document.getElementById(`slate-card-${s.id}`);
                if (cardEl) {
                    const headerTagEl = cardEl.querySelector('.slate-header-tag');
                    if (headerTagEl && headerTagEl.innerHTML !== statusTagHtml) {
                        headerTagEl.innerHTML = statusTagHtml;
                    }
                    const marginEl = cardEl.querySelector('.buffer-margin-val');
                    if (marginEl) {
                        if (marginEl.textContent !== bufferMargin) marginEl.textContent = bufferMargin;
                        marginEl.className = `metric-val ${marginClass} buffer-margin-val`;
                    }
                    const deadlineEl = cardEl.querySelector('.deadline-val');
                    if (deadlineEl && deadlineEl.textContent !== deadlineFormatted) {
                        deadlineEl.textContent = deadlineFormatted;
                    }
                } else {
                    const newCard = document.createElement('div');
                    newCard.className = `slate-card ${s.critical_path ? 'critical' : ''}`;
                    newCard.id = `slate-card-${s.id}`;
                    newCard.innerHTML = `
                        <div class="slate-header">
                            <div>
                                <div class="show-title">${s.name}</div>
                                <div class="show-client">${s.client}</div>
                            </div>
                            <div class="slate-header-tag">${statusTagHtml}</div>
                        </div>
                        <div class="slate-metrics">
                            <div class="metric-row">
                                <span class="metric-label">DEADLINE</span>
                                <span class="metric-val deadline-val">${deadlineFormatted}</span>
                            </div>
                            <div class="metric-row">
                                <span class="metric-label">BUFFER MARGIN</span>
                                <span class="metric-val ${marginClass} buffer-margin-val">${bufferMargin}</span>
                            </div>
                            <div class="metric-row">
                                <span class="metric-label">DAILY PENALTY</span>
                                <span class="metric-val">£${s.penalty_daily_amount.toLocaleString()} / day</span>
                            </div>
                            <div class="metric-row">
                                <span class="metric-label">PRIORITY TIER</span>
                                <span class="metric-val">${s.critical_path ? 'CRITICAL PATH' : 'STANDARD'}</span>
                            </div>
                        </div>
                    `;
                    container.appendChild(newCard);
                }
            });
        }

        function renderBriefing(mission) {
            const container = document.getElementById('briefing-container');
            const timeLabel = document.getElementById('briefing-timestamp');

            if (mission.timestamp) {
                const dateObj = new Date(mission.timestamp);
                const utcHours = String(dateObj.getUTCHours()).padStart(2, '0');
                const utcMinutes = String(dateObj.getUTCMinutes()).padStart(2, '0');
                const utcSeconds = String(dateObj.getUTCSeconds()).padStart(2, '0');
                timeLabel.innerText = `Mission completed: ${utcHours}:${utcMinutes}:${utcSeconds} UTC`;
            }

            let writebackHtml = '';
            if (mission.incident_url || mission.incident_id || (mission.deeplinks && Object.keys(mission.deeplinks).length > 0)) {
                const isResolved = (mission.incident_status === 'resolved');
                const badgeClass = isResolved ? 'badge-resolved' : 'badge-active';
                const timeSec = mission.time_intervention_to_resolved_seconds;
                const statusLabel = isResolved ? (timeSec ? `RESOLVED IN ${timeSec.toFixed(1)}s` : 'RESOLVED') : 'ACTIVE';
                const clearedSec = mission.quarantine_to_alert_cleared_seconds;
                const clearedBadgeHtml = clearedSec ? `<span class="incident-badge badge-resolved">ALERT CLEARED IN ${Number(clearedSec).toFixed(1)}s</span>` : '';
                const incUrl = mission.incident_url || '#';

                writebackHtml += `
                    <div class="grafana-writeback-card">
                        <div class="grafana-incident-row">
                            <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
                                <span class="incident-badge ${badgeClass}">${statusLabel}</span>
                                ${clearedBadgeHtml}
                                <span style="font-family: var(--font-condensed); font-weight: 700; font-size: 12px; color: var(--text-primary);">
                                    GRAFANA IRM INCIDENT #${mission.incident_id || ''}
                                </span>
                            </div>
                            <a href="${incUrl}" target="_blank" rel="noopener noreferrer" class="deeplink-btn" style="color: var(--accent);">
                                OPEN INCIDENT IN GRAFANA &rarr;
                            </a>
                        </div>
                `;

                if (mission.deeplinks && Object.keys(mission.deeplinks).length > 0) {
                    writebackHtml += `
                        <div style="font-family: var(--font-condensed); font-size: 10px; color: var(--text-dim); text-transform: uppercase; margin-bottom: 4px;">
                            PERSISTENT TELEMETRY EVIDENCE DEEPLINKS (PINNED ABSOLUTE TIME RANGE)
                        </div>
                        <div class="deeplinks-grid">
                            ${mission.deeplinks.prometheus ? `<a href="${mission.deeplinks.prometheus}" target="_blank" rel="noopener noreferrer" class="deeplink-btn">Prometheus Explore &nearr;</a>` : ''}
                            ${mission.deeplinks.loki ? `<a href="${mission.deeplinks.loki}" target="_blank" rel="noopener noreferrer" class="deeplink-btn">Loki Logs Explore &nearr;</a>` : ''}
                            ${mission.deeplinks.tempo ? `<a href="${mission.deeplinks.tempo}" target="_blank" rel="noopener noreferrer" class="deeplink-btn">Tempo Trace Explore &nearr;</a>` : ''}
                            ${mission.deeplinks.dashboard ? `<a href="${mission.deeplinks.dashboard}" target="_blank" rel="noopener noreferrer" class="deeplink-btn">Control Tower Dashboard &nearr;</a>` : ''}
                        </div>
                    `;
                }

                if (mission.panel_image_url) {
                    const reads = mission.mcp_read_calls || 0;
                    const writes = mission.mcp_write_calls || 0;
                    writebackHtml += `
                        <div class="panel-snapshot-box">
                            <img src="${mission.panel_image_url}" alt="Grafana Control Tower Panel Snapshot" />
                            <div class="panel-snapshot-meta">
                                <span>CONTROL TOWER PANEL SNAPSHOT (get_panel_image)</span>
                                <span>MISSION MCP TOOL CALLS: ${reads} READS / ${writes} WRITES</span>
                            </div>
                        </div>
                    `;
                }

                writebackHtml += `</div>`;
            }

            container.innerHTML = writebackHtml + formatMarkdown(mission.callsheet_briefing);
        }

        function renderTrail(steps) {
            const container = document.getElementById('trail-container');
            if (!steps || !steps.length) {
                container.innerHTML = '<div style="font-size: 12px; color: var(--text-dim);">No steps recorded.</div>';
                return;
            }

            container.innerHTML = steps.map(step => {
                let evidenceStr = '';
                if (step.evidence) {
                    evidenceStr = JSON.stringify(step.evidence, null, 2);
                }

                const execType = step.execution_type || 'DETERMINISTIC';
                const stepNum = step.step_number !== undefined ? step.step_number : 1;
                const isGen = (stepNum === 3 || stepNum === 7 || execType === 'GENERATIVE_SYNTHESIS');
                const isAlert = (stepNum === 0);
                const badgeHtml = isAlert 
                    ? '<span class="badge-det">GRAFANA ALERT</span>' 
                    : (isGen ? '<span class="badge-gen">GENERATIVE AI</span>' : '<span class="badge-det">DETERMINISTIC</span>');

                const stepName = (step.name || '').toUpperCase();

                return `
                    <div class="step-entry">
                        <div class="step-title">
                            <span>STEP ${stepNum}: ${stepName}</span>
                            ${badgeHtml}
                        </div>
                        <div class="step-desc">${step.description}</div>
                        ${evidenceStr && evidenceStr !== '{}' ? `<pre class="step-evidence">${evidenceStr}</pre>` : ''}
                    </div>
                `;
            }).join('');
        }

        function renderFleet(nodes, latestMission) {
            const container = document.getElementById('fleet-container');
            const summaryLabel = document.getElementById('fleet-summary');
            const nodeList = Object.values(nodes);
            if (!nodeList.length) return;

            const isMaintenance = n => n.status === 'OFFLINE' || n.status === 'MAINTENANCE' || (n.name && n.name.includes('Maintenance')) || (n.id === 'node-12' && !n.is_standby && !n.current_shot_id);
            const activeCount = nodeList.filter(n => !n.is_standby && n.status !== 'QUARANTINED' && !isMaintenance(n)).length;
            const quarantinedCount = nodeList.filter(n => n.status === 'QUARANTINED').length;
            const standbyCount = nodeList.filter(n => n.is_standby && !isMaintenance(n)).length;
            const maintenanceCount = nodeList.filter(n => isMaintenance(n)).length;

            const summaryParts = [`${activeCount} ACTIVE`];
            if (quarantinedCount > 0) summaryParts.push(`${quarantinedCount} QUARANTINED`);
            summaryParts.push(`${standbyCount} STANDBY`);
            if (maintenanceCount > 0) summaryParts.push(`${maintenanceCount} MAINTENANCE`);
            const summaryText = summaryParts.join(' / ');

            if (summaryLabel && summaryLabel.innerText !== summaryText) {
                summaryLabel.innerText = summaryText;
            }

            nodeList.forEach(n => {
                let nodeClass = 'node-tile';
                let tempColor = 'var(--state-healthy)';
                let shotLabel = 'IDLE';
                let tempVal = n.temperature_celsius;

                if (n.id === 'node-07' && (n.status === 'QUARANTINED' || n.status === 'THROTTLED')) {
                    if (latestMission && latestMission.intervention_record && latestMission.intervention_record.telemetry_evidence && latestMission.intervention_record.telemetry_evidence.source_temp) {
                        tempVal = Number(latestMission.intervention_record.telemetry_evidence.source_temp);
                    }
                }

                if (n.status === 'QUARANTINED') {
                    nodeClass += ' node-quarantined';
                    tempColor = 'var(--heat-fault)';
                    shotLabel = 'QUARANTINED (FAULT)';
                } else if (isMaintenance(n)) {
                    nodeClass += ' node-maintenance';
                    tempColor = 'var(--text-dim)';
                    shotLabel = 'MAINTENANCE';
                } else if (n.is_standby) {
                    nodeClass += ' node-standby';
                    tempColor = 'var(--text-dim)';
                    shotLabel = 'STANDBY SPARE';
                } else if (n.status === 'THROTTLED' || tempVal >= 90) {
                    nodeClass += ' node-fault';
                    tempColor = 'var(--heat-fault)';
                    shotLabel = n.current_shot_id ? 'SHOT ' + n.current_shot_id.replace('sh_', '') : 'DEGRADED (FAULT)';
                } else if (tempVal >= 78.0) {
                    nodeClass += ' node-hot';
                    tempColor = 'var(--heat-hot)';
                    shotLabel = n.current_shot_id ? 'SHOT ' + n.current_shot_id.replace('sh_', '') : 'HIGH LOAD';
                } else if (tempVal >= 68.0) {
                    nodeClass += ' node-warm';
                    tempColor = 'var(--heat-warm)';
                    shotLabel = n.current_shot_id ? 'SHOT ' + n.current_shot_id.replace('sh_', '') : 'WARM ACTIVE';
                } else if (tempVal >= 58.0) {
                    nodeClass += ' node-nominal';
                    tempColor = 'var(--state-healthy)';
                    shotLabel = n.current_shot_id ? 'SHOT ' + n.current_shot_id.replace('sh_', '') : 'NOMINAL';
                } else {
                    nodeClass += ' node-cool';
                    tempColor = 'var(--state-healthy)';
                    shotLabel = n.current_shot_id ? 'SHOT ' + n.current_shot_id.replace('sh_', '') : 'COOL ACTIVE';
                }

                const tileEl = document.getElementById(`tile-${n.id}`);
                if (tileEl) {
                    if (tileEl.className !== nodeClass) {
                        tileEl.className = nodeClass;
                    }
                    const tempEl = tileEl.querySelector('.node-temp');
                    if (tempEl) {
                        const tempText = `${tempVal.toFixed(1)}°C`;
                        if (tempEl.textContent !== tempText) tempEl.textContent = tempText;
                        if (tempEl.style.color !== tempColor) tempEl.style.color = tempColor;
                    }
                    const shotEl = tileEl.querySelector('.node-shot');
                    if (shotEl && shotEl.textContent !== shotLabel) {
                        shotEl.textContent = shotLabel;
                    }
                } else {
                    const newTile = document.createElement('div');
                    newTile.className = nodeClass;
                    newTile.id = `tile-${n.id}`;
                    newTile.setAttribute('data-node', n.id);
                    newTile.innerHTML = `
                        <div class="node-tile-header">
                            <span class="node-id">${n.id.toUpperCase()}</span>
                            <span class="node-pip"></span>
                        </div>
                        <div class="node-temp" style="color: ${tempColor};">${tempVal.toFixed(1)}°C</div>
                        <div class="node-shot">${shotLabel}</div>
                    `;
                    container.appendChild(newTile);
                }
            });

            // Trigger signature failover transfer motion once on load if an intervention happened
            if (!window.__failoverAnimated && (quarantinedCount > 0 || (latestMission && latestMission.intervention_record))) {
                window.__failoverAnimated = true;
                setTimeout(() => {
                    triggerFailoverMotion('node-07', 'node-11');
                }, 400);
            }
        }

        // On DOMContentLoaded, trigger failover motion if rendered server-side with quarantined node
        document.addEventListener('DOMContentLoaded', () => {
            const qTile = document.getElementById('tile-node-07');
            if (qTile && qTile.classList.contains('node-quarantined') && !window.__failoverAnimated) {
                window.__failoverAnimated = true;
                setTimeout(() => {
                    triggerFailoverMotion('node-07', 'node-11');
                }, 500);
            }
        });

        // Poll state every 3 seconds
        setInterval(fetchState, 3000);
        fetchState();
    </script>
</body>
</html>
"""

# Demonstration & Scenario Injection Surface (Isolated on /demo)
DEMO_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Callsheet // Demo & Scenario Control Harness</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-page: #0b0c0e;
            --bg-surface: #12141a;
            --bg-card: #161922;
            --bg-inset: #08090b;
            --border-subtle: rgba(255, 255, 255, 0.08);
            --border-card: rgba(255, 255, 255, 0.12);
            --border-active: rgba(235, 230, 220, 0.28);
            --text-primary: #f0f2f5;
            --text-secondary: #d0cbc2;
            --text-muted: #717888;
            --accent-warm: #d97706;
            --font-main: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            --font-mono: 'IBM Plex Mono', 'JetBrains Mono', monospace;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        html, body {
            height: 100vh;
            overflow: hidden;
            background-color: var(--bg-page);
            color: var(--text-secondary);
            font-family: var(--font-main);
        }

        body {
            display: flex;
            flex-direction: column;
            padding: 20px 28px;
            gap: 16px;
        }

        header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding-bottom: 12px;
            border-bottom: 1px solid var(--border-subtle);
        }

        .brand {
            display: flex;
            align-items: baseline;
            gap: 12px;
        }

        .brand h1 {
            font-size: 16px;
            font-weight: 600;
            letter-spacing: 0.05em;
            color: var(--text-primary);
            text-transform: uppercase;
        }

        .brand .tagline {
            font-family: var(--font-mono);
            font-size: 11px;
            color: var(--text-muted);
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }

        .nav-link {
            font-family: var(--font-mono);
            font-size: 11px;
            color: var(--text-secondary);
            text-decoration: none;
            padding: 6px 12px;
            border: 1px solid var(--border-subtle);
            border-radius: 4px;
            transition: all 0.15s ease;
        }

        .nav-link:hover {
            color: var(--text-primary);
            border-color: var(--border-active);
            background: rgba(255, 255, 255, 0.04);
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
        }

        .btn-scenario {
            background: var(--bg-surface);
            border: 1px solid var(--border-card);
            border-radius: 6px;
            padding: 16px 20px;
            cursor: pointer;
            text-align: left;
            display: flex;
            flex-direction: column;
            gap: 6px;
            transition: all 0.15s ease;
            color: var(--text-primary);
        }

        .btn-scenario:hover {
            background: var(--bg-card);
            border-color: var(--border-active);
            transform: translateY(-1px);
        }

        .btn-scenario:active {
            transform: translateY(0);
        }

        .btn-scenario .btn-title {
            font-size: 13px;
            font-weight: 600;
            line-height: 1.4;
            color: var(--text-primary);
        }

        .btn-scenario .btn-meta {
            font-family: var(--font-mono);
            font-size: 10px;
            color: var(--text-muted);
            letter-spacing: 0.06em;
            text-transform: uppercase;
        }

        .btn-scenario.restore {
            border-style: dashed;
            opacity: 0.85;
        }

        .btn-scenario.restore:hover {
            opacity: 1;
        }

        .log-panel {
            flex: 1;
            display: flex;
            flex-direction: column;
            background: var(--bg-surface);
            border: 1px solid var(--border-subtle);
            border-radius: 6px;
            overflow: hidden;
            min-height: 0;
        }

        .log-header {
            padding: 10px 16px;
            background: rgba(0, 0, 0, 0.25);
            border-bottom: 1px solid var(--border-subtle);
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .log-header-title {
            font-family: var(--font-mono);
            font-size: 11px;
            font-weight: 500;
            letter-spacing: 0.08em;
            color: var(--text-muted);
            text-transform: uppercase;
        }

        .log-body {
            flex: 1;
            padding: 14px 16px;
            background: var(--bg-inset);
            font-family: var(--font-mono);
            font-size: 11px;
            line-height: 1.6;
            color: var(--text-secondary);
            overflow-y: auto;
            white-space: pre-wrap;
            word-break: break-word;
        }

        .log-line {
            margin-bottom: 4px;
        }

        .log-ts {
            color: var(--text-muted);
            margin-right: 8px;
        }

        .log-action {
            color: var(--accent-warm);
        }
    </style>
</head>
<body>
    <header>
        <div class="brand">
            <h1>Callsheet</h1>
            <span class="tagline">// Demonstration Control Surface</span>
        </div>
        <a class="nav-link" href="/">Return to Producer Dashboard &rarr;</a>
    </header>

    <div class="grid">
        <button class="btn-scenario" onclick="injectScenario('THERMAL_THROTTLING')">
            <span class="btn-title">Thermal throttle on node-07, about three minutes to resolved</span>
            <span class="btn-meta">Scenario 1 // Automatic Tier 1 failover to node-11</span>
        </button>

        <button class="btn-scenario" onclick="injectScenario('DOUBLE_FAULT')">
            <span class="btn-title">Double fault, holds for your approval after about two minutes</span>
            <span class="btn-meta">Scenario 2 // Tier 2 hold and producer approval</span>
        </button>

        <button class="btn-scenario" onclick="runFailureMission()">
            <span class="btn-title">Forced verification failure, rollback to node-12</span>
            <span class="btn-meta">Scenario 3 // Closed-loop quarantine and rollback</span>
        </button>

        <button class="btn-scenario restore" onclick="injectScenario('BASELINE')">
            <span class="btn-title">Restore baseline</span>
            <span class="btn-meta">Reset // Clear synthetic faults across farm nodes</span>
        </button>
    </div>

    <div class="log-panel">
        <div class="log-header">
            <span class="log-header-title">Harness Activity Log</span>
            <span id="harness-status" class="log-header-title" style="color: var(--accent-warm);">Ready</span>
        </div>
        <div id="log-box" class="log-body">
            <div class="log-line"><span class="log-ts">[INIT]</span>Harness initialized. Autonomous watchdog active on live telemetry.</div>
        </div>
    </div>

    <script>
        function log(msg, isAction = false) {
            const box = document.getElementById('log-box');
            const now = new Date();
            const time = now.toISOString().substring(11, 19) + ' UTC';
            const line = document.createElement('div');
            line.className = 'log-line';
            line.innerHTML = `<span class="log-ts">[${time}]</span><span class="${isAction ? 'log-action' : ''}">${msg}</span>`;
            box.insertBefore(line, box.firstChild);
        }

        async function injectScenario(scenario) {
            log(`Injecting scenario: ${scenario}...`, true);
            const statusEl = document.getElementById('harness-status');
            if (statusEl) statusEl.textContent = `Injecting ${scenario}...`;
            try {
                const res = await fetch('/api/scenario', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ scenario: scenario })
                });
                const data = await res.json();
                log(`Scenario confirmed active: ${data.scenario}. Autonomous watchdog will observe and intervene.`);
                if (statusEl) statusEl.textContent = `Active: ${data.scenario}`;
            } catch (err) {
                log(`Error injecting scenario: ${err}`);
                if (statusEl) statusEl.textContent = 'Error';
            }
        }

        async function runFailureMission() {
            log('Triggering mission with FORCED post-intervention verification failure...', true);
            const statusEl = document.getElementById('harness-status');
            if (statusEl) statusEl.textContent = 'Running forced verification failure...';
            try {
                const res = await fetch('/api/mission', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ show_id: 'show-aethelgard', force_verification_fault: true })
                });
                const data = await res.json();
                log(`Mission complete: Verification status ${data.verification_status}. Rollback executed.`);
                if (statusEl) statusEl.textContent = `Completed: ${data.verification_status}`;
            } catch (err) {
                log(`Mission error: ${err}`);
                if (statusEl) statusEl.textContent = 'Error';
            }
        }
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def get_producer_dashboard():
    # Server-Side Render (SSR) current farm state and mission so first paint has 100% data
    worker.simulator.update_cycle_deadlines()
    state = worker.simulator.state
    mission = (
        worker.latest_mission.model_dump()
        if hasattr(worker.latest_mission, "model_dump")
        else worker.latest_mission
    )

    slate_html = render_ssr_slate(state.shows, mission)
    fleet_html, fleet_summary = render_ssr_fleet(state.nodes, mission=mission)
    delivery_banner_html = render_ssr_delivery_banner(state.shots.get("sh_118"))

    briefing_html = render_ssr_briefing(mission)
    steps = mission.get("steps", []) if mission else []
    trail_html = render_ssr_trail(steps)

    timestamp = mission.get("timestamp", "") if mission else ""
    if timestamp:
        try:
            if isinstance(timestamp, str):
                ts_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            else:
                ts_dt = timestamp
            time_label = f"Mission completed: {ts_dt.strftime('%H:%M:%S')} UTC"
        except Exception:
            time_label = f"Mission completed: {str(timestamp)[:19].replace('T', ' ')} UTC"
    else:
        time_label = "Autonomous Watch Active"

    mission_id = (mission.get("id") or mission.get("mission_id") or "") if mission else ""
    verification_status = mission.get("verification_status", "") if mission else ""
    incident_status = mission.get("incident_status", "") if mission else ""
    steps_len = len(steps)

    pending_approvals = [a for a in serialize_approvals() if a.get("status") == "PENDING"]
    if pending_approvals:
        cards = []
        for a in pending_approvals:
            app_id = a["id"]
            action_title = a["action_title"]
            plan_summary = a["plan_summary"]
            loss_rate = a["buffer_loss_rate"]
            cost = a["cost_of_waiting"]
            impact = a["deadline_impact"]
            cards.append(
                f'<div class="approval-banner">'
                f'<div class="approval-header">'
                f'<span class="approval-badge">TIER 2 APPROVAL REQUIRED // HOLD ACTIVE</span>'
                f'<span class="approval-id mono">{app_id}</span>'
                f'</div>'
                f'<div class="approval-title">{action_title}</div>'
                f'<div class="approval-plan">{plan_summary}</div>'
                f'<div class="approval-arithmetic mono">'
                f'<div><strong>Buffer Loss Rate:</strong> {loss_rate}</div>'
                f'<div><strong>Cost of Waiting:</strong> {cost}</div>'
                f'<div><strong>Deadline Impact:</strong> {impact}</div>'
                f'</div>'
                f'<div class="approval-controls">'
                f'<button class="btn-approve" onclick="handleProducerDecision(\'{app_id}\', \'approve\')">Approve Reallocation</button>'
                f'<button class="btn-decline" onclick="handleProducerDecision(\'{app_id}\', \'decline\')">Decline & Escalate</button>'
                f'</div>'
                f'</div>'
            )
        approval_cards_html = "".join(cards)
    else:
        approval_cards_html = ""

    has_pending_hold = bool(pending_approvals)
    if worker.watchdog_stalled:
        status_text_html = '<span class="status-pip" style="color: var(--state-critical);"></span> Agent stalled, check health'
    elif has_pending_hold:
        status_text_html = '<span class="status-pip" style="color: var(--heat-warm);"></span> Tier 2 hold: awaiting producer approval'
    elif worker.verification_progress and worker.verification_progress.get("active"):
        status_text_html = f'<span class="status-pip" style="color: var(--heat-warm);"></span> {worker.verification_progress.get("message")}'
    elif worker.is_investigating:
        status_text_html = '<span class="status-pip" style="color: var(--heat-warm);"></span> INVESTIGATING ANOMALY (MCP)'
    elif worker.alert_pending:
        status_text_html = '<div style="display: flex; flex-direction: column;"><div style="display: flex; align-items: center; gap: 6px;"><span class="status-pip" style="color: var(--state-critical);"></span> Grafana alert pending</div><div style="font-size: 10px; color: var(--text-muted); font-weight: normal; text-transform: none; letter-spacing: normal; margin-top: 2px;">Grafana evaluates every minute</div></div>'
    else:
        status_text_html = '<span class="status-pip"></span> AUTONOMOUS WATCH ACTIVE'

    html = (
        PRODUCER_UI_TEMPLATE
        .replace("<!-- SSR_DELIVERY_BANNER -->", delivery_banner_html)
        .replace("<!-- SSR_APPROVAL_CARD -->", approval_cards_html)
        .replace("<!-- SSR_SLATE -->", slate_html)
        .replace("<!-- SSR_STATUS_TEXT -->", status_text_html)
        .replace("<!-- SSR_BRIEFING_TIME -->", time_label)
        .replace("<!-- SSR_BRIEFING -->", briefing_html)
        .replace("<!-- SSR_TRAIL -->", trail_html)
        .replace("<!-- SSR_FLEET_SUMMARY -->", fleet_summary)
        .replace("<!-- SSR_FLEET -->", fleet_html)
        .replace("<!-- SSR_MISSION_ID -->", mission_id)
        .replace("<!-- SSR_VERIFICATION_STATUS -->", verification_status)
        .replace("<!-- SSR_INCIDENT_STATUS -->", incident_status)
        .replace("<!-- SSR_STEPS_LENGTH -->", str(steps_len))
    )
    return HTMLResponse(
        content=html,
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/demo", response_class=HTMLResponse)
async def get_demo_harness():
    return HTMLResponse(
        content=DEMO_UI_HTML,
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )
