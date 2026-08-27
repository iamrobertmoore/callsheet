"""
FastAPI application for Callsheet web interface and API endpoints.
Provides live status, scenario injection, mission execution, and telemetry audit feeds.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import os
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from callsheet.agent.mission import MultiStepMissionRunner
from callsheet.farm.models import ScenarioType
from callsheet.farm.simulator import RenderFarmSimulator
from callsheet.farm.worker import FarmWorker
from callsheet.interventions.dispatcher import InterventionDispatcher

# Global singleton instances
simulator = RenderFarmSimulator()
dispatcher = InterventionDispatcher(simulator=simulator)
mission_runner = MultiStepMissionRunner(
    dispatcher=dispatcher,
    project_id=os.getenv("GOOGLE_CLOUD_PROJECT", "agent-attest-2026"),
    location=os.getenv("VERTEX_AI_LOCATION", "global"),
    model_name="gemini-3.6-flash",
)
worker = FarmWorker(
    simulator=simulator,
    mission_runner=mission_runner,
    tick_interval_seconds=5.0,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start continuous background farm emitter and autonomous watchdog
    await worker.start()
    yield
    # Stop emitter on shutdown
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


@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "callsheet",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "worker_running": worker._running,
    }


@app.get("/api/state")
async def get_farm_state():
    """Returns the current state of shows, nodes, active shots, and latest mission."""
    state = simulator.state
    content = {
        "active_scenario": state.active_scenario.value,
        "last_updated": state.last_updated.isoformat(),
        "shows": {k: v.model_dump(mode="json") for k, v in state.shows.items()},
        "nodes": {k: v.model_dump(mode="json") for k, v in state.nodes.items()},
        "shots": {k: v.model_dump(mode="json") for k, v in state.shots.items()},
        "latest_mission": worker.latest_mission,
        "is_investigating": worker.is_investigating,
        "interventions_count": len(dispatcher.history),
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
        result = await mission_runner.execute_mission(
            show_id=req.show_id or "show-aethelgard",
            force_verification_fault=bool(req.force_verification_fault),
        )
        worker.latest_mission = result.model_dump(mode="json")
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

        if show_id == 'show-aethelgard':
            if is_intervened:
                status_tag = '<span class="state-tag tag-protected">Protected: Failover Applied</span>'
                rec = mission.get("intervention_record", {})
                margin_val = rec.get("buffer_margin_hours", 2.8) if isinstance(rec, dict) else getattr(rec, "buffer_margin_hours", 2.8)
                buffer_margin = f'+{margin_val:.1f} hours'
            else:
                status_tag = '<span class="state-tag tag-scheduled">On Schedule</span>'
                buffer_margin = '+2.8 hours'
        elif show_id in ('show-solarflare', 'show-solar'):
            status_tag = '<span class="state-tag tag-scheduled">On Schedule</span>'
            buffer_margin = '+5.5 hours'
        elif show_id == 'show-abyssal':
            status_tag = '<span class="state-tag tag-scheduled">On Schedule</span>'
            buffer_margin = '+9.4 hours'
        else:
            status_tag = '<span class="state-tag tag-scheduled">On Schedule</span>'
            buffer_margin = '+4.0 hours'

        crit_class = ' critical' if critical else ''
        cards.append(f"""
            <div class="slate-card{crit_class}">
                <div class="slate-header">
                    <div>
                        <div class="show-title">{show_name}</div>
                        <div class="show-client">{show_client}</div>
                    </div>
                    {status_tag}
                </div>
                <div class="slate-metrics">
                    <div class="metric-row">
                        <span>Deadline</span>
                        <span>{deadline_str}</span>
                    </div>
                    <div class="metric-row">
                        <span>Buffer Margin</span>
                        <span style="color: var(--success);">{buffer_margin}</span>
                    </div>
                    <div class="metric-row">
                        <span>Daily Penalty</span>
                        <span>£{penalty:,.0f} / day</span>
                    </div>
                    <div class="metric-row">
                        <span>Priority Tier</span>
                        <span>{'Critical Path' if critical else 'Standard'}</span>
                    </div>
                </div>
            </div>
        """)
    return ''.join(cards)


def render_ssr_trail(steps: list) -> str:
    if not steps:
        return '<div style="font-size: 12px; color: var(--text-muted);">No steps recorded.</div>'
    import json
    entries = []
    badges = {
        "DETERMINISTIC_TELEMETRY": '<span class="badge-type badge-telemetry">DETERMINISTIC TELEMETRY</span>',
        "DETERMINISTIC_ARITHMETIC": '<span class="badge-type badge-arithmetic">DETERMINISTIC ARITHMETIC</span>',
        "DETERMINISTIC_ACTION": '<span class="badge-type badge-action">DETERMINISTIC ACTION</span>',
        "DETERMINISTIC_VERIFICATION": '<span class="badge-type badge-verification">DETERMINISTIC VERIFICATION</span>',
        "GENERATIVE_SYNTHESIS": '<span class="badge-type badge-generative">GENERATIVE AI (EXPLANATORY)</span>',
    }
    for step in steps:
        step_num = step.get("step_number") if isinstance(step, dict) else getattr(step, "step_number", 1)
        name = step.get("name") if isinstance(step, dict) else getattr(step, "name", "")
        desc = step.get("description") if isinstance(step, dict) else getattr(step, "description", "")
        exec_type = step.get("execution_type") if isinstance(step, dict) else getattr(step, "execution_type", "DETERMINISTIC_TELEMETRY")
        evidence = step.get("evidence") if isinstance(step, dict) else getattr(step, "evidence", {})
        evidence_str = json.dumps(evidence, indent=2) if evidence else ""
        badge_html = badges.get(exec_type, '<span class="badge-type badge-telemetry">DETERMINISTIC</span>')

        entries.append(f"""
            <div class="step-entry">
                <div class="step-title">
                    <span>Step {step_num}: {name}</span>
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
        return '<div>Loading node status...</div>', '10 Active / 2 Standby'

    active_count = 0
    quarantined_count = 0
    standby_count = 0
    tiles = []

    for n in node_list:
        node_id = n.id if hasattr(n, "id") else n.get("id")
        status = n.status if hasattr(n, "status") else n.get("status")
        status_val = status.value if hasattr(status, "value") else str(status)
        is_standby = n.is_standby if hasattr(n, "is_standby") else n.get("is_standby", False)
        temp = n.temperature_celsius if hasattr(n, "temperature_celsius") else n.get("temperature_celsius", 55.0)
        shot_id = n.current_shot_id if hasattr(n, "current_shot_id") else n.get("current_shot_id")

        # Ensure node-07 reads the locked incident temperature from evidence
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
            node_class = "node-tile quarantined"
            temp_color = "var(--danger)"
            shot_label = "Quarantined (Fault)"
        elif is_standby:
            standby_count += 1
            node_class = "node-tile standby"
            temp_color = "var(--text-muted)"
            shot_label = "Standby Spare"
        else:
            active_count += 1
            if status_val == "THROTTLED" or temp > 90.0:
                node_class = "node-tile throttled"
                temp_color = "var(--danger)"
                shot_label = f"Shot {shot_id.replace('sh_', '')}" if shot_id else "Degraded (Fault)"
            else:
                node_class = "node-tile"
                temp_color = "var(--success)"
                shot_label = f"Shot {shot_id.replace('sh_', '')}" if shot_id else "Idle"

        tiles.append(f"""
            <div class="{node_class}">
                <div class="node-id">{node_id}</div>
                <div class="node-temp" style="color: {temp_color};">{temp:.1f}°C</div>
                <div class="node-shot">{shot_label}</div>
            </div>
        """)

    if quarantined_count > 0:
        summary_str = f"{active_count} Active / {quarantined_count} Quarantined / {standby_count} Standby"
    else:
        summary_str = f"{active_count} Active / {standby_count} Standby"

    return ''.join(tiles), summary_str


PRODUCER_UI_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Callsheet: Autonomous Operations Agent for Post-Production Delivery Producers</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #090d13;
            --surface: #121820;
            --surface-subtle: #171f2a;
            --border: #232f3e;
            --border-highlight: #2e3f54;
            --text: #c2cbd6;
            --text-heading: #f0f6fc;
            --text-muted: #7d8b99;
            --accent: #388bfd;
            --success: #3fb950;
            --success-bg: rgba(63, 185, 80, 0.15);
            --danger: #f85149;
            --danger-bg: rgba(248, 81, 73, 0.15);
            --warning: #d29922;
            --warning-bg: rgba(210, 153, 34, 0.15);
            --font-main: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            --font-mono: 'JetBrains Mono', monospace;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            background-color: var(--bg);
            color: var(--text);
            font-family: var(--font-main);
            padding: 24px;
            -webkit-font-smoothing: antialiased;
        }

        .container { max-width: 1400px; margin: 0 auto; }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--border);
            margin-bottom: 24px;
        }

        .brand h1 {
            font-size: 20px;
            font-weight: 700;
            color: var(--text-heading);
            letter-spacing: -0.5px;
        }

        .brand p {
            font-size: 13px;
            color: var(--text-muted);
            margin-top: 2px;
        }

        .header-meta {
            display: flex;
            align-items: center;
            gap: 16px;
        }

        .utc-clock {
            font-family: var(--font-mono);
            font-size: 13px;
            color: var(--text);
            background: var(--surface);
            padding: 6px 12px;
            border-radius: 4px;
            border: 1px solid var(--border);
        }

        .status-badge {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 12px;
            font-weight: 600;
            padding: 6px 12px;
            border-radius: 4px;
        }

        .badge-live {
            background: var(--success-bg);
            color: var(--success);
            border: 1px solid rgba(63, 185, 80, 0.3);
        }

        .badge-investigating {
            background: var(--warning-bg);
            color: var(--warning);
            border: 1px solid rgba(210, 153, 34, 0.3);
        }

        .badge-pulse {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: currentColor;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.4; transform: scale(0.9); }
            100% { opacity: 1; transform: scale(1); }
        }

        /* Slate Cards */
        .section-title {
            font-size: 14px;
            font-weight: 600;
            color: var(--text-heading);
            margin-bottom: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .slate-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: 16px;
            margin-bottom: 28px;
        }

        .slate-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 16px;
            transition: border-color 0.15s ease;
        }

        .slate-card.critical {
            border-color: var(--border-highlight);
            background: linear-gradient(180deg, var(--surface) 0%, rgba(23, 31, 42, 0.7) 100%);
        }

        .slate-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 12px;
        }

        .show-title {
            font-size: 15px;
            font-weight: 600;
            color: var(--text-heading);
        }

        .show-client {
            font-size: 12px;
            color: var(--text-muted);
            margin-top: 2px;
        }

        .state-tag {
            font-size: 11px;
            font-weight: 600;
            padding: 2px 8px;
            border-radius: 3px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .tag-scheduled { background: var(--success-bg); color: var(--success); }
        .tag-protected { background: var(--success-bg); color: var(--success); border: 1px solid var(--success); }
        .tag-at-risk { background: var(--danger-bg); color: var(--danger); border: 1px solid var(--danger); }

        .slate-metrics {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 10px;
            margin-top: 14px;
            padding-top: 12px;
            border-top: 1px solid var(--border);
            font-size: 12px;
        }

        .metric-row span:first-child {
            color: var(--text-muted);
            display: block;
            margin-bottom: 2px;
        }

        .metric-row span:last-child {
            font-family: var(--font-mono);
            font-weight: 600;
            color: var(--text-heading);
        }

        /* Main Grid: Left Briefing, Right Fleet */
        .main-layout {
            display: grid;
            grid-template-columns: 1.6fr 1fr;
            gap: 20px;
        }

        @media (max-width: 1024px) {
            .main-layout { grid-template-columns: 1fr; }
        }

        .card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 18px;
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 14px;
            padding-bottom: 10px;
            border-bottom: 1px solid var(--border);
        }

        .card-heading {
            font-size: 13px;
            font-weight: 600;
            color: var(--text-heading);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .briefing-content {
            font-size: 13px;
            line-height: 1.6;
            color: var(--text);
        }

        .briefing-content h3 {
            font-size: 13px;
            font-weight: 600;
            color: var(--accent);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin: 14px 0 6px 0;
        }

        .briefing-content p {
            margin-bottom: 12px;
            color: var(--text);
        }

        .briefing-content table {
            width: 100%;
            border-collapse: collapse;
            margin: 12px 0;
            font-size: 12px;
            font-family: var(--font-mono);
        }

        .briefing-content th, .briefing-content td {
            border: 1px solid var(--border);
            padding: 8px 10px;
            text-align: left;
        }

        .briefing-content th {
            background: var(--surface-subtle);
            color: var(--text-heading);
            font-weight: 600;
        }

        .briefing-content ul {
            padding-left: 18px;
            margin-bottom: 12px;
        }

        .briefing-content li {
            margin-bottom: 6px;
        }

        /* Expandable Reasoning Accordion */
        details.trail-accordion {
            background: var(--surface-subtle);
            border: 1px solid var(--border);
            border-radius: 4px;
            margin-top: 16px;
            padding: 12px 16px;
        }

        details.trail-accordion summary {
            font-size: 12px;
            font-weight: 600;
            font-family: var(--font-mono);
            color: var(--accent);
            cursor: pointer;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .step-timeline {
            margin-top: 14px;
        }

        .step-entry {
            border-left: 2px solid var(--border-highlight);
            padding-left: 14px;
            margin-bottom: 14px;
            position: relative;
        }

        .step-entry::before {
            content: '';
            position: absolute;
            left: -5px;
            top: 4px;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--accent);
        }

        .step-title {
            font-size: 12px;
            font-weight: 600;
            color: var(--text-heading);
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 8px;
            flex-wrap: wrap;
        }

        .badge-type {
            font-family: var(--font-mono);
            font-size: 9px;
            font-weight: 700;
            letter-spacing: 0.5px;
            text-transform: uppercase;
            padding: 2px 6px;
            border-radius: 3px;
            white-space: nowrap;
        }

        .badge-telemetry {
            color: #38bdf8;
            background: rgba(56, 189, 248, 0.12);
            border: 1px solid rgba(56, 189, 248, 0.3);
        }

        .badge-arithmetic {
            color: #34d399;
            background: rgba(52, 211, 153, 0.12);
            border: 1px solid rgba(52, 211, 153, 0.3);
        }

        .badge-action {
            color: #c084fc;
            background: rgba(192, 132, 252, 0.12);
            border: 1px solid rgba(192, 132, 252, 0.3);
        }

        .badge-verification {
            color: #2dd4bf;
            background: rgba(45, 212, 191, 0.12);
            border: 1px solid rgba(45, 212, 191, 0.3);
        }

        .badge-generative {
            color: #fbbf24;
            background: rgba(251, 191, 36, 0.12);
            border: 1px solid rgba(251, 191, 36, 0.3);
        }

        .btn-grafana {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 4px 10px;
            font-size: 11px;
            font-weight: 600;
            font-family: var(--font-mono);
            color: #f97316;
            background: rgba(249, 115, 22, 0.1);
            border: 1px solid rgba(249, 115, 22, 0.35);
            border-radius: 4px;
            text-decoration: none;
            transition: all 0.2s ease;
        }

        .btn-grafana:hover {
            background: rgba(249, 115, 22, 0.2);
            border-color: #f97316;
            text-decoration: none;
        }

        .step-desc {
            font-size: 12px;
            color: var(--text-muted);
            margin-top: 2px;
        }

        .step-evidence {
            font-family: var(--font-mono);
            font-size: 11px;
            background: var(--bg);
            border: 1px solid var(--border);
            padding: 6px 10px;
            border-radius: 3px;
            margin-top: 6px;
            color: var(--text);
            word-break: break-all;
        }

        /* Fleet Grid */
        .fleet-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(110px, 1fr));
            gap: 8px;
        }

        .node-tile {
            background: var(--bg);
            border: 1px solid var(--border);
            border-radius: 4px;
            padding: 10px 8px;
            text-align: center;
        }

        .node-tile.throttled {
            border-color: var(--danger);
            background: var(--danger-bg);
        }

        .node-tile.quarantined {
            border: 2px solid var(--danger);
            background: var(--danger-bg);
        }

        .node-tile.standby {
            border-style: dashed;
            opacity: 0.75;
        }

        .node-id {
            font-size: 12px;
            font-weight: 600;
            color: var(--text-heading);
            font-family: var(--font-mono);
        }

        .node-temp {
            font-family: var(--font-mono);
            font-size: 13px;
            font-weight: 600;
            margin: 4px 0;
        }

        .node-shot {
            font-size: 10px;
            color: var(--text-muted);
            font-family: var(--font-mono);
        }

        .footer-note {
            text-align: center;
            font-size: 11px;
            color: var(--text-muted);
            margin-top: 32px;
            padding-top: 16px;
            border-top: 1px solid var(--border);
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="brand">
                <h1>Callsheet</h1>
                <p>Autonomous Operations Agent for Post-Production Delivery Producers</p>
            </div>
            <div class="header-meta">
                <a href="https://bigforest2172.grafana.net/d/callsheet-control-tower/callsheet-media-production-control-tower" target="_blank" rel="noopener noreferrer" class="btn-grafana">
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path><polyline points="15 3 21 3 21 9"></polyline><line x1="10" y1="14" x2="21" y2="3"></line></svg>
                    Grafana Control Tower
                </a>
                <div id="utc-clock" class="utc-clock">--:--:-- UTC</div>
                <div id="agent-status-badge" class="status-badge badge-live">
                    <span class="badge-pulse"></span>
                    <span id="agent-status-text">Autonomous Watch Active</span>
                </div>
            </div>
        </header>

        <!-- Delivery Slate -->
        <div class="section-title">Active Delivery Slate</div>
        <div id="slate-container" class="slate-grid">
<!-- SSR_SLATE -->
        </div>

        <!-- Main Workspace -->
        <div class="main-layout">
            <!-- Left: Callsheet Briefing & Audit -->
            <div class="card">
                <div class="card-header">
                    <span class="card-heading">Production Callsheet Briefing</span>
                    <span id="briefing-timestamp" style="font-size: 11px; font-family: var(--font-mono); color: var(--text-muted);"><!-- SSR_BRIEFING_TIME --></span>
                </div>

                <div id="briefing-container" class="briefing-content">
<!-- SSR_BRIEFING -->
                </div>

                <details class="trail-accordion" open>
                    <summary>Grafana Cloud MCP Evidence Trail & Telemetry Chain</summary>
                    <div id="trail-container" class="step-timeline">
<!-- SSR_TRAIL -->
                    </div>
                </details>
            </div>

            <!-- Right: Fleet Hardware Grid -->
            <div>
                <div class="card">
                    <div class="card-header">
                        <span class="card-heading">Render Fleet Telemetry (12 Nodes)</span>
                        <span id="fleet-summary" style="font-size: 11px; font-family: var(--font-mono); color: var(--text-muted);"><!-- SSR_FLEET_SUMMARY --></span>
                    </div>
                    <div id="fleet-container" class="fleet-grid">
<!-- SSR_FLEET -->
                    </div>
                </div>
            </div>
        </div>

        <div class="footer-note">
            Callsheet Autonomous Post-Production Agent. Telemetry streamed continuously via OpenTelemetry OTLP to Grafana Cloud.
        </div>
    </div>

    <script>
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
            
            // Format tables
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

        async function fetchState() {
            try {
                const res = await fetch('/api/state');
                const data = await res.json();

                // Update Agent Status Badge
                const badge = document.getElementById('agent-status-badge');
                const badgeText = document.getElementById('agent-status-text');
                if (data.is_investigating) {
                    badge.className = 'status-badge badge-investigating';
                    badgeText.innerText = 'Investigating Anomaly (MCP)';
                } else {
                    badge.className = 'status-badge badge-live';
                    badgeText.innerText = 'Autonomous Watch Active';
                }

                // Render Delivery Slate
                renderSlate(data.shows, data.latest_mission);

                // Render Latest Mission Briefing
                if (data.latest_mission) {
                    renderBriefing(data.latest_mission);
                    renderTrail(data.latest_mission.steps);
                }

                // Render Fleet Grid
                renderFleet(data.nodes, data.latest_mission);

            } catch (err) {
                console.error("Failed to fetch farm state:", err);
            }
        }

        function renderSlate(shows, latestMission) {
            const container = document.getElementById('slate-container');
            const showList = Object.values(shows);
            if (!showList.length) return;

            const isIntervened = latestMission && latestMission.intervention_record;

            container.innerHTML = showList.map(s => {
                let statusTag = '<span class="state-tag tag-scheduled">On Schedule</span>';
                let bufferMargin = '+5.5 hours';

                if (s.id === 'show-aethelgard') {
                    if (isIntervened) {
                        statusTag = '<span class="state-tag tag-protected">Protected: Failover Applied</span>';
                        bufferMargin = '+' + (latestMission.intervention_record.buffer_margin_hours || 2.9).toFixed(1) + ' hours';
                    } else {
                        bufferMargin = '+2.9 hours';
                    }
                } else if (s.id === 'show-solarflare' || s.id === 'show-solar') {
                    bufferMargin = '+5.5 hours';
                } else if (s.id === 'show-abyssal') {
                    bufferMargin = '+9.4 hours';
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

                return `
                    <div class="slate-card ${s.critical_path ? 'critical' : ''}">
                        <div class="slate-header">
                            <div>
                                <div class="show-title">${s.name}</div>
                                <div class="show-client">${s.client}</div>
                            </div>
                            ${statusTag}
                        </div>
                        <div class="slate-metrics">
                            <div class="metric-row">
                                <span>Deadline</span>
                                <span>${deadlineFormatted}</span>
                            </div>
                            <div class="metric-row">
                                <span>Buffer Margin</span>
                                <span style="color: var(--success);">${bufferMargin}</span>
                            </div>
                            <div class="metric-row">
                                <span>Daily Penalty</span>
                                <span>£${s.penalty_daily_amount.toLocaleString()} / day</span>
                            </div>
                            <div class="metric-row">
                                <span>Priority Tier</span>
                                <span>${s.critical_path ? 'Critical Path' : 'Standard'}</span>
                            </div>
                        </div>
                    </div>
                `;
            }).join('');
        }

        function renderBriefing(mission) {
            const container = document.getElementById('briefing-container');
            const timeLabel = document.getElementById('briefing-timestamp');

            if (mission.timestamp) {
                const dateObj = new Date(mission.timestamp);
                const utcHours = String(dateObj.getUTCHours()).padStart(2, '0');
                const utcMinutes = String(dateObj.getUTCMinutes()).padStart(2, '0');
                const utcSeconds = String(dateObj.getUTCSeconds()).padStart(2, '0');
                timeLabel.innerText = `Last completed mission: ${utcHours}:${utcMinutes}:${utcSeconds} UTC`;
            }

            container.innerHTML = formatMarkdown(mission.callsheet_briefing);
        }

        function renderTrail(steps) {
            const container = document.getElementById('trail-container');
            if (!steps || !steps.length) {
                container.innerHTML = '<div style="font-size: 12px; color: var(--text-muted);">No steps recorded.</div>';
                return;
            }

            container.innerHTML = steps.map(step => {
                let evidenceStr = '';
                if (step.evidence) {
                    evidenceStr = JSON.stringify(step.evidence, null, 2);
                }

                const execType = step.execution_type || 'DETERMINISTIC_TELEMETRY';
                let badgeClass = 'badge-telemetry';
                let badgeLabel = 'DETERMINISTIC TELEMETRY';
                if (execType === 'DETERMINISTIC_ARITHMETIC') {
                    badgeClass = 'badge-arithmetic';
                    badgeLabel = 'DETERMINISTIC ARITHMETIC';
                } else if (execType === 'DETERMINISTIC_ACTION') {
                    badgeClass = 'badge-action';
                    badgeLabel = 'DETERMINISTIC ACTION';
                } else if (execType === 'DETERMINISTIC_VERIFICATION') {
                    badgeClass = 'badge-verification';
                    badgeLabel = 'DETERMINISTIC VERIFICATION';
                } else if (execType === 'GENERATIVE_SYNTHESIS') {
                    badgeClass = 'badge-generative';
                    badgeLabel = 'GENERATIVE AI (EXPLANATORY)';
                }

                return `
                    <div class="step-entry">
                        <div class="step-title">
                            <span>Step ${step.step_number}: ${step.name}</span>
                            <span class="badge-type ${badgeClass}">${badgeLabel}</span>
                        </div>
                        <div class="step-desc">${step.description}</div>
                        ${evidenceStr && evidenceStr !== '{}' ? `<div class="step-evidence">${evidenceStr}</div>` : ''}
                    </div>
                `;
            }).join('');
        }

        function renderFleet(nodes, latestMission) {
            const container = document.getElementById('fleet-container');
            const summaryLabel = document.getElementById('fleet-summary');
            const nodeList = Object.values(nodes);
            if (!nodeList.length) return;

            const activeCount = nodeList.filter(n => !n.is_standby && n.status !== 'QUARANTINED').length;
            const quarantinedCount = nodeList.filter(n => n.status === 'QUARANTINED').length;
            const standbyCount = nodeList.filter(n => n.is_standby).length;
            
            if (quarantinedCount > 0) {
                summaryLabel.innerText = `${activeCount} Active / ${quarantinedCount} Quarantined / ${standbyCount} Standby`;
            } else {
                summaryLabel.innerText = `${activeCount} Active / ${standbyCount} Standby`;
            }

            container.innerHTML = nodeList.map(n => {
                let nodeClass = 'node-tile';
                let tempColor = 'var(--success)';
                let shotLabel = 'Idle';
                let tempVal = n.temperature_celsius;

                if (n.id === 'node-07' && (n.status === 'QUARANTINED' || n.status === 'THROTTLED')) {
                    if (latestMission && latestMission.intervention_record && latestMission.intervention_record.telemetry_evidence && latestMission.intervention_record.telemetry_evidence.source_temp) {
                        tempVal = Number(latestMission.intervention_record.telemetry_evidence.source_temp);
                    }
                }

                if (n.status === 'QUARANTINED') {
                    nodeClass += ' quarantined';
                    tempColor = 'var(--danger)';
                    shotLabel = 'Quarantined (Fault)';
                } else if (n.status === 'THROTTLED' || tempVal > 90) {
                    nodeClass += ' throttled';
                    tempColor = 'var(--danger)';
                    shotLabel = n.current_shot_id ? 'Shot ' + n.current_shot_id.replace('sh_', '') : 'Degraded (Fault)';
                } else if (n.is_standby) {
                    nodeClass += ' standby';
                    tempColor = 'var(--text-muted)';
                    shotLabel = 'Standby Spare';
                } else {
                    shotLabel = n.current_shot_id ? 'Shot ' + n.current_shot_id.replace('sh_', '') : 'Idle';
                }

                return `
                    <div class="${nodeClass}">
                        <div class="node-id">${n.id}</div>
                        <div class="node-temp" style="color: ${tempColor};">${tempVal.toFixed(1)}°C</div>
                        <div class="node-shot">${shotLabel}</div>
                    </div>
                `;
            }).join('');
        }

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
    <title>Callsheet: Demo Control & Fault Injection Harness</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #090d13;
            --surface: #121820;
            --border: #232f3e;
            --text: #c2cbd6;
            --text-heading: #f0f6fc;
            --text-muted: #7d8b99;
            --accent: #388bfd;
            --danger: #f85149;
            --danger-bg: rgba(248, 81, 73, 0.15);
            --warning: #d29922;
            --warning-bg: rgba(210, 153, 34, 0.15);
            --font-main: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            --font-mono: 'JetBrains Mono', monospace;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            background-color: var(--bg);
            color: var(--text);
            font-family: var(--font-main);
            padding: 24px;
        }

        .container { max-width: 900px; margin: 0 auto; }
        
        .demo-notice {
            background: var(--warning-bg);
            border: 1px solid var(--warning);
            color: #e3b341;
            padding: 12px 16px;
            border-radius: 6px;
            margin-bottom: 24px;
            font-size: 13px;
        }

        .card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 20px;
            margin-bottom: 24px;
        }

        h1 { font-size: 20px; color: var(--text-heading); margin-bottom: 8px; }
        p { font-size: 13px; color: var(--text-muted); margin-bottom: 16px; }

        .btn-group {
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
        }

        button {
            font-family: var(--font-main);
            font-size: 13px;
            font-weight: 600;
            padding: 10px 16px;
            border-radius: 4px;
            border: 1px solid transparent;
            cursor: pointer;
            transition: all 0.15s ease;
        }

        .btn-danger {
            background: var(--danger-bg);
            color: var(--danger);
            border-color: rgba(248, 81, 73, 0.4);
        }
        .btn-danger:hover { background: rgba(248, 81, 73, 0.3); }

        .btn-secondary {
            background: #1c2430;
            color: var(--text-heading);
            border-color: var(--border);
        }
        .btn-secondary:hover { background: #2a3545; }

        .btn-primary {
            background: #238636;
            color: #ffffff;
        }
        .btn-primary:hover { background: #2ea043; }

        .log-box {
            background: #05080c;
            border: 1px solid var(--border);
            border-radius: 4px;
            padding: 14px;
            font-family: var(--font-mono);
            font-size: 12px;
            color: #58a6ff;
            height: 220px;
            overflow-y: auto;
        }

        a { color: var(--accent); text-decoration: none; font-size: 13px; }
        a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <div class="container">
        <div class="demo-notice">
            <strong>Demonstration Control Surface:</strong> This route is isolated for recording evaluation videos and manual scenario fault injection. The main product surface is at <a href="/">/ (Producer Dashboard)</a>.
        </div>

        <div class="card">
            <h1>Fault Injection & Scenario Controls</h1>
            <p>Inject synthetic hardware degradation scenarios into the running render farm or trigger manual mission executions.</p>

            <div class="btn-group">
                <button class="btn-danger" onclick="injectScenario('THERMAL_THROTTLING')">Inject Scenario: Node-07 Thermal Throttling</button>
                <button class="btn-secondary" onclick="injectScenario('MEMORY_LEAK_OOM')">Inject Scenario: Memory Leak (OOM)</button>
                <button class="btn-secondary" onclick="injectScenario('BASELINE')">Restore Baseline Operations</button>
                <button class="btn-primary" onclick="runManualMission()">Trigger Verified Mission</button>
                <button class="btn-danger" style="background: #7f1d1d; color: #fca5a5; border-color: #ef4444;" onclick="runFailureMission()">Force Verification Failure (Failover Stall)</button>
            </div>
        </div>

        <div class="card">
            <h1>Harness Activity Log</h1>
            <div id="log-box" class="log-box">Harness ready. Telemetry worker active.</div>
        </div>
    </div>

    <script>
        function log(msg) {
            const box = document.getElementById('log-box');
            const time = new Date().toISOString().substring(11, 19) + ' UTC';
            box.innerHTML = `[${time}] ${msg}<br>` + box.innerHTML;
        }

        async function injectScenario(scenario) {
            log(`Injecting scenario: ${scenario}...`);
            try {
                const res = await fetch('/api/scenario', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ scenario: scenario })
                });
                const data = await res.json();
                log(`Scenario active: ${data.scenario}. Watchdog will detect and intervene automatically.`);
            } catch (err) {
                log(`Error injecting scenario: ${err}`);
            }
        }

        async function runManualMission() {
            log('Triggering manual 7-step mission across Prometheus, Loki, Tempo, Vertex AI, and Closed-Loop Verification...');
            try {
                const res = await fetch('/api/mission', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ show_id: 'show-aethelgard', force_verification_fault: false })
                });
                const data = await res.json();
                log(`Mission complete: ${data.anomaly_detected} -> Verification: ${data.verification_status} (Buffer: ${data.intervention_record ? data.intervention_record.status : 'None'})`);
            } catch (err) {
                log(`Mission error: ${err}`);
            }
        }

        async function runFailureMission() {
            log('Triggering 7-step mission with FORCED post-intervention verification failure...');
            try {
                const res = await fetch('/api/mission', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ show_id: 'show-aethelgard', force_verification_fault: true })
                });
                const data = await res.json();
                log(`Mission completed with ESCALATION: Verification: ${data.verification_status}. Briefing generated with immediate human TD escalation recommendation.`);
            } catch (err) {
                log(`Mission error: ${err}`);
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

    briefing_text = mission.get("callsheet_briefing", "") if mission else ""
    briefing_html = format_markdown_to_html(briefing_text)
    steps = mission.get("steps", []) if mission else []
    trail_html = render_ssr_trail(steps)

    timestamp = mission.get("timestamp", "") if mission else ""
    if timestamp:
        try:
            if isinstance(timestamp, str):
                ts_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            else:
                ts_dt = timestamp
            time_label = f"Last completed mission: {ts_dt.strftime('%H:%M:%S')} UTC"
        except Exception:
            time_label = f"Last completed mission: {str(timestamp)[:19].replace('T', ' ')} UTC"
    else:
        time_label = "Autonomous Watch Active"

    html = (
        PRODUCER_UI_TEMPLATE
        .replace("<!-- SSR_SLATE -->", slate_html)
        .replace("<!-- SSR_BRIEFING_TIME -->", time_label)
        .replace("<!-- SSR_BRIEFING -->", briefing_html)
        .replace("<!-- SSR_TRAIL -->", trail_html)
        .replace("<!-- SSR_FLEET_SUMMARY -->", fleet_summary)
        .replace("<!-- SSR_FLEET -->", fleet_html)
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
