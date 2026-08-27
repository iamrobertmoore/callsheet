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
                        <span style="color: var(--state-healthy);">{buffer_margin}</span>
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
        return '<div style="font-size: 12px; color: var(--text-tertiary);">No steps recorded.</div>'
    import json
    entries = []
    badges = {
        "DETERMINISTIC_TELEMETRY": '<span class="badge-deterministic">⬡ DET: TELEMETRY</span>',
        "DETERMINISTIC_ARITHMETIC": '<span class="badge-deterministic">⬡ DET: ARITHMETIC</span>',
        "DETERMINISTIC_ACTION": '<span class="badge-deterministic">⬡ DET: WORKLOAD FAILOVER</span>',
        "DETERMINISTIC_VERIFICATION": '<span class="badge-deterministic">⬡ DET: POST-AUDIT</span>',
        "GENERATIVE_SYNTHESIS": '<span class="badge-generative">✦ GEN-AI: PRODUCER SYNTHESIS</span>',
    }
    for step in steps:
        step_num = step.get("step_number") if isinstance(step, dict) else getattr(step, "step_number", 1)
        name = step.get("name") if isinstance(step, dict) else getattr(step, "name", "")
        desc = step.get("description") if isinstance(step, dict) else getattr(step, "description", "")
        exec_type = step.get("execution_type") if isinstance(step, dict) else getattr(step, "execution_type", "DETERMINISTIC_TELEMETRY")
        evidence = step.get("evidence") if isinstance(step, dict) else getattr(step, "evidence", {})
        evidence_str = json.dumps(evidence, indent=2) if evidence else ""

        if step_num == 3:
            badge_html = '<span class="badge-generative">✦ GEN-AI: ROOT CAUSE DEDUCTION</span>'
        elif step_num == 7:
            badge_html = '<span class="badge-generative">✦ GEN-AI: PRODUCER BRIEFING</span>'
        else:
            badge_html = badges.get(exec_type, '<span class="badge-deterministic">⬡ DETERMINISTIC</span>')

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
            node_class = "node-tile node-quarantined"
            temp_color = "var(--state-fault)"
            shot_label = "Quarantined (Fault)"
        elif is_standby:
            standby_count += 1
            node_class = "node-tile node-standby"
            temp_color = "var(--text-tertiary)"
            shot_label = "Standby Spare"
        else:
            active_count += 1
            if status_val == "THROTTLED" or temp >= 90.0:
                node_class = "node-tile node-fault"
                temp_color = "var(--state-fault)"
                shot_label = f"Shot {shot_id.replace('sh_', '')}" if shot_id else "Degraded (Fault)"
            elif temp >= 78.0:
                node_class = "node-tile node-hot"
                temp_color = "var(--state-warn)"
                shot_label = f"Shot {shot_id.replace('sh_', '')}" if shot_id else "High Load"
            elif temp >= 68.0:
                node_class = "node-tile node-warm"
                temp_color = "var(--state-warn)"
                shot_label = f"Shot {shot_id.replace('sh_', '')}" if shot_id else "Warm Active"
            elif temp >= 58.0:
                node_class = "node-tile node-nominal"
                temp_color = "var(--state-healthy)"
                shot_label = f"Shot {shot_id.replace('sh_', '')}" if shot_id else "Nominal"
            else:
                node_class = "node-tile node-cool"
                temp_color = "var(--state-healthy)"
                shot_label = f"Shot {shot_id.replace('sh_', '')}" if shot_id else "Cool Active"

        tiles.append(f"""
            <div class="{node_class}" id="tile-{node_id}" data-node="{node_id}">
                <div class="node-tile-header">
                    <span class="node-id">{node_id}</span>
                    <span class="node-pip"></span>
                </div>
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
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            /* Ground & Layered Surfaces */
            --bg: #07090e;
            --surface: #0e131b;
            --surface-raised: #141b26;
            --surface-inset: #090c12;

            /* Hairline Separators in Two Weights */
            --border-subtle: 1px solid rgba(255, 255, 255, 0.08);
            --border-strong: 1px solid rgba(255, 255, 255, 0.16);

            /* Infrastructure Accent (Studio Technical Cobalt/Cyan at 3 Alphas) */
            --accent-solid: #0ea5e9;
            --accent-line: rgba(14, 165, 233, 0.35);
            --accent-wash: rgba(14, 165, 233, 0.10);

            /* Semantic States (Reserved Strictly for Operational State) */
            --state-healthy: #10b981;
            --state-healthy-border: rgba(16, 185, 129, 0.35);
            --state-healthy-wash: rgba(16, 185, 129, 0.08);

            --state-warn: #f59e0b;
            --state-warn-border: rgba(245, 158, 11, 0.35);
            --state-warn-wash: rgba(245, 158, 11, 0.08);

            --state-fault: #f43f5e;
            --state-fault-border: rgba(244, 63, 94, 0.50);
            --state-fault-wash: rgba(244, 63, 94, 0.12);

            --state-standby: #64748b;
            --state-standby-border: rgba(100, 116, 139, 0.25);
            --state-standby-wash: rgba(100, 116, 139, 0.06);

            /* Text Tints */
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --text-tertiary: #64748b;

            /* Typography Stacks */
            --font-display: 'Space Grotesk', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            --font-body: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            --font-mono: 'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, Monaco, monospace;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            background-color: var(--bg);
            background-image: radial-gradient(rgba(255, 255, 255, 0.04) 1px, transparent 1px);
            background-size: 24px 24px;
            color: var(--text-secondary);
            font-family: var(--font-body);
            padding: 24px;
            -webkit-font-smoothing: antialiased;
            min-height: 100vh;
        }

        .container { max-width: 1400px; margin: 0 auto; }

        /* Header Bar */
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding-bottom: 20px;
            border-bottom: var(--border-subtle);
            margin-bottom: 24px;
        }

        .brand h1 {
            font-family: var(--font-display);
            font-size: 22px;
            font-weight: 700;
            color: var(--text-primary);
            letter-spacing: -0.02em;
        }

        .brand p {
            font-size: 13px;
            color: var(--text-tertiary);
            margin-top: 3px;
        }

        .header-meta {
            display: flex;
            align-items: center;
            gap: 14px;
        }

        .btn-grafana {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 12px;
            font-size: 11px;
            font-weight: 600;
            font-family: var(--font-mono);
            color: var(--accent-solid);
            background: var(--accent-wash);
            border: 1px solid var(--accent-line);
            border-radius: 4px;
            text-decoration: none;
            transition: background 0.2s ease, border-color 0.2s ease, color 0.2s ease;
        }

        .btn-grafana:hover {
            background: rgba(14, 165, 233, 0.20);
            border-color: var(--accent-solid);
            color: var(--text-primary);
            text-decoration: none;
        }

        .utc-clock {
            font-family: var(--font-mono);
            font-size: 12px;
            font-weight: 600;
            font-variant-numeric: tabular-nums;
            color: var(--text-primary);
            background: var(--surface);
            padding: 6px 12px;
            border-radius: 4px;
            border: var(--border-subtle);
        }

        .status-badge {
            display: flex;
            align-items: center;
            gap: 8px;
            font-family: var(--font-display);
            font-size: 11px;
            font-weight: 600;
            letter-spacing: 0.02em;
            text-transform: uppercase;
            padding: 6px 12px;
            border-radius: 4px;
        }

        .badge-live {
            background: var(--state-healthy-wash);
            color: var(--state-healthy);
            border: 1px solid var(--state-healthy-border);
        }

        .badge-investigating {
            background: var(--state-warn-wash);
            color: var(--state-warn);
            border: 1px solid var(--state-warn-border);
        }

        .badge-pulse {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: currentColor;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.35; transform: scale(0.85); }
            100% { opacity: 1; transform: scale(1); }
        }

        /* Active Delivery Slate */
        .section-title {
            font-family: var(--font-display);
            font-size: 12px;
            font-weight: 600;
            color: var(--text-tertiary);
            margin-bottom: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }

        .slate-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }

        .slate-card {
            background: var(--surface);
            border: var(--border-subtle);
            border-radius: 6px;
            padding: 16px 18px;
            transition: border-color 0.2s ease;
        }

        .slate-card.critical {
            border: var(--border-strong);
            background: linear-gradient(180deg, var(--surface) 0%, #111722 100%);
            position: relative;
        }

        .slate-card.critical::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: var(--accent-solid);
            border-top-left-radius: 6px;
            border-top-right-radius: 6px;
        }

        .slate-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 12px;
            margin-bottom: 14px;
        }

        .show-title {
            font-family: var(--font-display);
            font-size: 15px;
            font-weight: 600;
            color: var(--text-primary);
            letter-spacing: -0.01em;
        }

        .show-client {
            font-size: 12px;
            color: var(--text-tertiary);
            margin-top: 3px;
        }

        .state-tag {
            font-family: var(--font-mono);
            font-size: 10px;
            font-weight: 600;
            padding: 3px 8px;
            border-radius: 3px;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            white-space: nowrap;
        }

        .tag-scheduled {
            background: var(--state-healthy-wash);
            color: var(--state-healthy);
            border: 1px solid var(--state-healthy-border);
        }

        .tag-protected {
            background: var(--state-healthy-wash);
            color: var(--state-healthy);
            border: 1px solid var(--state-healthy);
        }

        .tag-at-risk {
            background: var(--state-fault-wash);
            color: var(--state-fault);
            border: 1px solid var(--state-fault);
        }

        .slate-metrics {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
            padding-top: 12px;
            border-top: var(--border-subtle);
        }

        .metric-row span:first-child {
            font-size: 11px;
            color: var(--text-tertiary);
            text-transform: uppercase;
            letter-spacing: 0.03em;
            display: block;
            margin-bottom: 2px;
        }

        .metric-row span:last-child {
            font-family: var(--font-mono);
            font-size: 13px;
            font-weight: 600;
            font-variant-numeric: tabular-nums;
            color: var(--text-primary);
        }

        /* Main Workspace: Left Briefing & Audit, Right Fleet */
        .main-layout {
            display: grid;
            grid-template-columns: 1.55fr 1fr;
            gap: 20px;
        }

        @media (max-width: 1024px) {
            .main-layout { grid-template-columns: 1fr; }
        }

        .card {
            background: var(--surface);
            border: var(--border-subtle);
            border-radius: 6px;
            padding: 20px;
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
            padding-bottom: 12px;
            border-bottom: var(--border-subtle);
        }

        .card-heading {
            font-family: var(--font-display);
            font-size: 12px;
            font-weight: 600;
            color: var(--text-primary);
            text-transform: uppercase;
            letter-spacing: 0.06em;
        }

        /* Producer Briefing (Calm, Readable Prose) */
        .briefing-content {
            font-family: var(--font-body);
            font-size: 13.5px;
            line-height: 1.65;
            color: var(--text-primary);
            max-width: 72ch;
        }

        .briefing-content h3 {
            font-family: var(--font-display);
            font-size: 13px;
            font-weight: 600;
            color: var(--accent-solid);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin: 18px 0 8px 0;
        }

        .briefing-content p {
            margin-bottom: 12px;
            color: var(--text-secondary);
        }

        .briefing-content p strong {
            color: var(--text-primary);
            font-weight: 600;
        }

        .briefing-content table {
            width: 100%;
            border-collapse: collapse;
            margin: 16px 0;
            font-family: var(--font-mono);
            font-size: 12px;
            font-variant-numeric: tabular-nums;
            border: var(--border-subtle);
            border-radius: 4px;
            overflow: hidden;
        }

        .briefing-content th, .briefing-content td {
            padding: 8px 12px;
            text-align: left;
            border-bottom: var(--border-subtle);
        }

        .briefing-content th {
            background: var(--surface-raised);
            color: var(--text-primary);
            font-weight: 600;
            text-transform: uppercase;
            font-size: 11px;
            letter-spacing: 0.04em;
        }

        .briefing-content tr:last-child td {
            border-bottom: none;
        }

        .briefing-content ul {
            padding-left: 20px;
            margin-bottom: 14px;
        }

        .briefing-content li {
            margin-bottom: 6px;
            color: var(--text-secondary);
        }

        /* Evidence Trail Accordion & Staggered Steps */
        details.trail-accordion {
            background: var(--surface-inset);
            border: var(--border-subtle);
            border-radius: 4px;
            margin-top: 20px;
            padding: 14px 18px;
        }

        details.trail-accordion summary {
            font-family: var(--font-display);
            font-size: 12px;
            font-weight: 600;
            color: var(--accent-solid);
            cursor: pointer;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            user-select: none;
        }

        .step-timeline {
            margin-top: 16px;
            border-left: 1px solid var(--border-strong);
            padding-left: 16px;
        }

        .step-entry {
            margin-bottom: 18px;
            position: relative;
            opacity: 0;
            transform: translateY(6px);
            animation: stepReveal 0.35s cubic-bezier(0.16, 1, 0.3, 1) forwards;
        }

        .step-entry:nth-child(1) { animation-delay: 45ms; }
        .step-entry:nth-child(2) { animation-delay: 90ms; }
        .step-entry:nth-child(3) { animation-delay: 135ms; }
        .step-entry:nth-child(4) { animation-delay: 180ms; }
        .step-entry:nth-child(5) { animation-delay: 225ms; }
        .step-entry:nth-child(6) { animation-delay: 270ms; }
        .step-entry:nth-child(7) { animation-delay: 315ms; }

        @keyframes stepReveal {
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        .step-entry::before {
            content: '';
            position: absolute;
            left: -21px;
            top: 5px;
            width: 9px;
            height: 9px;
            border-radius: 50%;
            background: var(--surface-raised);
            border: 2px solid var(--accent-solid);
        }

        .step-title {
            font-family: var(--font-display);
            font-size: 13px;
            font-weight: 600;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 10px;
            flex-wrap: wrap;
        }

        /* Distinct Category Badges: Deterministic vs Generative */
        .badge-deterministic {
            font-family: var(--font-mono);
            font-size: 10px;
            font-weight: 600;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            padding: 2px 8px;
            border-radius: 2px;
            color: #38bdf8;
            background: rgba(56, 189, 248, 0.08);
            border: 1px solid rgba(56, 189, 248, 0.35);
            display: inline-flex;
            align-items: center;
            gap: 4px;
            white-space: nowrap;
        }

        .badge-generative {
            font-family: var(--font-body);
            font-size: 10px;
            font-weight: 600;
            letter-spacing: 0.02em;
            text-transform: uppercase;
            padding: 2px 10px;
            border-radius: 9999px;
            color: #c084fc;
            background: rgba(192, 132, 252, 0.12);
            border: 1px solid rgba(192, 132, 252, 0.40);
            display: inline-flex;
            align-items: center;
            gap: 4px;
            white-space: nowrap;
        }

        .step-desc {
            font-size: 12.5px;
            color: var(--text-secondary);
            line-height: 1.5;
            margin-top: 4px;
        }

        .step-evidence {
            font-family: var(--font-mono);
            font-size: 11px;
            font-variant-numeric: tabular-nums;
            background: var(--surface-inset);
            border: var(--border-subtle);
            padding: 8px 12px;
            border-radius: 3px;
            margin-top: 8px;
            color: var(--text-secondary);
            white-space: pre-wrap;
            word-break: break-word;
        }

        /* Hero Fleet Telemetry Grid */
        .fleet-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 10px;
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
            border: var(--border-subtle);
            border-radius: 4px;
            padding: 10px 12px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            min-height: 84px;
            transition: border-color 0.2s ease, background-color 0.2s ease;
            position: relative;
        }

        .node-tile-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .node-id {
            font-family: var(--font-mono);
            font-size: 11px;
            font-weight: 600;
            color: var(--text-secondary);
            letter-spacing: -0.01em;
        }

        .node-pip {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: currentColor;
            opacity: 0.85;
        }

        .node-temp {
            font-family: var(--font-mono);
            font-size: 17px;
            font-weight: 700;
            font-variant-numeric: tabular-nums;
            margin: 4px 0 2px 0;
            letter-spacing: -0.02em;
        }

        .node-shot {
            font-family: var(--font-mono);
            font-size: 10px;
            color: var(--text-tertiary);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        /* Thermal State Scales */
        .node-tile.node-cool {
            background: rgba(16, 185, 129, 0.04);
            border: 1px solid rgba(16, 185, 129, 0.20);
        }
        .node-tile.node-cool .node-pip { color: var(--state-healthy); }

        .node-tile.node-nominal {
            background: rgba(16, 185, 129, 0.08);
            border: 1px solid rgba(16, 185, 129, 0.35);
        }
        .node-tile.node-nominal .node-pip { color: var(--state-healthy); }

        .node-tile.node-warm {
            background: rgba(245, 158, 11, 0.07);
            border: 1px solid rgba(245, 158, 11, 0.30);
        }
        .node-tile.node-warm .node-pip { color: var(--state-warn); }

        .node-tile.node-hot {
            background: rgba(249, 115, 22, 0.10);
            border: 1px solid rgba(249, 115, 22, 0.40);
        }
        .node-tile.node-hot .node-pip { color: #f97316; }

        .node-tile.node-fault {
            background: var(--state-fault-wash);
            border: 1px solid var(--state-fault-border);
        }
        .node-tile.node-fault .node-pip { color: var(--state-fault); }

        .node-tile.node-quarantined {
            background: var(--state-fault-wash);
            border: 1px solid var(--state-fault-border);
            outline: 1px dashed var(--state-fault);
            outline-offset: 2px;
        }
        .node-tile.node-quarantined .node-pip { color: var(--state-fault); }

        .node-tile.node-standby {
            background: var(--surface-inset);
            border: 1px dashed var(--state-standby-border);
            opacity: 0.65;
        }
        .node-tile.node-standby .node-pip { color: var(--state-standby); }

        /* Signature Failover Takeover Arrival Pulse */
        .node-takeover-pulse {
            animation: takeoverPulse 1.2s ease-out;
        }

        @keyframes takeoverPulse {
            0% { box-shadow: 0 0 0 0 rgba(14, 165, 233, 0.7); }
            70% { box-shadow: 0 0 0 10px rgba(14, 165, 233, 0); }
            100% { box-shadow: 0 0 0 0 rgba(14, 165, 233, 0); }
        }

        .footer-note {
            text-align: center;
            font-size: 11px;
            font-family: var(--font-mono);
            color: var(--text-tertiary);
            margin-top: 36px;
            padding-top: 16px;
            border-top: var(--border-subtle);
        }

        /* Reduced Motion Fallback */
        @media (prefers-reduced-motion: reduce) {
            * {
                animation-duration: 0.01ms !important;
                animation-iteration-count: 1 !important;
                transition-duration: 0.01ms !important;
            }
            .step-entry {
                opacity: 1 !important;
                transform: none !important;
                animation: none !important;
            }
            .badge-pulse {
                animation: none !important;
            }
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
                <a href="https://bigforest2172.grafana.net/public-dashboards/a9028daf791643b8899a10531f6b31dd" target="_blank" rel="noopener noreferrer" class="btn-grafana">
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
                    <span id="briefing-timestamp" style="font-size: 11px; font-family: var(--font-mono); color: var(--text-tertiary);"><!-- SSR_BRIEFING_TIME --></span>
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
                        <span id="fleet-summary" style="font-size: 11px; font-family: var(--font-mono); color: var(--text-tertiary);"><!-- SSR_FLEET_SUMMARY --></span>
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
            path.setAttribute('stroke', 'rgba(14, 165, 233, 0.45)');
            path.setAttribute('stroke-width', '1.5');
            path.setAttribute('stroke-dasharray', '4 4');

            const bead = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            bead.setAttribute('r', '4.5');
            bead.setAttribute('fill', '#0ea5e9');

            svg.appendChild(path);
            svg.appendChild(bead);
            grid.appendChild(svg);

            const startTime = performance.now();
            const duration = 1200;
            const pathLen = path.getTotalLength();

            function step(now) {
                const elapsed = now - startTime;
                const p = Math.min(elapsed / duration, 1.0);
                // Cubic ease-out
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
                        bufferMargin = '+' + (latestMission.intervention_record.buffer_margin_hours || 2.8).toFixed(1) + ' hours';
                    } else {
                        bufferMargin = '+2.8 hours';
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
                                <span style="color: var(--state-healthy);">${bufferMargin}</span>
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
                timeLabel.innerText = `Mission logged: ${utcHours}:${utcMinutes}:${utcSeconds} UTC`;
            }

            container.innerHTML = formatMarkdown(mission.callsheet_briefing);
        }

        function renderTrail(steps) {
            const container = document.getElementById('trail-container');
            if (!steps || !steps.length) {
                container.innerHTML = '<div style="font-size: 12px; color: var(--text-tertiary);">No steps recorded.</div>';
                return;
            }

            const badges = {
                'DETERMINISTIC_TELEMETRY': '<span class="badge-deterministic">⬡ DET: TELEMETRY</span>',
                'DETERMINISTIC_ARITHMETIC': '<span class="badge-deterministic">⬡ DET: ARITHMETIC</span>',
                'DETERMINISTIC_ACTION': '<span class="badge-deterministic">⬡ DET: WORKLOAD FAILOVER</span>',
                'DETERMINISTIC_VERIFICATION': '<span class="badge-deterministic">⬡ DET: POST-AUDIT</span>',
                'GENERATIVE_SYNTHESIS': '<span class="badge-generative">✦ GEN-AI: PRODUCER SYNTHESIS</span>',
            };

            container.innerHTML = steps.map(step => {
                let evidenceStr = '';
                if (step.evidence) {
                    evidenceStr = JSON.stringify(step.evidence, null, 2);
                }

                const execType = step.execution_type || 'DETERMINISTIC_TELEMETRY';
                const stepNum = step.step_number || 1;
                let badgeHtml = '<span class="badge-deterministic">⬡ DETERMINISTIC</span>';

                if (stepNum === 3) {
                    badgeHtml = '<span class="badge-generative">✦ GEN-AI: ROOT CAUSE DEDUCTION</span>';
                } else if (stepNum === 7) {
                    badgeHtml = '<span class="badge-generative">✦ GEN-AI: PRODUCER BRIEFING</span>';
                } else if (badges[execType]) {
                    badgeHtml = badges[execType];
                }

                return `
                    <div class="step-entry">
                        <div class="step-title">
                            <span>Step ${stepNum}: ${step.name}</span>
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
                let tempColor = 'var(--state-healthy)';
                let shotLabel = 'Idle';
                let tempVal = n.temperature_celsius;

                if (n.id === 'node-07' && (n.status === 'QUARANTINED' || n.status === 'THROTTLED')) {
                    if (latestMission && latestMission.intervention_record && latestMission.intervention_record.telemetry_evidence && latestMission.intervention_record.telemetry_evidence.source_temp) {
                        tempVal = Number(latestMission.intervention_record.telemetry_evidence.source_temp);
                    }
                }

                if (n.status === 'QUARANTINED') {
                    nodeClass += ' node-quarantined';
                    tempColor = 'var(--state-fault)';
                    shotLabel = 'Quarantined (Fault)';
                } else if (n.is_standby) {
                    nodeClass += ' node-standby';
                    tempColor = 'var(--text-tertiary)';
                    shotLabel = 'Standby Spare';
                } else if (n.status === 'THROTTLED' || tempVal >= 90) {
                    nodeClass += ' node-fault';
                    tempColor = 'var(--state-fault)';
                    shotLabel = n.current_shot_id ? 'Shot ' + n.current_shot_id.replace('sh_', '') : 'Degraded (Fault)';
                } else if (tempVal >= 78.0) {
                    nodeClass += ' node-hot';
                    tempColor = 'var(--state-warn)';
                    shotLabel = n.current_shot_id ? 'Shot ' + n.current_shot_id.replace('sh_', '') : 'High Load';
                } else if (tempVal >= 68.0) {
                    nodeClass += ' node-warm';
                    tempColor = 'var(--state-warn)';
                    shotLabel = n.current_shot_id ? 'Shot ' + n.current_shot_id.replace('sh_', '') : 'Warm Active';
                } else if (tempVal >= 58.0) {
                    nodeClass += ' node-nominal';
                    tempColor = 'var(--state-healthy)';
                    shotLabel = n.current_shot_id ? 'Shot ' + n.current_shot_id.replace('sh_', '') : 'Nominal';
                } else {
                    nodeClass += ' node-cool';
                    tempColor = 'var(--state-healthy)';
                    shotLabel = n.current_shot_id ? 'Shot ' + n.current_shot_id.replace('sh_', '') : 'Cool Active';
                }

                return `
                    <div class="${nodeClass}" id="tile-${n.id}" data-node="${n.id}">
                        <div class="node-tile-header">
                            <span class="node-id">${n.id}</span>
                            <span class="node-pip"></span>
                        </div>
                        <div class="node-temp" style="color: ${tempColor};">${tempVal.toFixed(1)}°C</div>
                        <div class="node-shot">${shotLabel}</div>
                    </div>
                `;
            }).join('');

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
