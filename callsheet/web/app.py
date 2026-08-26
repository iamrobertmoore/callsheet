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
    return {
        "active_scenario": state.active_scenario.value,
        "last_updated": state.last_updated.isoformat(),
        "shows": {k: v.model_dump(mode="json") for k, v in state.shows.items()},
        "nodes": {k: v.model_dump(mode="json") for k, v in state.nodes.items()},
        "shots": {k: v.model_dump(mode="json") for k, v in state.shots.items()},
        "latest_mission": worker.latest_mission,
        "is_investigating": worker.is_investigating,
        "interventions_count": len(dispatcher.history),
    }


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
    """Executes the 6-step observability to intervention mission."""
    try:
        result = await mission_runner.execute_mission(show_id=req.show_id or "show-aethelgard")
        worker.latest_mission = result.model_dump(mode="json")
        return worker.latest_mission
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@app.get("/api/interventions")
async def get_interventions():
    """Returns the history of interventions executed by Callsheet."""
    return [r.model_dump(mode="json") for r in dispatcher.list_history()]


# Producer Surface (Main Interface)
PRODUCER_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Callsheet: Autonomous Operations Agent for Studio Delivery Producers</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #090d13;
            --surface: #121820;
            --surface-subtle: #16202c;
            --border: #232f3e;
            --border-highlight: #34465d;
            --text: #c2cbd6;
            --text-heading: #f0f6fc;
            --text-muted: #7d8b99;
            --accent: #388bfd;
            --danger: #f85149;
            --danger-bg: rgba(248, 81, 73, 0.12);
            --warning: #d29922;
            --warning-bg: rgba(210, 153, 34, 0.12);
            --success: #3fb950;
            --success-bg: rgba(63, 185, 80, 0.12);
            --font-main: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            --font-mono: 'JetBrains Mono', monospace;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            background-color: var(--bg);
            color: var(--text);
            font-family: var(--font-main);
            line-height: 1.5;
            padding: 24px;
        }

        .container { max-width: 1280px; margin: 0 auto; }
        
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding-bottom: 18px;
            border-bottom: 1px solid var(--border);
            margin-bottom: 24px;
        }

        .brand h1 {
            font-size: 20px;
            font-weight: 700;
            color: var(--text-heading);
            letter-spacing: 0.5px;
            text-transform: uppercase;
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
            font-size: 12px;
            color: var(--text-muted);
            background: var(--surface-subtle);
            padding: 5px 10px;
            border-radius: 4px;
            border: 1px solid var(--border);
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 5px 10px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
            font-family: var(--font-mono);
            text-transform: uppercase;
        }

        .badge-live {
            background: var(--success-bg);
            color: var(--success);
            border: 1px solid rgba(63, 185, 80, 0.35);
        }

        .badge-investigating {
            background: var(--warning-bg);
            color: var(--warning);
            border: 1px solid rgba(210, 153, 34, 0.35);
        }

        .badge-pulse {
            width: 7px;
            height: 7px;
            background: var(--success);
            border-radius: 50%;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.3; transform: scale(1.2); }
            100% { opacity: 1; transform: scale(1); }
        }

        /* Section Layout */
        .section-title {
            font-size: 13px;
            font-weight: 700;
            letter-spacing: 0.75px;
            text-transform: uppercase;
            color: var(--text-muted);
            margin-bottom: 12px;
        }

        .slate-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }

        .slate-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 16px;
        }

        .slate-card.critical {
            border-left: 3px solid var(--accent);
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
            margin-top: 1px;
        }

        .state-tag {
            font-family: var(--font-mono);
            font-size: 11px;
            font-weight: 600;
            padding: 3px 8px;
            border-radius: 3px;
            text-transform: uppercase;
        }

        .tag-protected {
            background: var(--success-bg);
            color: var(--success);
            border: 1px solid rgba(63, 185, 80, 0.3);
        }

        .tag-scheduled {
            background: var(--surface-subtle);
            color: var(--accent);
            border: 1px solid rgba(56, 139, 253, 0.3);
        }

        .slate-metrics {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            background: var(--bg);
            border: 1px solid var(--border);
            border-radius: 4px;
            padding: 10px;
            font-size: 12px;
        }

        .metric-row span:first-child {
            color: var(--text-muted);
            display: block;
            font-size: 11px;
        }

        .metric-row span:last-child {
            font-family: var(--font-mono);
            color: var(--text-heading);
            font-weight: 500;
        }

        /* Two column main content */
        .main-layout {
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 24px;
            margin-bottom: 24px;
        }

        @media (max-width: 960px) {
            .main-layout { grid-template-columns: 1fr; }
        }

        .card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 20px;
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
            padding-bottom: 12px;
            border-bottom: 1px solid var(--border);
        }

        .card-heading {
            font-size: 14px;
            font-weight: 600;
            color: var(--text-heading);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        /* Callsheet Report */
        .briefing-content {
            background: var(--bg);
            border: 1px solid var(--border);
            border-radius: 4px;
            padding: 18px;
            font-size: 13px;
            line-height: 1.6;
        }

        .briefing-content h3 {
            color: var(--text-heading);
            font-size: 13px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin: 16px 0 8px;
            border-bottom: 1px solid var(--border);
            padding-bottom: 4px;
        }

        .briefing-content h3:first-child {
            margin-top: 0;
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
            <div class="slate-card">Loading delivery slate...</div>
        </div>

        <!-- Main Workspace -->
        <div class="main-layout">
            <!-- Left: Callsheet Briefing & Audit -->
            <div class="card">
                <div class="card-header">
                    <span class="card-heading">Production Callsheet Briefing</span>
                    <span id="briefing-timestamp" style="font-size: 11px; font-family: var(--font-mono); color: var(--text-muted);">Synchronizing...</span>
                </div>

                <div id="briefing-container" class="briefing-content">
                    Synchronizing latest delivery briefing from autonomous agent...
                </div>

                <details class="trail-accordion" open>
                    <summary>Grafana Cloud MCP Evidence Trail & Telemetry Chain</summary>
                    <div id="trail-container" class="step-timeline">
                        <!-- Populated by JS -->
                    </div>
                </details>
            </div>

            <!-- Right: Fleet Hardware Grid -->
            <div>
                <div class="card">
                    <div class="card-header">
                        <span class="card-heading">Render Fleet Telemetry (12 Nodes)</span>
                        <span id="fleet-summary" style="font-size: 11px; font-family: var(--font-mono); color: var(--text-muted);">10 Active / 2 Standby</span>
                    </div>
                    <div id="fleet-container" class="fleet-grid">
                        Loading node status...
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
                renderFleet(data.nodes);

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
                let bufferMargin = '+2.9 hours';

                if (s.id === 'show-aethelgard' && isIntervened) {
                    statusTag = '<span class="state-tag tag-protected">Protected: Failover Applied</span>';
                    bufferMargin = '+' + (latestMission.intervention_record.buffer_margin_hours || 2.9).toFixed(1) + ' hours';
                } else if (s.id === 'show-solar') {
                    bufferMargin = '+24.0 hours';
                } else if (s.id === 'show-abyssal') {
                    bufferMargin = '+48.0 hours';
                }

                const deadlineFormatted = new Date(s.delivery_deadline).toUTCString().replace(':00 GMT', ' UTC');

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
                timeLabel.innerText = 'Last updated ' + dateObj.toLocaleTimeString() + ' (' + dateObj.toDateString() + ')';
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

                return `
                    <div class="step-entry">
                        <div class="step-title">Step ${step.step_number}: ${step.name}</div>
                        <div class="step-desc">${step.description}</div>
                        ${evidenceStr && evidenceStr !== '{}' ? `<div class="step-evidence">${evidenceStr}</div>` : ''}
                    </div>
                `;
            }).join('');
        }

        function renderFleet(nodes) {
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

                if (n.status === 'QUARANTINED') {
                    nodeClass += ' quarantined';
                    tempColor = 'var(--danger)';
                } else if (n.status === 'THROTTLED' || n.temperature_celsius > 90) {
                    nodeClass += ' throttled';
                    tempColor = 'var(--danger)';
                } else if (n.is_standby) {
                    nodeClass += ' standby';
                    tempColor = 'var(--text-muted)';
                }

                let shotLabel = 'Idle';
                if (n.status === 'QUARANTINED') {
                    shotLabel = 'Quarantined (Fault)';
                } else if (n.current_shot_id) {
                    shotLabel = 'Shot ' + n.current_shot_id.replace('sh_', '');
                } else if (n.is_standby) {
                    shotLabel = 'Standby Spare';
                }

                return `
                    <div class="${nodeClass}">
                        <div class="node-id">${n.id}</div>
                        <div class="node-temp" style="color: ${tempColor};">${n.temperature_celsius.toFixed(1)}°C</div>
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
                <button class="btn-primary" onclick="runManualMission()">Trigger Manual Agent Mission</button>
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
            const time = new Date().toLocaleTimeString();
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
            log('Triggering manual 6-step mission across Prometheus, Loki, Tempo, and Vertex AI...');
            try {
                const res = await fetch('/api/mission', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ show_id: 'show-aethelgard' })
                });
                const data = await res.json();
                log(`Mission complete: ${data.anomaly_detected} -> Intervention: ${data.intervention_record ? data.intervention_record.status : 'None'}`);
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
    return PRODUCER_UI_HTML


@app.get("/demo", response_class=HTMLResponse)
async def get_demo_harness():
    return DEMO_UI_HTML
