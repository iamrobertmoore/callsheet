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
worker = FarmWorker(simulator=simulator, tick_interval_seconds=5.0)
dispatcher = InterventionDispatcher(simulator=simulator)
mission_runner = MultiStepMissionRunner(
    dispatcher=dispatcher,
    project_id=os.getenv("GOOGLE_CLOUD_PROJECT", "agent-attest-2026"),
    location=os.getenv("VERTEX_AI_LOCATION", "global"),
    model_name="gemini-3.6-flash",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start continuous background farm emitter
    await worker.start()
    yield
    # Stop emitter on shutdown
    await worker.stop()


app = FastAPI(
    title="Callsheet",
    description="Autonomous render pipeline agent for post-production delivery producers",
    version="0.1.0",
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
    """Returns the current state of shows, nodes, and active shots."""
    state = simulator.state
    return {
        "active_scenario": state.active_scenario.value,
        "last_updated": state.last_updated.isoformat(),
        "shows": {k: v.model_dump(mode="json") for k, v in state.shows.items()},
        "nodes": {k: v.model_dump(mode="json") for k, v in state.nodes.items()},
        "shots": {k: v.model_dump(mode="json") for k, v in state.shots.items()},
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
        result = await mission_runner.execute_mission(show_id=req.show_id or "show-dune")
        return result.model_dump(mode="json")
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@app.get("/api/interventions")
async def get_interventions():
    """Returns the history of interventions executed by Callsheet."""
    return [r.model_dump(mode="json") for r in dispatcher.list_history()]


# Mount UI HTML
UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Callsheet — Autonomous Operations Agent for Studio Crews</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #0d1117;
            --surface: #161b22;
            --border: #30363d;
            --text: #c9d1d9;
            --text-heading: #f0f6fc;
            --accent: #58a6ff;
            --accent-hover: #1f6feb;
            --danger: #f85149;
            --danger-bg: rgba(248, 81, 73, 0.15);
            --warning: #d29922;
            --warning-bg: rgba(210, 153, 34, 0.15);
            --success: #3fb950;
            --success-bg: rgba(63, 185, 80, 0.15);
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

        .container { max-width: 1200px; margin: 0 auto; }
        
        header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--border);
            margin-bottom: 24px;
        }

        .brand h1 {
            font-size: 24px;
            font-weight: 700;
            color: var(--text-heading);
            letter-spacing: -0.5px;
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .brand p {
            font-size: 14px;
            color: #8b949e;
            margin-top: 4px;
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 600;
        }

        .badge-live {
            background: var(--success-bg);
            color: var(--success);
            border: 1px solid rgba(63, 185, 80, 0.4);
        }

        .badge-pulse {
            width: 8px;
            height: 8px;
            background: var(--success);
            border-radius: 50%;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.4; transform: scale(1.2); }
            100% { opacity: 1; transform: scale(1); }
        }

        .grid {
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 24px;
            margin-bottom: 24px;
        }

        @media (max-width: 900px) {
            .grid { grid-template-columns: 1fr; }
        }

        .card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 20px;
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }

        .card-title {
            font-size: 16px;
            font-weight: 600;
            color: var(--text-heading);
        }

        /* Controls */
        .controls {
            display: flex;
            gap: 12px;
            margin-bottom: 24px;
            flex-wrap: wrap;
        }

        button {
            font-family: var(--font-main);
            font-size: 14px;
            font-weight: 600;
            padding: 10px 16px;
            border-radius: 6px;
            border: 1px solid transparent;
            cursor: pointer;
            transition: all 0.15s ease;
        }

        .btn-primary {
            background: #238636;
            color: #ffffff;
        }
        .btn-primary:hover { background: #2ea043; }

        .btn-danger {
            background: var(--danger-bg);
            color: var(--danger);
            border-color: rgba(248, 81, 73, 0.4);
        }
        .btn-danger:hover { background: rgba(248, 81, 73, 0.3); }

        .btn-secondary {
            background: #21262d;
            color: var(--text-heading);
            border-color: var(--border);
        }
        .btn-secondary:hover { background: #30363d; }

        /* Shows & Deadlines */
        .show-item {
            padding: 12px;
            background: #0d1117;
            border: 1px solid var(--border);
            border-radius: 6px;
            margin-bottom: 10px;
        }

        .show-header {
            display: flex;
            justify-content: space-between;
            font-weight: 600;
            color: var(--text-heading);
        }

        .show-meta {
            display: flex;
            justify-content: space-between;
            font-size: 13px;
            color: #8b949e;
            margin-top: 4px;
        }

        /* Node Farm Grid */
        .nodes-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(130px, 1fr));
            gap: 10px;
        }

        .node-box {
            background: #0d1117;
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 10px;
            text-align: center;
        }

        .node-name { font-size: 13px; font-weight: 600; color: var(--text-heading); }
        .node-temp { font-family: var(--font-mono); font-size: 13px; margin: 4px 0; }
        .node-shot { font-size: 11px; color: #8b949e; }

        .status-healthy { border-color: var(--success); }
        .status-throttled { border-color: var(--danger); background: var(--danger-bg); }
        .status-standby { border-color: #8b949e; opacity: 0.7; }

        /* Callsheet Report */
        .briefing-box {
            background: #090d13;
            border: 1px solid var(--border);
            border-left: 4px solid var(--accent);
            border-radius: 6px;
            padding: 20px;
            font-size: 14px;
            white-space: pre-wrap;
            font-family: var(--font-main);
        }

        .briefing-box h2, .briefing-box h3 {
            color: var(--text-heading);
            margin: 14px 0 8px;
            font-size: 16px;
        }

        .briefing-box table {
            width: 100%;
            border-collapse: collapse;
            margin: 12px 0;
            font-size: 13px;
        }

        .briefing-box th, .briefing-box td {
            border: 1px solid var(--border);
            padding: 8px 12px;
            text-align: left;
        }

        .briefing-box th {
            background: #161b22;
            color: var(--text-heading);
        }

        /* Reasoning Trail */
        .step-timeline {
            margin-top: 16px;
        }

        .step-card {
            border-left: 2px solid var(--accent);
            padding-left: 16px;
            margin-bottom: 16px;
            position: relative;
        }

        .step-card::before {
            content: '';
            position: absolute;
            left: -6px;
            top: 4px;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: var(--accent);
        }

        .step-title {
            font-weight: 600;
            color: var(--text-heading);
            font-size: 14px;
        }

        .step-desc {
            font-size: 13px;
            color: #8b949e;
            margin-top: 2px;
        }

        .step-code {
            font-family: var(--font-mono);
            font-size: 12px;
            background: #090d13;
            padding: 8px 12px;
            border-radius: 4px;
            margin-top: 6px;
            color: #58a6ff;
            word-break: break-all;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="brand">
                <h1>Callsheet</h1>
                <p>Autonomous Operations Agent for Post-Production Delivery Producers (Studio Crews)</p>
            </div>
            <div class="status-badge badge-live">
                <span class="badge-pulse"></span>
                <span>Grafana Cloud MCP Connected</span>
            </div>
        </header>

        <div class="controls">
            <button class="btn-primary" onclick="runInvestigation()">⚡ Investigate & Intervene</button>
            <button class="btn-danger" onclick="injectScenario('THERMAL_THROTTLING')">🔥 Inject Scenario: Node 07 Thermal Throttle</button>
            <button class="btn-secondary" onclick="injectScenario('BASELINE')">↺ Restore Baseline</button>
        </div>

        <div class="grid">
            <div class="card">
                <div class="card-header">
                    <span class="card-title">Production Callsheet Briefing</span>
                    <span id="briefing-time" style="font-size: 12px; color: #8b949e;">Waiting for agent mission...</span>
                </div>
                <div id="briefing-container" class="briefing-box">
Click <strong>"Investigate & Intervene"</strong> to trigger the multi-step reasoning agent. The agent will inspect Prometheus telemetry via Grafana Cloud MCP, correlate Loki logs and Tempo traces, execute automated shot reallocations to protect contractual delivery deadlines, and generate the producer summary here.
                </div>

                <div style="margin-top: 24px;">
                    <div class="card-title">Grafana MCP Reasoning Trail & Evidence Chain</div>
                    <div id="steps-container" class="step-timeline">
                        <div style="font-size: 13px; color: #8b949e; padding: 12px 0;">No active mission running. Intermediate Grafana telemetry evidence will populate here during execution.</div>
                    </div>
                </div>
            </div>

            <div>
                <div class="card" style="margin-bottom: 24px;">
                    <div class="card-header">
                        <span class="card-title">Shows & Delivery Deadlines</span>
                    </div>
                    <div id="shows-container">Loading shows...</div>
                </div>

                <div class="card">
                    <div class="card-header">
                        <span class="card-title">Render Farm Telemetry (12 Nodes)</span>
                    </div>
                    <div id="nodes-container" class="nodes-grid">Loading farm nodes...</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        async function fetchState() {
            try {
                const res = await fetch('/api/state');
                const data = await res.json();
                renderShows(data.shows);
                renderNodes(data.nodes);
            } catch (err) {
                console.error("State fetch error:", err);
            }
        }

        function renderShows(shows) {
            const container = document.getElementById('shows-container');
            container.innerHTML = Object.values(shows).map(s => `
                <div class="show-item">
                    <div class="show-header">
                        <span>${s.name}</span>
                        <span style="color: ${s.critical_path ? '#f85149' : '#58a6ff'}; font-size: 12px;">${s.critical_path ? 'CRITICAL PATH' : 'STANDARD'}</span>
                    </div>
                    <div class="show-meta">
                        <span>Client: ${s.client}</span>
                        <span>Penalty: £${s.penalty_daily_amount.toLocaleString()}/day</span>
                    </div>
                </div>
            `).join('');
        }

        function renderNodes(nodes) {
            const container = document.getElementById('nodes-container');
            container.innerHTML = Object.values(nodes).map(n => {
                let statusClass = 'status-healthy';
                if (n.status === 'THROTTLED') statusClass = 'status-throttled';
                if (n.is_standby) statusClass = 'status-standby';
                
                return `
                    <div class="node-box ${statusClass}">
                        <div class="node-name">${n.id}</div>
                        <div class="node-temp" style="color: ${n.temperature_celsius > 90 ? '#f85149' : '#3fb950'}">${n.temperature_celsius.toFixed(1)}°C</div>
                        <div class="node-shot">${n.current_shot_id ? 'Shot ' + n.current_shot_id.replace('sh_', '') : (n.is_standby ? 'Standby' : 'Idle')}</div>
                    </div>
                `;
            }).join('');
        }

        async function injectScenario(scenario) {
            await fetch('/api/scenario', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ scenario: scenario })
            });
            fetchState();
        }

        async function runInvestigation() {
            const briefingBox = document.getElementById('briefing-container');
            const stepsBox = document.getElementById('steps-container');
            const timeLabel = document.getElementById('briefing-time');

            briefingBox.innerHTML = '<em>Executing multi-step reasoning mission across Grafana Cloud MCP (Prometheus, Loki, Tempo) and Vertex AI Gemini...</em>';
            stepsBox.innerHTML = '<div style="color: #58a6ff;">Mission in progress: querying MCP tools...</div>';

            try {
                const res = await fetch('/api/mission', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ show_id: 'show-aethelgard' })
                });
                const result = await res.json();

                timeLabel.innerText = 'Generated ' + new Date(result.timestamp).toLocaleTimeString();
                briefingBox.innerHTML = result.callsheet_briefing
                    .replace(/\\*\\*(.*?)\\*\\*/g, '<strong>$1</strong>')
                    .replace(/\\*(.*?)\\*/g, '<em>$1</em>');

                // Render intermediate reasoning steps
                stepsBox.innerHTML = result.steps.map(step => `
                    <div class="step-card">
                        <div class="step-title">Step ${step.step_number}: ${step.name}</div>
                        <div class="step-desc">${step.description}</div>
                        ${step.evidence.tool ? `<div class="step-code">Tool: ${step.evidence.tool} | ${JSON.stringify(step.evidence.expr || step.evidence.logql || '')}</div>` : ''}
                    </div>
                `).join('');

                fetchState();
            } catch (err) {
                briefingBox.innerText = 'Error executing mission: ' + err;
            }
        }

        // Poll state every 4 seconds
        setInterval(fetchState, 4000);
        fetchState();
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def get_index():
    return UI_HTML
