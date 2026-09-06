# Callsheet

Elena Vance is the delivery producer at Cinefex Northern Pictures, an independent visual effects house in Manchester. Her team finishes shots for episodic streaming television. Right now, Elena is responsible for delivering Chronicles of Aethelgard: Episode 6, facing a contractual delivery deadline later that afternoon. If her delivery slips past the contractual deadline, Cinefex incurs an immediate financial penalty of £25,000 per day. Elena is the user persona I designed Callsheet around rather than a real customer.

When a render blade degrades in the middle of the night, standard monitoring alerts an infrastructure engineer with hardware temperatures and fan speeds. That engineer is rarely equipped to evaluate shot dependencies, delivery buffers, or contractual SLA penalties. I built Callsheet for Elena and the studio crews who answer for delivery commitments.

Without Grafana, Callsheet would be a post-mortem tool that tells you why you missed the deadline after the money is already lost.

Callsheet monitors render operations through Grafana Cloud over the Model Context Protocol (MCP). A Grafana Cloud alerting rule serves as the trigger for autonomous action. When hardware degradation fires an alert, Callsheet investigates metrics, logs, and traces, assesses delivery margin risk, opens an incident in Grafana IRM at Step 4, and begins a dashboard annotation region. It isolates the degraded node and executes workload reallocation under blast-radius policy, holding for producer approval if pre-empting another show under Tier 2. It verifies recovery against live telemetry on the target blade with an automatic rollback path to a secondary standby node if verification fails. Once verified, Callsheet clears the alert, completes the annotation region, drafts an executive briefing for Elena, and marks the incident resolved last.

## Live Deployment & Dashboards

- Production Application: [https://callsheet-746874807798.us-central1.run.app](https://callsheet-746874807798.us-central1.run.app)
- Public Grafana Control Tower: [Callsheet Media Production Control Tower](https://bigforest2172.grafana.net/public-dashboards/a9028daf791643b8899a10531f6b31dd)
- Hackathon Walkthrough Guide: [JUDGING.md](JUDGING.md) (10-minute evaluation guide with 3 interactive demos)
- Operational Boundaries & Constraints: [LIMITATIONS.md](LIMITATIONS.md)

## Architecture

```
[ Synthetic Render Farm ]
          │ (OTel: Prometheus Metrics, Loki Logs, Tempo Traces via OTLP)
          ▼
   [ Grafana Cloud ]
          │ (Alert Rule cfxbt56wwbocge firing on 60s evaluation floor)
          ▼
   [ mcp-grafana Server ]
          │ (Model Context Protocol / Streamable HTTP)
          ▼
 [ Callsheet Operations Agent (Google ADK + Vertex AI Gemini) ]
          │
          ├─► Step 0: Alert Trigger (Grafana Cloud Alerting rule fires)
          ├─► Step 1: Anomaly Detection (Prometheus node metrics)
          ├─► Step 2: Telemetry Correlation (Loki logs & Tempo trace spans)
          ├─► Step 3: Root Cause Isolation (Gemini qualitative synthesis)
          ├─► Step 4: SLA Arithmetic & Incident Open (IRM incident declared)
          ├─► Step 5: Workload Reallocation & Annotation (quarantine blade)
          │         └─► Tier 2 hold if cross-show (producer approval required)
          ├─► Step 6: Closed-Loop Verification (Prometheus, Loki, Tempo)
          │         └─► Automatic rollback to node-12 if verification fails
          └─► Step 7: Annotation Region End, Briefing & Incident Resolved
```

Grafana Cloud evaluates this rule group once a minute on this stack regardless of the configured 10 second interval, so an alert arrives between 0 and 60 seconds after a fault and clears within a minute of quarantine.

## Google ADK Integration

Callsheet is built natively on the Google Agent Development Kit (ADK). It uses `google.adk.tools.mcp_tool.McpToolset` with `StreamableHTTPConnectionParams` to manage Model Context Protocol tool lifecycle, streaming HTTP connections, and dynamic schema binding directly to the Grafana Cloud MCP server.

### Model Context Protocol (MCP) Tool Integration

Callsheet queries telemetry and updates Grafana Cloud through these Model Context Protocol tools:

| MCP Tool | Access | Step | Cloud Resource / What It Touches |
| :--- | :--- | :--- | :--- |
| `list_datasources` | Read | Setup & Health Check | Discovers Prometheus, Loki, and Tempo datasources; verifies MCP connectivity |
| `query_prometheus` | Read | Step 1 & Step 6 | Prometheus time-series metrics (node temperatures, clock speeds, sample timestamps) |
| `query_loki_logs` | Read | Step 2 & Step 6 | Loki structured log stream (frame completion durations, thermal throttle events) |
| `grafana_api_request` | Read / Write | Step 2, Step 6, Step 7 | Tempo trace spans (`/api/search`, `/api/traces`), alert rule state, and IRM Twirp key updates |
| `alerting_manage_rules` | Read at step 0, Write at setup | Step 0 (and setup) | Grafana Alerting rules and evaluation state |
| `search_dashboards` | Read | Setup & Step 7 | Grafana production dashboard discovery and panel UIDs |
| `generate_deeplink` | Read | Step 6 | Grafana dashboard URL deeplinks for verification evidence and incident timeline |
| `create_incident` | Write | Step 4 | Grafana Incident Management (IRM) active incident declaration |
| `add_activity_to_incident` | Write | Step 4, Step 5, Step 7 | Grafana IRM timeline notes, hold records, and resolution updates |
| `update_incident` | Write | Step 7 | Grafana IRM incident status (Resolved) and title |
| `create_annotation` | Write | Step 5 | Grafana dashboard annotation start point marking intervention start |
| `update_annotation` | Write | Step 7 | Grafana dashboard annotation end point completing remediation region |
| `get_panel_image` | Read | Step 7 | Grafana dashboard panel PNG rendering for executive briefing |

## Blast Radius and Authority Policy

Callsheet enforces strict operational authority boundaries classified into three tiers:

- **Tier 1 (Autonomous Execution)**: Moving a shot onto an idle standby node; quarantining a node that has breached its own thermal limit. Reversible, touches no other show, and introduces no producer-visible deadline change.
- **Tier 2 (Producer Approval Required)**: Pre-empting a node currently rendering another show's shot; anything touching more than one show; anything changing a contractual delivery commitment. Holds execution and presents real-time buffer loss arithmetic for human sign-off via `POST /api/approvals/{id}`.
- **Tier 3 (Prohibited)**: Actions outside the farm boundary. No external cloud capacity, no vendor escalation calls, and no financial spend.

## Deterministic Action vs. Generative Explanation

A non-negotiable architectural principle in Callsheet is the boundary between deterministic operational decisions and generative language synthesis:

- Generative models cannot trigger, alter, or approve any operational intervention.
- Interventions are 100% deterministic: Thermal limits (Step 1) and delivery buffer calculations (Step 4) are evaluated purely with mathematical arithmetic in Python. The workload failover (Step 5) is executed only when code assertions confirm a negative buffer margin and a thermal limit breach.
- Verification closes the loop: Acting without asking is the harder engineering problem because taking action obliges the agent to prove the action worked. In Step 6, Callsheet re-queries Grafana Cloud telemetry on the standby blade to independently verify recovery against strict numerical criteria:
  - Retrieved frame duration must be at or below 1.25 times the stated baseline (<= 25.0s for a 20.0s baseline).
  - Node temperature must be below 90.0°C.
  - Telemetry samples must be timestamped after the intervention.
  - Loki log evidence and Prometheus metric samples are required.
  - Tempo trace verification is accepted when it arrives within a further 30 seconds.
  - The verification polling window is bounded at 150 seconds.
  - If nothing arrives within the window, the verification status is marked inconclusive and escalated.
  - No synthesised or guessed values are permitted; every number must be retrieved from Grafana Cloud over MCP.
  If metrics fail verification criteria, Callsheet executes an automatic rollback to secondary standby node-12 or escalates with full diagnostics for technical directors.
- Generative AI is strictly explanatory: Vertex AI Gemini 3.8 Flash is employed exclusively for qualitative synthesis: Step 3 (deducing root causes from correlated logs and traces) and Step 7 (drafting plain-language correspondence briefings for delivery producers). If Gemini returns nothing, the mission stops and reports; it does not guess.

## Decision Path Integrity and the Rip-Out Test

Callsheet enforces strict telemetry boundary isolation across its entire decision path:

- Telemetry vs. Production Metadata: Every telemetry value that drives a decision (temperatures, frame durations, log lines, spans, alert state) comes from Grafana through MCP; production metadata (which shot is on which node, deadlines, frame counts) comes from the farm scheduler, which in a studio would be the render queue manager. The rip-out test covers the telemetry path.
- The Rip-Out Test: The proof of this isolation is that pointing Callsheet at an unreachable or severed Grafana endpoint causes the mission to fail immediately. The agent contains no mock fallbacks, local memory shortcuts, or side-channel cheats. This failure invariant is asserted in the automated test suite: `tests/test_agent_mission.py::test_mission_fails_when_grafana_unreachable`.
- What the Farm Is: There is no actual render farm behind this; the machines are simulated. But the metrics, logs, and trace spans they emit are genuine OpenTelemetry sent to a real Grafana Cloud stack via an OTLP gateway, and the agent reads them back the same way it would read real hardware. Callsheet queries that telemetry through MCP exactly as it would against physical on-premise blade servers, cloud instances, or a render queue manager.
- Transition to Physical Infrastructure: The telemetry emitter and the scheduler adapter change, the agent does not.
- Stated Baseline Render Rate: The baseline render rate (20.0 seconds per frame) is an explicit stated parameter of the simulator representing nominal throughput, rather than a wall-clock measurement subject to vCPU jitter. Downstream contractual buffer arithmetic is derived deterministically from this stated baseline.
- Telemetry Stream Isolation: Every emitter stamps deployment_id (cloud-run in production, local-<hostname> elsewhere) across metrics, logs, and spans, ensuring agent queries strictly isolate their own streams and prevent test runs from polluting production.

## Local Setup and Verification

### Prerequisites
- Python 3.12 or higher
- Git
- Google Cloud project with Vertex AI enabled
- Grafana Cloud stack with Prometheus, Loki, and Tempo access

### Installation
```bash
git clone https://github.com/iamrobertmoore/callsheet.git
cd callsheet
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### Environment Configuration
Create a `.env` file in the repository root:
```env
GRAFANA_URL=https://<your-instance>.grafana.net
GRAFANA_SERVICE_ACCOUNT_TOKEN=<your-token>
OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp-gateway-<region>.grafana.net/otlp
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Basic <base64-credentials>
GOOGLE_CLOUD_PROJECT=<your-project-id>
GOOGLE_CLOUD_LOCATION=global
```

### Running Tests
Execute the test suite to verify the simulation, MCP round trip, and failover mechanics:
```bash
pytest -v
```

To run the rip-out test confirming that decisions depend entirely on Grafana Cloud:
```bash
pytest tests/test_agent_mission.py::test_mission_fails_when_grafana_unreachable -v
```

### Running the Web Application
```bash
./scripts/start_server.sh
```
Open `http://localhost:8080` in your browser to view the active Call Sheet dashboard, or visit `/demo` to inject scenarios on demand.

## Telemetry Verification and Health Endpoint

The `/api/health` endpoint proves live operational state in a single request with zero secrets:
- `status`: Overall service health (`"healthy"` or `"degraded"`)
- `service`: Service name (`"callsheet"`)
- `timestamp`: Current UTC timestamp in ISO format
- `agent_runtime`: `"live"` (live agent loop, not a playback)
- `replay_mode`: `false` (all actions run against live backends)
- `mcp_server`: `"grafana/mcp-grafana v1.2.0, self-hosted sidecar"`
- `mcp_transport`: `"streamable-http"`
- `mcp_reachable`: Evaluated live at request time via `list_datasources` tool probe
- `grafana_stack`: `"bigforest2172"`
- `alert_rule_uid`: `"cfxbt56wwbocge"` (registered alert rule in Grafana Cloud)
- `alert_rule_state`: Current evaluation state in Grafana Alerting (lowercase `"normal"` or `"firing"`)
- `alert_rule_interval_configured`: `"10s"`
- `alert_rule_interval_observed`: `"60s"` (Grafana Cloud scheduler floor)
- `model`: `"gemini-3.8-flash"`
- `model_location`: `"global"`
- `last_mission_at`: UTC timestamp of the most recent mission completion, or `null`
- `last_mission_trigger`: Trigger source of the latest mission (`"grafana_alert"` or `"api"`, or `null`)
- `last_verification_status`: Status of the last verification attempt (`"VERIFIED_PROTECTED"`, `"PENDING_APPROVAL"`, `"ESCALATED"`, `"VERIFICATION_INCONCLUSIVE"`, or `null`)
- `cycle_epoch`: Integer 6-hour cycle epoch index (e.g. `82809`)
- `next_reset_at_utc`: Next scheduled cycle reset timestamp in ISO format
- `pending_approvals`: Current list of Tier 2 actions held for producer review
- `worker_running`: Background loop execution status (`true` / `false`)
- `instance_started_at`: UTC timestamp when this Cloud Run container instance booted
- `missions_this_instance`: Total missions completed by this container instance
- `deployment_id`: Emitter telemetry partition tag (`"cloud-run"`)
- `alert_rule_status`: Alert rule polling status (`"ok"`)
- `alert_rule_error`: Alert polling error description if any, or `null`
- `scenario_primed_by`: Origin of active scenario baseline (`"instance_start"`, `"cycle_boundary"`, `"demo"`, or `"api"`)
- `tick_cadence`: Live statistics for the 5-second watchdog loop ticks and latency
- `watchdog_stalled`: Boolean flag indicating if an alert has been unserviced for >180 seconds
- `watchdog_stalled_since`: UTC timestamp when the stall condition was first flagged, or `null`

See [LIMITATIONS.md](LIMITATIONS.md) for full operational constraints, simulator architecture details, and Gemini boundaries.

## Technologies Used

- **Google Agent Development Kit (ADK)**: Built natively on the ADK using `google.adk.tools.mcp_tool.McpToolset` and `StreamableHTTPConnectionParams` to manage Model Context Protocol tool lifecycle and streaming HTTP connections to Grafana Cloud.
- **Vertex AI Gemini**: Gemini 3.8 Flash (`google-genai`) accessed via the global endpoint (`location="global"`), avoiding regional endpoint 404 errors observed during development.
- **Grafana Stack**: Grafana Cloud, `grafana/mcp-grafana` MCP Server, Prometheus metrics, Loki logs, and OpenTelemetry ingestion (Tempo traces).
- **Google Cloud Platform**: Cloud Run deployed with instance-based billing and dedicated CPU allocation (addressing request-based billing constraints where "CPU is only allocated during request processing" and idle instances can shut down at any time) and Cloud Build.
- **Python Runtime**: Python 3.12, FastAPI, asyncio background workers, and OpenTelemetry instrumentation SDKs.

## License

This project is licensed under the Apache 2.0 License. See [LICENSE](LICENSE) for details.
