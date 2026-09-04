# Callsheet

Elena Vance is the delivery producer at Cinefex Northern Pictures, an independent visual effects house in Manchester. Her team finishes shots for episodic streaming television. Right now, Elena is responsible for delivering Chronicles of Aethelgard: Episode 6, facing a contractual delivery deadline later that afternoon. If her delivery slips past the contractual deadline, Cinefex incurs an immediate financial penalty of £25,000 per day. Elena is the user persona I designed Callsheet around rather than a real customer.

When a render blade degrades in the middle of the night, standard monitoring alerts an infrastructure engineer with hardware temperatures and fan speeds. That engineer is rarely equipped to evaluate shot dependencies, delivery buffers, or contractual SLA penalties. I built Callsheet for Elena and the studio crews who answer for delivery commitments.

Without Grafana, Callsheet would be a post-mortem tool that tells you why you missed the deadline after the money is already lost.

Callsheet monitors render operations through Grafana Cloud over the Model Context Protocol (MCP). When hardware degradation threatens an episode delivery, Callsheet acts autonomously. It calculates the delivery margin deficit, isolates the throttled node, reallocates the at-risk shot to an idle standby blade, and interrogates the target node's live telemetry to confirm that render throughput recovered. It then writes a plain-language production briefing that Elena can forward directly to her client.

## Live Deployment & Dashboards

- Production Application: [https://callsheet-746874807798.us-central1.run.app](https://callsheet-746874807798.us-central1.run.app)
- Public Grafana Control Tower: [Callsheet Media Production Control Tower](https://bigforest2172.grafana.net/public-dashboards/a9028daf791643b8899a10531f6b31dd)

## Architecture

```
[ Synthetic Render Farm ]
          │ (OTel: Prometheus Metrics, Loki Logs, Tempo Traces via OTLP)
          ▼
   [ Grafana Cloud ]
          │
          │ (Streamable HTTP / Service Account Token)
          ▼
   [ mcp-grafana Server ]
          │
          │ (Model Context Protocol)
          ▼
 [ Callsheet Operations Agent (Google ADK + Vertex AI Gemini 3.6 Flash) ]
          │
          ├─► Step 1: Anomaly Detection (Prometheus)                 [DETERMINISTIC TELEMETRY]
          ├─► Step 2: Correlation (Loki Logs & Tempo Traces)         [DETERMINISTIC TELEMETRY]
          ├─► Step 3: Root Cause Isolation (Gemini 3.6 Flash)        [GENERATIVE AI]
          ├─► Step 4: Production Impact Mapping                      [DETERMINISTIC ARITHMETIC]
          ├─► Step 5: Autonomous Workload Reallocation               [DETERMINISTIC ACTION]
          ├─► Step 6: Post-Intervention Verification (Grafana Cloud) [DETERMINISTIC VERIFICATION]
          └─► Step 7: Producer Callsheet Briefing (Gemini 3.6 Flash) [GENERATIVE AI]
```

## Architecture and Native Google ADK Integration

Callsheet is built natively on the Google Agent Development Kit (ADK). It uses `google.adk.tools.mcp_tool.McpToolset` with `StreamableHTTPConnectionParams` to manage Model Context Protocol tool lifecycle, streaming HTTP connections, and dynamic schema binding directly to the Grafana Cloud MCP server.

## Deterministic Action vs. Generative Explanation

A non-negotiable architectural principle in Callsheet is the boundary between deterministic operational decisions and generative language synthesis:

- Generative models cannot trigger, alter, or approve any operational intervention.
- Interventions are 100% deterministic: Thermal limits (Step 1) and delivery buffer calculations (Step 4) are evaluated purely with mathematical arithmetic in Python. The workload failover (Step 5) is executed only when code assertions confirm a negative buffer margin and a thermal limit breach.
- Verification closes the loop: Acting without asking is the harder engineering problem because taking action obliges the agent to prove the action worked. In Step 6, Callsheet re-queries Grafana Cloud telemetry on the standby blade to independently verify nominal frame durations (20 seconds per frame) and stable junction temperatures. If metrics remain degraded, Callsheet disallows the `PROTECTED` status, records the verified fault, and escalates within the producer briefing with diagnostics for technical directors.
- Generative AI is strictly explanatory: Vertex AI Gemini 3.6 Flash is employed exclusively for qualitative synthesis: Step 3 (deducing root causes from correlated logs and traces) and Step 7 (drafting plain-language correspondence briefings for delivery producers).

## Decision Path Integrity and the Rip-Out Test

Callsheet enforces strict telemetry boundary isolation across its entire decision path:

- No number reaches a decision without a round trip through Grafana Cloud: The Callsheet agent never reads the simulator's internal memory or local state. Every metric sample, log record, and trace duration that drives an intervention decision is retrieved dynamically from Grafana Cloud over the Model Context Protocol.
- The Rip-Out Test: The proof of this isolation is that pointing Callsheet at an unreachable or severed Grafana endpoint causes the mission to fail immediately. The agent contains no mock fallbacks, local memory shortcuts, or side-channel cheats. This failure invariant is asserted in the automated test suite: `tests/test_agent_mission.py::test_mission_fails_when_grafana_unreachable`.
- What the Farm Is: There is no actual render farm behind this; the machines are simulated. But the metrics, logs, and trace spans they emit are genuine OpenTelemetry sent to a real Grafana Cloud stack via an OTLP gateway, and the agent reads them back the same way it would read real hardware. Callsheet queries that telemetry through MCP exactly as it would against physical on-premise blade servers, cloud instances, or commercial render farm managers.
- Transition to Physical Infrastructure: To connect Callsheet to physical studio hardware, only the telemetry emitter changes. The agent reasoning loop, MCP tool bindings, deterministic gates, and briefing pipelines remain identical.
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

## Technologies Used

- **Google Agent Development Kit (ADK)**: Built natively on the ADK using `google.adk.tools.mcp_tool.McpToolset` and `StreamableHTTPConnectionParams` to manage Model Context Protocol tool lifecycle and streaming HTTP connections to Grafana Cloud.
- **Vertex AI Gemini**: Gemini 3.6 Flash (`google-genai`) accessed via the global endpoint (`location="global"`), avoiding regional endpoint 404 errors observed during development.
- **Grafana Stack**: Grafana Cloud, `grafana/mcp-grafana` MCP Server, Prometheus metrics, Loki logs, and OpenTelemetry ingestion (Tempo traces).
- **Google Cloud Platform**: Cloud Run deployed with instance-based billing and dedicated CPU allocation (addressing request-based billing constraints where "CPU is only allocated during request processing" and idle instances can shut down at any time) and Cloud Build.
- **Python Runtime**: Python 3.12, FastAPI, asyncio background workers, and OpenTelemetry instrumentation SDKs.

## License

This project is licensed under the Apache 2.0 License. See [LICENSE](LICENSE) for details.

