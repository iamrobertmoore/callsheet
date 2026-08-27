# Callsheet: Autonomous Operations Agent for Post-Production Delivery Producers

### Testing Instructions (for Devpost form field, 239 chars / 255 max):
Visit live service: https://callsheet-746874807798.us-central1.run.app. Trigger incident tests via /demo. Audit telemetry on public Grafana Control Tower: https://bigforest2172.grafana.net/public-dashboards/a9028daf791643b8899a10531f6b31dd

---

## What it does

Elena Vance represents the delivery producer persona I designed Callsheet around: she works at an independent visual effects house like Cinefex Northern Pictures in Manchester. Her team delivers final visual effects shots for episodic streaming television. Right now, Elena is managing the delivery of Chronicles of Aethelgard: Episode 6, facing a contractual delivery deadline later that afternoon. If her team misses that delivery, the studio contract triggers an immediate penalty clause of £25,000 per day.

When a render blade degrades at two in the morning, conventional observability systems alert the infrastructure engineer with raw metrics like junction temperatures and clock frequencies. That engineer rarely understands shot dependencies, client commitments, or financial penalty clauses. I built Callsheet for Elena and the studio crews who answer for delivery dates.

Callsheet watches the render farm through Grafana Cloud over the Model Context Protocol. When an infrastructure fault threatens a deadline, Callsheet does not merely flag the problem or wait for a sleepy operator to approve a fix. It calculates the delivery buffer deficit, quarantines the degraded node, shifts the at-risk shot to an idle standby blade, and then interrogates the target node's live telemetry to confirm that render throughput recovered. Once recovery is verified, Callsheet generates a concise production briefing that Elena can forward directly to her client.

## Features and functionality

- Autonomous Workload Intervention: When hardware throttling creates a negative delivery buffer margin, Callsheet executes an immediate shot failover from the degraded node to a standby blade. Unlike competing tools that merely propose recommendations and require human approval, Callsheet acts autonomously to protect the delivery date.
- Telemetry Correlation Across Three Pillars: Callsheet queries Prometheus metrics to detect thermal spikes, correlates Loki logs to isolate hardware down-clocking events, and inspects Tempo distributed traces to verify frame completion times, all through Grafana Cloud MCP.
- Post-Intervention Verification Loop: Callsheet never assumes an intervention worked based on arithmetic alone. Step 6 queries Grafana Cloud for the target node's actual telemetry after reallocation. It confirms that frame durations returned to the nominal baseline of 20 seconds per frame and that junction temperatures remain stable before issuing a PROTECTED status. If the telemetry fails to show recovery, Callsheet refrains from claiming success and escalates the ticket directly to technical directors.
- Strict Separation of Deterministic Arithmetic and Generative AI: Calculations that govern financial exposure, deadline buffers, and workload failover are handled strictly by deterministic Python logic. Generative AI is restricted to qualitative root-cause deduction from logs and drafting client correspondence. Code assertions guarantee that model output cannot trigger or override an operational failover.
- Call Sheet Production Dashboard: The user interface is modeled after physical call sheets and finishing-suite ergonomics. It provides high contrast, tabular telemetry, ruled paperwork dividers, and a readable editorial briefing for studio producers.
- Public Auditability via Grafana Control Tower: Anyone can audit the live fleet metrics on a public Grafana Cloud dashboard without authentication.

## Technologies used

- Grafana Cloud: Prometheus for hardware metrics, Loki for application and system logs, Tempo for distributed frame traces.
- Model Context Protocol (MCP): The official grafana/mcp-grafana server running in streamable HTTP mode, enabling dynamic tool calls against Grafana Cloud.
- Google Cloud Run: Production hosting for the autonomous operations service, configured with instance-based billing and dedicated CPU allocation.
- Google Cloud Build & Artifact Registry: Automated container packaging and deployment pipelines.
- Vertex AI Gemini: Gemini 3.6 Flash accessed via the Google GenAI SDK and Vertex AI global endpoint for log synthesis and correspondence generation.
- Python 3.12: FastAPI application runtime, asyncio background workers, and OpenTelemetry instrumentation SDKs.

## Other data sources used

- Production Scheduling Metadata: Active show registries including client identifiers, contractual delivery deadlines, critical path priorities, and daily penalty amounts.
- Render Shot State Records: Shot identifiers, frame ranges, remaining frame counts, and baseline completion estimates.

## Findings and learnings

### What the render farm is and is not
I built an operational simulator that emits genuine OpenTelemetry metrics, structured logs, and distributed trace spans to Grafana Cloud via an OTLP gateway. It is not physical silicon, but the telemetry it produces is completely real. Callsheet reads all telemetry back through the Grafana MCP server exactly as it would read physical render blades, AWS Deadline workers, or Pixar Tractor nodes. To transition Callsheet to a physical studio environment, only the OTel emitter changes. The agent, MCP tool calls, reasoning loop, failover dispatcher, and briefing engine remain untouched.

### The Vertex AI global endpoint discovery
During initial deployment, Gemini model calls failed with HTTP 404 Not Found errors on regional Vertex AI endpoints like us-central1. The error message resembled complete service unavailability or missing permissions. I discovered that Gemini 3.x models on Vertex AI require routing through the global endpoint (location="global"). Diagnosing this saved the project from stalled development and represents an important architectural detail for anyone deploying new Gemini models on Google Cloud.

### The Cloud Run CPU allocation trap
When deploying the autonomous background loop to Google Cloud Run, setting min-instances=1 was insufficient. In standard request-based billing, Cloud Run throttles CPU allocation as soon as an inbound HTTP request finishes processing. Because the autonomous monitoring worker runs in an asyncio task loop between external user visits, Cloud Run throttled its CPU to near zero, freezing the background evaluation loop. This defect was completely invisible during local testing because active development requests kept the instance alive. Switching the Cloud Run service to instance-based billing with CPU always allocated resolved the issue and allowed background monitoring to proceed continuously.

### Grafana Cloud ingestion boundaries
Grafana Cloud enforces strict ingestion time windows: two hours for Prometheus metrics and one hour for Loki logs. Any telemetry emitted with timestamps outside these windows is rejected immediately. This constraint eliminated any possibility of backfilling synthetic historical incidents. Every incident, metric spike, and recovery curve had to run in real time, requiring precise synchronization between the simulator clock and live UTC timestamps.

### What Callsheet cannot do
Callsheet cannot physically repair failed cooling hardware or blown chassis fans. It infers thermal throttling from temperature metrics, clock rate drops, and frame duration spikes, not from acoustic or vibration sensors. Callsheet assumes that standby nodes already have shared storage volumes mounted and visual assets accessible. If a studio lacks hot standby capacity or network storage is disconnected, Callsheet cannot manufacture compute out of thin air. Finally, Callsheet cannot negotiate contractual delivery extensions with streaming executives; its sole role is to alert the producer and protect the schedule before deadlines lapse.
