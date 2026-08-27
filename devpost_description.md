# Callsheet: Autonomous Operations Agent for Post-Production Delivery Producers

### Testing Instructions (for Devpost form field, 239 chars / 255 max):
Visit live service: https://callsheet-746874807798.us-central1.run.app. Trigger incident tests via /demo. Audit telemetry on public Grafana Control Tower: https://bigforest2172.grafana.net/public-dashboards/a9028daf791643b8899a10531f6b31dd

---

## What it does

Elena Vance is the delivery producer at Cinefex Northern Pictures, an independent visual effects house in Manchester. Her team delivers final visual effects shots for episodic streaming television. Right now, Elena is managing the delivery of Chronicles of Aethelgard: Episode 6, facing a contractual delivery deadline later that afternoon. If her team misses that delivery, the client contract triggers an immediate penalty clause of £25,000 per day. Elena is the user persona I designed Callsheet around rather than a real customer.

When a render blade degrades at two in the morning, conventional observability systems alert the infrastructure engineer with raw metrics like junction temperatures and clock frequencies. That engineer rarely understands shot dependencies, client commitments, or financial penalty clauses. I built Callsheet for Elena and the studio crews who answer for delivery dates.

Callsheet watches the render farm through Grafana Cloud over the Model Context Protocol. When an infrastructure fault threatens a deadline, Callsheet takes direct operational responsibility for protecting the schedule. It calculates the delivery buffer deficit, quarantines the degraded node, shifts the at-risk shot to an idle standby blade, and then interrogates the target node's live telemetry to confirm that render throughput recovered. Once recovery is verified, Callsheet generates a concise production briefing that Elena can forward directly to her client.

## Features and functionality

- Autonomous Workload Intervention: When hardware throttling creates a negative delivery buffer margin, Callsheet executes an immediate shot failover from the degraded node to a standby blade. Acting autonomously is the harder engineering commitment because taking action without asking obliges the agent to prove the action worked.
- Telemetry Correlation Across Three Pillars: Callsheet queries Prometheus metrics to detect thermal spikes, correlates Loki logs to isolate hardware down-clocking events, and inspects Tempo distributed traces to verify frame completion times, all through Grafana Cloud MCP.
- Post-Intervention Verification Loop: Callsheet never assumes an intervention worked based on arithmetic alone. Step 6 queries Grafana Cloud for the target node's actual telemetry after reallocation. It confirms that frame durations returned to the nominal baseline of 20 seconds per frame and that junction temperatures remain stable before issuing a PROTECTED status. If the telemetry fails to show recovery, Callsheet refrains from claiming success and escalates within the producer briefing with full diagnostics for technical directors.
- Strict Separation of Deterministic Arithmetic and Generative AI: Calculations that govern financial exposure, deadline buffers, and workload failover are handled strictly by deterministic Python logic. Generative AI is restricted to qualitative root-cause deduction from logs and drafting client correspondence. Code assertions guarantee that model output cannot trigger or override an operational failover.
- Call Sheet Production Dashboard: The user interface is modeled after physical call sheets and finishing-suite ergonomics. It provides high contrast, tabular telemetry, ruled paperwork dividers, and a readable editorial briefing for studio producers.
- Public Auditability via Grafana Control Tower: Anyone can audit the live fleet metrics on a public Grafana Cloud dashboard without authentication.

## Technologies used

- Google Agent Development Kit (ADK): Built natively on the ADK using google.adk.tools.mcp_tool.McpToolset and StreamableHTTPConnectionParams to manage the lifecycle, authentication headers, and tool invocation contracts for the streamable HTTP connection to the grafana/mcp-grafana server.
- Vertex AI Gemini: Gemini 3.6 Flash accessed via the Google GenAI SDK and Vertex AI global endpoint for log synthesis and correspondence generation.
- Grafana Cloud: Prometheus for hardware metrics, Loki for application and system logs, Tempo for distributed frame traces.
- Model Context Protocol (MCP): The official grafana/mcp-grafana server running in streamable HTTP mode, enabling dynamic tool calls against Grafana Cloud.
- Google Cloud Run: Production hosting for the autonomous operations service, configured with instance-based billing and dedicated CPU allocation.
- Google Cloud Build & Artifact Registry: Automated container packaging and deployment pipelines.
- Python 3.12: FastAPI application runtime, asyncio background workers, and OpenTelemetry instrumentation SDKs.

## Other data sources used

- Production Scheduling Metadata: Active show registries including client identifiers, contractual delivery deadlines, critical path priorities, and daily penalty amounts.
- Render Shot State Records: Shot identifiers, frame ranges, remaining frame counts, and baseline completion estimates.

## Findings and learnings

### What the render farm is and is not
There is no actual render farm behind this; the machines are simulated. But the metrics, logs, and trace spans they emit are genuine OpenTelemetry sent to a real Grafana Cloud stack via an OTLP gateway, and the agent reads them back the same way it would read real hardware. Callsheet queries that telemetry through the Grafana MCP server exactly as it would query physical render blades, cloud instances, or commercial render farm managers. To transition Callsheet to a physical studio environment, only the OTel telemetry emitter changes. The agent, MCP tool calls, reasoning loop, failover dispatcher, and briefing engine remain untouched.

### The Vertex AI regional endpoint observation
During initial deployment, Gemini 3.x model calls failed with HTTP 404 Not Found errors on the us-central1 regional endpoint in my project. A not-found error reads like missing API access or project permissions rather than wrong routing. Moving the client configuration to location="global" resolved the issue immediately, with Gemini 3.6 Flash responding as expected. Diagnosing that routing mismatch cost me an entire iteration cycle. I kept this observation in my notes because regional endpoint behavior can mislead anyone debugging new Gemini model deployments on Vertex AI.

### The Cloud Run CPU allocation and lifecycle trap
When deploying the autonomous background loop to Google Cloud Run, configuring min-instances=1 was insufficient to keep the monitor running. In Cloud Run's documentation for request-based billing, "CPU is only allocated during request processing." Furthermore, Google's documentation explicitly warns that "Idle instances, including those kept warm using minimum instances, can be shut down at any time." Because Callsheet's autonomous agent evaluates the render farm in a background asyncio loop between inbound HTTP user visits, the instance lost CPU allocation and background tasks stalled. This failure was invisible during local development because active testing requests kept the container warm. Switching the Cloud Run service to instance-based billing with CPU always allocated resolved both issues, guaranteeing that background monitoring and closed-loop evaluations execute continuously.

### Grafana Cloud ingestion boundaries
Grafana Cloud enforces strict ingestion boundaries for out-of-order data. For Prometheus metrics, samples cannot arrive behind the newest sample already ingested for that series by more than the configured out-of-order window (typically up to two hours). For Loki logs, entries cannot be older than the stream's current position by more than the out-of-order ceiling (typically one hour), bounded by the tenant's maximum rejection window. Any historical telemetry emitted outside these windows relative to the stream head is rejected. This constraint ruled out backfilling synthetic historical incidents offline. Every incident, thermal spike, and recovery curve had to run against live wall-clock UTC in real time, synchronized with the simulator's tick loop.

### What Callsheet cannot do
Callsheet cannot physically repair failed cooling hardware or blown chassis fans. It infers thermal throttling from temperature metrics, clock rate drops, and frame duration spikes, not from acoustic or vibration sensors. Callsheet assumes that standby nodes already have shared storage volumes mounted and visual assets accessible. If a studio lacks hot standby capacity or network storage is disconnected, Callsheet cannot manufacture compute out of thin air. Finally, Callsheet cannot negotiate contractual delivery extensions with streaming executives; its sole role is to alert the producer and protect the schedule before deadlines lapse.
