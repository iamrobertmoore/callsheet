# Callsheet Video Demonstration Script

**Target Duration**: 2:40 total elapsed (Leaves a hard 20-second cushion before the 3:00 evaluation limit)  
**Spoken Word Count**: 316 words across 160 seconds (118.5 words/minute average; steady and deliberate)  
**Voice**: First-person singular, direct engineering tone, zero em dashes, zero prohibited buzzwords.  

---

### Production Timing and Cadence Budget

| Shot | Window | Elapsed | Spoken Words | Cadence | Focus |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | 0:00 - 0:18 | 18s | **41 words** | 136.7 wpm | Producer persona & penalty stakes |
| **2** | 0:18 - 0:45 | 27s | **53 words** | 117.8 wpm | Thermal throttling & schedule deficit |
| **3** | 0:45 - 1:15 | 30s | **59 words** | 118.0 wpm | Google ADK & MCP correlation |
| **4** | 1:15 - 1:55 | 40s | **71 words** | 106.5 wpm | Closed-loop telemetry verification |
| **5** | 1:55 - 2:20 | 25s | **48 words** | 115.2 wpm | Client briefing & public Grafana dashboard |
| **6** | 2:20 - 2:40 | 20s | **44 words** | 132.0 wpm | On-demand demo trigger & closing thesis |

---

### Production Recording Table

| Shot | On Screen | Spoken Words | Elapsed Time |
| :--- | :--- | :--- | :--- |
| **1** | Full view of live deployment at `https://callsheet-746874807798.us-central1.run.app`. Cursor hovers over **Chronicles of Aethelgard: Episode 6** card on the Active Delivery Slate. | "Elena Vance is the delivery producer at Cinefex Northern Pictures, finishing visual effects for episodic television. Her episode is due this afternoon, and missing deadline triggers a twenty-five thousand pound daily penalty. Elena is the user persona I designed Callsheet around." *(41 words)* | **0:00 - 0:18** (18s) |
| **2** | Pan across the Call Sheet masthead and the 12-node fleet grid. Node 07 is displayed in quarantined fault state; Node 11 is active running Shot 118. The slate card indicates `PROTECTED` status with positive buffer margin. | "At two in the morning, render node zero-seven throttles under heat. Standard monitoring alerts an infrastructure engineer with raw temperatures. But that engineer does not manage client schedules. Without intervention, Shot 118 finishes four hours late and breaches the deadline. Callsheet catches the fault, calculates the schedule deficit, and reallocates the workload automatically." *(53 words)* | **0:18 - 0:45** (27s) |
| **3** | Click to expand the **Grafana Cloud MCP Evidence Trail** accordion. Highlight Step 1 (Prometheus query), Step 2 (Loki log and Tempo trace correlation), and Step 3 (Gemini root-cause deduction). | "Callsheet is built natively on Google's Agent Development Kit. Using the ADK McpToolset, it connects to Grafana Cloud over the Model Context Protocol to query Prometheus metrics and correlate Loki logs. Vertex AI Gemini deduces the root cause, and deterministic Python arithmetic reallocates the shot to standby node eleven. Model output never triggers failovers; mathematical assertions govern every intervention." *(59 words)* | **0:45 - 1:15** (30s) |
| **4** | Scroll to and emphasize **Step 6: Post-Intervention Telemetry Verification (Grafana Cloud)**. Expand the JSON evidence block displaying retrieved frame duration (20s) and stabilized junction temperature (65.6°C). | "Here in step six, closed-loop verification anchors the architecture. Acting without asking is the harder engineering problem because taking action obliges you to prove the action worked. Callsheet does not declare victory on its own arithmetic. After failover, it queries Grafana Cloud for node eleven's actual telemetry. Only after confirming that render durations returned to twenty seconds per frame does it confirm protection. If verification fails, it escalates within the briefing." *(71 words)* | **1:15 - 1:55** (40s) |
| **5** | Highlight the **Production Callsheet Briefing** in Newsreader serif prose. Then click the top-right link **GRAFANA CONTROL TOWER ↗** to open the live public Grafana dashboard in a new tab, showing real-time temperature and frame duration panels. | "With delivery secured, Callsheet synthesizes a briefing that Elena can forward to her client, explaining the mitigation in clear commercial terms. In the Grafana Control Tower, all telemetry is public and independently auditable. Every metric sample, log record, and trace span is genuine OpenTelemetry emitted to Grafana Cloud." *(48 words)* | **1:55 - 2:20** (25s) |
| **6** | Open a new browser tab navigating to `/demo`. Click to trigger an on-demand thermal throttling injection. Return to dashboard to show live reactive failover motion and closing masthead. | "Judges can trigger incidents on demand using the slash demo endpoint. I built Callsheet so delivery producers never face unforced deadline penalties. Without Grafana, Callsheet would be a post-mortem tool that tells you why you missed the deadline after the money is already lost." *(44 words)* | **2:20 - 2:40** (20s) |

---

### Recording Notes
- **On-Demand Incidents**: `/demo` can be called anytime to trigger fresh thermal throttling incidents on demand for repeatable takes without waiting for simulator cycles.
- **Visual Hygiene**: No credentials, private tokens, or secret keys appear on screen. The Grafana Control Tower dashboard is public and requires no login.
- **Timing Discipline**: The recording ends at 2:40, leaving an absolute 20-second buffer before the 3:00 hard evaluation cutoff. Speak at an unhurried, measured pace (~118 words per minute).
