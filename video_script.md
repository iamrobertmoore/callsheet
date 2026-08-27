# Callsheet Video Demonstration Script

**Target Duration**: 2:40 total elapsed (Leaves a hard 20-second cushion before the 3:00 evaluation limit)  
**Spoken Word Count**: 316 words across 160 seconds (118.5 words/minute average; steady and deliberate)  
**Voice**: First-person singular, direct engineering tone, zero em dashes, zero prohibited buzzwords.  

---

## Before You Hit Record

### 1. Browser Setup for 1080p Capture
- **Display Resolution**: 1920x1080 (16:9 full screen or browser window sized exactly to 1080p).
- **Browser Cleanliness**: Hide the bookmarks bar (`Cmd+Shift+B` on macOS).
- **Extension Icons**: Hide all extension icons or use a clean guest profile.
- **System Notifications**: Turn macOS "Do Not Disturb" Focus mode ON to suppress banners and sound alerts.
- **Clutter**: Hide the macOS dock and close all background application windows.

### 2. Pre-Loaded Browser Tabs (In Exact Order)
Open exactly three tabs in a single browser window before starting:
- **Tab 1 (Foreground at 0:00)**: `https://callsheet-746874807798.us-central1.run.app/`  
  *Producer Dashboard*. Must be in the resolved state showing `VERIFIED_PROTECTED` on Chronicles of Aethelgard. Node-07 shows in quarantined fault status; Node-11 shows active running Shot 118. The **Grafana Cloud MCP Evidence Trail** accordion must be **collapsed** before recording begins.
- **Tab 2**: `https://bigforest2172.grafana.net/public-dashboards/a9028daf791643b8899a10531f6b31dd`  
  *Grafana Control Tower*. Pre-loaded with time range set to **Last 30 minutes** (top right) so the thermal spike curve and nominal frame durations are already drawn. This ensures Shot 5 is an instantaneous tab switch with zero network loading latency.
- **Tab 3**: `https://callsheet-746874807798.us-central1.run.app/demo`  
  *Demo Harness*. Pre-loaded with the scenario buttons and live activity log box visible.

### 3. Measured Incident Timing and Pre-Arming Procedure
I measured the autonomous incident lifecycle on the deployed Cloud Run service:
- **Measured Duration**: **37.3 seconds** from button press to completed dashboard update.
- **Timing Breakdown**:
  - 0.0s to 5.0s: Simulator tick advances and node-07 temperature exceeds 90.0°C.
  - 5.0s to 13.0s: Watchdog flags the anomaly and waits 8.0 seconds for Grafana Cloud telemetry ingestion and index flush.
  - 13.0s to 37.3s (24.3s): Multi-step agent mission executes against Grafana Cloud MCP (Prometheus metric query, Loki log query, Tempo trace query, Gemini 3.6 Flash root-cause deduction, deterministic buffer arithmetic, failover dispatch, Step 6 post-intervention telemetry verification query, and Gemini 3.6 Flash briefing synthesis).
  - 37.3s: Dashboard state updates atomically to `VERIFIED_PROTECTED`.

**Implication for Recording**: Because the full loop takes 37 seconds, you **cannot** trigger a fresh incident live in Shot 6 and wait for it to resolve within the 20-second window. The dashboard on Tab 1 must be **pre-armed before recording**:
1. Open Tab 3 (`/demo`) and click **Inject Scenario: Node-07 Thermal Throttling**.
2. Wait **40 seconds** until Tab 1 updates to `VERIFIED_PROTECTED`.
3. Verify the evidence accordion on Tab 1 is **collapsed**.
4. Switch to Tab 1 as your active foreground tab and start recording.

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

| Shot | Tab & Physical Action | On Screen | Spoken Words | Elapsed Time |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Tab 1 (Foreground at start). Rest cursor over the Chronicles of Aethelgard: Episode 6 card on the Active Delivery Slate.** | Full view of live deployment at `callsheet-746874807798.us-central1.run.app`. Cursor hovers over Chronicles of Aethelgard card. | "Elena Vance is the delivery producer at Cinefex Northern Pictures, finishing visual effects for episodic television. Her episode is due this afternoon, and missing deadline triggers a twenty-five thousand pound daily penalty. Elena is the user persona I designed Callsheet around." *(41 words)* | **0:00 - 0:18** (18s) |
| **2** | **Tab 1. Move cursor smoothly across the masthead to the 12-node fleet grid. Hover momentarily over Node 07 (red fault wash), then over Node 11 (amber active wash).** | Masthead metrics and fleet grid. Node 07 shows quarantined fault status; Node 11 shows active running Shot 118. Slate shows `PROTECTED`. | "At two in the morning, render node zero-seven throttles under heat. Standard monitoring alerts an infrastructure engineer with raw temperatures. But that engineer does not manage client schedules. Without intervention, Shot 118 finishes four hours late and breaches the deadline. Callsheet catches the fault, calculates the schedule deficit, and reallocates the workload automatically." *(53 words)* | **0:18 - 0:45** (27s) |
| **3** | **Tab 1. Click the Grafana Cloud MCP Evidence Trail accordion to expand it. Hover cursor over Step 1, Step 2, and Step 3 titles.** | Expanded evidence accordion displaying Step 1 (Prometheus query), Step 2 (Loki log & Tempo trace correlation), and Step 3 (Gemini root cause). | "Callsheet is built natively on Google's Agent Development Kit. Using the ADK McpToolset, it connects to Grafana Cloud over the Model Context Protocol to query Prometheus metrics and correlate Loki logs. Vertex AI Gemini deduces the root cause, and deterministic Python arithmetic reallocates the shot to standby node eleven. Model output never triggers failovers; mathematical assertions govern every intervention." *(59 words)* | **0:45 - 1:15** (30s) |
| **4** | **Tab 1. Scroll down to Step 6: Post-Intervention Telemetry Verification (Grafana Cloud). Hover cursor over the JSON block showing node-11 frame duration 20s and junction temperature 65.6°C.** | Step 6 evidence block showing retrieved node-11 telemetry proving baseline recovery, with `VERIFIED_PROTECTED` tag. | "Here in step six, closed-loop verification anchors the architecture. Acting without asking is the harder engineering problem because taking action obliges you to prove the action worked. Callsheet does not declare victory on its own arithmetic. After failover, it queries Grafana Cloud for node eleven's actual telemetry. Only after confirming that render durations returned to twenty seconds per frame does it confirm protection. If verification fails, it escalates within the briefing." *(71 words)* | **1:15 - 1:55** (40s) |
| **5** | **Switch to Tab 2 (Grafana Control Tower). Allow camera to settle on live temperature and frame duration panels for 3 seconds, then move cursor over the recovered node-11 series.** | Public Grafana Cloud dashboard with real-time Prometheus metric panels, showing node-07 temperature spike and node-11 nominal 20s frame render rate. | "With delivery secured, Callsheet synthesizes a briefing that Elena can forward to her client, explaining the mitigation in clear commercial terms. In the Grafana Control Tower, all telemetry is public and independently auditable. Every metric sample, log record, and trace span is genuine OpenTelemetry emitted to Grafana Cloud." *(48 words)* | **1:55 - 2:20** (25s) |
| **6** | **Switch to Tab 3 (`/demo`). Click Inject Scenario: Node-07 Thermal Throttling. Show activity log entry update. Then Switch back to Tab 1 and hold on masthead.** | Demo control harness showing button click and log output, then clean return to Tab 1 dashboard. | "Judges can trigger incidents on demand using the slash demo endpoint. I built Callsheet so delivery producers never face unforced deadline penalties. Without Grafana, Callsheet would be a post-mortem tool that tells you why you missed the deadline after the money is already lost." *(44 words)* | **2:20 - 2:40** (20s) |

---

### Recording Notes
- **On-Demand Incidents**: `/demo` can be called anytime to trigger fresh thermal throttling incidents on demand for repeatable takes without waiting for simulator cycles.
- **Visual Hygiene**: No credentials, private tokens, or secret keys appear on screen. The Grafana Control Tower dashboard is public and requires no login.
- **Timing Discipline**: The recording ends at 2:40, leaving an absolute 20-second buffer before the 3:00 hard evaluation cutoff. Speak at an unhurried, measured pace (~118 words per minute).
