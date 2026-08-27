# Callsheet Video Demonstration Script

**Target Duration**: 3:00 minutes hard (Evaluated window: 0:00 to 3:00)  
**Spoken Word Count**: 343 words (at 140 words/minute = 2 minutes 27 seconds of speech, leaving 33 seconds for visual pacing, clicks, and transitions)  
**Voice**: First-person singular, direct engineering tone, no em dashes, no prohibited buzzwords.

---

### Production Recording Table

| Shot | On Screen | Spoken Words | Elapsed Time |
| :--- | :--- | :--- | :--- |
| **1** | Full view of live deployment at `https://callsheet-746874807798.us-central1.run.app`. Cursor hovers over **Chronicles of Aethelgard: Episode 6** card on the Active Delivery Slate. | "This is Elena Vance, the delivery producer persona I designed Callsheet around. She finishes visual effects for episodic streaming television. Her episode is due later this afternoon. If she misses that delivery, her contract triggers an immediate penalty of twenty-five thousand pounds per day." | **0:00 - 0:15** (15s) |
| **2** | Pan across the Call Sheet masthead and the 12-node fleet grid. Node 07 is displayed in quarantined fault state; Node 11 is active running Shot 118. The slate card indicates `PROTECTED` status with positive buffer margin. | "At two in the morning, render node zero-seven began thermal throttling. Conventional monitoring paged an infrastructure engineer with junction temperatures and clock speeds. But that engineer does not manage shot schedules. Without intervention, Shot 118 would have finished four hours late, triggering that penalty. Instead, Callsheet caught the fault, calculated the schedule deficit, and protected the date automatically." | **0:15 - 0:45** (30s) |
| **3** | Click to expand the **Grafana Cloud MCP Evidence Trail** accordion. Highlight Step 1 (Prometheus query), Step 2 (Loki log and Tempo trace correlation), and Step 3 (Gemini root-cause deduction). | "Callsheet does not just propose ideas and wait for human approval. It acts. Over the Model Context Protocol, it queries Prometheus metrics, correlates Loki logs, and uses Vertex AI Gemini to deduce root causes. Then, deterministic code evaluates the delivery buffer and moves the shot from node zero-seven to standby spare node eleven. Generative AI never triggers failovers; mathematical assertions govern every intervention." | **0:45 - 1:20** (35s) |
| **4** | Scroll to and emphasize **Step 6: Post-Intervention Telemetry Verification (Grafana Cloud)**. Expand the JSON evidence block displaying retrieved frame duration (20s) and stabilized junction temperature (65.6°C). | "This is step six, and this is where Callsheet separates itself from every other tool. An agent that acts without permission owes the producer proof that the action worked. Callsheet does not declare victory on its own arithmetic. After failover, it queries Grafana Cloud to inspect node eleven's actual telemetry. Only after confirming that render durations returned to twenty seconds per frame does it issue the protected status. If verification fails, it escalates immediately." | **1:20 - 2:05** (45s) |
| **5** | Highlight the **Production Callsheet Briefing** in Newsreader serif prose. Then click the top-right link **GRAFANA CONTROL TOWER ↗** to open the live public Grafana dashboard in a new tab, showing real-time temperature and frame duration panels. | "With delivery secured, Callsheet drafts an executive briefing that Elena can forward directly to her studio client, explaining the mitigation in plain commercial language. Next, look at the Grafana Control Tower. This is a public, unauthenticated dashboard. Every metric sample, log record, and trace span is genuine OpenTelemetry. You can audit the real-time health of the fleet independently." | **2:05 - 2:40** (35s) |
| **6** | Open a new browser tab navigating to `/demo`. Click to trigger an on-demand thermal throttling injection. Return to dashboard to show live reactive failover motion and closing masthead. | "Judges can trigger incidents instantly using the slash demo endpoint. I built Callsheet so delivery producers never have to explain an unforced deadline failure. Without Grafana, Callsheet would be a post-mortem tool that tells you why you missed the deadline after the money is already lost." | **2:40 - 3:00** (20s) |

---

### Recording Notes
- **Testing Route**: `/demo` can be called anytime to trigger fresh thermal throttling incidents on demand for repeat takes.
- **Visual Hygiene**: No credentials or private tokens are shown on screen. The Grafana Control Tower dashboard is public and requires no login.
- **Timing Discipline**: Speak at an even conversational pace. The 343 words comfortably fill under 2:30 of acoustic time, leaving generous buffer across cuts.
