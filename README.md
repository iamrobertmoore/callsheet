# Callsheet

When a render node degrades at 2 AM, Callsheet tells the delivery producer whether Tuesday's delivery will land on time or cost money. Most observability tools alert the engineer who built the farm; Callsheet protects the delivery producer who is accountable for the date.

Without Grafana, Callsheet would be a post-mortem tool that tells you why you missed the deadline after the money is already lost.

Callsheet is an autonomous operations agent designed for the delivery producer at a boutique post-production studio (studio crews). When render nodes degrade, Callsheet connects directly to Grafana Cloud over the Model Context Protocol (MCP) to correlate metrics, logs, and traces, isolate the root cause, reallocate affected shots to protect contractual delivery deadlines, and explain the situation in plain production language.

## Who This Is For

This project serves one person: the delivery producer at a post-production house with thirty to sixty staff and a dozen render nodes. She manages multiple shows with hard contractual delivery dates and financial penalty clauses. She is accountable for the schedule and needs actionable production impact briefings rather than raw infrastructure metrics.

## Architecture

```
[ Synthetic Render Farm ]
          │ (OTel: Metrics, Logs, Traces via single OTLP endpoint)
          ▼
   [ Grafana Cloud ]
          │
          │ (Streamable HTTP / Service Account Token)
          ▼
   [ mcp-grafana Server ]
          │
          │ (Model Context Protocol)
          ▼
 [ Callsheet Operations Agent (ADK + Vertex AI Gemini) ]
          │
          ├─► 1. Anomaly Detection (Prometheus)                   [DETERMINISTIC TELEMETRY]
          ├─► 2. Correlation (Loki Logs & Tempo Traces)           [DETERMINISTIC TELEMETRY]
          ├─► 3. Root Cause Isolation (Gemini 3.6 Flash)          [GENERATIVE AI]
          ├─► 4. Production Impact Mapping                        [DETERMINISTIC ARITHMETIC]
          ├─► 5. Automated Job Reallocation                       [DETERMINISTIC ACTION]
          ├─► 6. Post-Intervention Verification (Grafana Cloud)   [DETERMINISTIC VERIFICATION]
          └─► 7. Producer Callsheet Briefing (Gemini 3.6 Flash)   [GENERATIVE AI]
```

## Deterministic Remediation vs. Generative Explanation

A core architectural invariant of Callsheet is the strict separation between deterministic remediation and generative language explanation:

- **Gemini cannot trigger, alter, or override any operational verdict.**
- **Remediation is purely deterministic**: Hardware threshold breaches (Step 1) and delivery buffer deficits (Step 4) are evaluated strictly with pure mathematical arithmetic. The workload failover intervention (Step 5) is executed only when code assertions confirm a negative buffer margin and hardware thermal breach.
- **Verification closes the loop**: Unlike systems that propose actions or assume success upon command execution, Step 6 re-queries Grafana Cloud telemetry on the standby node to independently verify nominal frame render rates (20s) and junction temperatures (<70°C). If metrics remain degraded, the agent disallows the `PROTECTED` status, records the verified failure, and escalates to human technical directors.
- **Generative AI is explanatory only**: Vertex AI Gemini 3.6 Flash is employed exclusively for explanatory synthesis (Step 3 Root Cause Deduction and Step 7 Producer Callsheet Briefings), translating raw correlated telemetry into actionable commercial language for delivery producers.

## Live Observability & Grafana Control Tower

A dedicated Grafana Cloud dashboard provides real-time visibility into the render farm's node temperatures, worker frame durations, and active render queue:
- **Control Tower Dashboard**: [Callsheet Media Production Control Tower](https://bigforest2172.grafana.net/d/callsheet-control-tower/callsheet-media-production-control-tower)

## Technologies Used

- **Google Cloud AI**: Google Agent Development Kit (`google-adk`), Vertex AI Gemini (`google-genai`, `google-cloud-aiplatform`), Cloud Run, and Cloud Build.
- **Grafana Stack**: Grafana Cloud, `grafana/mcp-grafana` MCP Server, Prometheus metrics, Loki logs, and OpenTelemetry ingestion (Tempo traces).

## License

This project is licensed under the Apache 2.0 License. See [LICENSE](LICENSE) for details.

