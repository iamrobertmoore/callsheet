# Callsheet

When a render node degrades at 2 AM, Callsheet tells the delivery producer whether Tuesday's delivery will land on time or cost money. Most observability tools alert the engineer who built the farm; Callsheet protects the delivery producer who is accountable for the date.

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
 [ Google ADK Agent (Vertex AI Gemini) ]
          │
          ├─► 1. Anomaly Detection (Prometheus)
          ├─► 2. Correlation (Loki Logs & Tempo Traces)
          ├─► 3. Root Cause Isolation
          ├─► 4. Shot Delivery Risk Projection
          ├─► 5. Automated Job Reallocation
          └─► 6. Producer Callsheet Briefing
```

## Technologies Used

- **Google Cloud AI**: Google Agent Development Kit (`google-adk`), Vertex AI Gemini (`google-genai`, `google-cloud-aiplatform`), Cloud Run, and Firestore.
- **Grafana Stack**: Grafana Cloud, `grafana/mcp-grafana` MCP Server, Prometheus metrics, Loki logs, and OpenTelemetry ingestion.

## License

This project is licensed under the Apache 2.0 License. See [LICENSE](LICENSE) for details.
