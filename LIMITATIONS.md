# Callsheet: Operational Boundaries and Limitations

This document states the operational realities, technical constraints, and boundaries of Callsheet plainly.

## 1. Grafana Cloud Rule Evaluation Scheduler Floor

Grafana Cloud evaluates the Callsheet rule group once a minute on this stack, so fault to alert is between 0 and 60 seconds and the alert clears within a minute of quarantine. While the rule group is configured with an evaluation interval of 10s (`alert_rule_interval_configured: "10s"`), the underlying multi-tenant ruler engine on this Grafana Cloud stack advances evaluations only on 60-second boundaries (`alert_rule_interval_observed: "60s"`).

## 2. Render Farm Simulator

The render farm is a synthetic simulation of a 12-node visual effects render cluster. It emits real OpenTelemetry metrics (`render_farm_node_temperature_celsius`, frame render durations), streams structured logs to Loki, and traces raytrace volumetrics spans to Tempo. While the telemetry and network pipelines to Grafana Cloud are real and load-bearing, the underlying hardware nodes are simulated processes rather than physical GPU racks.

## 3. Source Fault Persistence

Remediation isolates the degraded node via software quarantine; it does not repair physical hardware. The thermal fault on the source node (node-07) is never cleared by remediation, so verification cannot pass on the source node by construction. Node-07 stays hot after quarantine on purpose to accurately reflect real-world hardware failure where physical intervention (e.g. fan replacement, heat-sink re-pasting) is required.

## 4. Deterministic Decision Engine (Gemini Boundary)

Gemini writes prose and never decides. All threshold evaluations, alert detections, buffer margin calculations, standby node selections, and verification checks are 100% deterministic Python logic. Vertex AI Gemini is invoked strictly for natural language synthesis: translating technical telemetry into client-facing delivery briefings and root-cause summaries. No LLM output, prompt heuristic, or sampling token can alter an intervention path or change a deadline calculation.

## 5. Deployment Architecture and In-Memory State

Callsheet is designed as a single-instance, single-tenant post-production supervisor. Pending producer approvals for Tier 2 cross-show pre-emptions reside in instance memory. They survive browser page refreshes, but are cleared when the rolling 6-hour cycle epoch resets or when the container restarts.

## 6. Demonstration Route Security

The `/demo` route is unauthenticated to enable immediate, frictionless hackathon evaluation and automated judging walkthroughs. In a production studio deployment, `/demo` would be disabled or gated behind studio single sign-on (SSO) and role-based access control.

## 7. Telemetry Retention and 6-Hour Cycle Epochs

Grafana Cloud free-tier retention constraints require bounded data volumes. The farm operates on a rolling 6-hour cycle epoch. At each 6-hour boundary, delivery deadlines, completed frames, and node assignments cleanly reset to provide a consistent, repeatable baseline for evaluation.

## 8. Failure Mode When MCP is Unreachable

If the Grafana Cloud MCP sidecar is unreachable or returns an error, the autonomous watchdog fails closed. It logs loudly, sets `mcp_reachable: false` and `status: "degraded"` in `/api/health`, and halts automated mission triggers. Callsheet will not invent telemetry, guess state, or silently fall back to synthetic values when Grafana Cloud telemetry is unavailable.

## 9. What Callsheet Cannot Do

- Callsheet cannot procure external cloud bursting capacity, spin up third-party compute instances, or incur financial spend (strictly Tier 3 Prohibited).
- Callsheet cannot renegotiate contractual delivery dates or penalty clauses with studio clients; it can only alert producers when contractual margins slip.
- Callsheet cannot repair broken hardware or clear thermal throttling without physical maintenance on the failing node.
