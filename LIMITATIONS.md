# Callsheet: Operational Boundaries and Limitations

This document states the operational realities, technical constraints, and boundaries of Callsheet plainly.

## 1. Grafana Cloud Rule Evaluation Scheduler Floor

Grafana Cloud evaluates the Callsheet alert rule group once a minute on this stack, so fault to alert is between 0 and 60 seconds and the alert clears within a minute of quarantine. While the rule group is configured with an evaluation interval of 10s (`alert_rule_interval_configured: "10s"`), the underlying multi-tenant ruler engine on Grafana Cloud evaluates alert rules only on 60-second boundaries (`alert_rule_interval_observed: "60s"`).

## 2. Render Farm Simulator Specifics

The render farm is a synthetic simulation of a 12-node visual effects render cluster. The Prometheus metrics (`render_farm_node_temperature_celsius`, frame render durations), Loki structured logs, and Tempo raytrace volumetrics spans are real OpenTelemetry streams ingested into a live Grafana Cloud stack over OTLP. Grafana Alertmanager, Grafana IRM incidents, and the `mcp-grafana` server are genuine cloud infrastructure. The render nodes themselves are simulated VFX worker daemons generating synthetic frames and telemetry rather than physical GPU racks in a datacenter.

## 3. Source Fault Persistence

Remediation isolates the degraded node via software quarantine; it does not repair physical hardware. The thermal fault on the source node (node-07) is never cleared by remediation, so verification cannot pass on the source node by construction. Node-07 stays throttled and hot after quarantine on purpose, accurately reflecting real-world hardware failures where physical intervention (e.g. fan replacement, chassis re-seating) is required.

## 4. Deterministic Decisions vs. Generative Synthesis (Gemini Boundary)

Gemini writes prose and never decides. All threshold evaluations, alert detections, buffer margin calculations, standby node selections, failovers, rollbacks, and verification gates are 100% deterministic Python logic. Vertex AI Gemini is invoked strictly for natural language synthesis: translating technical telemetry into client-facing delivery briefings and root-cause summaries. No LLM output, prompt heuristic, or sampling token can alter an intervention path or change a deadline calculation.

## 5. Deployment Architecture and In-Memory State

Callsheet is deployed on Google Cloud Run as a single-instance, single-tenant post-production supervisor. Dedicated CPU allocation is enabled to prevent throttling between requests. Pending producer approvals for Tier 2 cross-show pre-emptions reside in instance memory. They survive browser page refreshes, but reset when the rolling 6-hour cycle epoch resets or when the container restarts. A multi-instance production deployment would back approvals with a persistent transactional store (such as Cloud SQL or Firestore).

## 6. Demonstration Route Security

The `/demo` route is unauthenticated to enable immediate, frictionless evaluation and automated judging walkthroughs. In an enterprise studio deployment, `/demo` would be disabled or gated behind studio single sign-on (SSO) and role-based access control.

## 7. Telemetry Retention and 6-Hour Cycle Epochs

Grafana Cloud free-tier retention constraints require bounded data volumes. The farm operates on a rolling 6-hour cycle epoch. At each 6-hour boundary, delivery deadlines, completed frames, and node assignments cleanly reset to provide a consistent, repeatable baseline for evaluation.

## 8. Failure Mode When MCP is Unreachable

If the Grafana Cloud MCP sidecar is unreachable or returns an error, the autonomous watchdog fails closed. It logs loudly, sets `mcp_reachable: false` and `status: "degraded"` in `/api/health`, and halts automated mission triggers. Callsheet will not invent telemetry, guess state, or silently fall back to synthetic values when Grafana Cloud telemetry is unavailable.

## 9. What Callsheet Cannot Do

- Callsheet is not a replacement for full cluster queue managers like Slurm, Pixar Tractor, or AWS Thinkbox Deadline. It is an agentic supervisor that observes telemetry, evaluates business SLA risk, and executes targeted interventions and human-in-the-loop approvals.
- Callsheet cannot procure external cloud bursting capacity, spin up third-party compute instances, or incur financial spend (strictly Tier 3 Prohibited).
- Callsheet cannot renegotiate contractual delivery dates or penalty clauses with studio clients; it can only alert producers when contractual margins slip.
- Callsheet cannot physically repair broken hardware or clear thermal throttling without physical maintenance on the failing node.
