# Callsheet: Operational Boundaries and Limitations

This document states the operational realities, technical constraints, and boundaries of Callsheet plainly.

## 1. Grafana Cloud Rule Evaluation Scheduler Floor and Watchdog Stall Detector

Grafana Cloud evaluates this rule group once a minute on this stack regardless of the configured 10 second interval, so an alert arrives between 0 and 60 seconds after a fault and clears within a minute of quarantine. If any node remains over its thermal limit with an active shot allocated for more than 180 seconds without a mission starting, the worker flags a watchdog stall, exposes `watchdog_stalled: true` in `/api/health`, and displays a critical warning on the supervisor strip.

## 2. Render Farm Simulator Specifics

The render farm is a synthetic simulation of a 12-node visual effects render cluster. The Prometheus metrics (`render_farm_node_temperature_celsius`, frame render durations), Loki structured logs, and Tempo raytrace volumetrics spans are real OpenTelemetry streams ingested into a live Grafana Cloud stack over OTLP. Grafana Alertmanager and Grafana IRM incidents are genuine cloud infrastructure, while the `mcp-grafana` server runs as a self-hosted sidecar in the Cloud Run container. The render nodes themselves are run as a Python simulation loop generating synthetic frames and telemetry rather than physical GPU racks in a datacenter. Every telemetry value that drives a decision (temperatures, frame durations, log lines, spans, alert state) comes from Grafana through MCP; production metadata (which shot is on which node, deadlines, frame counts) comes from the farm scheduler, which in a studio would be the render queue manager.

## 3. Source Fault Persistence

Remediation isolates the degraded node via software quarantine; it does not repair physical hardware. The thermal fault on the source node (node-07) is never cleared by remediation, so verification cannot pass on the source node by construction. Node-07 stays throttled and hot after quarantine on purpose, where physical intervention (such as fan replacement or chassis re-seating) is required.

## 4. Deterministic Decisions vs. Generative Synthesis (Gemini Boundary)

Gemini writes prose and never decides. All threshold evaluations, alert detections, buffer margin calculations, standby node selections, failovers, rollbacks, and verification gates are 100% deterministic Python logic. Vertex AI Gemini is invoked strictly for natural language synthesis: translating technical telemetry into client-facing delivery briefings and root-cause summaries. No LLM output, prompt heuristic, or sampling token can alter an intervention path or change a deadline calculation. If Gemini returns nothing, the mission stops and reports; it does not guess.

## 5. Deployment Architecture and In-Memory State

Callsheet is deployed on Google Cloud Run as a single-instance, single-tenant post-production supervisor. Dedicated CPU allocation is enabled to prevent throttling between requests. Pending producer approvals for Tier 2 cross-show pre-emptions, mission history, and panel images are held in instance memory. They survive browser page refreshes, but a Cloud Run instance replacement discards them and starts a fresh cycle with `scenario_primed_by: instance_start`, which is why an unscheduled mission can appear between cycle boundaries. A multi-instance production deployment would back approvals and mission history with a persistent transactional store (such as Cloud SQL or Firestore).

## 6. Demonstration Route Security

The `/demo` route is unauthenticated to enable immediate, frictionless evaluation and automated judging walkthroughs. In an enterprise studio deployment, `/demo` would be disabled or gated behind studio single sign-on (SSO) and role-based access control.

## 7. Telemetry Retention and 6-Hour Cycle Epochs

Grafana Cloud free-tier retention constraints require bounded data volumes. The farm operates on a rolling 6-hour cycle epoch. At each 6-hour boundary, delivery deadlines, completed frames, and node assignments cleanly reset to provide a consistent, repeatable baseline for evaluation.

## 8. Incident List and Test Suite Isolation

Local runs of the test suite write to the same Grafana stack as drills with a laptop prefix. The real incident list in Grafana Incident Management is the Incidents type, not Drills.

## 9. Failure Mode When MCP is Unreachable

If the Grafana Cloud MCP sidecar is unreachable or returns an error, the autonomous watchdog fails closed. It logs loudly, sets `mcp_reachable: false` and `status: "degraded"` in `/api/health`, and halts automated mission triggers. Callsheet will not invent telemetry, guess state, or silently fall back to synthetic values when Grafana Cloud telemetry is unavailable.

## 10. What Callsheet Cannot Do

- Callsheet is not a replacement for a render queue manager. It is an agentic supervisor that observes telemetry, evaluates business SLA risk, and executes targeted interventions and human-in-the-loop approvals.
- Callsheet cannot procure external cloud bursting capacity, spin up third-party compute instances, or incur financial spend (strictly Tier 3 Prohibited).
- Callsheet cannot renegotiate contractual delivery dates or penalty clauses with studio clients; it can only alert producers when contractual margins slip.
- Callsheet cannot physically repair broken hardware or clear thermal throttling without physical maintenance on the failing node.
