# Callsheet: 10-Minute Walkthrough for Hackathon Judges

This guide walks judges through evaluating Callsheet in ten minutes, from live health checks and public Grafana observability to interactive scenario execution.

## 1. The Persona: Elena Vance

Elena Vance is the delivery producer at Cinefex Northern Pictures, an independent visual effects house in Manchester. Her studio finishes high-end visual effects shots for episodic streaming television. Today, Elena is delivering *Chronicles of Aethelgard: Episode 6* against a contractual deadline with a hard £25,000 per day SLA penalty.

When a GPU blade overheats at 3:00 AM, standard monitoring tools page an infrastructure engineer with raw junction temperatures and fan RPMs. That engineer has no context on shot dependencies, scene complexity, delivery margins, or client SLA penalties.

Callsheet bridges this gap. It connects telemetry to business reality, autonomous remediation to verification, and technical triage to plain-language production correspondence.

---

## 2. Live Health and System Verification (One Command)

Verify that Callsheet is running live against genuine Grafana Cloud infrastructure with zero mocks:

```bash
curl -s https://callsheet-746874807798.us-central1.run.app/api/health | jq .
```

### Key Fields to Check:
- `agent_runtime`: `"live"` (live agent loop, not a replay or playback)
- `replay_mode`: `false` (all operations execute against live backends)
- `mcp_server`: `"grafana/mcp-grafana v1.2.0, self-hosted sidecar"`
- `mcp_transport`: `"streamable-http"`
- `mcp_reachable`: `true` (verified on-demand via an active `list_datasources` probe)
- `grafana_stack`: `"bigforest2172"`
- `alert_rule_uid`: `"cfxbt56wwbocge"` (active alert rule registered in Grafana Cloud)
- `alert_rule_state`: Current evaluation state in Grafana Cloud Alertmanager
- `alert_rule_interval_configured`: `"10s"`
- `alert_rule_interval_observed`: `"60s"` (Grafana Cloud multi-tenant evaluation scheduler floor)
- `model`: `"gemini-3.8-flash"` (Vertex AI model accessed via global endpoint)
- `deployment_id`: `"cloud-run"` (guarantees metric, log, and trace isolation)

---

## 3. Public Grafana Control Tower and Producer Dashboard

Open these two tabs side-by-side:

1. **Callsheet Producer Dashboard**: [https://callsheet-746874807798.us-central1.run.app](https://callsheet-746874807798.us-central1.run.app)
   - Live production slate showing delivery countdowns, buffer margins, and shot status.
   - Farm node allocation matrix (12 nodes across active shows and standby spares).
   - Agent Supervisor strip with real-time state and verification progress.
   - Executive briefing with client-ready communication notes and audit trail.

2. **Public Grafana Control Tower**: [Callsheet Media Production Control Tower](https://bigforest2172.grafana.net/public-dashboards/a9028daf791643b8899a10531f6b31dd)
   - Live Prometheus node temperatures, clock frequencies, and frame render rates.
   - Loki structured log streams showing frame start, render pass, and completion events.
   - Tempo distributed traces measuring raytracing and volumetrics render stages.

---

## 4. The Default Story: Autonomous Unattended Watch

When left running unattended, Callsheet operates continuously without human intervention:

1. **Watchdog Evaluation**: Callsheet polls Grafana Alertmanager every 5 seconds.
2. **Alert Trigger (Step 0)**: When a hardware fault occurs, Grafana Cloud Alertmanager evaluates on its 60s floor and fires alert `cfxbt56wwbocge`.
3. **Investigation & Correlation (Steps 1 to 3)**: Callsheet queries Prometheus metrics, Loki logs, and Tempo traces via MCP. Gemini 3.6 Flash deduces the root cause (e.g. thermal junction limit breach on node-07).
4. **SLA Buffer Arithmetic (Step 4)**: Calculates delivery margin impact deterministically in Python (`(deadline - now) - remaining_work`).
5. **Tier 1 Autonomous Failover (Step 5)**: Reallocates shot 118 from failing node-07 to idle standby node-11.
6. **Closed-Loop Verification (Step 6)**: Polls node-11 telemetry over MCP. Waits until a post-intervention frame renders, confirming nominal frame duration (20.0s) and safe temperatures (<90C).
7. **Incident Resolution & Briefing (Step 7)**: Posts an incident summary note to Grafana IRM, updates incident status to Resolved, and generates an executive briefing for Elena.

---

## 5. Grafana Artifacts and MCP Tools Reference

Callsheet interacts with Grafana Cloud strictly through the Model Context Protocol:

| MCP Tool | Protocol / Transport | Purpose in Callsheet | Cloud Resource Affected |
| :--- | :--- | :--- | :--- |
| `list_datasources` | MCP (Streamable HTTP) | Verifies connectivity and discovers datasources | Prometheus, Loki, Tempo instances |
| `query_prometheus` | MCP (Streamable HTTP) | Step 1 & 6: Node temperatures, clocks, sample timestamps | Prometheus time-series metrics |
| `query_loki_logs` | MCP (Streamable HTTP) | Step 2 & 6: Frame completion durations, thermal log lines | Loki structured log stream |
| `grafana_api_request` | MCP (Streamable HTTP) | Step 2 & 6: Tempo trace search and span durations; alert status | Tempo traces (`/api/search`, `/api/traces`) and Alertmanager |
| `search_incidents` | MCP (Streamable HTTP) | Step 0: Discovers active Grafana IRM incidents | Grafana Incident Management (IRM) |
| `create_incident` | MCP (Streamable HTTP) | Step 0: Opens new incident if none exists for firing alert | Grafana Incident Management (IRM) |
| `update_incident` | MCP (Streamable HTTP) | Step 7: Resolves incident upon verified remediation | Grafana Incident Management (IRM) |
| `CreateKeyUpdate` | Twirp RPC | Step 5 & 7: Posts timeline notes and resolution summaries | Grafana IRM incident timeline |

---

## 6. Three Interactive Demos (Available on `/demo`)

Open [https://callsheet-746874807798.us-central1.run.app/demo](https://callsheet-746874807798.us-central1.run.app/demo) in a new tab to drive three evaluation scenarios.

### Scenario 1: Thermal Throttle on Node-07 (Tier 1 Autonomous Failover)
- **Button**: `Thermal throttle on node-07, about three minutes to resolved`
- **Expected Duration**: ~3 minutes total (dictated by the 60s alert evaluation floor).
- **What to Observe**:
  1. Click the button. Node-07 begins thermal throttling in Prometheus (temperature rises to 94.8C, frame render duration slows to 40.0s).
  2. Within 60 seconds, Grafana Cloud Alertmanager fires alert `cfxbt56wwbocge`.
  3. The supervisor strip on `/` switches to `INVESTIGATING ANOMALY (MCP)`.
  4. Callsheet inspects metrics, logs, and traces. It reallocates shot 118 to standby node-11.
  5. The supervisor strip switches to `[Tier 1 Attempt 1] Awaiting frame on node-11`.
  6. When node-11 emits a post-intervention frame in Loki (20.0s) and Prometheus confirms safe temperature (62C), verification succeeds (`VERIFIED PROTECTED`).
  7. Node-07 is quarantined, the alert clears in Grafana Alertmanager, and the incident is marked Resolved.

### Scenario 2: Double Fault (Tier 2 Hold & Producer Approval Card)
- **Button**: `Double fault, holds for your approval after about two minutes`
- **Expected Duration**: Holds after ~2 minutes; resolves ~1 minute after approval.
- **What to Observe**:
  1. Click the button. A double fault is primed where node-11 is already unavailable, forcing Callsheet to evaluate pre-empting node-08 from *Solar Flare: Redux*.
  2. Because pre-empting another show crosses the Tier 2 policy boundary, Callsheet halts autonomous action and enters a safety hold.
  3. The supervisor strip displays `Tier 2 hold: awaiting producer approval`.
  4. On the slate, *Solar Flare: Redux* switches to `AT RISK`.
  5. An approval card appears at the top of the dashboard with live decaying arithmetic:
     - Buffer Loss Rate: 1.0 hr/hr
     - Cost of Waiting: Real-time countdown ticking down to contractual breach.
     - Action: Pre-empt blade node-08 from Solar Flare.
  6. Click **Approve Reallocation**:
     - Callsheet resumes execution, reallocates shot 118 to node-08, and verifies post-intervention telemetry.
     - Grafana alert clears, briefing is generated, and the incident is resolved.
  7. (Alternative): Click **Decline & Escalate**: Callsheet records the refusal, leaves the incident open, and generates an emergency TD escalation briefing.

### Scenario 3: Forced Verification Failure & Rollback to Node-12
- **Button**: `Forced verification failure, rollback to node-12`
- **Expected Duration**: ~3 minutes total.
- **What to Observe**:
  1. Click the button. Callsheet triggers a mission where Attempt 1 failover to node-11 is forced to fail post-intervention verification (frame duration remains throttled at 40.0s).
  2. The closed-loop verification watchdog detects that node-11 failed verification criteria.
  3. Callsheet immediately quarantines node-11, rejects the attempt, and executes an automatic rollback to secondary standby blade node-12.
  4. Verification polls node-12, confirms clean 20.0s frame rate and 62C temperature.
  5. Mission completes with `VERIFIED_PROTECTED` via rollback. The briefing documents the failed attempt on node-11 and successful recovery on node-12.

---

## 7. Operational Blast Radius Policy

Callsheet enforces hard deterministic boundaries on operational authority:

- **Tier 1 (Autonomous Execution)**: Safe, local, reversible interventions within the same show. Reallocating a shot to an idle standby node; quarantining an overheating blade. Requires no human approval.
- **Tier 2 (Producer Approval Required)**: Interventions that affect other productions, pre-empt external show workloads, or alter contractual delivery dates. Enforces a mandatory safety hold and presents live buffer loss arithmetic for human sign-off.
- **Tier 3 (Prohibited)**: Out-of-bounds operations. Callsheet cannot spin up external cloud bursting capacity, procure commercial instances, or incur financial spend.

---

## 8. The Rip-Out Test and Decision Integrity

Callsheet maintains absolute separation between deterministic logic and generative AI:

1. **Severing Grafana Cloud (Fail-Closed Test)**:
   Every metric sample, log entry, and trace span must make a round trip through Grafana Cloud. Pointing Callsheet at an unreachable Grafana endpoint causes the mission to fail closed immediately. The agent has no mock fallbacks, local memory shortcuts, or side-channel cheats. This is asserted in `tests/test_agent_mission.py::test_mission_fails_when_grafana_unreachable`.

2. **Removing Gemini**:
   If Vertex AI Gemini were completely disabled, all threshold evaluations, failovers, holds, rollbacks, and verification gates would continue to function with 100% precision. Gemini generates plain-language executive prose; it never makes policy decisions or triggers actions.

---

## 9. Known Constraints and Limitations

For a concise, honest overview of simulator architecture, source fault persistence, single-tenant deployment details, and the 6-hour cycle reset, see [LIMITATIONS.md](LIMITATIONS.md).
