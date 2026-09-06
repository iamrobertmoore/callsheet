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
- `status`: Overall service health (`"healthy"` or `"degraded"`)
- `service`: `"callsheet"`
- `timestamp`: Current UTC timestamp in ISO format
- `agent_runtime`: `"live"` (live agent loop, not a playback)
- `replay_mode`: `false` (all operations execute against live backends)
- `mcp_server`: `"grafana/mcp-grafana v1.2.0, self-hosted sidecar"`
- `mcp_transport`: `"streamable-http"`
- `mcp_reachable`: `true` (verified live on-demand via an active `list_datasources` probe)
- `grafana_stack`: `"bigforest2172"`
- `alert_rule_uid`: `"cfxbt56wwbocge"` (active alert rule registered in Grafana Cloud)
- `alert_rule_state`: Current evaluation state in Grafana Cloud Alertmanager (`"Normal"`, `"Firing"`)
- `alert_rule_interval_configured`: `"10s"`
- `alert_rule_interval_observed`: `"60s"` (Grafana Cloud multi-tenant evaluation scheduler floor)
- `model`: `"gemini-3.8-flash"` (Vertex AI model accessed via global endpoint)
- `model_location`: `"global"`
- `last_mission_at`: UTC timestamp of the most recent mission completion
- `last_mission_trigger`: Trigger source of the latest mission
- `last_verification_status`: Status of the last verification attempt
- `cycle_epoch`: Current 6-hour cycle epoch start timestamp
- `next_reset_at_utc`: Next scheduled cycle reset timestamp
- `pending_approvals`: Current list of Tier 2 actions held for producer review
- `worker_running`: Background loop execution status
- `instance_started_at`: UTC timestamp when this Cloud Run container instance booted
- `missions_this_instance`: Total missions completed by this container instance
- `deployment_id`: `"cloud-run"` (guarantees metric, log, and trace isolation)
- `alert_rule_status`: Alert rule polling status (`"ok"`)
- `alert_rule_error`: Alert polling error description if any
- `scenario_primed_by`: Origin of active scenario baseline (`"instance_start"`, `"cycle_reset"`, `"manual_injection"`)
- `tick_cadence`: Live statistics for the 5-second watchdog loop ticks and latency
- `watchdog_stalled`: Boolean flag indicating if an alert has been unserviced for >180 seconds
- `watchdog_stalled_since`: UTC timestamp when the stall condition was first flagged

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

1. **Watchdog Evaluation**: Callsheet reads the alert rule state through MCP on every five second tick.
2. **Alert Trigger (Step 0)**: When a hardware fault occurs, Grafana Cloud Alertmanager evaluates on its 60s floor and fires alert `cfxbt56wwbocge`.
3. **Investigation & Correlation (Steps 1 to 3)**: Callsheet queries Prometheus metrics, Loki logs, and Tempo traces via MCP. Gemini 3.8 Flash deduces the root cause (e.g. thermal junction limit breach on node-07).
4. **SLA Buffer Arithmetic & Incident Declaration (Step 4)**: Calculates delivery margin impact deterministically in Python (`(deadline - now) - remaining_work`) and declares an incident in Grafana IRM.
5. **Tier 1 Autonomous Failover (Step 5)**: Quarantines failing node-07, opens an annotation region, and reallocates shot 118 to idle standby node-11.
6. **Closed-Loop Verification (Step 6)**: Polls node-11 telemetry over MCP. Waits until a post-intervention frame renders, confirming nominal frame duration (<=25.0s) and safe temperatures (<90.0C).
7. **Incident Resolution & Briefing (Step 7)**: Completes the dashboard annotation region, posts an incident summary note to Grafana IRM, updates incident status to Resolved, and generates an executive briefing for Elena.

---

## 5. Grafana Artifacts and MCP Tools Reference

Callsheet interacts with Grafana Cloud strictly through the Model Context Protocol:

| MCP Tool | Access | Step | Cloud Resource / What It Touches |
| :--- | :--- | :--- | :--- |
| `list_datasources` | Read | Setup & Health Check | Discovers Prometheus, Loki, and Tempo datasources; verifies MCP connectivity |
| `query_prometheus` | Read | Step 1 & Step 6 | Prometheus time-series metrics (node temperatures, clock speeds, sample timestamps) |
| `query_loki_logs` | Read | Step 2 & Step 6 | Loki structured log stream (frame completion durations, thermal throttle events) |
| `grafana_api_request` | Read / Write | Step 2, Step 6, Step 7 | Tempo trace spans (`/api/search`, `/api/traces`), Alertmanager rule state, and IRM Twirp key updates |
| `alerting_manage_rules` | Read / Write | Setup & Step 0 | Grafana Cloud Alerting rules and evaluation state |
| `search_dashboards` | Read | Setup & Step 7 | Grafana production dashboard discovery and panel UIDs |
| `generate_deeplink` | Read | Step 7 | Grafana dashboard URL deeplinks for incident timeline and briefing |
| `create_incident` | Write | Step 4 | Grafana Incident Management (IRM) active incident declaration |
| `add_activity_to_incident` | Write | Step 4, Step 5, Step 7 | Grafana IRM timeline notes, hold records, and resolution updates |
| `update_incident` | Write | Step 7 | Grafana IRM incident status (Resolved) and title |
| `create_annotation` | Write | Step 5 | Grafana dashboard annotation start point marking intervention start |
| `update_annotation` | Write | Step 7 | Grafana dashboard annotation end point completing remediation region |
| `get_panel_image` | Read | Step 7 | Grafana dashboard panel PNG rendering for executive briefing |

---

## 6. Three Interactive Demos (Available on `/demo`)

Open [https://callsheet-746874807798.us-central1.run.app/demo](https://callsheet-746874807798.us-central1.run.app/demo) in a new tab to drive three evaluation scenarios.

### Scenario 1: Thermal Throttle on Node-07 (Tier 1 Autonomous Failover)
- **Button**: `Thermal throttle on node-07, about three minutes to resolved`
- **Measured Timings**: Alert arrives within 60 seconds, verification takes about 50 seconds after the move, alert clears within a minute, incident resolved about 100 to 150 seconds after the fault.
- **What to Observe**:
  1. Click the button. Node-07 begins thermal throttling in Prometheus (temperature rises to 95.9C, frame render duration slows to 120.0s).
  2. Within 60 seconds, Grafana Cloud Alertmanager fires alert `cfxbt56wwbocge`.
  3. The supervisor strip on `/` switches to `INVESTIGATING ANOMALY (MCP)`.
  4. Callsheet inspects metrics, logs, and traces. At Step 4, it declares an incident in Grafana IRM. At Step 5, it quarantines failing node-07, opens an annotation region, and reallocates shot 118 to standby node-11.
  5. The supervisor strip switches to `[Tier 1 Attempt 1] Awaiting frame on node-11. Verification window 150s, <N>s elapsed.` and the slate badge displays `VERIFYING (<N>s)`.
  6. Verification completes about 50 seconds after the move: node-11 emits a post-intervention frame in Loki (20.0s) and Prometheus confirms nominal temperature (62.0C), transitioning the slate badge to `VERIFIED PROTECTED`.
  7. Node-07 remains quarantined. The alert clears in Grafana Alertmanager within a minute of quarantine, Callsheet completes the annotation region, drafts an executive briefing, and marks the incident Resolved (about 100 to 150 seconds after the fault).

### Scenario 2: Double Fault (Tier 2 Hold & Producer Approval Card)
- **Button**: `Double fault, holds for your approval after about two minutes`
- **Measured Timings**: Double fault holds after about two minutes; approval path takes about a further 90 seconds.
- **What to Observe**:
  1. Click the button. Node-12 is in maintenance. When the double fault occurs, node-07 overheats with shot 118 and node-03 overheats with shot 204 (*Solar Flare: Redux*).
  2. Shot 118 takes standby node-11 under Tier 1 autonomous failover.
  3. With both standby blades (node-11 and node-12) now occupied or unavailable, shot 204's only option is pre-empting node-08 from *Abyssal Trench*.
  4. Because pre-empting another show crosses the Tier 2 policy boundary, Callsheet halts autonomous action after about two minutes and enters a safety hold.
  5. The supervisor strip displays `Tier 2 hold: awaiting producer approval`.
  6. On the slate, *Solar Flare: Redux* switches to `AT RISK`.
  7. An approval card appears at the top of the dashboard with live decaying arithmetic:
     - Buffer Loss Rate: 1.0 hr/hr
     - Cost of Waiting: Real-time countdown ticking down to contractual breach.
     - Action: Pre-empt blade node-08 from Abyssal Trench for shot 204.
  8. Click **Approve Reallocation**:
     - Callsheet resumes execution, pre-empts blade node-08 for shot 204, and verifies post-intervention telemetry (taking about a further 90 seconds).
     - Node-03 is quarantined, Grafana alert clears, briefing is generated, and the incident is marked Resolved.
  9. (Alternative) Click **Decline & Escalate**:
     - Node-03 is quarantined anyway, shot 204 goes back to the queue, *Solar Flare: Redux* stays `AT RISK`, and the incident stays active with an emergency escalation briefing recorded.

### Scenario 3: Forced Verification Failure & Rollback to Node-12
- **Button**: `Forced verification failure, rollback to node-12`
- **Expected Duration**: ~3 minutes total.
- **What to Observe**:
  1. Click the button. Callsheet triggers a mission where Attempt 1 failover to node-11 is forced to fail post-intervention verification (temperature rises to 94.8C, frame render duration slows to 40.0s).
  2. The supervisor strip shows `[Tier 1 Attempt 1] Awaiting frame on node-11. Verification window 150s, <N>s elapsed.` and the slate badge displays `VERIFYING (<N>s)`.
  3. The closed-loop verification watchdog detects that node-11 breached verification criteria.
  4. Callsheet immediately quarantines node-11, rejects the attempt, and executes an automatic rollback to secondary standby blade node-12.
  5. The supervisor strip switches to `[Tier 1 Attempt 2 - Rollback] Awaiting frame on node-12. Verification window 150s, <N>s elapsed.`.
  6. Verification polls node-12, confirms nominal 20.0s frame rate and 62.0C temperature, transitioning the slate badge to `VERIFIED PROTECTED`.
  7. Alert clears in Grafana Alertmanager, briefing documents the failed attempt on node-11 and successful recovery on node-12, and the incident is resolved.

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

2. **Deterministic Role of Gemini**:
   Gemini never decides anything. All threshold evaluations, alert detections, buffer margin calculations, standby node selections, failovers, holds, rollbacks, and verification gates are 100% deterministic Python logic. Gemini generates plain-language executive prose and root cause explanations; it never makes policy decisions or triggers actions. If Gemini returns nothing, the mission stops and says so rather than guessing.

---

## 9. Known Constraints and Limitations

For a concise, honest overview of simulator architecture, source fault persistence, single-tenant deployment details, and the 6-hour cycle reset, see [LIMITATIONS.md](LIMITATIONS.md).
