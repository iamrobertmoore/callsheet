"""
Unit tests for Callsheet Policy Classification, Double Fault Scenario,
Approval API, and Section 6 Health Check Metadata.
"""

from datetime import datetime, timezone
import pytest
from httpx import ASGITransport, AsyncClient

from callsheet.farm.models import (
    ApprovalRecord,
    PENDING_APPROVALS,
    NodeStatus,
    ScenarioType,
    ShotStatus,
)
from callsheet.farm.simulator import RenderFarmSimulator
from callsheet.policy import (
    ActionTier,
    ActionType,
    PolicyDecision,
    classify_action,
)
from callsheet.web.app import app, worker


def test_policy_classification_tiers():
    """Verifies that all three tiers are correctly classified according to blast radius rules."""
    # Tier 1: Standby failover
    t1 = classify_action(
        ActionType.FAILOVER_TO_STANDBY,
        {"shot_id": "sh_118", "target_node_id": "node-11"},
    )
    assert t1.tier == ActionTier.TIER_1
    assert t1.allowed_autonomous is True
    assert t1.requires_approval is False

    # Tier 2: Cross-show pre-emption
    t2 = classify_action(
        ActionType.PREEMPT_ACTIVE_NODE,
        {
            "shot_id": "sh_204",
            "preempted_node_id": "node-10",
            "preempted_shot_id": "sh_303",
            "preempted_show_name": "The Lost Realm",
        },
    )
    assert t2.tier == ActionTier.TIER_2
    assert t2.allowed_autonomous is False
    assert t2.requires_approval is True

    # Tier 3: Prohibited actions
    t3_burst = classify_action(
        ActionType.EXTERNAL_BURSTING,
        {"provider": "AWS", "instances": 10},
    )
    assert t3_burst.tier == ActionTier.TIER_3
    assert t3_burst.allowed_autonomous is False

    t3_spend = classify_action(
        ActionType.VENDOR_ESCALATION_SPEND,
        {"cost_usd": 5000},
    )
    assert t3_spend.tier == ActionTier.TIER_3
    assert t3_spend.allowed_autonomous is False


def test_double_fault_scenario_injection():
    """
    Verifies that DOUBLE_FAULT throttles node-07 and node-03 simultaneously,
    leaves node-11 as the sole standby, and places node-12 in offline maintenance.
    """
    sim = RenderFarmSimulator()
    sim.inject_scenario(ScenarioType.DOUBLE_FAULT)

    # Throttled nodes
    n7 = sim.state.nodes["node-07"]
    n3 = sim.state.nodes["node-03"]
    assert n7.status == NodeStatus.THROTTLED
    assert n7.temperature_celsius > 90.0
    assert n3.status == NodeStatus.THROTTLED
    assert n3.temperature_celsius > 90.0

    # Standby and maintenance nodes
    n11 = sim.state.nodes["node-11"]
    n12 = sim.state.nodes["node-12"]
    assert n11.status == NodeStatus.STANDBY
    assert n11.is_standby is True
    assert n12.status == NodeStatus.OFFLINE

    # Pre-emption execution: sh_204 from node-03 to node-10
    raw_res = sim.preempt_and_reallocate(
        shot_id="sh_204",
        target_node_id="node-10",
        source_node_id="node-03",
    )
    assert raw_res["target_node"] == "node-10"
    assert raw_res["shot_id"] == "sh_204"
    assert raw_res["status"] == "PROTECTED"


@pytest.mark.asyncio
async def test_approvals_api_workflow():
    """
    Verifies the full lifecycle of Tier 2 approvals:
    1. GET /api/approvals initially returns empty or existing items.
    2. Adding a pending approval makes it discoverable.
    3. POST /api/approvals/{id} with decision=approve approves the request.
    4. POST /api/approvals/{id} with decision=decline declines a request.
    5. Nonexistent approval IDs return 404.
    """
    PENDING_APPROVALS.clear()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # 1. Empty approvals list
        res = await client.get("/api/approvals")
        assert res.status_code == 200
        assert res.json() == []

        # 2. Add sample pending approval with genuine farm shot sh_204
        record = ApprovalRecord(
            id="apprv-test-1",
            tier=2,
            tier_reason="Cross-show pre-emption touches active production for The Lost Realm",
            action_title="Pre-empt node-10 for sh_204",
            target_node_id="node-10",
            source_node_id="node-03",
            shot_id="sh_204",
            preempted_shot_id="sh_303",
            preempted_show_name="The Lost Realm",
            plan_summary="Pre-empt node-10 from The Lost Realm to save sh_204",
            buffer_loss_rate="-0.5h margin per hour delayed",
            cost_of_waiting="Buffer erosion 0.5h/hr",
            deadline_impact="The Lost Realm margin decreases from +9.4h to +7.2h (still safe)",
            status="PENDING",
        )
        PENDING_APPROVALS[record.id] = record

        # Verify discovery
        res_list = await client.get("/api/approvals")
        assert res_list.status_code == 200
        data = res_list.json()
        assert len(data) == 1
        assert data[0]["id"] == "apprv-test-1"
        assert data[0]["status"] == "PENDING"
        assert data[0]["target_node_id"] == "node-10"

        # 3. Approve the request
        res_approve = await client.post(
            "/api/approvals/apprv-test-1",
            json={"decision": "approve", "reason": "Approved by lead producer"},
        )
        assert res_approve.status_code == 200
        approved_data = res_approve.json()
        assert approved_data["status"] == "APPROVED"
        assert approved_data["decision_reason"] == "Approved by lead producer"
        assert approved_data["resolved_at"] is not None

        # 4. Test decline on second record
        record2 = ApprovalRecord(
            id="apprv-test-2",
            tier=2,
            tier_reason="Cross-show pre-emption",
            action_title="Pre-empt node-09 for sh_118",
            target_node_id="node-09",
            source_node_id="node-07",
            shot_id="sh_118",
            plan_summary="Pre-empt node-09",
            buffer_loss_rate="-0.4h/hr",
            cost_of_waiting="Buffer erosion",
            deadline_impact="Minor impact",
            status="PENDING",
        )
        PENDING_APPROVALS[record2.id] = record2

        res_decline = await client.post(
            "/api/approvals/apprv-test-2",
            json={"decision": "decline", "reason": "Declined by supervisor"},
        )
        assert res_decline.status_code == 200
        declined_data = res_decline.json()
        assert declined_data["status"] == "DECLINED"
        assert declined_data["decision_reason"] == "Declined by supervisor"

        # 5. Nonexistent approval ID returns 404
        res_404 = await client.post(
            "/api/approvals/nonexistent-id",
            json={"decision": "approve"},
        )
        assert res_404.status_code == 404

    PENDING_APPROVALS.clear()


@pytest.mark.asyncio
async def test_health_check_section_6_metadata():
    """
    Verifies that GET /api/health returns all required fields for Section 6:
    - agent_runtime: "live"
    - replay_mode: False
    - mcp_server: contains "grafana/mcp-grafana"
    - mcp_transport: "streamable-http"
    - mcp_reachable: boolean
    - alert_rule_interval_configured: "10s"
    - alert_rule_interval_observed: "60s"
    - model: "gemini-3.8-flash"
    - model_location: "global"
    - cycle_epoch and next_reset_at_utc present
    """
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.get("/api/health")
        assert res.status_code == 200
        data = res.json()

        assert data["agent_runtime"] == "live"
        assert data["replay_mode"] is False
        assert "grafana/mcp-grafana" in data["mcp_server"]
        assert data["mcp_transport"] == "streamable-http"
        assert isinstance(data["mcp_reachable"], bool)
        assert data["alert_rule_interval_configured"] == "10s"
        assert data["alert_rule_interval_observed"] == "60s"
        assert data["model"] == "gemini-3.8-flash"
        assert data["model_location"] == "global"
        assert "cycle_epoch" in data
        assert "next_reset_at_utc" in data
        assert "pending_approvals" in data


@pytest.mark.asyncio
async def test_multi_mission_approval_isolation(monkeypatch):
    """
    Verifies that concurrent approval decisions on distinct missions remain strictly isolated.
    Approving Mission A (Incident 101) and Declining Mission B (Incident 102) concurrently:
    - Asserts that all activity notes, summaries, and resolutions for Mission A land in Incident 101.
    - Asserts that all activity notes for Mission B land in Incident 102.
    - Asserts no cross-incident contamination occurs.
    """
    import asyncio
    from callsheet.web.app import mission_runner

    PENDING_APPROVALS.clear()

    approval_a = ApprovalRecord(
        id="appr-A",
        mission_id="mission-alpha",
        incident_id="incident-101",
        tier=2,
        tier_reason="Cross-show pre-emption for Mission A",
        action_title="Pre-empt node-08 for sh_204",
        target_node_id="node-08",
        source_node_id="node-03",
        shot_id="sh_204",
        preempted_shot_id="sh_301",
        preempted_show_name="Abyssal Trench 3D",
        plan_summary="Pre-empt node-08 for sh_204",
        buffer_loss_rate="1.0 min buffer lost per minute of delay",
        cost_of_waiting="Buffer erosion 1.0 min/min",
        deadline_impact="Abyssal Trench margin stays protected",
        status="PENDING",
        tier1_target_node="node-11",
        tier1_shot_id="sh_118",
        tier1_rate_sec=20.0,
        tier1_temp_c=63.5,
        tier1_buffer_margin=2.8,
        tier1_witnesses=["Loki", "Prometheus", "Tempo"],
    )

    approval_b = ApprovalRecord(
        id="appr-B",
        mission_id="mission-beta",
        incident_id="incident-102",
        tier=2,
        tier_reason="Cross-show pre-emption for Mission B",
        action_title="Pre-empt node-08 for sh_204",
        target_node_id="node-08",
        source_node_id="node-03",
        shot_id="sh_204",
        preempted_shot_id="sh_301",
        preempted_show_name="Abyssal Trench 3D",
        plan_summary="Pre-empt node-08 for sh_204",
        buffer_loss_rate="1.0 min buffer lost per minute of delay",
        cost_of_waiting="Buffer erosion 1.0 min/min",
        deadline_impact="Abyssal Trench margin stays protected",
        status="PENDING",
        tier1_target_node="node-11",
        tier1_shot_id="sh_118",
        tier1_rate_sec=20.0,
        tier1_temp_c=63.5,
        tier1_buffer_margin=2.8,
        tier1_witnesses=["Loki", "Prometheus", "Tempo"],
    )

    PENDING_APPROVALS[approval_a.id] = approval_a
    PENDING_APPROVALS[approval_b.id] = approval_b

    executed_calls = []

    async def mock_execute_mcp_tool(toolset, tool_name, arguments):
        executed_calls.append({"tool": tool_name, "args": arguments})
        if tool_name == "create_annotation":
            return {"id": 12345}
        return {}

    async def mock_verify_node_telemetry(**kwargs):
        return {
            "rate_sec": 19.0,
            "temp_c": 64.2,
            "log_line": "Frame 1203 rendered on node-08 successfully in 19.0s.",
            "witnesses_accepted": ["Loki", "Prometheus", "Tempo"],
        }

    async def mock_wait_for_node_alert_cleared(**kwargs):
        inc_id = kwargs.get("incident_id")
        if inc_id:
            executed_calls.append({
                "tool": "add_activity_to_incident",
                "args": {
                    "incidentId": inc_id,
                    "body": f"Grafana alert cleared for node-03 at 18:09:04 UTC.",
                }
            })
        return ("2026-09-05T18:09:04Z", 4.2)

    monkeypatch.setattr(mission_runner, "_execute_mcp_tool", mock_execute_mcp_tool)
    monkeypatch.setattr(mission_runner, "_verify_node_telemetry", mock_verify_node_telemetry)
    monkeypatch.setattr(mission_runner, "_wait_for_node_alert_cleared", mock_wait_for_node_alert_cleared)

    # Execute approve on mission A and decline on mission B concurrently
    res_a, res_b = await asyncio.gather(
        mission_runner.handle_approval_decision("appr-A", "approve", "Approved by Lead Producer"),
        mission_runner.handle_approval_decision("appr-B", "decline", "Declined by Executive Producer"),
    )

    assert res_a["status"] == "APPROVED"
    assert res_a["incident_status"] == "resolved"
    assert res_b["status"] == "DECLINED"
    assert res_b["incident_status"] == "active"

    # Analyze all MCP tool calls targeting incidents
    inc_101_activities = []
    inc_102_activities = []
    inc_101_resolutions = []
    inc_102_resolutions = []

    for call in executed_calls:
        tool = call["tool"]
        args = call["args"]
        if tool == "add_activity_to_incident":
            target_inc = args.get("incidentId")
            body = args.get("body", "")
            if target_inc == "incident-101":
                inc_101_activities.append(body)
            elif target_inc == "incident-102":
                inc_102_activities.append(body)
            else:
                pytest.fail(f"Unexpected incident target: {target_inc}")
        elif tool == "update_incident":
            target_inc = args.get("incidentId")
            status = args.get("status")
            if target_inc == "incident-101":
                inc_101_resolutions.append(status)
            elif target_inc == "incident-102":
                inc_102_resolutions.append(status)
            else:
                pytest.fail(f"Unexpected incident update target: {target_inc}")

    # Assertions for Incident 101 (Approved Mission A)
    assert len(inc_101_activities) >= 3
    assert any("Producer APPROVED Tier 2 Plan" in act for act in inc_101_activities)
    assert any("Step 6 Post-Intervention Verification" in act for act in inc_101_activities)
    assert any("Resolution Summary" in act for act in inc_101_activities)
    assert not any("DECLINED" in act for act in inc_101_activities)
    assert inc_101_resolutions == ["resolved"]

    # Assertions for Incident 102 (Declined Mission B)
    assert len(inc_102_activities) >= 1
    assert any("Producer DECLINED Tier 2 Plan" in act for act in inc_102_activities)
    assert not any("APPROVED" in act for act in inc_102_activities)
    assert not any("Resolution Summary" in act for act in inc_102_activities)
    assert inc_102_resolutions == []  # Never resolved, remains active

    PENDING_APPROVALS.clear()
