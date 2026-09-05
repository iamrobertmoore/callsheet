"""
Callsheet Blast Radius and Authority Policy.

Classifies proposed operational actions into authority tiers:
- Tier 1: Autonomous execution (failover to idle standby, thermal quarantine).
- Tier 2: Producer approval required (cross-show pre-emption, delivery impact).
- Tier 3: Prohibited actions (external capacity, cloud bursting, vendor spend).
"""

from enum import IntEnum
from typing import Any, Dict, Optional
from pydantic import BaseModel


class ActionTier(IntEnum):
    TIER_1 = 1  # Autonomous execution
    TIER_2 = 2  # Producer approval required
    TIER_3 = 3  # Prohibited / never executed


class ActionType:
    FAILOVER_TO_STANDBY = "FAILOVER_TO_STANDBY"
    QUARANTINE_NODE = "QUARANTINE_NODE"
    PREEMPT_ACTIVE_NODE = "PREEMPT_ACTIVE_NODE"
    CROSS_SHOW_REALLOCATION = "CROSS_SHOW_REALLOCATION"
    MODIFY_DEADLINE = "MODIFY_DEADLINE"
    EXTERNAL_BURSTING = "EXTERNAL_BURSTING"
    VENDOR_ESCALATION_SPEND = "VENDOR_ESCALATION_SPEND"


class PolicyDecision(BaseModel):
    tier: ActionTier
    tier_name: str
    allowed_autonomous: bool
    requires_approval: bool
    reason: str
    summary: str


def classify_action(action_type: str, details: Optional[Dict[str, Any]] = None) -> PolicyDecision:
    """
    Classifies a proposed operational action into an authority tier and states the reason.

    Tier 1 (Autonomous):
    - Moving a shot onto an idle standby node.
    - Quarantining a node that has breached its own thermal limit.
    - Reversible, touches no other show, no producer-visible change to any deadline.

    Tier 2 (Producer Approval Required):
    - Pre-empting a node that is currently rendering another show's shot.
    - Anything that touches more than one show.
    - Anything that changes a delivery commitment or contractual margin.

    Tier 3 (Prohibited):
    - Anything outside the farm: external cloud capacity, third-party vendor calls, cloud spend.
    """
    details = details or {}
    act = action_type.upper()

    # Tier 3 checks
    if act in (ActionType.EXTERNAL_BURSTING, ActionType.VENDOR_ESCALATION_SPEND) or details.get("external_capacity"):
        return PolicyDecision(
            tier=ActionTier.TIER_3,
            tier_name="Tier 3: Prohibited",
            allowed_autonomous=False,
            requires_approval=False,
            reason="Actions outside farm boundary (external capacity, vendor spend) are strictly prohibited.",
            summary="Prohibited: Farm boundaries are strict. No external spend or vendor calls permitted.",
        )

    # Tier 2 checks
    touches_multiple_shows = details.get("touches_multiple_shows", False)
    preempts_active_node = details.get("preempts_active_node", False)
    changes_deadline = details.get("changes_deadline", False)
    source_show = details.get("source_show_id")
    target_show = details.get("target_show_id")
    if source_show and target_show and source_show != target_show:
        touches_multiple_shows = True

    if (
        act in (ActionType.PREEMPT_ACTIVE_NODE, ActionType.CROSS_SHOW_REALLOCATION, ActionType.MODIFY_DEADLINE)
        or touches_multiple_shows
        or preempts_active_node
        or changes_deadline
    ):
        preempted_shot = details.get("preempted_shot_id", "active shot")
        target_node = details.get("target_node_id", "active node")
        other_show = details.get("preempted_show_name") or target_show or "another show"
        
        reason = (
            f"Pre-empting node {target_node} rendering {preempted_shot} touches {other_show} and requires human approval."
            if preempts_active_node
            else "Action touches multiple shows or modifies contractual delivery commitments."
        )

        return PolicyDecision(
            tier=ActionTier.TIER_2,
            tier_name="Tier 2: Producer Approval Required",
            allowed_autonomous=False,
            requires_approval=True,
            reason=reason,
            summary=f"Requires Approval: {reason}",
        )

    # Tier 1 defaults (idle standby failover or thermal quarantine)
    if act == ActionType.QUARANTINE_NODE:
        node_id = details.get("node_id", "degraded node")
        temp = details.get("temperature_celsius", 90.0)
        reason = f"Quarantining node {node_id} exceeding thermal limit ({temp:.1f}C) is self-contained and reversible."
        return PolicyDecision(
            tier=ActionTier.TIER_1,
            tier_name="Tier 1: Autonomous",
            allowed_autonomous=True,
            requires_approval=False,
            reason=reason,
            summary=f"Autonomous: {reason}",
        )

    target_node = details.get("target_node_id", "standby node")
    shot_code = details.get("shot_code") or details.get("shot_id", "shot")
    reason = f"Moving shot {shot_code} onto idle standby {target_node} is reversible and touches no other show."

    return PolicyDecision(
        tier=ActionTier.TIER_1,
        tier_name="Tier 1: Autonomous",
        allowed_autonomous=True,
        requires_approval=False,
        reason=reason,
        summary=f"Autonomous: {reason}",
    )
