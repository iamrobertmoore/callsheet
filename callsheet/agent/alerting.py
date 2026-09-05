"""
Grafana Cloud alert rule management and status polling.
Supports alert-driven autonomous intervention by checking firing alert rules.
"""

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

PRODUCTION_ALERT_RULE_UID = "cfxbt56wwbocge"
CALLSHEET_FOLDER_UID = "callsheet-folder"
CALLSHEET_RULE_GROUP = "callsheet-farm"


async def list_alert_rules(toolset) -> List[Dict[str, Any]]:
    """Lists all alert rules currently configured on the Grafana stack."""
    res = await toolset._execute_with_session(
        lambda session: session.call_tool("alerting_manage_rules", arguments={"operation": "list"}),
        "List alert rules"
    )
    if hasattr(res, "content") and res.content:
        for item in res.content:
            text = getattr(item, "text", "")
            if text:
                try:
                    data = json.loads(text)
                    if isinstance(data, list):
                        return data
                except Exception:
                    pass
    return []


async def get_alert_rule(toolset, rule_uid: str) -> Dict[str, Any]:
    """Retrieves full rule details including active firing alerts for a rule UID."""
    res = await toolset._execute_with_session(
        lambda session: session.call_tool(
            "alerting_manage_rules",
            arguments={"operation": "get", "rule_uid": rule_uid}
        ),
        f"Get alert rule {rule_uid}"
    )
    if hasattr(res, "content") and res.content:
        for item in res.content:
            text = getattr(item, "text", "")
            if text:
                try:
                    return json.loads(text)
                except Exception:
                    return {"raw_text": text}
    return {}


async def ensure_alert_rule(toolset, deployment_id: str) -> Dict[str, Any]:
    """
    Ensures that an alert rule for the specified deployment_id exists.
    Fires strictly when a node is both over 90C and rendering (has an active shot allocated).
    When quarantined, the node's active shot drops to 0 and the alert clears.
    """
    expected_title = f"Callsheet: render node thermal limit ({deployment_id})"
    expr_query = (
        f'max by (node_id) (render_farm_node_temperature_celsius{{deployment_id="{deployment_id}"}} > 90) '
        f'and on (node_id) (render_farm_node_active_shot{{deployment_id="{deployment_id}"}} == 1)'
    )
    expected_summary = "Render node {{ $labels.node_id }} over 90C with a shot allocated"
    target_uid = PRODUCTION_ALERT_RULE_UID if deployment_id == "cloud-run" else None

    # Check if target rule already exists
    existing_rule = None
    if target_uid:
        rule = await get_alert_rule(toolset, target_uid)
        if rule and rule.get("uid") == target_uid:
            existing_rule = rule
    else:
        all_rules = await list_alert_rules(toolset)
        for r in all_rules:
            if r.get("title") == expected_title:
                uid = r.get("uid")
                if uid:
                    existing_rule = await get_alert_rule(toolset, uid)
                    target_uid = uid
                    break

    if existing_rule:
        curr_expr = ""
        try:
            curr_expr = existing_rule.get("data", [{}])[0].get("model", {}).get("expr", "")
        except Exception:
            pass
        curr_summary = existing_rule.get("annotations", {}).get("summary", "")
        if "render_farm_node_active_shot" in curr_expr and curr_summary == expected_summary:
            return existing_rule

        # Recreate rule to update condition and annotation with identical UID
        logger.info("Updating existing rule %s with new active_shot condition...", target_uid)
        try:
            await toolset._execute_with_session(
                lambda session: session.call_tool(
                    "alerting_manage_rules",
                    arguments={"operation": "delete", "rule_uid": target_uid}
                ),
                f"Delete rule {target_uid} for update"
            )
        except Exception as ex:
            logger.warning("Could not delete rule %s for update: %s", target_uid, ex)

    logger.info("Provisioning Grafana alert rule '%s' in folder '%s'...", expected_title, CALLSHEET_FOLDER_UID)
    rule_data = [
        {
            "datasourceUid": "grafanacloud-prom",
            "model": {
                "expr": expr_query,
                "instant": True,
                "intervalMs": 1000,
                "maxDataPoints": 43200,
                "refId": "A",
            },
            "refId": "A",
            "relativeTimeRange": {"from": 60},
        },
        {
            "datasourceUid": "__expr__",
            "model": {
                "conditions": [
                    {
                        "evaluator": {"params": [0], "type": "gt"},
                        "operator": {"type": "and"},
                        "query": {"params": ["A"]},
                        "reducer": {"type": "last"},
                    }
                ],
                "expression": "A",
                "intervalMs": 1000,
                "maxDataPoints": 43200,
                "refId": "B",
                "type": "threshold",
            },
            "refId": "B",
            "relativeTimeRange": {},
        },
    ]

    create_args: Dict[str, Any] = {
        "operation": "create",
        "org_id": 1,
        "title": expected_title,
        "folder_uid": CALLSHEET_FOLDER_UID,
        "rule_group": CALLSHEET_RULE_GROUP,
        "condition": "B",
        "no_data_state": "OK",
        "exec_err_state": "Error",
        "for": "0s",
        "labels": {
            "severity": "critical",
            "team": "callsheet",
            "deployment_id": deployment_id,
        },
        "annotations": {
            "summary": expected_summary,
        },
        "data": rule_data,
    }
    if target_uid:
        create_args["rule_uid"] = target_uid

    res = await toolset._execute_with_session(
        lambda session: session.call_tool("alerting_manage_rules", arguments=create_args),
        f"Create alert rule {expected_title}"
    )

    if hasattr(res, "content") and res.content:
        for item in res.content:
            text = getattr(item, "text", "")
            if text:
                try:
                    created_data = json.loads(text)
                    uid = created_data.get("uid")
                    if uid:
                        return created_data
                except Exception:
                    pass

    all_rules = await list_alert_rules(toolset)
    for r in all_rules:
        if r.get("title") == expected_title:
            uid = r.get("uid")
            if uid:
                return await get_alert_rule(toolset, uid)

    raise RuntimeError(f"Failed to create alert rule for deployment_id '{deployment_id}'")


async def delete_alert_rule(toolset, rule_uid: str) -> bool:
    """Deletes an alert rule by UID. Used by test fixtures to maintain clean stack."""
    if rule_uid == PRODUCTION_ALERT_RULE_UID:
        logger.warning("Refusing to delete production alert rule %s", PRODUCTION_ALERT_RULE_UID)
        return False

    try:
        await toolset._execute_with_session(
            lambda session: session.call_tool(
                "alerting_manage_rules",
                arguments={"operation": "delete", "rule_uid": rule_uid}
            ),
            f"Delete alert rule {rule_uid}"
        )
        return True
    except Exception as ex:
        logger.warning("Failed to delete alert rule %s: %s", rule_uid, ex)
        return False
