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
    For deployment_id == 'cloud-run', uses the verified production rule UID.
    For local test environments, creates the rule if not already present.
    """
    if deployment_id == "cloud-run":
        rule = await get_alert_rule(toolset, PRODUCTION_ALERT_RULE_UID)
        if rule and rule.get("uid") == PRODUCTION_ALERT_RULE_UID:
            return rule

    expected_title = f"Callsheet: render node thermal limit ({deployment_id})"
    all_rules = await list_alert_rules(toolset)
    for r in all_rules:
        if r.get("title") == expected_title:
            uid = r.get("uid")
            if uid:
                return await get_alert_rule(toolset, uid)

    logger.info("Creating Grafana alert rule '%s' in folder '%s'...", expected_title, CALLSHEET_FOLDER_UID)
    expr_query = f'max by (node_id) (render_farm_node_temperature_celsius{{deployment_id="{deployment_id}"}})'
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
                        "evaluator": {"params": [90], "type": "gt"},
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

    create_args = {
        "operation": "create",
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
            "summary": "Render node {{ $labels.node_id }} junction temperature exceeded 90C thermal limit",
        },
        "data": rule_data,
    }

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
