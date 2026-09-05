"""
Tests for Callsheet FastAPI web application and API endpoints.
"""

import pytest
from httpx import ASGITransport, AsyncClient
from callsheet.web.app import app


@pytest.mark.asyncio
async def test_health_and_state_endpoints():
    """Verify health and state endpoints return valid data."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # Health check
        res_health = await client.get("/api/health")
        assert res_health.status_code == 200
        data_health = res_health.json()
        assert data_health["status"] == "healthy"

        # State check
        res_state = await client.get("/api/state")
        assert res_state.status_code == 200
        data_state = res_state.json()
        assert "shows" in data_state
        assert "nodes" in data_state
        assert len(data_state["nodes"]) == 12
        assert "latest_mission" in data_state
        assert data_state["latest_mission"] is not None

        # Producer Dashboard HTML
        res_html = await client.get("/")
        assert res_html.status_code == 200
        assert "Callsheet" in res_html.text
        assert "Active Delivery Slate" in res_html.text

        # Demo Harness HTML
        res_demo = await client.get("/demo")
        assert res_demo.status_code == 200
        assert "Demonstration Control Surface" in res_demo.text


@pytest.mark.asyncio
async def test_scenario_injection_endpoint():
    """Verify scenario injection endpoint modulates farm state."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.post("/api/scenario", json={"scenario": "THERMAL_THROTTLING"})
        assert res.status_code == 200
        data = res.json()
        assert data["scenario"] == "THERMAL_THROTTLING"

        # Verify state reflects throttling
        res_state = await client.get("/api/state")
        data_state = res_state.json()
        assert data_state["active_scenario"] == "THERMAL_THROTTLING"

        # Restore baseline
        res_base = await client.post("/api/scenario", json={"scenario": "BASELINE"})
        assert res_base.status_code == 200


@pytest.mark.asyncio
async def test_mission_panel_png_endpoint(monkeypatch):
    """Verify mission panel png endpoint serves registered image bytes, dynamic render, or 404."""
    from callsheet.agent.mission import MISSION_PANEL_IMAGES
    from callsheet.web.app import mission_runner

    test_id = "test_mission_img_123"
    fake_png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDRtest"
    MISSION_PANEL_IMAGES[test_id] = fake_png

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # 200 for registered image
        res = await client.get(f"/api/missions/{test_id}/panel.png")
        assert res.status_code == 200
        assert res.headers["content-type"] == "image/png"
        assert res.content == fake_png

        # Dynamic re-render on cache miss
        res_dyn = await client.get("/api/missions/unknown_id/panel.png")
        assert res_dyn.status_code in (200, 404)
        if res_dyn.status_code == 200:
            assert res_dyn.headers["content-type"] == "image/png"

        # 404 when dynamic render fails
        async def mock_fail_render(*args, **kwargs):
            return None

        monkeypatch.setattr(mission_runner, "render_panel_image", mock_fail_render)
        MISSION_PANEL_IMAGES.pop("unknown_id_fail", None)
        res_404 = await client.get("/api/missions/unknown_id_fail/panel.png")
        assert res_404.status_code == 404

    MISSION_PANEL_IMAGES.pop(test_id, None)
    MISSION_PANEL_IMAGES.pop("unknown_id", None)


@pytest.mark.asyncio
async def test_alert_rule_health_fail_closed(monkeypatch):
    """
    Section 3d: Verify that if the Grafana alert rule cannot be created or read,
    /api/health reports status: 'degraded' with alert_rule_status: 'error'.
    When healthy, it reports status: 'healthy' with alert_rule_status: 'ok'.
    """
    from callsheet.web.app import worker

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # 1. Error state (fails closed, degraded)
        monkeypatch.setattr(worker, "alert_rule_status", "error")
        monkeypatch.setattr(worker, "alert_rule_error", "connection refused to mcp")
        res_deg = await client.get("/api/health")
        assert res_deg.status_code == 200
        data_deg = res_deg.json()
        assert data_deg["status"] == "degraded"
        assert data_deg["alert_rule_status"] == "error"
        assert data_deg["alert_rule_error"] == "connection refused to mcp"

        # 2. Healthy state
        monkeypatch.setattr(worker, "alert_rule_status", "ok")
        monkeypatch.setattr(worker, "alert_rule_error", None)
        res_ok = await client.get("/api/health")
        assert res_ok.status_code == 200
        data_ok = res_ok.json()
        assert data_ok["status"] == "healthy"
        assert data_ok["alert_rule_status"] == "ok"


@pytest.mark.asyncio
async def test_alert_pending_and_cleared_badge():
    """Verify alert_pending in state and ALERT CLEARED badge rendering on incident card."""
    from callsheet.web.app import worker
    from callsheet.farm.models import ScenarioType

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # 1. Baseline: alert_pending should be False
        await client.post("/api/scenario", json={"scenario": "BASELINE"})
        res = await client.get("/api/state")
        data = res.json()
        assert data["alert_pending"] is False

        # 2. Thermal throttling: alert_pending should become True
        await client.post("/api/scenario", json={"scenario": "THERMAL_THROTTLING"})
        res_throttled = await client.get("/api/state")
        data_throttled = res_throttled.json()
        assert data_throttled["alert_pending"] is True

        # SSR should show GRAFANA ALERT PENDING
        res_html = await client.get("/")
        assert "GRAFANA ALERT PENDING" in res_html.text

        # 3. Verify ALERT CLEARED badge rendering in SSR when latest_mission has quarantine_to_alert_cleared_seconds
        sample_mission = dict(worker.latest_mission)
        sample_mission["incident_id"] = "999"
        sample_mission["incident_status"] = "resolved"
        sample_mission["time_intervention_to_resolved_seconds"] = 38.5
        sample_mission["quarantine_to_alert_cleared_seconds"] = 22.4
        sample_mission["panel_image_url"] = "/api/missions/sample/panel.png"
        sample_mission["mcp_read_calls"] = 41
        sample_mission["mcp_write_calls"] = 8
        worker.latest_mission = sample_mission

        res_mission_html = await client.get("/")
        assert "ALERT CLEARED IN 22.4s" in res_mission_html.text
        assert "MISSION MCP TOOL CALLS: 41 READS / 8 WRITES" in res_mission_html.text

        # Restore baseline
        await client.post("/api/scenario", json={"scenario": "BASELINE"})

