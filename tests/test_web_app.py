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
async def test_mission_panel_png_endpoint():
    """Verify mission panel png endpoint serves registered image bytes or 404."""
    from callsheet.agent.mission import MISSION_PANEL_IMAGES

    test_id = "test_mission_img_123"
    fake_png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDRtest"
    MISSION_PANEL_IMAGES[test_id] = fake_png

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # 200 for registered image
        res = await client.get(f"/api/missions/{test_id}/panel.png")
        assert res.status_code == 200
        assert res.headers["content-type"] == "image/png"
        assert res.content == fake_png

        # 404 for unknown image
        res_404 = await client.get("/api/missions/unknown_id/panel.png")
        assert res_404.status_code == 404

    MISSION_PANEL_IMAGES.pop(test_id, None)
