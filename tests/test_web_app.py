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

        # Index HTML
        res_html = await client.get("/")
        assert res_html.status_code == 200
        assert "Callsheet" in res_html.text


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
