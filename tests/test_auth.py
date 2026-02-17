"""
Tests for auth-related endpoints:
  - DELETE /auth/delete-account
  - Unauthenticated access (auth required endpoints return 401)
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from tests.conftest import FAKE_USER_ID, FAKE_DRIVER_ID


class TestAuthRequired:
    """Endpoints that require authentication should return 401 without a token."""

    @pytest.mark.asyncio
    async def test_optimize_requires_auth(self, unauth_client):
        response = await unauth_client.post("/optimize", json={
            "locations": [{"lat": 40.0, "lng": -3.0}]
        })
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_routes_requires_auth(self, unauth_client):
        response = await unauth_client.get("/routes")
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_drivers_requires_auth(self, unauth_client):
        response = await unauth_client.get("/drivers")
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_delete_account_requires_auth(self, unauth_client):
        response = await unauth_client.delete("/auth/delete-account")
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_stats_daily_requires_auth(self, unauth_client):
        response = await unauth_client.get("/stats/daily")
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_referral_code_requires_auth(self, unauth_client):
        response = await unauth_client.get("/referral/code")
        assert response.status_code == 401


class TestDeleteAccount:
    """Tests for DELETE /auth/delete-account"""

    @pytest.mark.asyncio
    async def test_delete_account_success(self, client):
        """Deleting an account should return 200 with deletion confirmation."""
        with patch("main.supabase") as mock_sb:
            # Driver lookup
            driver_result = MagicMock()
            driver_result.data = [{"id": FAKE_DRIVER_ID}]

            # Routes lookup (no routes)
            routes_result = MagicMock()
            routes_result.data = []

            # Chain: table("drivers").select("id").eq("user_id", ...).execute()
            drivers_chain = MagicMock()
            drivers_chain.select.return_value.eq.return_value.execute.return_value = driver_result

            # Make table() return the right chain depending on table name
            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.execute.return_value = driver_result
                    chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                elif name == "routes":
                    chain.select.return_value.eq.return_value.execute.return_value = routes_result
                    chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                else:
                    chain.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
                    chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            mock_sb.auth.admin.delete_user = MagicMock(return_value=True)

            response = await client.delete("/auth/delete-account")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "deleted"
        assert "eliminada" in data["message"].lower() or "deleted" in data["message"].lower()

    @pytest.mark.asyncio
    async def test_delete_account_no_driver(self, client):
        """Should still succeed if user has no driver profile."""
        with patch("main.supabase") as mock_sb:
            # No driver found for user
            driver_result = MagicMock()
            driver_result.data = []

            def table_dispatch(name):
                chain = MagicMock()
                chain.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
                chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            mock_sb.auth.admin.delete_user = MagicMock(return_value=True)

            response = await client.delete("/auth/delete-account")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "deleted"

    @pytest.mark.asyncio
    async def test_delete_account_with_routes(self, client):
        """Should delete routes, stops, and related data."""
        with patch("main.supabase") as mock_sb:
            # Driver lookup
            driver_result = MagicMock()
            driver_result.data = [{"id": FAKE_DRIVER_ID}]

            # Routes with stops
            routes_result = MagicMock()
            routes_result.data = [{"id": "route-1"}, {"id": "route-2"}]

            stops_result = MagicMock()
            stops_result.data = [{"id": "stop-1"}, {"id": "stop-2"}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.execute.return_value = driver_result
                    chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                elif name == "routes":
                    chain.select.return_value.eq.return_value.execute.return_value = routes_result
                    chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                elif name == "stops":
                    chain.select.return_value.eq.return_value.execute.return_value = stops_result
                    chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                else:
                    chain.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
                    chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            mock_sb.auth.admin.delete_user = MagicMock(return_value=True)

            response = await client.delete("/auth/delete-account")

        assert response.status_code == 200
        assert response.json()["status"] == "deleted"
