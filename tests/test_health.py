"""
Tests for the /health and / (root) endpoints.
"""

import pytest
from unittest.mock import MagicMock, patch


# === Root Endpoint Tests ===

class TestRootEndpoint:
    """Tests for GET /"""

    @pytest.mark.asyncio
    async def test_root_returns_200(self, client):
        response = await client.get("/")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_root_response_structure(self, client):
        response = await client.get("/")
        data = response.json()
        assert data["status"] == "ok"
        assert data["service"] == "Xpedit API"
        assert "version" in data
        assert "stripe_ok" in data
        assert "jwks_ok" in data


# === Health Endpoint Tests ===

class TestHealthEndpoint:
    """Tests for GET /health"""

    @pytest.mark.asyncio
    async def test_health_returns_200_when_db_ok(self, client):
        """Health check should return 200 when database is reachable."""
        # The supabase mock's table().select().limit().execute() returns
        # a result with .data=[] and .count=0 by default, which is fine --
        # the health endpoint just needs the call to not throw.
        with patch("main.supabase") as mock_sb:
            mock_result = MagicMock()
            mock_result.count = 5
            mock_sb.table.return_value.select.return_value.limit.return_value.execute.return_value = mock_result
            # Also need scheduler mock
            with patch("main.social_scheduler") as mock_sched:
                mock_sched.running = True
                response = await client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_health_response_structure(self, client):
        """Health check response should contain checks dict with expected keys."""
        with patch("main.supabase") as mock_sb:
            mock_result = MagicMock()
            mock_result.count = 3
            mock_sb.table.return_value.select.return_value.limit.return_value.execute.return_value = mock_result
            with patch("main.social_scheduler") as mock_sched:
                mock_sched.running = False
                response = await client.get("/health")

        data = response.json()
        assert "checks" in data
        checks = data["checks"]
        assert "database" in checks
        assert "sentry" in checks
        assert "scheduler" in checks
        assert "version" in checks
        assert "uptime_seconds" in checks
        assert "environment" in checks

    @pytest.mark.asyncio
    async def test_health_version_matches(self, client):
        """Version in health check should be 1.1.3."""
        with patch("main.supabase") as mock_sb:
            mock_result = MagicMock()
            mock_result.count = 0
            mock_sb.table.return_value.select.return_value.limit.return_value.execute.return_value = mock_result
            with patch("main.social_scheduler") as mock_sched:
                mock_sched.running = False
                response = await client.get("/health")

        data = response.json()
        assert data["checks"]["version"] == "1.1.3"

    @pytest.mark.asyncio
    async def test_health_returns_503_when_db_fails(self, client):
        """Health check should return 503 when database query fails."""
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.select.return_value.limit.return_value.execute.side_effect = Exception("DB down")
            with patch("main.social_scheduler") as mock_sched:
                mock_sched.running = True
                response = await client.get("/health")

        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "degraded"
        assert data["checks"]["database"]["status"] == "error"

    @pytest.mark.asyncio
    async def test_health_sentry_not_configured(self, client):
        """Sentry should show as not_configured when DSN is empty."""
        with patch("main.supabase") as mock_sb:
            mock_result = MagicMock()
            mock_result.count = 0
            mock_sb.table.return_value.select.return_value.limit.return_value.execute.return_value = mock_result
            with patch("main.social_scheduler") as mock_sched:
                mock_sched.running = False
                with patch("main.SENTRY_DSN", ""):
                    response = await client.get("/health")

        data = response.json()
        assert data["checks"]["sentry"]["status"] == "not_configured"
