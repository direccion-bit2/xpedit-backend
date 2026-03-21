"""
Tests for the in-memory rate limiting system:
  - check_rate_limit raises 429 after exceeding max_requests
  - Requests within limit succeed
  - Window resets after window_seconds expire
  - Middleware applies rate limits to different endpoint groups
"""

from unittest.mock import patch

import pytest
from fastapi import HTTPException

from main import _rate_limits, check_rate_limit

# ==========================================================================
# Unit tests for check_rate_limit function
# ==========================================================================

class TestCheckRateLimit:
    """Direct unit tests for the check_rate_limit function."""

    def test_requests_within_limit_succeed(self):
        """Requests below max_requests should not raise."""
        key = "test:within-limit"
        for i in range(5):
            check_rate_limit(key, max_requests=5, window_seconds=60)
        # All 5 calls succeeded without raising

    def test_raises_429_after_exceeding_max_requests(self):
        """Once max_requests is reached, the next call should raise 429."""
        key = "test:exceed-limit"
        # Fill up to the limit
        for _ in range(3):
            check_rate_limit(key, max_requests=3, window_seconds=60)

        # Next request should raise 429
        with pytest.raises(HTTPException) as exc_info:
            check_rate_limit(key, max_requests=3, window_seconds=60)

        assert exc_info.value.status_code == 429
        assert "solicitudes" in exc_info.value.detail.lower()

    def test_window_resets_after_expiry(self):
        """After window_seconds pass, the counter should reset."""
        key = "test:window-reset"
        # Use a very short window
        for _ in range(3):
            check_rate_limit(key, max_requests=3, window_seconds=1)

        # At this point we are at the limit; next call would fail
        with pytest.raises(HTTPException):
            check_rate_limit(key, max_requests=3, window_seconds=1)

        # Simulate time passing by manipulating the stored timestamps
        # Push all timestamps back beyond the window
        _rate_limits[key] = [t - 2.0 for t in _rate_limits[key]]

        # Now the same key should accept requests again
        check_rate_limit(key, max_requests=3, window_seconds=1)

    def test_different_keys_are_independent(self):
        """Rate limits for different keys do not interfere."""
        key_a = "test:key-a"
        key_b = "test:key-b"

        # Exhaust key_a
        for _ in range(2):
            check_rate_limit(key_a, max_requests=2, window_seconds=60)

        with pytest.raises(HTTPException):
            check_rate_limit(key_a, max_requests=2, window_seconds=60)

        # key_b should still work
        check_rate_limit(key_b, max_requests=2, window_seconds=60)

    def test_single_request_succeeds(self):
        """A single request should always succeed."""
        check_rate_limit("test:single", max_requests=1, window_seconds=60)

    def test_exact_limit_boundary(self):
        """Exactly max_requests calls succeed; max_requests+1 fails."""
        key = "test:boundary"
        max_req = 10
        for _ in range(max_req):
            check_rate_limit(key, max_requests=max_req, window_seconds=60)

        with pytest.raises(HTTPException) as exc_info:
            check_rate_limit(key, max_requests=max_req, window_seconds=60)
        assert exc_info.value.status_code == 429


# ==========================================================================
# Integration tests: rate limiting via middleware on actual endpoints
# ==========================================================================

class TestRateLimitMiddleware:
    """Tests that the middleware applies rate limiting to real endpoints."""

    @pytest.mark.asyncio
    async def test_optimize_endpoint_rate_limited(self, client):
        """The /optimize endpoint has a 10 req/min limit via middleware."""
        with patch("main.hybrid_optimize_route") as mock_opt:
            mock_opt.return_value = {
                "success": True,
                "route": [],
                "total_distance_km": 0,
                "total_distance_meters": 0,
                "num_stops": 0,
                "solver": "none",
            }
            payload = {"locations": [{"lat": 40.0, "lng": -3.0}], "start_index": 0}

            # Send 10 requests (the limit for /optimize)
            for _ in range(10):
                resp = await client.post("/optimize", json=payload)
                assert resp.status_code == 200

            # 11th request should be rate limited
            resp = await client.post("/optimize", json=payload)
            assert resp.status_code == 429

    @pytest.mark.asyncio
    async def test_fleet_login_rate_limited(self, client):
        """The /fleet/login endpoint has a strict 5 req/min limit."""
        with patch("main.supabase") as mock_sb:
            # Make fleet/login return an auth error each time (we just need status != 429)
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.side_effect = Exception("not found")

            for _ in range(5):
                resp = await client.post(
                    "/fleet/login",
                    json={"email": "test@test.com", "password": "pass"},
                )
                # These will fail with 500 (auth error), but NOT 429
                assert resp.status_code != 429

            # 6th request should be rate limited
            resp = await client.post(
                "/fleet/login",
                json={"email": "test@test.com", "password": "pass"},
            )
            assert resp.status_code == 429
