"""Tests for POST /drivers/claim-trial.

This endpoint is the path that silently lost 208 trials in April 2026
(fire-and-forget call from the app + RLS silencio on trial_claims).
These tests guard the abuse-prevention branches (device_id, IP, disposable
email, already-has-plan, driver-not-found) so regressions are caught in CI.
"""

from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import FAKE_DRIVER_ID, FAKE_USER_ID


def _mock_driver_select(mock_sb, promo_plan=None):
    """Helper: wire the drivers.select().eq().single().execute() chain."""
    result = MagicMock()
    result.data = {"id": FAKE_DRIVER_ID, "promo_plan": promo_plan}
    mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = result
    return result


class TestClaimTrial:
    """POST /drivers/claim-trial"""

    @pytest.mark.asyncio
    async def test_invalid_json_returns_400(self, client):
        response = await client.post(
            "/drivers/claim-trial",
            content="not-json-{{{",
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 400
        assert "Invalid JSON" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_missing_device_id_returns_400(self, client):
        response = await client.post("/drivers/claim-trial", json={})
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_short_device_id_returns_400(self, client):
        response = await client.post(
            "/drivers/claim-trial", json={"device_id": "abc"},  # <8 chars
        )
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_disposable_email_denied(self, client, fake_user):
        # Override email to a disposable domain
        fake_user["email"] = "user@mailinator.com"
        with patch("main.DISPOSABLE_EMAIL_DOMAINS", {"mailinator.com"}):
            response = await client.post(
                "/drivers/claim-trial", json={"device_id": "device12345"},
            )
        assert response.status_code == 200
        data = response.json()
        assert data["granted"] is False
        assert data["reason"] == "disposable_email"

    @pytest.mark.asyncio
    async def test_driver_not_found_returns_404(self, client):
        with patch("main.DISPOSABLE_EMAIL_DOMAINS", set()), \
             patch("main.supabase") as mock_sb:
            # drivers.select returns no data
            driver_result = MagicMock()
            driver_result.data = None
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = driver_result

            response = await client.post(
                "/drivers/claim-trial", json={"device_id": "device12345"},
            )
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_already_has_plan_denied(self, client):
        with patch("main.DISPOSABLE_EMAIL_DOMAINS", set()), \
             patch("main.supabase") as mock_sb:
            _mock_driver_select(mock_sb, promo_plan="pro")

            response = await client.post(
                "/drivers/claim-trial", json={"device_id": "device12345"},
            )
        assert response.status_code == 200
        data = response.json()
        assert data["granted"] is False
        assert data["reason"] == "already_has_plan"

    @pytest.mark.asyncio
    async def test_device_already_claimed_denied(self, client):
        # The endpoint hits drivers.select.single().execute() for the driver
        # lookup, then drivers.select().eq().execute() for the trial_claims
        # device lookup. Both chains diverge after .eq — we return the driver
        # on the .single path and a populated .data on the non-single path.
        with patch("main.DISPOSABLE_EMAIL_DOMAINS", set()), \
             patch("main.supabase") as mock_sb:
            driver_result = MagicMock()
            driver_result.data = {"id": FAKE_DRIVER_ID, "promo_plan": None}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = driver_result

            # Existing trial claim with this device_id
            claim_result = MagicMock()
            claim_result.data = [{"id": "claim-1", "driver_id": "other-driver"}]
            mock_sb.table.return_value.select.return_value.eq.return_value.execute.return_value = claim_result

            response = await client.post(
                "/drivers/claim-trial", json={"device_id": "device12345"},
            )
        assert response.status_code == 200
        data = response.json()
        assert data["granted"] is False
        assert data["reason"] == "device_already_claimed"

    @pytest.mark.asyncio
    async def test_grants_trial_on_happy_path(self, client):
        """No abuse flags + driver without plan → trial granted."""
        with patch("main.DISPOSABLE_EMAIL_DOMAINS", set()), \
             patch("main.supabase") as mock_sb:
            driver_result = MagicMock()
            driver_result.data = {"id": FAKE_DRIVER_ID, "promo_plan": None}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = driver_result

            # No existing claims (for device + IP checks)
            no_claims = MagicMock()
            no_claims.data = []
            mock_sb.table.return_value.select.return_value.eq.return_value.execute.return_value = no_claims
            mock_sb.table.return_value.select.return_value.eq.return_value.gte.return_value.execute.return_value = no_claims

            response = await client.post(
                "/drivers/claim-trial", json={"device_id": "device12345"},
            )
        assert response.status_code == 200
        data = response.json()
        assert data["granted"] is True
        assert data["plan"] == "pro"
        assert "expires_at" in data
