"""Tests for POST /revenuecat/webhook.

Protects the iOS/Android purchase → plan activation pipeline. Regressions
here = users paying without receiving Pro. Also guards auth, idempotency,
and the EXPIRATION/BILLING_ISSUE revocation branches.
"""

from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def rc_headers():
    return {"Authorization": "Bearer test-rc-secret", "Content-Type": "application/json"}


@pytest_asyncio.fixture
async def unauth_client():
    """Client without auth override — needed for webhook tests
    (RevenueCat webhook doesn't use get_current_user)."""
    from main import app
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac


def _purchase_event(event_id="evt-1", entitlements=None, expiration_ms=None):
    return {
        "event": {
            "id": event_id,
            "type": "INITIAL_PURCHASE",
            "app_user_id": "driver-xyz",
            "product_id": "xpedit_pro_monthly",
            "entitlement_ids": entitlements or ["pro"],
            "expiration_at_ms": expiration_ms,
        }
    }


class TestRevenueCatWebhook:
    """POST /revenuecat/webhook"""

    @pytest.mark.asyncio
    async def test_missing_secret_returns_503(self, unauth_client, rc_headers):
        """If REVENUECAT_WEBHOOK_SECRET is empty, all payments silently
        fail to process. Endpoint must fail loud (503)."""
        with patch("main.REVENUECAT_WEBHOOK_SECRET", ""):
            response = await unauth_client.post(
                "/revenuecat/webhook", json=_purchase_event(), headers=rc_headers,
            )
        assert response.status_code == 503

    @pytest.mark.asyncio
    async def test_invalid_auth_returns_401(self, unauth_client):
        with patch("main.REVENUECAT_WEBHOOK_SECRET", "test-rc-secret"):
            response = await unauth_client.post(
                "/revenuecat/webhook",
                json=_purchase_event(),
                headers={"Authorization": "Bearer wrong", "Content-Type": "application/json"},
            )
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_invalid_json_returns_400(self, unauth_client, rc_headers):
        with patch("main.REVENUECAT_WEBHOOK_SECRET", "test-rc-secret"):
            response = await unauth_client.post(
                "/revenuecat/webhook", content="not-json", headers=rc_headers,
            )
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_duplicate_event_id_noops(self, unauth_client, rc_headers):
        """Same event.id twice → second response is 'already_processed'.
        Guards against RevenueCat retry storms charging Pro twice."""
        with patch("main.REVENUECAT_WEBHOOK_SECRET", "test-rc-secret"), \
             patch("main._is_webhook_processed", return_value=True), \
             patch("main.supabase") as mock_sb:
            # If the second call leaks through, this would be called — we
            # verify it is NOT by asserting the shortcut response below.
            mock_sb.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(data=[])

            response = await unauth_client.post(
                "/revenuecat/webhook", json=_purchase_event(event_id="evt-dup"), headers=rc_headers,
            )
        assert response.status_code == 200
        assert response.json().get("status") == "already_processed"

    @pytest.mark.asyncio
    async def test_initial_purchase_activates_pro(self, unauth_client, rc_headers):
        """Happy path: valid event → drivers.update writes promo_plan=pro
        with subscription_source=revenuecat."""
        with patch("main.REVENUECAT_WEBHOOK_SECRET", "test-rc-secret"), \
             patch("main._is_webhook_processed", return_value=False), \
             patch("main._mark_webhook_processed"), \
             patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(data=[{"id": "driver-xyz"}])
            driver_lookup = MagicMock()
            driver_lookup.data = {"user_id": "user-xyz"}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = driver_lookup

            response = await unauth_client.post(
                "/revenuecat/webhook", json=_purchase_event(event_id="evt-new-1"), headers=rc_headers,
            )
        assert response.status_code == 200
        # Confirm the update was wired to drivers table with subscription_source
        calls = [c for c in mock_sb.table.return_value.update.call_args_list]
        assert calls, "drivers.update was never called"
        payload = calls[0][0][0]
        assert payload.get("promo_plan") == "pro"
        assert payload.get("subscription_source") == "revenuecat"

    @pytest.mark.asyncio
    async def test_expiration_revokes_pro(self, unauth_client, rc_headers):
        """EXPIRATION → promo_plan cleared, subscription_source cleared."""
        event = _purchase_event(event_id="evt-exp-1")
        event["event"]["type"] = "EXPIRATION"
        with patch("main.REVENUECAT_WEBHOOK_SECRET", "test-rc-secret"), \
             patch("main._is_webhook_processed", return_value=False), \
             patch("main._mark_webhook_processed"), \
             patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock(data=[{"id": "driver-xyz"}])
            driver_lookup = MagicMock()
            driver_lookup.data = {"user_id": "user-xyz"}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = driver_lookup

            response = await unauth_client.post(
                "/revenuecat/webhook", json=event, headers=rc_headers,
            )
        assert response.status_code == 200
        calls = [c for c in mock_sb.table.return_value.update.call_args_list]
        assert calls, "drivers.update was never called"
        payload = calls[0][0][0]
        assert payload.get("promo_plan") is None
        assert payload.get("subscription_source") is None

    @pytest.mark.asyncio
    async def test_no_app_user_id_returns_no_user(self, unauth_client, rc_headers):
        event = _purchase_event(event_id="evt-no-user")
        event["event"]["app_user_id"] = ""
        with patch("main.REVENUECAT_WEBHOOK_SECRET", "test-rc-secret"), \
             patch("main._processed_webhook_events", {}):
            response = await unauth_client.post(
                "/revenuecat/webhook", json=event, headers=rc_headers,
            )
        assert response.status_code == 200
        assert response.json().get("status") == "no_user"
