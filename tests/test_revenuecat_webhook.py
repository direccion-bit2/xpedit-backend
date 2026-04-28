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

    @pytest.mark.asyncio
    async def test_yearly_purchase_writes_subscription_period(self, unauth_client, rc_headers):
        """product_id 'xpedit_pro_yearly' → subscription_period='yearly'."""
        event = _purchase_event(event_id="evt-yearly-1")
        event["event"]["product_id"] = "xpedit_pro_yearly"
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
        payload = mock_sb.table.return_value.update.call_args_list[0][0][0]
        assert payload.get("subscription_period") == "yearly"

    @pytest.mark.asyncio
    async def test_monthly_purchase_writes_subscription_period(self, unauth_client, rc_headers):
        """product_id 'xpedit_pro_monthly' → subscription_period='monthly'."""
        event = _purchase_event(event_id="evt-monthly-1")  # default product_id is monthly
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
        payload = mock_sb.table.return_value.update.call_args_list[0][0][0]
        assert payload.get("subscription_period") == "monthly"

    @pytest.mark.asyncio
    async def test_expiration_clears_subscription_period(self, unauth_client, rc_headers):
        """EXPIRATION → subscription_period cleared to NULL."""
        event = _purchase_event(event_id="evt-exp-period")
        event["event"]["type"] = "EXPIRATION"
        event["event"]["product_id"] = "xpedit_pro_yearly"
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
        payload = mock_sb.table.return_value.update.call_args_list[0][0][0]
        assert payload.get("subscription_period") is None

    @pytest.mark.asyncio
    async def test_yearly_without_expiration_uses_365_days(self, unauth_client, rc_headers):
        """If RevenueCat omits expiration_at_ms (rare), yearly product fallback
        is 365 days, not 30 — otherwise app would mark user as expired in a month
        despite paying for a year."""
        event = _purchase_event(event_id="evt-yearly-no-exp")
        event["event"]["product_id"] = "xpedit_pro_yearly"
        # expiration_at_ms is None (not provided)
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
        payload = mock_sb.table.return_value.update.call_args_list[0][0][0]
        from datetime import datetime, timezone
        expires_at = datetime.fromisoformat(payload["promo_plan_expires_at"])
        days_diff = (expires_at - datetime.now(timezone.utc)).days
        # Allow 1-day slack for clock drift / test runtime
        assert 363 <= days_diff <= 366, f"yearly fallback should be ~365 days, got {days_diff}"
