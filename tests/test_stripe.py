"""
Tests for Stripe endpoints in main.py:
  - POST /stripe/create-checkout
  - POST /stripe/webhook (all event types)
  - POST /stripe/portal

Focuses on edge cases, error paths, and deeper behavior verification
beyond the basic coverage in test_endpoints.py.
"""

from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import FAKE_USER_ID

# ===================== /stripe/create-checkout =====================


class TestStripeCreateCheckout:
    """Extended tests for POST /stripe/create-checkout"""

    @pytest.mark.asyncio
    async def test_checkout_pro_plus_plan(self, client):
        """Verify pro_plus plan also works."""
        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"), \
             patch("main.STRIPE_PLANS", {
                 "pro": {"name": "Pro", "price_id": "price_pro"},
                 "pro_plus": {"name": "Pro+", "price_id": "price_pro_plus"},
             }), \
             patch("main.supabase") as mock_sb, \
             patch("main.stripe") as mock_stripe:
            user_result = MagicMock()
            user_result.data = {"email": "user@test.com"}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = user_result

            mock_session = MagicMock()
            mock_session.url = "https://checkout.stripe.com/session_456"
            mock_stripe.checkout.Session.create.return_value = mock_session

            response = await client.post("/stripe/create-checkout", json={"plan": "pro_plus"})
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["url"] == "https://checkout.stripe.com/session_456"

    @pytest.mark.asyncio
    async def test_checkout_price_id_not_configured(self, client):
        """When price_id is empty string, return 503."""
        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"), \
             patch("main.STRIPE_PLANS", {"pro": {"name": "Pro", "price_id": ""}}):
            response = await client.post("/stripe/create-checkout", json={"plan": "pro"})
        assert response.status_code == 503

    @pytest.mark.asyncio
    async def test_checkout_without_user_email(self, client):
        """Checkout works even if user email lookup fails."""
        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"), \
             patch("main.STRIPE_PLANS", {"pro": {"name": "Pro", "price_id": "price_123"}}), \
             patch("main.supabase") as mock_sb, \
             patch("main.stripe") as mock_stripe:
            # Simulate email lookup failure
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.side_effect = Exception("DB error")

            mock_session = MagicMock()
            mock_session.url = "https://checkout.stripe.com/no_email"
            mock_stripe.checkout.Session.create.return_value = mock_session

            response = await client.post("/stripe/create-checkout", json={"plan": "pro"})
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_checkout_stripe_error(self, client):
        """Stripe API error returns 500."""
        import stripe as stripe_mod

        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"), \
             patch("main.STRIPE_PLANS", {"pro": {"name": "Pro", "price_id": "price_123"}}), \
             patch("main.supabase") as mock_sb, \
             patch("main.stripe") as mock_stripe:
            user_result = MagicMock()
            user_result.data = {"email": "user@test.com"}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = user_result

            mock_stripe.StripeError = stripe_mod.StripeError
            mock_stripe.checkout.Session.create.side_effect = stripe_mod.StripeError("API down")

            response = await client.post("/stripe/create-checkout", json={"plan": "pro"})
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_checkout_user_email_empty(self, client):
        """When user has no email in DB, checkout proceeds without customer_email."""
        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"), \
             patch("main.STRIPE_PLANS", {"pro": {"name": "Pro", "price_id": "price_123"}}), \
             patch("main.supabase") as mock_sb, \
             patch("main.stripe") as mock_stripe:
            user_result = MagicMock()
            user_result.data = {"email": None}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = user_result

            mock_session = MagicMock()
            mock_session.url = "https://checkout.stripe.com/no_cust_email"
            mock_stripe.checkout.Session.create.return_value = mock_session

            response = await client.post("/stripe/create-checkout", json={"plan": "pro"})
        assert response.status_code == 200
        # Verify customer_email was not passed to Stripe
        call_kwargs = mock_stripe.checkout.Session.create.call_args
        assert "customer_email" not in call_kwargs.kwargs

    @pytest.mark.asyncio
    async def test_checkout_missing_plan_field(self, client):
        """Missing plan field returns 422 (validation error)."""
        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"):
            response = await client.post("/stripe/create-checkout", json={})
        assert response.status_code == 422


# ===================== /stripe/webhook =====================


class TestStripeWebhookExtended:
    """Extended tests for POST /stripe/webhook"""

    @pytest.mark.asyncio
    async def test_webhook_invalid_signature(self, client):
        """Invalid Stripe signature returns 400."""
        import stripe as stripe_mod

        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe:
            mock_stripe.SignatureVerificationError = stripe_mod.SignatureVerificationError
            mock_stripe.Webhook.construct_event.side_effect = stripe_mod.SignatureVerificationError("bad sig", "header")

            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json", "stripe-signature": "bad_sig"}
            )
        assert response.status_code == 400
        assert "Invalid signature" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_webhook_invalid_payload(self, client):
        """Malformed payload returns 400."""
        import stripe as stripe_mod

        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe:
            mock_stripe.SignatureVerificationError = stripe_mod.SignatureVerificationError
            mock_stripe.Webhook.construct_event.side_effect = ValueError("bad payload")

            response = await client.post(
                "/stripe/webhook",
                content=b'not-json',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_webhook_duplicate_event_idempotent(self, client):
        """Duplicate event IDs are processed only once."""
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe, \
             patch("main.supabase") as mock_sb, \
             patch("main._processed_webhook_events", {"evt_dup": True}):

            mock_data_obj = MagicMock()
            mock_data_obj.client_reference_id = FAKE_USER_ID
            mock_data_obj.metadata = {"plan": "pro"}
            mock_data_obj.customer = "cus_123"

            mock_event = MagicMock()
            mock_event.type = "checkout.session.completed"
            mock_event.id = "evt_dup"
            mock_event.data.object = mock_data_obj
            mock_event.get.return_value = "evt_dup"
            mock_stripe.Webhook.construct_event.return_value = mock_event

            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 200
        assert response.json()["status"] == "already_processed"

    @pytest.mark.asyncio
    async def test_webhook_invoice_payment_succeeded_renewal(self, client):
        """invoice.payment_succeeded with billing_reason=subscription_cycle extends plan."""
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe, \
             patch("main.supabase") as mock_sb, \
             patch("main._processed_webhook_events", {}):

            mock_data_obj = MagicMock()
            mock_data_obj.customer = "cus_renew"
            mock_data_obj.billing_reason = "subscription_cycle"

            mock_event = MagicMock()
            mock_event.type = "invoice.payment_succeeded"
            mock_event.id = "evt_renew"
            mock_event.data.object = mock_data_obj
            mock_event.get.return_value = "evt_renew"
            mock_stripe.Webhook.construct_event.return_value = mock_event

            user_result = MagicMock()
            user_result.data = [{"id": FAKE_USER_ID}]

            def table_dispatch(name):
                chain = MagicMock()
                chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = user_result
                chain.update.return_value.eq.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 200
        assert response.json()["received"] is True

    @pytest.mark.asyncio
    async def test_webhook_invoice_payment_succeeded_not_renewal(self, client):
        """invoice.payment_succeeded with billing_reason != subscription_cycle is a no-op."""
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe, \
             patch("main.supabase") as mock_sb, \
             patch("main._processed_webhook_events", {}):

            mock_data_obj = MagicMock()
            mock_data_obj.customer = "cus_new"
            mock_data_obj.billing_reason = "subscription_create"

            mock_event = MagicMock()
            mock_event.type = "invoice.payment_succeeded"
            mock_event.id = "evt_create"
            mock_event.data.object = mock_data_obj
            mock_event.get.return_value = "evt_create"
            mock_stripe.Webhook.construct_event.return_value = mock_event

            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_webhook_invoice_payment_failed(self, client):
        """invoice.payment_failed logs warning and sends alert on attempt >= 2."""
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe, \
             patch("main.supabase") as mock_sb, \
             patch("main._processed_webhook_events", {}), \
             patch("main.sentry_sdk") as mock_sentry, \
             patch("main.send_alert_email") as mock_alert:

            mock_data_obj = MagicMock()
            mock_data_obj.customer = "cus_fail"
            mock_data_obj.attempt_count = 3

            mock_event = MagicMock()
            mock_event.type = "invoice.payment_failed"
            mock_event.id = "evt_fail"
            mock_event.data.object = mock_data_obj
            mock_event.get.return_value = "evt_fail"
            mock_stripe.Webhook.construct_event.return_value = mock_event

            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 200
        mock_sentry.capture_message.assert_called_once()
        mock_alert.assert_called_once()

    @pytest.mark.asyncio
    async def test_webhook_invoice_payment_failed_first_attempt(self, client):
        """invoice.payment_failed with attempt_count < 2 does not send alert email."""
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe, \
             patch("main.supabase") as mock_sb, \
             patch("main._processed_webhook_events", {}), \
             patch("main.sentry_sdk") as mock_sentry, \
             patch("main.send_alert_email") as mock_alert:

            mock_data_obj = MagicMock()
            mock_data_obj.customer = "cus_fail"
            mock_data_obj.attempt_count = 1

            mock_event = MagicMock()
            mock_event.type = "invoice.payment_failed"
            mock_event.id = "evt_fail_1"
            mock_event.data.object = mock_data_obj
            mock_event.get.return_value = "evt_fail_1"
            mock_stripe.Webhook.construct_event.return_value = mock_event

            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 200
        mock_alert.assert_not_called()

    @pytest.mark.asyncio
    async def test_webhook_unhandled_event_type(self, client):
        """Unhandled event types are silently accepted."""
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe, \
             patch("main._processed_webhook_events", {}):

            mock_event = MagicMock()
            mock_event.type = "payment_intent.created"
            mock_event.id = "evt_unknown"
            mock_event.data.object = MagicMock()
            mock_event.get.return_value = "evt_unknown"
            mock_stripe.Webhook.construct_event.return_value = mock_event

            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 200
        assert response.json()["received"] is True

    @pytest.mark.asyncio
    async def test_webhook_checkout_no_user_id(self, client):
        """checkout.session.completed with no client_reference_id should not crash."""
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe, \
             patch("main.supabase") as mock_sb, \
             patch("main._processed_webhook_events", {}):

            mock_data_obj = MagicMock()
            mock_data_obj.client_reference_id = None
            mock_data_obj.metadata = {"plan": "pro"}
            mock_data_obj.customer = "cus_orphan"

            mock_event = MagicMock()
            mock_event.type = "checkout.session.completed"
            mock_event.id = "evt_no_uid"
            mock_event.data.object = mock_data_obj
            mock_event.get.return_value = "evt_no_uid"
            mock_stripe.Webhook.construct_event.return_value = mock_event

            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 200
        # supabase.table().update() should not be called since user_id is None
        mock_sb.table.assert_not_called()

    @pytest.mark.asyncio
    async def test_webhook_subscription_deleted_no_matching_user(self, client):
        """customer.subscription.deleted with no matching user is a no-op."""
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe, \
             patch("main.supabase") as mock_sb, \
             patch("main._processed_webhook_events", {}):

            mock_data_obj = MagicMock()
            mock_data_obj.customer = "cus_unknown"

            mock_event = MagicMock()
            mock_event.type = "customer.subscription.deleted"
            mock_event.id = "evt_del_unknown"
            mock_event.data.object = mock_data_obj
            mock_event.get.return_value = "evt_del_unknown"
            mock_stripe.Webhook.construct_event.return_value = mock_event

            user_result = MagicMock()
            user_result.data = []  # No matching user

            def table_dispatch(name):
                chain = MagicMock()
                chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = user_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_webhook_processing_error_returns_500(self, client):
        """Internal error during webhook processing returns 500."""
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe, \
             patch("main.supabase") as mock_sb, \
             patch("main._processed_webhook_events", {}), \
             patch("main.sentry_sdk") as mock_sentry:

            mock_data_obj = MagicMock()
            mock_data_obj.client_reference_id = FAKE_USER_ID
            mock_data_obj.metadata = {"plan": "pro"}
            mock_data_obj.customer = "cus_err"

            mock_event = MagicMock()
            mock_event.type = "checkout.session.completed"
            mock_event.id = "evt_err"
            mock_event.data.object = mock_data_obj
            mock_event.get.return_value = "evt_err"
            mock_stripe.Webhook.construct_event.return_value = mock_event

            # Simulate supabase error during update
            def table_dispatch(name):
                chain = MagicMock()
                chain.update.return_value.eq.return_value.execute.side_effect = Exception("DB down")
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 500
        mock_sentry.capture_exception.assert_called_once()


# ===================== /stripe/portal =====================


class TestStripePortalExtended:
    """Extended tests for POST /stripe/portal"""

    @pytest.mark.asyncio
    async def test_portal_stripe_api_error(self, client):
        """Stripe API error during portal creation returns 500."""
        import stripe as stripe_mod

        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"), \
             patch("main.supabase") as mock_sb, \
             patch("main.stripe") as mock_stripe:
            user_result = MagicMock()
            user_result.data = {"stripe_customer_id": "cus_123"}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = user_result

            mock_stripe.StripeError = stripe_mod.StripeError
            mock_stripe.billing_portal.Session.create.side_effect = stripe_mod.StripeError("Stripe down")

            response = await client.post("/stripe/portal")
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_portal_db_error(self, client):
        """Database error when looking up customer returns 500."""
        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"), \
             patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.side_effect = Exception("DB timeout")

            response = await client.post("/stripe/portal")
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_portal_no_data_returned(self, client):
        """When user lookup returns None data, customer_id is None and returns 404."""
        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"), \
             patch("main.supabase") as mock_sb:
            user_result = MagicMock()
            user_result.data = None
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = user_result

            response = await client.post("/stripe/portal")
        assert response.status_code == 404
