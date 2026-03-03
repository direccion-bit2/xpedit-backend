"""
Tests for uncovered endpoint groups in main.py:
  - Stripe: /stripe/create-checkout, /stripe/webhook, /stripe/portal
  - Drivers: /drivers, /drivers/{id}, /drivers/{id}/push-token
  - Location: /location, /location/{driver_id}/latest, /location/{driver_id}/history
  - Email: /email/welcome, /email/delivery-started, /email/delivery-completed,
           /email/delivery-failed, /email/daily-summary
  - Company: /company/register, /company/{id}, /company/{id}/drivers, etc.
  - Referral: /referral/code, /referral/redeem, /referral/stats
  - Places: /places/autocomplete, /places/details, /places/directions, /places/snap
  - OCR: /ocr/label
  - Stops: /stops/{id}/complete, /stops/{id}/fail
  - Promo: /promo/redeem, /promo/check/{id}
  - Stats: /stats/daily
  - Download: /download/apk
  - Notifications: /notifications/customer/send
  - Admin email: /admin/users/{id}/send-email, /admin/broadcast-email,
                 /admin/reengagement-broadcast, /admin/push-blast
  - Admin: /admin/audit-log, /admin/drivers/{id}/features,
           /admin/companies/{id} (toggle)
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import FAKE_DRIVER_ID, FAKE_USER_ID

# ===================== STRIPE ENDPOINTS =====================

class TestStripeCheckout:
    """Tests for POST /stripe/create-checkout"""

    @pytest.mark.asyncio
    async def test_create_checkout_success(self, client):
        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"), \
             patch("main.STRIPE_PLANS", {"pro": {"name": "Pro", "price_id": "price_123"}}), \
             patch("main.supabase") as mock_sb, \
             patch("main.stripe") as mock_stripe:
            user_result = MagicMock()
            user_result.data = {"email": "user@test.com"}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = user_result

            mock_session = MagicMock()
            mock_session.url = "https://checkout.stripe.com/session_123"
            mock_stripe.checkout.Session.create.return_value = mock_session

            response = await client.post("/stripe/create-checkout", json={"plan": "pro"})
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "url" in data

    @pytest.mark.asyncio
    async def test_create_checkout_invalid_plan(self, client):
        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"):
            response = await client.post("/stripe/create-checkout", json={"plan": "invalid"})
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_create_checkout_stripe_not_configured(self, client):
        with patch("main.STRIPE_SECRET_KEY", ""):
            response = await client.post("/stripe/create-checkout", json={"plan": "pro"})
        assert response.status_code == 503


class TestStripePortal:
    """Tests for POST /stripe/portal"""

    @pytest.mark.asyncio
    async def test_portal_success(self, client):
        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"), \
             patch("main.supabase") as mock_sb, \
             patch("main.stripe") as mock_stripe:
            user_result = MagicMock()
            user_result.data = {"stripe_customer_id": "cus_123"}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = user_result

            mock_session = MagicMock()
            mock_session.url = "https://billing.stripe.com/session_123"
            mock_stripe.billing_portal.Session.create.return_value = mock_session

            response = await client.post("/stripe/portal")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "url" in data

    @pytest.mark.asyncio
    async def test_portal_no_customer(self, client):
        with patch("main.STRIPE_SECRET_KEY", "sk_test_fake"), \
             patch("main.supabase") as mock_sb:
            user_result = MagicMock()
            user_result.data = {"stripe_customer_id": None}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = user_result

            response = await client.post("/stripe/portal")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_portal_stripe_not_configured(self, client):
        with patch("main.STRIPE_SECRET_KEY", ""):
            response = await client.post("/stripe/portal")
        assert response.status_code == 503


class TestStripeWebhook:
    """Tests for POST /stripe/webhook"""

    @pytest.mark.asyncio
    async def test_webhook_missing_signature(self, client):
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"):
            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json"}
            )
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_webhook_not_configured(self, client):
        with patch("main.STRIPE_WEBHOOK_SECRET", ""):
            response = await client.post(
                "/stripe/webhook",
                content=b'{}',
                headers={"content-type": "application/json", "stripe-signature": "test"}
            )
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_webhook_checkout_completed(self, client):
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe, \
             patch("main.supabase") as mock_sb, \
             patch("main._processed_webhook_events", {}):

            mock_data_obj = MagicMock()
            mock_data_obj.client_reference_id = FAKE_USER_ID
            mock_data_obj.metadata = {"plan": "pro"}
            mock_data_obj.customer = "cus_123"

            mock_event = MagicMock()
            mock_event.type = "checkout.session.completed"
            mock_event.id = "evt_test_123"
            mock_event.data.object = mock_data_obj
            mock_event.get.return_value = "evt_test_123"
            mock_stripe.Webhook.construct_event.return_value = mock_event

            def table_dispatch(name):
                chain = MagicMock()
                chain.update.return_value.eq.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post(
                "/stripe/webhook",
                content=b'{"type": "checkout.session.completed"}',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 200
        assert response.json()["received"] is True

    @pytest.mark.asyncio
    async def test_webhook_subscription_deleted(self, client):
        with patch("main.STRIPE_WEBHOOK_SECRET", "whsec_test"), \
             patch("main.stripe") as mock_stripe, \
             patch("main.supabase") as mock_sb, \
             patch("main._processed_webhook_events", {}):

            mock_data_obj = MagicMock()
            mock_data_obj.customer = "cus_123"

            mock_event = MagicMock()
            mock_event.type = "customer.subscription.deleted"
            mock_event.id = "evt_test_del"
            mock_event.data.object = mock_data_obj
            mock_event.get.return_value = "evt_test_del"
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
                content=b'{"type": "customer.subscription.deleted"}',
                headers={"content-type": "application/json", "stripe-signature": "test_sig"}
            )
        assert response.status_code == 200


# ===================== DRIVER ENDPOINTS =====================

class TestDriverEndpoints:
    """Tests for /drivers and /drivers/{id}"""

    @pytest.mark.asyncio
    async def test_list_drivers(self, client):
        with patch("main.supabase") as mock_sb:
            drivers_result = MagicMock()
            drivers_result.data = [{"id": "d1", "name": "Driver 1"}]
            mock_sb.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = drivers_result

            response = await client.get("/drivers")
        assert response.status_code == 200
        assert "drivers" in response.json()

    @pytest.mark.asyncio
    async def test_get_driver(self, client):
        with patch("main.supabase") as mock_sb:
            driver_access = MagicMock()
            driver_access.data = [{"id": FAKE_DRIVER_ID, "user_id": FAKE_USER_ID}]

            driver_result = MagicMock()
            driver_result.data = {"id": FAKE_DRIVER_ID, "name": "Test Driver"}

            call_count = {"drivers": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    call_count["drivers"] += 1
                    if call_count["drivers"] <= 2:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_access
                    else:
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = driver_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get(f"/drivers/{FAKE_DRIVER_ID}")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_update_push_token(self, client):
        with patch("main.supabase") as mock_sb:
            driver_result = MagicMock()
            driver_result.data = [{"id": FAKE_DRIVER_ID, "user_id": FAKE_USER_ID}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.execute.return_value = driver_result
                    chain.update.return_value.eq.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.put(
                f"/drivers/{FAKE_DRIVER_ID}/push-token",
                json={"push_token": "ExponentPushToken[abc123]"}
            )
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_update_push_token_wrong_driver(self, client):
        with patch("main.supabase") as mock_sb:
            driver_result = MagicMock()
            driver_result.data = [{"id": "other-driver", "user_id": "other-user"}]

            def table_dispatch(name):
                chain = MagicMock()
                chain.select.return_value.eq.return_value.execute.return_value = driver_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.put(
                "/drivers/other-driver/push-token",
                json={"push_token": "ExponentPushToken[abc123]"}
            )
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_update_push_token_not_found(self, client):
        with patch("main.supabase") as mock_sb:
            empty_result = MagicMock()
            empty_result.data = []

            def table_dispatch(name):
                chain = MagicMock()
                chain.select.return_value.eq.return_value.execute.return_value = empty_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.put(
                "/drivers/nonexistent/push-token",
                json={"push_token": "ExponentPushToken[abc123]"}
            )
        assert response.status_code == 404


# ===================== LOCATION ENDPOINTS =====================

class TestLocationEndpoints:
    """Tests for location tracking endpoints"""

    @pytest.mark.asyncio
    async def test_update_location(self, client):
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            location_result = MagicMock()
            location_result.data = [{"id": "loc-1"}]

            call_count = {"drivers": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "location_history":
                    chain.insert.return_value.execute.return_value = location_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post("/location", json={
                "driver_id": FAKE_DRIVER_ID,
                "lat": 40.416,
                "lng": -3.703
            })
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_get_latest_location(self, client):
        with patch("main.supabase") as mock_sb:
            # verify_driver_access needs driver lookup
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            location_result = MagicMock()
            location_result.data = [{"id": "loc-1", "lat": 40.416, "lng": -3.703}]

            call_count = {"calls": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "location_history":
                    chain.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = location_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get(f"/location/{FAKE_DRIVER_ID}/latest")
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_get_location_history(self, client):
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            history_result = MagicMock()
            history_result.data = [
                {"id": "loc-1", "lat": 40.416, "lng": -3.703},
                {"id": "loc-2", "lat": 40.417, "lng": -3.704},
            ]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "location_history":
                    chain.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = history_result
                    chain.select.return_value.eq.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = history_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get(f"/location/{FAKE_DRIVER_ID}/history")
        assert response.status_code == 200
        assert "locations" in response.json()


# ===================== EMAIL API ENDPOINTS =====================

class TestEmailEndpoints:
    """Tests for /email/* endpoints"""

    @pytest.mark.asyncio
    async def test_send_welcome_email(self, client):
        with patch("main.send_welcome_email", return_value={"success": True, "id": "e1"}):
            response = await client.post("/email/welcome", json={
                "to_email": "user@test.com",
                "user_name": "Test User"
            })
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_send_welcome_email_failure(self, client):
        with patch("main.send_welcome_email", return_value={"success": False, "error": "fail"}):
            response = await client.post("/email/welcome", json={
                "to_email": "user@test.com",
                "user_name": "Test User"
            })
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_send_delivery_started_email(self, client):
        with patch("main.send_delivery_started_email", return_value={"success": True, "id": "e2"}):
            response = await client.post("/email/delivery-started", json={
                "to_email": "client@test.com",
                "client_name": "Client",
                "driver_name": "Driver"
            })
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_send_delivery_completed_email(self, client):
        with patch("main.send_delivery_completed_email", return_value={"success": True, "id": "e3"}):
            response = await client.post("/email/delivery-completed", json={
                "to_email": "client@test.com",
                "client_name": "Client",
                "delivery_time": "14:30"
            })
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_send_delivery_failed_email(self, client):
        with patch("main.send_delivery_failed_email", return_value={"success": True, "id": "e4"}):
            response = await client.post("/email/delivery-failed", json={
                "to_email": "client@test.com",
                "client_name": "Client"
            })
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_send_daily_summary_email(self, client):
        with patch("main.send_daily_summary_email", return_value={"success": True, "id": "e5"}):
            response = await client.post("/email/daily-summary", json={
                "to_email": "dispatcher@test.com",
                "dispatcher_name": "Boss",
                "date": "2026-03-01",
                "total_routes": 10,
                "total_stops": 50,
                "completed_stops": 45,
                "failed_stops": 5
            })
        assert response.status_code == 200


# ===================== STOP ENDPOINTS =====================

class TestStopEndpoints:
    """Tests for /stops/{id}/complete and /stops/{id}/fail"""

    @pytest.mark.asyncio
    async def test_complete_stop(self, client):
        with patch("main.supabase") as mock_sb:
            stop_result = MagicMock()
            stop_result.data = [{"id": "stop-1", "route_id": "route-1"}]

            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]

            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            update_result = MagicMock()
            update_result.data = [{"id": "stop-1", "status": "completed"}]

            call_count = {"stops": 0, "routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "stops":
                    call_count["stops"] += 1
                    if call_count["stops"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = stop_result
                    else:
                        chain.update.return_value.eq.return_value.execute.return_value = update_result
                elif name == "routes":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.patch("/stops/stop-1/complete")
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_fail_stop(self, client):
        with patch("main.supabase") as mock_sb:
            stop_result = MagicMock()
            stop_result.data = [{"id": "stop-1", "route_id": "route-1"}]

            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]

            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            update_result = MagicMock()
            update_result.data = [{"id": "stop-1", "status": "failed"}]

            call_count = {"stops": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "stops":
                    call_count["stops"] += 1
                    if call_count["stops"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = stop_result
                    else:
                        chain.update.return_value.eq.return_value.execute.return_value = update_result
                elif name == "routes":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.patch("/stops/stop-1/fail")
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_complete_stop_not_found(self, client):
        with patch("main.supabase") as mock_sb:
            empty_result = MagicMock()
            empty_result.data = []

            def table_dispatch(name):
                chain = MagicMock()
                chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = empty_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.patch("/stops/nonexistent/complete")
        assert response.status_code == 404


# ===================== REFERRAL ENDPOINTS =====================

class TestReferralEndpoints:
    """Tests for /referral/*"""

    @pytest.mark.asyncio
    async def test_get_referral_code_existing(self, client):
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            code_result = MagicMock()
            code_result.data = {"referral_code": "XPD-ABCD"}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                    chain.select.return_value.eq.return_value.single.return_value.execute.return_value = code_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/referral/code")
        assert response.status_code == 200
        assert response.json()["code"] == "XPD-ABCD"

    @pytest.mark.asyncio
    async def test_get_referral_stats(self, client):
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            referrals_result = MagicMock()
            referrals_result.data = [{"id": "r1"}, {"id": "r2"}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "referrals":
                    chain.select.return_value.eq.return_value.execute.return_value = referrals_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/referral/stats")
        assert response.status_code == 200
        assert response.json()["total_referrals"] == 2


# ===================== PLACES ENDPOINTS =====================

class TestPlacesEndpoints:
    """Tests for /places/* proxy endpoints"""

    @pytest.mark.asyncio
    async def test_places_autocomplete_success(self, client):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "OK",
            "predictions": [{"description": "Madrid, Spain"}]
        }

        mock_http = AsyncMock()
        mock_http.get.return_value = mock_response
        mock_http.__aenter__.return_value = mock_http
        mock_http.__aexit__.return_value = False

        with patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get("/places/autocomplete?input=Madrid")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_places_details(self, client):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "OK",
            "result": {"geometry": {"location": {"lat": 40.416, "lng": -3.703}}}
        }

        mock_http = AsyncMock()
        mock_http.get.return_value = mock_response
        mock_http.__aenter__.return_value = mock_http
        mock_http.__aexit__.return_value = False

        with patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get("/places/details?place_id=ChIJgTwKgJcpQg0RaSKMYcHeNsQ")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_places_directions(self, client):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "OK",
            "routes": [{"overview_polyline": {"points": "abc"}}]
        }

        mock_http = AsyncMock()
        mock_http.get.return_value = mock_response
        mock_http.__aenter__.return_value = mock_http
        mock_http.__aexit__.return_value = False

        with patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get("/places/directions?origin=40.416,-3.703&destination=40.453,-3.688")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_places_snap(self, client):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "OK",
            "results": [{"geometry": {"location": {"lat": 40.416, "lng": -3.703}}, "formatted_address": "Sol, Madrid"}]
        }

        mock_http = AsyncMock()
        mock_http.get.return_value = mock_response
        mock_http.__aenter__.return_value = mock_http
        mock_http.__aexit__.return_value = False

        with patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get("/places/snap?lat=40.416&lng=-3.703")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "OK"


# ===================== OCR ENDPOINT =====================

class TestOCREndpoint:
    """Tests for POST /ocr/label"""

    @pytest.mark.asyncio
    async def test_ocr_not_configured(self, client):
        with patch("main.ANTHROPIC_API_KEY", ""):
            response = await client.post("/ocr/label", json={
                "image_base64": "base64data",
                "media_type": "image/jpeg"
            })
        assert response.status_code == 503

    @pytest.mark.asyncio
    async def test_ocr_success(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"text": '{"name":"Test","street":"Calle 1","city":"Madrid","postalCode":"28001","province":"Madrid"}'}]
        }

        mock_http = AsyncMock()
        mock_http.post.return_value = mock_response
        mock_http.__aenter__.return_value = mock_http
        mock_http.__aexit__.return_value = False

        with patch("main.ANTHROPIC_API_KEY", "test-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",
                "media_type": "image/jpeg"
            })
        assert response.status_code == 200
        assert response.json()["success"] is True


# ===================== STATS ENDPOINT =====================

class TestDailyStats:
    """Tests for GET /stats/daily"""

    @pytest.mark.asyncio
    async def test_daily_stats_no_driver(self, client):
        with patch("main.supabase") as mock_sb:
            no_driver = MagicMock()
            no_driver.data = []

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = no_driver
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/stats/daily")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["routes"]["total"] == 0

    @pytest.mark.asyncio
    async def test_daily_stats_with_routes(self, client):
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            routes_result = MagicMock()
            routes_result.data = [
                {
                    "id": "r1", "status": "completed", "total_distance_km": 15.2,
                    "stops": [
                        {"id": "s1", "status": "completed"},
                        {"id": "s2", "status": "failed"},
                    ]
                }
            ]

            call_count = {"drivers": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "routes":
                    chain.select.return_value.gte.return_value.eq.return_value.execute.return_value = routes_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/stats/daily")
        assert response.status_code == 200


# ===================== DOWNLOAD APK =====================

class TestDownloadAPK:
    """Tests for GET /download/apk"""

    @pytest.mark.asyncio
    async def test_download_redirects(self, client):
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.insert.return_value.execute.return_value = MagicMock()

            response = await client.get("/download/apk", follow_redirects=False)
        assert response.status_code == 302


# ===================== ADMIN EMAIL ENDPOINTS =====================

class TestAdminEmailEndpoints:
    """Tests for admin email and push endpoints"""

    @pytest.mark.asyncio
    async def test_admin_send_email_to_user(self, admin_client):
        with patch("main.supabase") as mock_sb, \
             patch("main.send_custom_email", return_value={"success": True, "id": "msg1"}):
            driver_result = MagicMock()
            driver_result.data = {"email": "user@test.com", "name": "User"}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.single.return_value.execute.return_value = driver_result
                elif name == "email_log":
                    chain.insert.return_value.execute.return_value = MagicMock()
                elif name == "audit_log":
                    chain.insert.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.post(
                f"/admin/users/{FAKE_USER_ID}/send-email",
                json={"subject": "Test", "body": "<p>Test</p>"}
            )
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_admin_broadcast_email(self, admin_client):
        with patch("main.supabase") as mock_sb, \
             patch("main.send_broadcast_email", return_value={"sent": 2, "failed": 0}):
            drivers_result = MagicMock()
            drivers_result.data = [
                {"email": "a@test.com", "name": "A", "promo_plan": None},
                {"email": "b@test.com", "name": "B", "promo_plan": None},
            ]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.is_.return_value.execute.return_value = drivers_result
                    chain.select.return_value.execute.return_value = drivers_result
                elif name in ("email_log", "audit_log"):
                    chain.insert.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.post("/admin/broadcast-email", json={
                "subject": "Test Broadcast",
                "body": "<p>Hello all</p>",
                "target": "all"
            })
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_admin_reengagement_broadcast(self, admin_client):
        with patch("main.supabase") as mock_sb, \
             patch("main.send_reengagement_broadcast", return_value={"sent": 1, "failed": 0}):
            drivers_result = MagicMock()
            drivers_result.data = [
                {"id": "d1", "email": "a@test.com", "name": "A"},
            ]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.not_.is_.return_value.execute.return_value = drivers_result
                elif name in ("email_log", "audit_log"):
                    chain.insert.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.post("/admin/reengagement-broadcast")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_admin_push_blast(self, admin_client):
        with patch("main.supabase") as mock_sb, \
             patch("main.send_push_to_token", new_callable=AsyncMock, return_value=True):
            drivers_result = MagicMock()
            drivers_result.data = [
                {"id": "d1", "name": "Driver 1", "push_token": "ExponentPushToken[abc]"},
            ]
            routes_result = MagicMock()
            routes_result.data = []  # no routes = inactive

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.not_.is_.return_value.execute.return_value = drivers_result
                elif name == "routes":
                    chain.select.return_value.in_.return_value.execute.return_value = routes_result
                elif name == "audit_log":
                    chain.insert.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.post("/admin/push-blast", json={
                "title": "Test Push",
                "body": "Hello!",
                "target": "inactive"
            })
        assert response.status_code == 200
        assert response.json()["success"] is True


# ===================== ADMIN FEATURE TOGGLE =====================

class TestAdminFeatureToggle:
    """Tests for PATCH /admin/drivers/{id}/features"""

    @pytest.mark.asyncio
    async def test_toggle_ambassador(self, admin_client):
        with patch("main.supabase") as mock_sb:
            update_result = MagicMock()
            update_result.data = [{"id": "d1", "is_ambassador": True}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.update.return_value.eq.return_value.execute.return_value = update_result
                elif name == "audit_log":
                    chain.insert.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.patch("/admin/drivers/d1/features", json={
                "is_ambassador": True
            })
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_toggle_no_fields(self, admin_client):
        with patch("main.supabase"):
            response = await admin_client.patch("/admin/drivers/d1/features", json={})
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_toggle_driver_not_found(self, admin_client):
        with patch("main.supabase") as mock_sb:
            empty_result = MagicMock()
            empty_result.data = []

            def table_dispatch(name):
                chain = MagicMock()
                chain.update.return_value.eq.return_value.execute.return_value = empty_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.patch("/admin/drivers/nonexistent/features", json={
                "voice_assistant_enabled": True
            })
        assert response.status_code == 404


# ===================== ADMIN TOGGLE COMPANY =====================

class TestAdminToggleCompany:
    """Tests for PATCH /admin/companies/{id}"""

    @pytest.mark.asyncio
    async def test_toggle_company_success(self, admin_client):
        with patch("main.supabase") as mock_sb:
            update_result = MagicMock()
            update_result.data = [{"id": "c1", "active": False}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "companies":
                    chain.update.return_value.eq.return_value.execute.return_value = update_result
                elif name == "audit_log":
                    chain.insert.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.patch("/admin/companies/c1", json={
                "active": False
            })
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_toggle_company_no_fields(self, admin_client):
        with patch("main.supabase"):
            response = await admin_client.patch("/admin/companies/c1", json={})
        assert response.status_code == 400


# ===================== ADMIN AUDIT LOG =====================

class TestAdminAuditLog:
    """Tests for GET /admin/audit-log"""

    @pytest.mark.asyncio
    async def test_audit_log_success(self, admin_client):
        with patch("main.supabase") as mock_sb:
            logs_result = MagicMock()
            logs_result.data = [
                {"id": "log1", "admin_id": "admin1", "action": "grant_plan", "resource_type": "driver", "resource_id": "d1"},
            ]

            count_result = MagicMock()
            count_result.count = 1

            admins_result = MagicMock()
            admins_result.data = [{"user_id": "admin1", "name": "Admin", "email": "admin@test.com"}]

            drivers_result = MagicMock()
            drivers_result.data = [{"user_id": "d1", "name": "Driver", "email": "d@test.com"}]

            driver_by_id = MagicMock()
            driver_by_id.data = [{"id": "d1", "name": "Driver", "email": "d@test.com"}]

            call_count = {"audit_log": 0, "drivers": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "audit_log":
                    call_count["audit_log"] += 1
                    if call_count["audit_log"] == 1:
                        chain.select.return_value.order.return_value.range.return_value.execute.return_value = logs_result
                    else:
                        chain.select.return_value.execute.return_value = count_result
                elif name == "drivers":
                    call_count["drivers"] += 1
                    if call_count["drivers"] <= 1:
                        chain.select.return_value.in_.return_value.execute.return_value = admins_result
                    elif call_count["drivers"] == 2:
                        chain.select.return_value.in_.return_value.execute.return_value = drivers_result
                    else:
                        chain.select.return_value.in_.return_value.execute.return_value = driver_by_id
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.get("/admin/audit-log")
        assert response.status_code == 200
        assert response.json()["success"] is True


# ===================== NOTIFICATIONS ENDPOINT =====================

class TestCustomerNotification:
    """Tests for POST /notifications/customer/send"""

    @pytest.mark.asyncio
    async def test_send_upcoming_notification(self, client):
        with patch("main.supabase") as mock_sb, \
             patch("main.send_upcoming_email", return_value={"success": True}):
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            notif_result = MagicMock()
            notif_result.data = [{"id": "notif-1"}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "customer_notifications":
                    chain.insert.return_value.execute.return_value = notif_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post("/notifications/customer/send", json={
                "alert_type": "upcoming",
                "customer_email": "client@test.com",
                "customer_name": "Client",
                "driver_name": "Driver",
                "stop_address": "Calle Test 1",
                "stop_id": "stop-1",
                "stops_away": 3,
            })
        assert response.status_code == 200
        assert "email" in response.json()["sent_via"]


# ===================== PROMO ENDPOINTS =====================

class TestPromoEndpoints:
    """Tests for /promo/redeem and /promo/check/{driver_id}"""

    @pytest.mark.asyncio
    async def test_promo_check_no_plan(self, client):
        with patch("main.supabase") as mock_sb:
            driver_check = MagicMock()
            driver_check.data = {"user_id": FAKE_USER_ID}

            driver_result = MagicMock()
            driver_result.data = {"promo_plan": None, "promo_plan_expires_at": None, "is_ambassador": False}

            call_count = {"drivers": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    call_count["drivers"] += 1
                    if call_count["drivers"] == 1:
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = driver_check
                    else:
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = driver_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get(f"/promo/check/{FAKE_DRIVER_ID}")
        assert response.status_code == 200
        assert response.json()["has_promo"] is False


# ===================== COMPANY ENDPOINTS =====================

class TestCompanyEndpoints:
    """Tests for /company/* endpoints"""

    @pytest.mark.asyncio
    async def test_register_company_wrong_user(self, client):
        """Cannot register company for another user."""
        response = await client.post("/company/register", json={
            "name": "Test Co",
            "email": "co@test.com",
            "owner_user_id": "someone-else"
        })
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_register_company_invalid_email(self, client):
        """Invalid email should be rejected."""
        response = await client.post("/company/register", json={
            "name": "Test Co",
            "email": "invalid-email",
            "owner_user_id": FAKE_USER_ID
        })
        assert response.status_code == 400


# ===================== CLUSTER ENDPOINT =====================

class TestClusterEndpoint:
    """Tests for POST /cluster-zones"""

    @pytest.mark.asyncio
    async def test_cluster_success(self, client):
        with patch("main.cluster_stops_by_zone") as mock_cluster:
            mock_cluster.return_value = {
                "zones": [{"id": 0, "center": {"lat": 40.0, "lng": -3.0}, "stops": [], "num_stops": 3}],
                "num_zones": 1
            }
            response = await client.post("/cluster-zones", json={
                "stops": [
                    {"lat": 40.0, "lng": -3.0},
                    {"lat": 40.1, "lng": -3.1},
                    {"lat": 40.2, "lng": -3.2},
                ]
            })
        assert response.status_code == 200
        assert "zones" in response.json()

    @pytest.mark.asyncio
    async def test_cluster_too_many_stops(self, client):
        stops = [{"lat": 40.0 + i * 0.001, "lng": -3.0} for i in range(501)]
        response = await client.post("/cluster-zones", json={"stops": stops})
        assert response.status_code == 400


# ===================== ROUTE ETAS =====================

class TestRouteETAs:
    """Tests for POST /route-etas"""

    @pytest.mark.asyncio
    async def test_route_etas_success(self, client):
        with patch("main.calculate_route_etas") as mock_etas:
            mock_etas.return_value = [
                {"lat": 40.0, "lng": -3.0, "eta": "2026-03-01T10:00:00", "sequence": 1}
            ]
            response = await client.post("/route-etas", json={
                "route": [{"lat": 40.0, "lng": -3.0}]
            })
        assert response.status_code == 200
        assert response.json()["success"] is True


# ===================== ASSIGN DRIVERS =====================

class TestAssignDrivers:
    """Tests for POST /assign-drivers"""

    @pytest.mark.asyncio
    async def test_assign_drivers_success(self, client):
        with patch("main.assign_drivers_to_zones") as mock_assign:
            mock_assign.return_value = {
                "assignments": {0: "driver-1"},
                "unassigned_zones": [],
                "assigned_drivers": ["driver-1"]
            }
            response = await client.post("/assign-drivers", json={
                "zones": [{"id": 0, "center": {"lat": 40.0, "lng": -3.0}, "stops": [], "num_stops": 3}],
                "drivers": [{"id": "driver-1"}],
                "driver_routes": {}
            })
        assert response.status_code == 200
        assert response.json()["success"] is True
