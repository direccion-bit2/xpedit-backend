"""
Tests for email API endpoints in main.py (the HTTP layer):
  - POST /email/welcome
  - POST /email/delivery-started
  - POST /email/delivery-completed
  - POST /email/delivery-failed
  - POST /email/daily-summary

Focuses on edge cases, error handling, and validation beyond
the basic happy-path tests in test_endpoints.py.
"""

from unittest.mock import patch

import pytest


# ===================== /email/welcome =====================


class TestWelcomeEmailEndpoint:
    """Tests for POST /email/welcome"""

    @pytest.mark.asyncio
    async def test_success_returns_result(self, client):
        with patch("main.send_welcome_email", return_value={"success": True, "id": "msg_1"}):
            response = await client.post("/email/welcome", json={
                "to_email": "user@test.com",
                "user_name": "Test User"
            })
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["id"] == "msg_1"

    @pytest.mark.asyncio
    async def test_failure_returns_500(self, client):
        with patch("main.send_welcome_email", return_value={"success": False, "error": "Resend API down"}):
            response = await client.post("/email/welcome", json={
                "to_email": "user@test.com",
                "user_name": "User"
            })
        assert response.status_code == 500
        assert "Resend API down" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_missing_to_email_returns_422(self, client):
        response = await client.post("/email/welcome", json={
            "user_name": "User"
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_missing_user_name_returns_422(self, client):
        response = await client.post("/email/welcome", json={
            "to_email": "user@test.com"
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_empty_body_returns_422(self, client):
        response = await client.post("/email/welcome", json={})
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_calls_send_function_with_correct_args(self, client):
        with patch("main.send_welcome_email", return_value={"success": True, "id": "msg_x"}) as mock_fn:
            await client.post("/email/welcome", json={
                "to_email": "specific@example.com",
                "user_name": "Specific Name"
            })
        mock_fn.assert_called_once_with("specific@example.com", "Specific Name")


# ===================== /email/delivery-started =====================


class TestDeliveryStartedEmailEndpoint:
    """Tests for POST /email/delivery-started"""

    @pytest.mark.asyncio
    async def test_success_with_all_optional_fields(self, client):
        with patch("main.send_delivery_started_email", return_value={"success": True, "id": "msg_2"}) as mock_fn:
            response = await client.post("/email/delivery-started", json={
                "to_email": "client@test.com",
                "client_name": "Client",
                "driver_name": "Driver",
                "estimated_time": "15 minutos",
                "tracking_url": "https://track.xpedit.es/abc"
            })
        assert response.status_code == 200
        mock_fn.assert_called_once_with(
            "client@test.com", "Client", "Driver", "15 minutos", "https://track.xpedit.es/abc"
        )

    @pytest.mark.asyncio
    async def test_success_without_optional_fields(self, client):
        with patch("main.send_delivery_started_email", return_value={"success": True, "id": "msg_3"}):
            response = await client.post("/email/delivery-started", json={
                "to_email": "client@test.com",
                "client_name": "Client",
                "driver_name": "Driver"
            })
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_failure_returns_500(self, client):
        with patch("main.send_delivery_started_email", return_value={"success": False, "error": "fail"}):
            response = await client.post("/email/delivery-started", json={
                "to_email": "client@test.com",
                "client_name": "Client",
                "driver_name": "Driver"
            })
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_missing_required_driver_name(self, client):
        response = await client.post("/email/delivery-started", json={
            "to_email": "client@test.com",
            "client_name": "Client"
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_missing_required_client_name(self, client):
        response = await client.post("/email/delivery-started", json={
            "to_email": "client@test.com",
            "driver_name": "Driver"
        })
        assert response.status_code == 422


# ===================== /email/delivery-completed =====================


class TestDeliveryCompletedEmailEndpoint:
    """Tests for POST /email/delivery-completed"""

    @pytest.mark.asyncio
    async def test_success_with_all_fields(self, client):
        with patch("main.send_delivery_completed_email", return_value={"success": True, "id": "msg_4"}) as mock_fn:
            response = await client.post("/email/delivery-completed", json={
                "to_email": "client@test.com",
                "client_name": "Client",
                "delivery_time": "14:30",
                "photo_url": "https://storage.xpedit.es/proof.jpg",
                "recipient_name": "Recipient"
            })
        assert response.status_code == 200
        mock_fn.assert_called_once_with(
            "client@test.com", "Client", "14:30",
            "https://storage.xpedit.es/proof.jpg", "Recipient"
        )

    @pytest.mark.asyncio
    async def test_success_minimal_fields(self, client):
        with patch("main.send_delivery_completed_email", return_value={"success": True, "id": "msg_5"}):
            response = await client.post("/email/delivery-completed", json={
                "to_email": "client@test.com",
                "client_name": "Client",
                "delivery_time": "16:00"
            })
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_failure_returns_500(self, client):
        with patch("main.send_delivery_completed_email", return_value={"success": False, "error": "timeout"}):
            response = await client.post("/email/delivery-completed", json={
                "to_email": "client@test.com",
                "client_name": "Client",
                "delivery_time": "14:30"
            })
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_missing_delivery_time(self, client):
        response = await client.post("/email/delivery-completed", json={
            "to_email": "client@test.com",
            "client_name": "Client"
        })
        assert response.status_code == 422


# ===================== /email/delivery-failed =====================


class TestDeliveryFailedEmailEndpoint:
    """Tests for POST /email/delivery-failed"""

    @pytest.mark.asyncio
    async def test_success_with_all_fields(self, client):
        with patch("main.send_delivery_failed_email", return_value={"success": True, "id": "msg_6"}) as mock_fn:
            response = await client.post("/email/delivery-failed", json={
                "to_email": "client@test.com",
                "client_name": "Client",
                "reason": "No one home",
                "next_attempt": "Tomorrow 10:00-14:00"
            })
        assert response.status_code == 200
        mock_fn.assert_called_once_with(
            "client@test.com", "Client", "No one home", "Tomorrow 10:00-14:00"
        )

    @pytest.mark.asyncio
    async def test_success_minimal_fields(self, client):
        with patch("main.send_delivery_failed_email", return_value={"success": True, "id": "msg_7"}):
            response = await client.post("/email/delivery-failed", json={
                "to_email": "client@test.com",
                "client_name": "Client"
            })
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_failure_returns_500(self, client):
        with patch("main.send_delivery_failed_email", return_value={"success": False, "error": "API error"}):
            response = await client.post("/email/delivery-failed", json={
                "to_email": "client@test.com",
                "client_name": "Client"
            })
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_missing_client_name(self, client):
        response = await client.post("/email/delivery-failed", json={
            "to_email": "client@test.com"
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_failure_error_message_in_detail(self, client):
        """The error message from the email function should appear in the response detail."""
        with patch("main.send_delivery_failed_email", return_value={"success": False, "error": "Rate limited"}):
            response = await client.post("/email/delivery-failed", json={
                "to_email": "client@test.com",
                "client_name": "Client"
            })
        assert response.status_code == 500
        assert "Rate limited" in response.json()["detail"]


# ===================== /email/daily-summary =====================


class TestDailySummaryEmailEndpoint:
    """Tests for POST /email/daily-summary"""

    @pytest.mark.asyncio
    async def test_success(self, client):
        with patch("main.send_daily_summary_email", return_value={"success": True, "id": "msg_8"}) as mock_fn:
            response = await client.post("/email/daily-summary", json={
                "to_email": "dispatcher@test.com",
                "dispatcher_name": "Boss",
                "date": "2026-03-07",
                "total_routes": 5,
                "total_stops": 30,
                "completed_stops": 28,
                "failed_stops": 2
            })
        assert response.status_code == 200
        mock_fn.assert_called_once_with(
            "dispatcher@test.com", "Boss", "2026-03-07", 5, 30, 28, 2
        )

    @pytest.mark.asyncio
    async def test_failure_returns_500(self, client):
        with patch("main.send_daily_summary_email", return_value={"success": False, "error": "fail"}):
            response = await client.post("/email/daily-summary", json={
                "to_email": "dispatcher@test.com",
                "dispatcher_name": "Boss",
                "date": "2026-03-07",
                "total_routes": 0,
                "total_stops": 0,
                "completed_stops": 0,
                "failed_stops": 0
            })
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_missing_date_returns_422(self, client):
        response = await client.post("/email/daily-summary", json={
            "to_email": "dispatcher@test.com",
            "dispatcher_name": "Boss",
            "total_routes": 5,
            "total_stops": 30,
            "completed_stops": 28,
            "failed_stops": 2
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_missing_numeric_fields_returns_422(self, client):
        response = await client.post("/email/daily-summary", json={
            "to_email": "dispatcher@test.com",
            "dispatcher_name": "Boss",
            "date": "2026-03-07"
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_zero_stats_accepted(self, client):
        """A daily summary with all zeros is a valid request."""
        with patch("main.send_daily_summary_email", return_value={"success": True, "id": "msg_zeros"}):
            response = await client.post("/email/daily-summary", json={
                "to_email": "dispatcher@test.com",
                "dispatcher_name": "Boss",
                "date": "2026-03-07",
                "total_routes": 0,
                "total_stops": 0,
                "completed_stops": 0,
                "failed_stops": 0
            })
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_failure_without_error_key(self, client):
        """When the email function returns success=False without an error key, a default message is used."""
        with patch("main.send_daily_summary_email", return_value={"success": False}):
            response = await client.post("/email/daily-summary", json={
                "to_email": "dispatcher@test.com",
                "dispatcher_name": "Boss",
                "date": "2026-03-07",
                "total_routes": 1,
                "total_stops": 1,
                "completed_stops": 1,
                "failed_stops": 0
            })
        assert response.status_code == 500
        assert "Error enviando email" in response.json()["detail"]
