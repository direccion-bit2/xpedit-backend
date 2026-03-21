"""
Tests for the Supabase Auth webhook endpoint in main.py:
  - POST /webhooks/supabase-auth

Covers: valid INSERT payload sends welcome email, invalid secret returns 401,
non-INSERT events are skipped, missing email is skipped, email log insertion,
driver name lookup, and error handling.
"""

from unittest.mock import MagicMock, patch

import pytest

# ===================== POST /webhooks/supabase-auth =====================


class TestSupabaseAuthWebhook:
    """Tests for POST /webhooks/supabase-auth"""

    @pytest.mark.asyncio
    async def test_valid_insert_sends_welcome_email(self, client):
        """INSERT event with email sends welcome email and logs it."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"), \
             patch("main.supabase") as mock_sb, \
             patch("main.send_welcome_email", return_value={"success": True, "id": "msg_welcome"}) as mock_send:

            driver_result = MagicMock()
            driver_result.data = [{"name": "Test User"}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.execute.return_value = driver_result
                elif name == "email_log":
                    chain.insert.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "INSERT",
                    "record": {"email": "newuser@test.com"}
                },
                headers={"x-supabase-webhook-secret": "test-secret"}
            )
        assert response.status_code == 200
        data = response.json()
        assert data["received"] is True
        assert data["email_sent"] is True
        mock_send.assert_called_once_with("newuser@test.com", "Test User")

    @pytest.mark.asyncio
    async def test_invalid_secret_returns_401(self, client):
        """Wrong webhook secret returns 401."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "correct-secret"):
            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "INSERT",
                    "record": {"email": "newuser@test.com"}
                },
                headers={"x-supabase-webhook-secret": "wrong-secret"}
            )
        assert response.status_code == 401
        assert "Invalid" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_missing_secret_header_returns_401(self, client):
        """Missing x-supabase-webhook-secret header returns 401."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "correct-secret"):
            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "INSERT",
                    "record": {"email": "newuser@test.com"}
                }
            )
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_webhook_secret_not_configured_returns_500(self, client):
        """When SUPABASE_WEBHOOK_SECRET is not configured, return 500."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", ""):
            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "INSERT",
                    "record": {"email": "newuser@test.com"}
                },
                headers={"x-supabase-webhook-secret": "anything"}
            )
        assert response.status_code == 500
        assert "not configured" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_non_insert_event_skipped(self, client):
        """UPDATE events are skipped."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"), \
             patch("main.send_welcome_email") as mock_send:
            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "UPDATE",
                    "record": {"email": "existing@test.com"}
                },
                headers={"x-supabase-webhook-secret": "test-secret"}
            )
        assert response.status_code == 200
        data = response.json()
        assert data["received"] is True
        assert data["skipped"] is True
        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_event_skipped(self, client):
        """DELETE events are skipped."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"), \
             patch("main.send_welcome_email") as mock_send:
            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "DELETE",
                    "record": {"email": "deleted@test.com"}
                },
                headers={"x-supabase-webhook-secret": "test-secret"}
            )
        assert response.status_code == 200
        assert response.json()["skipped"] is True
        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_email_skipped(self, client):
        """INSERT event without email in record is skipped."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"), \
             patch("main.send_welcome_email") as mock_send:
            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "INSERT",
                    "record": {"id": "some-id"}
                },
                headers={"x-supabase-webhook-secret": "test-secret"}
            )
        assert response.status_code == 200
        assert response.json()["skipped"] is True
        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_record_skipped(self, client):
        """INSERT event with empty record is skipped."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"), \
             patch("main.send_welcome_email") as mock_send:
            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "INSERT",
                    "record": {}
                },
                headers={"x-supabase-webhook-secret": "test-secret"}
            )
        assert response.status_code == 200
        assert response.json()["skipped"] is True
        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_driver_name_not_found_uses_email_fallback(self, client):
        """When driver name lookup fails, uses email-based name."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"), \
             patch("main.supabase") as mock_sb, \
             patch("main.send_welcome_email", return_value={"success": True, "id": "msg_fb"}) as mock_send:

            driver_result = MagicMock()
            driver_result.data = []  # No driver found

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.execute.return_value = driver_result
                elif name == "email_log":
                    chain.insert.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "INSERT",
                    "record": {"email": "john.doe@test.com"}
                },
                headers={"x-supabase-webhook-secret": "test-secret"}
            )
        assert response.status_code == 200
        # Fallback name from email: "john.doe" -> "John Doe"
        mock_send.assert_called_once_with("john.doe@test.com", "John Doe")

    @pytest.mark.asyncio
    async def test_driver_name_lookup_exception_uses_fallback(self, client):
        """When driver lookup raises exception, fallback name is used."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"), \
             patch("main.supabase") as mock_sb, \
             patch("main.send_welcome_email", return_value={"success": True, "id": "msg_exc"}) as mock_send:

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.execute.side_effect = Exception("DB down")
                elif name == "email_log":
                    chain.insert.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "INSERT",
                    "record": {"email": "maria_garcia@test.com"}
                },
                headers={"x-supabase-webhook-secret": "test-secret"}
            )
        assert response.status_code == 200
        # Fallback: "maria_garcia" -> "Maria Garcia"
        mock_send.assert_called_once_with("maria_garcia@test.com", "Maria Garcia")

    @pytest.mark.asyncio
    async def test_welcome_email_failure_still_returns_200(self, client):
        """Even if the welcome email fails, the webhook returns 200."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"), \
             patch("main.supabase") as mock_sb, \
             patch("main.send_welcome_email", return_value={"success": False, "error": "API error"}):

            driver_result = MagicMock()
            driver_result.data = [{"name": "User"}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.execute.return_value = driver_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "INSERT",
                    "record": {"email": "user@test.com"}
                },
                headers={"x-supabase-webhook-secret": "test-secret"}
            )
        assert response.status_code == 200
        data = response.json()
        assert data["received"] is True
        assert data["email_sent"] is False

    @pytest.mark.asyncio
    async def test_email_log_failure_does_not_crash(self, client):
        """If logging the email fails, the webhook still succeeds."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"), \
             patch("main.supabase") as mock_sb, \
             patch("main.send_welcome_email", return_value={"success": True, "id": "msg_log_fail"}):

            driver_result = MagicMock()
            driver_result.data = [{"name": "User"}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.execute.return_value = driver_result
                elif name == "email_log":
                    chain.insert.return_value.execute.side_effect = Exception("Log insert failed")
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "INSERT",
                    "record": {"email": "user@test.com"}
                },
                headers={"x-supabase-webhook-secret": "test-secret"}
            )
        assert response.status_code == 200
        assert response.json()["email_sent"] is True

    @pytest.mark.asyncio
    async def test_invalid_json_returns_400(self, client):
        """Invalid JSON payload returns 400."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"):
            response = await client.post(
                "/webhooks/supabase-auth",
                content=b"not valid json",
                headers={
                    "x-supabase-webhook-secret": "test-secret",
                    "content-type": "application/json"
                }
            )
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_missing_record_key_uses_empty_dict(self, client):
        """If record key is missing, email will be None and it gets skipped."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"), \
             patch("main.send_welcome_email") as mock_send:
            response = await client.post(
                "/webhooks/supabase-auth",
                json={"type": "INSERT"},
                headers={"x-supabase-webhook-secret": "test-secret"}
            )
        assert response.status_code == 200
        assert response.json()["skipped"] is True
        mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_driver_name_empty_string_uses_email_fallback(self, client):
        """If driver exists but name is empty string, use email-based fallback."""
        with patch("main.SUPABASE_WEBHOOK_SECRET", "test-secret"), \
             patch("main.supabase") as mock_sb, \
             patch("main.send_welcome_email", return_value={"success": True, "id": "msg_empty"}) as mock_send:

            driver_result = MagicMock()
            driver_result.data = [{"name": ""}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.execute.return_value = driver_result
                elif name == "email_log":
                    chain.insert.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post(
                "/webhooks/supabase-auth",
                json={
                    "type": "INSERT",
                    "record": {"email": "carlos.lopez@test.com"}
                },
                headers={"x-supabase-webhook-secret": "test-secret"}
            )
        assert response.status_code == 200
        # Empty name triggers fallback: "carlos.lopez" -> "Carlos Lopez"
        mock_send.assert_called_once_with("carlos.lopez@test.com", "Carlos Lopez")
