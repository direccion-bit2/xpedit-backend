"""Tests for OCR label extraction endpoint (/ocr/label)."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest


class TestOCRLabelAuth:
    """Authentication tests for /ocr/label."""

    @pytest.mark.asyncio
    async def test_ocr_label_requires_auth(self, unauth_client):
        resp = await unauth_client.post("/ocr/label", json={
            "image_base64": "abc123",
            "media_type": "image/jpeg",
        })
        assert resp.status_code in (401, 403)


class TestOCRLabelValidation:
    """Request validation tests."""

    @pytest.mark.asyncio
    async def test_missing_image_base64(self, client):
        resp = await client.post("/ocr/label", json={})
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_invalid_media_type(self, client):
        resp = await client.post("/ocr/label", json={
            "image_base64": "abc123",
            "media_type": "image/bmp",
        })
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_valid_media_types_accepted(self, client):
        """All four allowed media types should pass validation (may fail on API call, not validation)."""
        for media_type in ("image/jpeg", "image/png", "image/gif", "image/webp"):
            with patch("main.ANTHROPIC_API_KEY", ""):
                resp = await client.post("/ocr/label", json={
                    "image_base64": "abc123",
                    "media_type": media_type,
                })
                # 503 means validation passed but API key is missing
                assert resp.status_code == 503


class TestOCRLabelServiceNotConfigured:
    """Tests when ANTHROPIC_API_KEY is not set."""

    @pytest.mark.asyncio
    async def test_returns_503_when_no_api_key(self, client):
        with patch("main.ANTHROPIC_API_KEY", ""):
            resp = await client.post("/ocr/label", json={
                "image_base64": "abc123",
                "media_type": "image/jpeg",
            })
            assert resp.status_code == 503
            assert "not configured" in resp.json()["detail"]


class TestOCRLabelSuccess:
    """Tests for successful OCR extraction."""

    @pytest.mark.asyncio
    async def test_successful_extraction(self, client):
        ocr_result = json.dumps({
            "name": "Juan Garcia",
            "street": "Calle Mayor 5",
            "city": "Sevilla",
            "postalCode": "41001",
            "province": "Sevilla",
        })
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "content": [{"text": ocr_result}],
        }

        mock_http_client = AsyncMock()
        mock_http_client.post = AsyncMock(return_value=mock_response)
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("main.ANTHROPIC_API_KEY", "sk-ant-test-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "iVBORw0KGgoAAAANSUhEUg==",
                "media_type": "image/png",
            })

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["content"] == ocr_result

    @pytest.mark.asyncio
    async def test_sends_correct_headers_and_payload(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"content": [{"text": "{}"}]}

        mock_http_client = AsyncMock()
        mock_http_client.post = AsyncMock(return_value=mock_response)
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("main.ANTHROPIC_API_KEY", "sk-ant-test-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http_client):
            await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",
                "media_type": "image/webp",
            })

        call_args = mock_http_client.post.call_args
        url = call_args[0][0] if call_args[0] else call_args[1].get("url", "")
        assert "api.anthropic.com" in url

        headers = call_args[1].get("headers", {})
        assert headers["x-api-key"] == "sk-ant-test-key"
        assert headers["anthropic-version"] == "2023-06-01"

        payload = call_args[1].get("json", {})
        assert payload["model"] == "claude-haiku-4-5-20251001"
        msg_content = payload["messages"][0]["content"]
        image_block = msg_content[0]
        assert image_block["source"]["media_type"] == "image/webp"
        assert image_block["source"]["data"] == "dGVzdA=="

    @pytest.mark.asyncio
    async def test_empty_content_array(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"content": [{}]}

        mock_http_client = AsyncMock()
        mock_http_client.post = AsyncMock(return_value=mock_response)
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("main.ANTHROPIC_API_KEY", "sk-ant-test-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "abc",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 200
        assert resp.json()["content"] == ""

    @pytest.mark.asyncio
    async def test_default_media_type_is_jpeg(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"content": [{"text": "{}"}]}

        mock_http_client = AsyncMock()
        mock_http_client.post = AsyncMock(return_value=mock_response)
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("main.ANTHROPIC_API_KEY", "sk-ant-test-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "abc",
            })

        assert resp.status_code == 200
        payload = mock_http_client.post.call_args[1]["json"]
        image_block = payload["messages"][0]["content"][0]
        assert image_block["source"]["media_type"] == "image/jpeg"


class TestOCRLabelErrors:
    """Error handling tests."""

    @pytest.mark.asyncio
    async def test_anthropic_api_non_200(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 429

        mock_http_client = AsyncMock()
        mock_http_client.post = AsyncMock(return_value=mock_response)
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("main.ANTHROPIC_API_KEY", "sk-ant-test-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "abc",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 502
        assert "OCR API error 429" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_anthropic_api_500(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_http_client = AsyncMock()
        mock_http_client.post = AsyncMock(return_value=mock_response)
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("main.ANTHROPIC_API_KEY", "sk-ant-test-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "abc",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 502
        assert "500" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_network_timeout(self, client):
        mock_http_client = AsyncMock()
        mock_http_client.post = AsyncMock(side_effect=httpx.TimeoutException("Connection timed out"))
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("main.ANTHROPIC_API_KEY", "sk-ant-test-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "abc",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 500
        assert "Error interno" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_connection_error(self, client):
        mock_http_client = AsyncMock()
        mock_http_client.post = AsyncMock(side_effect=httpx.ConnectError("DNS resolution failed"))
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("main.ANTHROPIC_API_KEY", "sk-ant-test-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "abc",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 500
        assert "Error interno" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_unexpected_exception(self, client):
        mock_http_client = AsyncMock()
        mock_http_client.post = AsyncMock(side_effect=RuntimeError("something broke"))
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("main.ANTHROPIC_API_KEY", "sk-ant-test-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "abc",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 500
        assert "Error interno" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_malformed_json_response(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("err", "", 0)

        mock_http_client = AsyncMock()
        mock_http_client.post = AsyncMock(return_value=mock_response)
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("main.ANTHROPIC_API_KEY", "sk-ant-test-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "abc",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 500
        assert "Error interno" in resp.json()["detail"]
