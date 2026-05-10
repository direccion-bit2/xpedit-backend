"""Tests for OCR label extraction endpoint (/ocr/label).

Migrated from Anthropic Claude → Gemini 2.5 Flash on 2026-05-10 (#244).
Tests now mock `get_gemini_client()` instead of `httpx.AsyncClient`.
"""

import json
from unittest.mock import MagicMock, patch

import pytest


def _gemini_response(payload: dict | str | None) -> MagicMock:
    """Build a fake Gemini response.text wrapper.

    - If payload is a dict, it's JSON-encoded into response.text.
    - If payload is a string, used verbatim (handy for malformed JSON tests).
    - If payload is None, response.text is empty.
    """
    resp = MagicMock()
    if payload is None:
        resp.text = ""
    elif isinstance(payload, str):
        resp.text = payload
    else:
        resp.text = json.dumps(payload, ensure_ascii=False)
    return resp


def _patched_client(generate_return=None, generate_side_effect=None):
    """Build a fake Gemini client whose .models.generate_content returns/raises."""
    client = MagicMock()
    if generate_side_effect is not None:
        client.models.generate_content.side_effect = generate_side_effect
    else:
        client.models.generate_content.return_value = generate_return
    return client


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
        """All four allowed media types should pass validation. We force the
        Gemini client to be missing so we know it's a 503 (service not
        configured), proving validation passed."""
        for media_type in ("image/jpeg", "image/png", "image/gif", "image/webp"):
            with patch("main.get_gemini_client", return_value=None):
                resp = await client.post("/ocr/label", json={
                    "image_base64": "abc123",
                    "media_type": media_type,
                })
                assert resp.status_code == 503


class TestOCRLabelServiceNotConfigured:
    """Tests when the Gemini client is not available."""

    @pytest.mark.asyncio
    async def test_returns_503_when_no_client(self, client):
        with patch("main.get_gemini_client", return_value=None):
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
        ocr_payload = {
            "name": "Juan Garcia",
            "street": "Calle Mayor 5",
            "city": "Sevilla",
            "postalCode": "41001",
            "province": "Sevilla",
        }
        gemini_client = _patched_client(generate_return=_gemini_response(ocr_payload))

        with patch("main.get_gemini_client", return_value=gemini_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "iVBORw0KGgoAAAANSUhEUg==",
                "media_type": "image/png",
            })

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        # `content` keeps the legacy JSON-string contract
        assert json.loads(body["content"]) == ocr_payload
        # `data` is the new convenience field — same payload, parsed
        assert body["data"] == ocr_payload

    @pytest.mark.asyncio
    async def test_calls_gemini_with_correct_model_and_image(self, client):
        gemini_client = _patched_client(generate_return=_gemini_response({
            "name": "", "street": "", "city": "", "postalCode": "", "province": "",
        }))

        with patch("main.get_gemini_client", return_value=gemini_client):
            await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",  # b64('test')
                "media_type": "image/webp",
            })

        gemini_client.models.generate_content.assert_called_once()
        kwargs = gemini_client.models.generate_content.call_args.kwargs
        assert kwargs["model"] == "gemini-2.5-flash"

        contents = kwargs["contents"]
        # contents = [Content(role='user', parts=[text_part, image_part])]
        assert len(contents) == 1
        parts = contents[0].parts
        # Two parts: prompt text + image bytes
        assert len(parts) == 2
        # The image part carries the decoded bytes + the requested mime type
        image_part = parts[1]
        assert image_part.inline_data.mime_type == "image/webp"
        assert image_part.inline_data.data == b"test"

        # Config requests structured JSON output
        config = kwargs["config"]
        assert config.response_mime_type == "application/json"

    @pytest.mark.asyncio
    async def test_default_media_type_is_jpeg(self, client):
        gemini_client = _patched_client(generate_return=_gemini_response({
            "name": "", "street": "", "city": "", "postalCode": "", "province": "",
        }))

        with patch("main.get_gemini_client", return_value=gemini_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",
            })

        assert resp.status_code == 200
        kwargs = gemini_client.models.generate_content.call_args.kwargs
        image_part = kwargs["contents"][0].parts[1]
        assert image_part.inline_data.mime_type == "image/jpeg"


class TestOCRLabelErrors:
    """Error handling tests."""

    @pytest.mark.asyncio
    async def test_gemini_raises_returns_502(self, client):
        gemini_client = _patched_client(generate_side_effect=RuntimeError("rate limited"))

        with patch("main.get_gemini_client", return_value=gemini_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 502
        assert "OCR API error" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_empty_response_returns_blank_fields(self, client):
        """Empty response from Gemini → return blanks, don't fail.
        The UI shows 'couldn't read, try again' instead of a generic 502."""
        gemini_client = _patched_client(generate_return=_gemini_response(None))

        with patch("main.get_gemini_client", return_value=gemini_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["data"] == {"name": "", "street": "", "city": "", "postalCode": "", "province": ""}

    @pytest.mark.asyncio
    async def test_unparseable_response_returns_blank_fields(self, client):
        """Gemini returned text but it's not parseable as JSON. Same gentle
        fallback so the UI doesn't crash on a hard-to-read label."""
        gemini_client = _patched_client(generate_return=_gemini_response("not actually json at all"))

        with patch("main.get_gemini_client", return_value=gemini_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 200
        body = resp.json()
        assert body["data"]["name"] == ""

    @pytest.mark.asyncio
    async def test_fenced_json_is_extracted(self, client):
        """Gemini occasionally wraps the JSON in ```json fences despite the
        response_mime_type config. We strip the fences and parse the inner
        object."""
        fenced = '```json\n{"name":"Ana","street":"Av Marina 8","city":"Cadiz","postalCode":"11001","province":"Cadiz"}\n```'
        gemini_client = _patched_client(generate_return=_gemini_response(fenced))

        with patch("main.get_gemini_client", return_value=gemini_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 200
        body = resp.json()
        assert body["data"]["name"] == "Ana"
        assert body["data"]["postalCode"] == "11001"

    @pytest.mark.asyncio
    async def test_json_with_prefix_text_is_extracted(self, client):
        """If Gemini prefixes the JSON with a sentence (which it sometimes does
        for difficult labels), we still extract the inner object via regex."""
        text = 'Here is the data:\n{"name":"Luis","street":"C/ Sol 3","city":"Sevilla","postalCode":"41001","province":"Sevilla"}'
        gemini_client = _patched_client(generate_return=_gemini_response(text))

        with patch("main.get_gemini_client", return_value=gemini_client):
            resp = await client.post("/ocr/label", json={
                "image_base64": "dGVzdA==",
                "media_type": "image/jpeg",
            })

        assert resp.status_code == 200
        body = resp.json()
        assert body["data"]["name"] == "Luis"
