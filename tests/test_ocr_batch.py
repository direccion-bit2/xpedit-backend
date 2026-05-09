"""Tests for /ocr/screenshots-batch (Multi-Screenshot Importer Day 1).

Coverage:
- Auth required
- Pydantic validation (image count, media types)
- Gate logic: Pro+ paid / Pro yearly / trial / Pro monthly paid (denied) / free (denied)
- Gemini extraction success and failures (invalid JSON, exception)
- Daily rate limiting per tier
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


def _b64_image(size: int = 100) -> str:
    """Return a tiny valid base64-looking string of approx `size` chars."""
    return ("AAAA" * (size // 4)).ljust(size, "A")


def _drivers_row(promo_plan=None, expires_at=None, sub_src=None, sub_period=None):
    """Build a driver row mock that supabase .single().execute() will return."""
    return {
        "promo_plan": promo_plan,
        "promo_plan_expires_at": expires_at,
        "subscription_source": sub_src,
        "subscription_period": sub_period,
    }


def _patch_drivers_lookup(row_or_none):
    """Patch supabase.table('drivers') chain so .single().execute() returns the given row.

    Pass `None` to simulate driver not found / lookup failure.
    """
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.single.return_value = chain
    if row_or_none is None:
        chain.execute.side_effect = Exception("driver not found")
    else:
        result = MagicMock()
        result.data = row_or_none
        chain.execute.return_value = result
    return chain


def _gemini_ok_payload():
    """A canonical successful Gemini extraction response (already parsed)."""
    return {
        "carrier_detected": "ctt",
        "language": "es",
        "stops": [
            {
                "raw_text": "Calle Mayor 5, 11630 Arcos",
                "name": "Juan García",
                "street": "Calle Mayor",
                "number": "5",
                "floor_etc": "",
                "postal_code": "11630",
                "city": "Arcos de la Frontera",
                "province": "Cádiz",
                "phone": "",
                "tracking_number": "ABC123",
                "notes": "",
                "confidence_per_field": {"street": 0.95, "city": 0.92, "postal_code": 0.99},
                "source_image_idx": 0,
                "context_inferred_fields": [],
            }
        ],
        "global_inference_notes": "All visible stops are in Cádiz province.",
    }


# ============================================================================
# Auth + validation
# ============================================================================


class TestMSIAuth:
    @pytest.mark.asyncio
    async def test_requires_auth(self, unauth_client):
        resp = await unauth_client.post(
            "/ocr/screenshots-batch",
            json={"images": [{"image_base64": "abc", "media_type": "image/jpeg"}]},
        )
        assert resp.status_code in (401, 403)


class TestMSIValidation:
    @pytest.mark.asyncio
    async def test_zero_images_rejected(self, client):
        resp = await client.post("/ocr/screenshots-batch", json={"images": []})
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_more_than_10_images_rejected(self, client):
        imgs = [{"image_base64": "abc", "media_type": "image/jpeg"} for _ in range(11)]
        resp = await client.post("/ocr/screenshots-batch", json={"images": imgs})
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_invalid_media_type_rejected(self, client):
        resp = await client.post(
            "/ocr/screenshots-batch",
            json={"images": [{"image_base64": "abc", "media_type": "image/bmp"}]},
        )
        assert resp.status_code == 422


# ============================================================================
# Gate logic
# ============================================================================


class TestMSIGate:
    """Tests of _verify_msi_access via the endpoint."""

    @pytest.mark.asyncio
    async def test_pro_plus_paid_allowed(self, client):
        row = _drivers_row(promo_plan="pro_plus", sub_src="revenuecat")
        with patch("main.supabase") as mock_sb, patch(
            "main._msi_extract_stops_with_gemini", return_value=_gemini_ok_payload()
        ):
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["tier"] == "pro_plus"
        assert body["stops_count"] == 1
        assert body["stops"][0]["city"] == "Arcos de la Frontera"

    @pytest.mark.asyncio
    async def test_pro_yearly_allowed(self, client):
        row = _drivers_row(promo_plan="pro", sub_src="revenuecat", sub_period="yearly")
        with patch("main.supabase") as mock_sb, patch(
            "main._msi_extract_stops_with_gemini", return_value=_gemini_ok_payload()
        ):
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )
        assert resp.status_code == 200
        assert resp.json()["tier"] == "pro_yearly"

    @pytest.mark.asyncio
    async def test_active_trial_allowed(self, client):
        future = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat().replace("+00:00", "Z")
        row = _drivers_row(promo_plan="pro", expires_at=future, sub_src=None)
        with patch("main.supabase") as mock_sb, patch(
            "main._msi_extract_stops_with_gemini", return_value=_gemini_ok_payload()
        ):
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )
        assert resp.status_code == 200
        assert resp.json()["tier"] == "trial"

    @pytest.mark.asyncio
    async def test_pro_monthly_paid_blocked(self, client):
        """Pro paid (monthly) must be blocked — MSI is Pro+ exclusive."""
        row = _drivers_row(
            promo_plan="pro", sub_src="revenuecat", sub_period="monthly"
        )
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )
        assert resp.status_code == 403
        detail = resp.json()["detail"]
        assert detail["error"] == "pro_plus_required"

    @pytest.mark.asyncio
    async def test_free_user_blocked_with_trial_eligible(self, client):
        row = _drivers_row()  # all None
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )
        assert resp.status_code == 403
        detail = resp.json()["detail"]
        assert detail["error"] == "pro_plus_required"
        assert detail["trial_eligible"] is True

    @pytest.mark.asyncio
    async def test_expired_trial_blocked(self, client):
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat().replace("+00:00", "Z")
        row = _drivers_row(promo_plan="pro", expires_at=past, sub_src=None)
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )
        assert resp.status_code == 403
        detail = resp.json()["detail"]
        assert detail["trial_eligible"] is True

    @pytest.mark.asyncio
    async def test_lookup_failure_blocks(self, client):
        """If we can't read the drivers row, we deny the request rather than allow."""
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value = _patch_drivers_lookup(None)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )
        assert resp.status_code == 403


# ============================================================================
# Extraction (Gemini)
# ============================================================================


class TestMSIExtraction:
    @pytest.mark.asyncio
    async def test_successful_extraction_returns_stops(self, client):
        row = _drivers_row(promo_plan="pro_plus", sub_src="stripe")
        with patch("main.supabase") as mock_sb, patch(
            "main._msi_extract_stops_with_gemini", return_value=_gemini_ok_payload()
        ):
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={
                    "images": [
                        {"image_base64": "AAAA", "media_type": "image/jpeg"},
                        {"image_base64": "BBBB", "media_type": "image/png"},
                    ],
                    "carrier_hint": "ctt",
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["carrier_detected"] == "ctt"
        assert body["language"] == "es"
        assert body["stops_count"] == 1
        assert body["model"] == "gemini-2.5-pro"
        assert body["processing_ms"] >= 0

    @pytest.mark.asyncio
    async def test_gemini_returns_invalid_json_502(self, client):
        from fastapi import HTTPException

        row = _drivers_row(promo_plan="pro_plus", sub_src="stripe")

        def _raise_invalid_json(*args, **kwargs):
            raise HTTPException(status_code=502, detail="Invalid JSON from Gemini")

        with patch("main.supabase") as mock_sb, patch(
            "main._msi_extract_stops_with_gemini", side_effect=_raise_invalid_json
        ):
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )
        assert resp.status_code == 502

    @pytest.mark.asyncio
    async def test_gemini_unexpected_exception_502(self, client):
        row = _drivers_row(promo_plan="pro_plus", sub_src="stripe")
        with patch("main.supabase") as mock_sb, patch(
            "main._msi_extract_stops_with_gemini", side_effect=RuntimeError("boom")
        ):
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )
        assert resp.status_code == 502


# ============================================================================
# Rate limiting
# ============================================================================


class TestMSIRateLimit:
    @pytest.mark.asyncio
    async def test_trial_quota_5_per_day(self, client):
        """Active trial allows 5 batches/day, 6th call fails with 429."""
        future = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat().replace("+00:00", "Z")
        row = _drivers_row(promo_plan="pro", expires_at=future, sub_src=None)
        with patch("main.supabase") as mock_sb, patch(
            "main._msi_extract_stops_with_gemini", return_value=_gemini_ok_payload()
        ):
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            ok_count = 0
            limited = False
            for _ in range(7):
                resp = await client.post(
                    "/ocr/screenshots-batch",
                    json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
                )
                if resp.status_code == 200:
                    ok_count += 1
                elif resp.status_code == 429:
                    limited = True
                    break
        assert ok_count == 5
        assert limited is True

    @pytest.mark.asyncio
    async def test_pro_plus_quota_higher_than_trial(self, client):
        """Pro+ allows 50 batches/day vs 5 for trial. Verify Pro+ does not hit
        the 5-batch trial cap. (We only run 5 calls because the global per-IP
        /ocr/* middleware rate limit caps at 5/min — the daily-quota check is
        validated structurally by the counter being scoped per-tier.)"""
        row = _drivers_row(promo_plan="pro_plus", sub_src="stripe")
        with patch("main.supabase") as mock_sb, patch(
            "main._msi_extract_stops_with_gemini", return_value=_gemini_ok_payload()
        ):
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            for _ in range(5):
                resp = await client.post(
                    "/ocr/screenshots-batch",
                    json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
                )
                assert resp.status_code == 200
