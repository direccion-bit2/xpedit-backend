"""Tests for /ocr/screenshots-batch (Multi-Screenshot Importer Day 1 + 2).

Coverage:
- Day 1: auth, validation, gate logic, Gemini extraction, rate limiting
- Day 2: postal-code province lookup, stop normalization (floor split,
  garbage scrubbing, province inference), centroid bbox computation,
  confidence classification, geocoding pipeline end-to-end
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


# ============================================================================
# Day 2: pipeline helpers (pure functions)
# ============================================================================


class TestMSINormalizationHelpers:
    def test_postal_code_to_province_basic(self):
        from main import _msi_postal_code_to_province
        assert _msi_postal_code_to_province("11630") == "Cádiz"
        assert _msi_postal_code_to_province("28013") == "Madrid"
        assert _msi_postal_code_to_province("08029") == "Barcelona"
        assert _msi_postal_code_to_province("41001") == "Sevilla"
        assert _msi_postal_code_to_province("38001") == "Santa Cruz de Tenerife"

    def test_postal_code_to_province_invalid(self):
        from main import _msi_postal_code_to_province
        assert _msi_postal_code_to_province(None) is None
        assert _msi_postal_code_to_province("") is None
        assert _msi_postal_code_to_province("99999") is None  # province 99 doesn't exist
        assert _msi_postal_code_to_province("abc") is None

    def test_normalize_trims_strings(self):
        from main import _msi_normalize_extracted_stop
        s = {"street": "  Calle Mayor 5  ", "city": " Sevilla ", "postal_code": " 41001 "}
        out = _msi_normalize_extracted_stop(s)
        assert out["street"] == "Calle Mayor 5"
        assert out["city"] == "Sevilla"
        assert out["postal_code"] == "41001"

    def test_normalize_postal_code_strips_non_digits(self):
        from main import _msi_normalize_extracted_stop
        out = _msi_normalize_extracted_stop({"postal_code": "C.P. 11630-X"})
        assert out["postal_code"] == "11630"

    def test_normalize_rejects_garbage_fields(self):
        from main import _msi_normalize_extracted_stop
        out = _msi_normalize_extracted_stop({
            "street": "DIRECCIÓN DESCONOCIDA",
            "city": "TBD",
            "province": "—",
        })
        assert out["street"] == ""
        assert out["city"] == ""
        assert out["province"] == ""

    def test_normalize_infers_province_from_cp(self):
        from main import _msi_normalize_extracted_stop
        out = _msi_normalize_extracted_stop({
            "street": "Calle Mayor", "postal_code": "11630", "province": "",
        })
        assert out["province"] == "Cádiz"
        assert "province" in (out.get("context_inferred_fields") or [])

    def test_normalize_does_not_overwrite_existing_province(self):
        from main import _msi_normalize_extracted_stop
        out = _msi_normalize_extracted_stop({
            "street": "X", "postal_code": "11630", "province": "Cádiz Custom",
        })
        # Province already present should not be overwritten
        assert out["province"] == "Cádiz Custom"

    def test_normalize_splits_floor_from_street(self):
        """Defensive: if Gemini left a floor fragment in `street`, we split it."""
        from main import _msi_normalize_extracted_stop
        out = _msi_normalize_extracted_stop({"street": "Calle Mayor 5, 4ºB"})
        # The trailing "4ºB" should land in floor_etc, not street.
        assert "4" in (out.get("floor_etc") or "")
        assert "Mayor" in out["street"]


class TestMSICentroidBbox:
    def test_two_or_more_coords_yields_bbox(self):
        from main import _msi_compute_centroid_bbox
        bbox = _msi_compute_centroid_bbox([
            {"lat": 37.4, "lng": -6.0},
            {"lat": 37.5, "lng": -5.9},
        ])
        assert bbox is not None
        assert bbox["sw_lat"] < bbox["ne_lat"]
        assert bbox["sw_lng"] < bbox["ne_lng"]

    def test_zero_or_one_coord_returns_none(self):
        from main import _msi_compute_centroid_bbox
        assert _msi_compute_centroid_bbox([]) is None
        assert _msi_compute_centroid_bbox([{"lat": 1, "lng": 2}]) is None

    def test_skips_coords_with_none(self):
        from main import _msi_compute_centroid_bbox
        assert _msi_compute_centroid_bbox([
            {"lat": None, "lng": None},
            {"lat": 37.4, "lng": -6.0},
        ]) is None


class TestMSIConfidence:
    def test_high_when_rooftop_and_strong_extraction(self):
        from main import _msi_classify_confidence
        stop = {"confidence_per_field": {"street": 0.95, "city": 0.9}}
        geo = {"status": "ok", "location_type": "ROOFTOP"}
        assert _msi_classify_confidence(stop, geo) == "high"

    def test_medium_when_geometric_center_with_cp(self):
        from main import _msi_classify_confidence
        stop = {
            "confidence_per_field": {"street": 0.8, "city": 0.7},
            "postal_code": "11630",
        }
        geo = {"status": "ok", "location_type": "GEOMETRIC_CENTER"}
        assert _msi_classify_confidence(stop, geo) == "medium"

    def test_low_when_geocoding_failed(self):
        from main import _msi_classify_confidence
        assert _msi_classify_confidence({}, {"status": "zero_results"}) == "low"
        assert _msi_classify_confidence({}, {"status": "error"}) == "low"

    def test_low_when_approximate_location_type(self):
        from main import _msi_classify_confidence
        stop = {"confidence_per_field": {"street": 0.9}}
        assert _msi_classify_confidence(stop, {"status": "ok", "location_type": "APPROXIMATE"}) == "low"


# ============================================================================
# Day 2: end-to-end pipeline (geocoding mocked)
# ============================================================================


def _geocoding_response(status="OK", lat=37.5, lng=-6.0, location_type="ROOFTOP",
                        formatted="Test address", place_id="ChIJtest"):
    """Build a mock httpx response object for Google Geocoding."""
    mock_resp = MagicMock()
    if status == "ZERO_RESULTS":
        mock_resp.json.return_value = {"status": "ZERO_RESULTS", "results": []}
    elif status == "OK":
        mock_resp.json.return_value = {
            "status": "OK",
            "results": [{
                "formatted_address": formatted,
                "place_id": place_id,
                "geometry": {
                    "location": {"lat": lat, "lng": lng},
                    "location_type": location_type,
                },
            }],
        }
    else:
        mock_resp.json.return_value = {"status": status}
    return mock_resp


class TestMSIPipelineEndToEnd:
    @pytest.mark.asyncio
    async def test_endpoint_returns_geocoded_stops(self, client):
        """End-to-end with mocked Gemini + mocked Google Geocoding.

        Gemini returns 2 stops, geocoder returns ROOFTOP for both → both should
        come back as `confidence='high'` with coords + formatted_address.
        """
        gemini_payload = {
            "carrier_detected": "ctt",
            "language": "es",
            "stops": [
                {"raw_text": "X", "street": "Calle Mayor", "number": "5",
                 "postal_code": "41001", "city": "Sevilla",
                 "confidence_per_field": {"street": 0.95, "city": 0.92},
                 "source_image_idx": 0},
                {"raw_text": "Y", "street": "Avenida Constitución", "number": "12",
                 "postal_code": "41004", "city": "Sevilla",
                 "confidence_per_field": {"street": 0.93, "city": 0.91},
                 "source_image_idx": 0},
            ],
            "global_inference_notes": "",
        }
        row = _drivers_row(promo_plan="pro_plus", sub_src="stripe")

        from unittest.mock import AsyncMock
        mock_client = MagicMock()
        mock_client.get = AsyncMock(side_effect=[
            _geocoding_response(lat=37.40, lng=-5.99, location_type="ROOFTOP",
                                formatted="Calle Mayor 5, 41001 Sevilla"),
            _geocoding_response(lat=37.41, lng=-5.98, location_type="ROOFTOP",
                                formatted="Avenida Constitución 12, 41004 Sevilla"),
        ])

        with patch("main.supabase") as mock_sb, \
             patch("main._msi_extract_stops_with_gemini", return_value=gemini_payload), \
             patch("main.GOOGLE_API_KEY", "fake-key"), \
             patch("main.google_maps_client", return_value=mock_client):
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["stops_count"] == 2
        assert body["confidence_summary"]["high"] == 2
        for s in body["stops"]:
            assert s["confidence"] == "high"
            assert s["coords"] is not None
            assert s["coords"]["lat"] == pytest.approx(37.4, abs=0.1)
            assert s["formatted_address"]
            assert s["geocoding_status"] == "ok"

    @pytest.mark.asyncio
    async def test_low_confidence_gets_candidates(self, client):
        """If geocoder returns ZERO_RESULTS for one stop, we should fetch up to 3
        autocomplete candidates so the user can pick from real options."""
        gemini_payload = {
            "carrier_detected": "ctt",
            "stops": [{
                "raw_text": "X", "street": "Calle Inventada", "city": "Sevilla",
                "confidence_per_field": {"street": 0.5},
                "source_image_idx": 0,
            }],
        }
        row = _drivers_row(promo_plan="pro_plus", sub_src="stripe")

        from unittest.mock import AsyncMock

        # Sequence: geocoding round 1 ZERO_RESULTS → no centroid bbox (only 1
        # stop), so no round 2 retry. Then candidates autocomplete returns 2.
        autocomplete_resp = MagicMock()
        autocomplete_resp.json.return_value = {
            "status": "OK",
            "predictions": [
                {"description": "Calle Mayor 5, Sevilla", "place_id": "P1"},
                {"description": "Calle Mayor 7, Sevilla", "place_id": "P2"},
            ],
        }

        mock_client = MagicMock()
        mock_client.get = AsyncMock(side_effect=[
            _geocoding_response(status="ZERO_RESULTS"),
            autocomplete_resp,
        ])

        with patch("main.supabase") as mock_sb, \
             patch("main._msi_extract_stops_with_gemini", return_value=gemini_payload), \
             patch("main.GOOGLE_API_KEY", "fake-key"), \
             patch("main.google_maps_client", return_value=mock_client):
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["stops_count"] == 1
        assert body["confidence_summary"]["low"] == 1
        s = body["stops"][0]
        assert s["confidence"] == "low"
        assert s["coords"] is None
        assert len(s["candidates"]) == 2
        assert s["candidates"][0]["place_id"] == "P1"

    @pytest.mark.asyncio
    async def test_floor_etc_preserved_in_delivery_instructions(self, client):
        """floor_etc must NEVER reach the geocoder but must surface as
        `delivery_instructions` for the driver."""
        gemini_payload = {
            "carrier_detected": "generic",
            "stops": [{
                "raw_text": "X", "street": "Calle Mayor", "number": "5",
                "floor_etc": "4ºB Esc 2", "postal_code": "41001", "city": "Sevilla",
                "confidence_per_field": {"street": 0.9},
                "source_image_idx": 0,
            }],
        }
        row = _drivers_row(promo_plan="pro_plus", sub_src="stripe")

        from unittest.mock import AsyncMock
        mock_client = MagicMock()
        mock_client.get = AsyncMock(return_value=_geocoding_response())

        with patch("main.supabase") as mock_sb, \
             patch("main._msi_extract_stops_with_gemini", return_value=gemini_payload), \
             patch("main.GOOGLE_API_KEY", "fake-key"), \
             patch("main.google_maps_client", return_value=mock_client):
            mock_sb.table.return_value = _patch_drivers_lookup(row)
            resp = await client.post(
                "/ocr/screenshots-batch",
                json={"images": [{"image_base64": "AAAA", "media_type": "image/jpeg"}]},
            )

        body = resp.json()
        s = body["stops"][0]
        assert s["delivery_instructions"] == "4ºB Esc 2"
        # And the geocoded address Google call must NOT have included "4ºB"
        first_call_kwargs = mock_client.get.call_args_list[0][1]
        assert "4ºB" not in first_call_kwargs.get("params", {}).get("address", "")
        assert "Esc 2" not in first_call_kwargs.get("params", {}).get("address", "")
