"""Regression suite for Places API v1 (New) migration (21 may 2026).

Covers:
- Field mask whitelist defense (any Pro field triggers $17/1000 SKU instead of $5/1000)
- Autocomplete mapper v1 → Legacy format
- Details mapper v1 → Legacy format
- Handler flag branching: 'legacy' → no v1 call, 'v1' → v1 call
- v1 failure handling: HTTP error returns clean ZERO_RESULTS / UNKNOWN_ERROR
- distanceMeters propagation (this is the reason we migrated)
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import places_v1

# ───────────────────────────── Field mask defense ──────────────────────────

class TestFieldMaskWhitelist:
    """If anyone adds a Pro field to _DETAILS_FIELD_MASK by mistake, factura
    salta 3.4x ($17/1000 vs $5/1000). These tests block that at CI time."""

    def test_details_mask_only_essentials_fields(self):
        """_DETAILS_FIELD_MASK must contain ONLY allowlisted Essentials fields.

        Adding displayName, rating, regularOpeningHours, etc. would silently
        promote every Details call from Essentials ($5) to Pro ($17) SKU.
        """
        from places_v1 import _DETAILS_FIELD_MASK, DETAILS_ESSENTIALS_ALLOWLIST

        used = set(_DETAILS_FIELD_MASK.split(","))
        not_in_allowlist = used - DETAILS_ESSENTIALS_ALLOWLIST
        assert not_in_allowlist == set(), (
            f"Place Details field mask contains non-Essentials fields: {not_in_allowlist}. "
            f"This would 3.4x billing per call. Update DETAILS_ESSENTIALS_ALLOWLIST "
            f"INTENTIONALLY only if you've verified the new field is Essentials tier."
        )

    def test_details_mask_known_pro_fields_rejected(self):
        """Specific check: well-known Pro fields must NEVER appear in mask."""
        from places_v1 import _DETAILS_FIELD_MASK
        pro_fields = {
            "displayName", "rating", "regularOpeningHours", "phoneNumber",
            "websiteUri", "businessStatus", "priceLevel", "userRatingCount",
            "primaryType", "primaryTypeDisplayName", "timeZone",
        }
        used = set(_DETAILS_FIELD_MASK.split(","))
        leaked = used & pro_fields
        assert leaked == set(), f"Pro fields leaked into Essentials mask: {leaked}"

    def test_autocomplete_mask_has_distance_meters(self):
        """distanceMeters is the entire reason for the v1 migration — if it's
        ever removed by mistake, re-orden by proximity stops working."""
        from places_v1 import _AUTOCOMPLETE_FIELD_MASK
        assert "distanceMeters" in _AUTOCOMPLETE_FIELD_MASK


# ─────────────────────────── Autocomplete mapper ───────────────────────────

class TestAutocompleteMapper:

    def test_basic_v1_response_maps_to_legacy_shape(self):
        v1_response = {
            "suggestions": [
                {
                    "placePrediction": {
                        "placeId": "ChIJ123",
                        "text": {"text": "Calle Real 5, Sanlúcar de Barrameda, España"},
                        "structuredFormat": {
                            "mainText": {"text": "Calle Real 5"},
                            "secondaryText": {"text": "Sanlúcar de Barrameda, España"},
                        },
                        "types": ["street_address"],
                    }
                }
            ]
        }
        result = places_v1._map_v1_autocomplete_to_legacy(v1_response)
        assert result["status"] == "OK"
        assert len(result["predictions"]) == 1
        p = result["predictions"][0]
        assert p["place_id"] == "ChIJ123"
        assert p["description"] == "Calle Real 5, Sanlúcar de Barrameda, España"
        assert p["structured_formatting"]["main_text"] == "Calle Real 5"
        assert p["structured_formatting"]["secondary_text"] == "Sanlúcar de Barrameda, España"
        assert p["types"] == ["street_address"]
        assert "distance_meters" not in p  # Not present in this fixture

    def test_distance_meters_propagated_when_present(self):
        """The whole point of migrating: distanceMeters arrives and gets renamed
        to distance_meters (Legacy-style snake_case) so app re-orden works."""
        v1_response = {
            "suggestions": [
                {
                    "placePrediction": {
                        "placeId": "ChIJ1",
                        "text": {"text": "A"},
                        "distanceMeters": 1234,
                    }
                },
                {
                    "placePrediction": {
                        "placeId": "ChIJ2",
                        "text": {"text": "B"},
                        "distanceMeters": 25000,
                    }
                },
            ]
        }
        result = places_v1._map_v1_autocomplete_to_legacy(v1_response)
        assert result["predictions"][0]["distance_meters"] == 1234
        assert result["predictions"][1]["distance_meters"] == 25000

    def test_empty_suggestions_maps_to_zero_results(self):
        result = places_v1._map_v1_autocomplete_to_legacy({"suggestions": []})
        assert result["status"] == "ZERO_RESULTS"
        assert result["predictions"] == []

    def test_missing_suggestions_key_maps_to_zero_results(self):
        result = places_v1._map_v1_autocomplete_to_legacy({})
        assert result["status"] == "ZERO_RESULTS"
        assert result["predictions"] == []

    def test_query_prediction_suggestions_are_ignored(self):
        """v1 can return queryPrediction (not placePrediction) — we skip those."""
        v1_response = {
            "suggestions": [
                {"queryPrediction": {"text": {"text": "pizza"}}},  # ignored
                {
                    "placePrediction": {
                        "placeId": "ChIJ1",
                        "text": {"text": "Pizza Hut"},
                    }
                },
            ]
        }
        result = places_v1._map_v1_autocomplete_to_legacy(v1_response)
        assert len(result["predictions"]) == 1
        assert result["predictions"][0]["place_id"] == "ChIJ1"


# ───────────────────────────── Details mapper ──────────────────────────────

class TestDetailsMapper:

    def test_basic_v1_response_maps_to_legacy_shape(self):
        v1_response = {
            "id": "ChIJ123",
            "location": {"latitude": 36.7765, "longitude": -6.3527},
            "formattedAddress": "Calle Real 5, 11540 Sanlúcar de Barrameda, Cádiz, España",
            "addressComponents": [
                {"longText": "5", "shortText": "5", "types": ["street_number"]},
                {"longText": "Calle Real", "shortText": "Real", "types": ["route"]},
                {"longText": "Sanlúcar de Barrameda", "shortText": "Sanlúcar", "types": ["locality"]},
                {"longText": "11540", "shortText": "11540", "types": ["postal_code"]},
            ],
            "types": ["street_address"],
        }
        result = places_v1._map_v1_details_to_legacy(v1_response)
        assert result["status"] == "OK"
        r = result["result"]
        assert r["geometry"]["location"]["lat"] == 36.7765
        assert r["geometry"]["location"]["lng"] == -6.3527
        assert r["formatted_address"] == "Calle Real 5, 11540 Sanlúcar de Barrameda, Cádiz, España"
        assert r["place_id"] == "ChIJ123"
        assert r["types"] == ["street_address"]
        assert len(r["address_components"]) == 4
        # Address component v1 longText/shortText → Legacy long_name/short_name
        ac0 = r["address_components"][0]
        assert ac0["long_name"] == "5"
        assert ac0["short_name"] == "5"
        assert ac0["types"] == ["street_number"]

    def test_missing_optional_fields_safe(self):
        result = places_v1._map_v1_details_to_legacy({
            "id": "ChIJ1",
            "location": {"latitude": 0, "longitude": 0},
            "formattedAddress": "X",
        })
        assert result["status"] == "OK"
        assert result["result"]["address_components"] == []
        assert result["result"]["types"] == []


# ──────────────────────────── HTTP error handling ──────────────────────────

class TestAutocompleteV1Errors:

    @pytest.mark.asyncio
    async def test_non_200_returns_zero_results(self):
        client = MagicMock()
        client.post = AsyncMock(return_value=MagicMock(status_code=500, text="boom"))
        result = await places_v1.autocomplete_v1(
            client, "fake-key", input="test"
        )
        assert result["status"] == "ZERO_RESULTS"
        assert result["predictions"] == []
        assert "v1 http 500" in result["error_message"]

    @pytest.mark.asyncio
    async def test_exception_returns_zero_results(self):
        client = MagicMock()
        client.post = AsyncMock(side_effect=Exception("network"))
        result = await places_v1.autocomplete_v1(
            client, "fake-key", input="test"
        )
        assert result["status"] == "ZERO_RESULTS"
        assert "v1 request error" in result["error_message"]


class TestDetailsV1Errors:

    @pytest.mark.asyncio
    async def test_404_returns_not_found(self):
        client = MagicMock()
        client.get = AsyncMock(return_value=MagicMock(status_code=404, text="missing"))
        result = await places_v1.details_v1(
            client, "fake-key", place_id="ChIJbad"
        )
        assert result["status"] == "NOT_FOUND"

    @pytest.mark.asyncio
    async def test_500_returns_unknown_error(self):
        client = MagicMock()
        client.get = AsyncMock(return_value=MagicMock(status_code=500, text="boom"))
        result = await places_v1.details_v1(
            client, "fake-key", place_id="ChIJbad"
        )
        assert result["status"] == "UNKNOWN_ERROR"


# ─────────────────────────── v1 body composition ───────────────────────────

class TestAutocompleteV1RequestBody:

    @pytest.mark.asyncio
    async def test_lat_lng_without_origin_gets_wide_bias_30km(self):
        """Sin origin (primera parada / búsqueda sin contexto), bias amplio
        30 km centrado en GPS. Permite descubrir direcciones por toda la zona."""
        client = MagicMock()
        captured = {}

        async def capture(url, json=None, headers=None, timeout=None):
            captured["json"] = json
            return MagicMock(status_code=200, json=lambda: {"suggestions": []})

        client.post = capture
        await places_v1.autocomplete_v1(
            client, "k", input="x", lat=36.78, lng=-6.35
        )
        assert captured["json"]["locationBias"]["circle"]["center"]["latitude"] == 36.78
        assert captured["json"]["locationBias"]["circle"]["center"]["longitude"] == -6.35
        assert captured["json"]["locationBias"]["circle"]["radius"] == 30000.0
        assert "origin" not in captured["json"]

    @pytest.mark.asyncio
    async def test_origin_gets_narrow_bias_5km_centered_on_origin(self):
        """Con origin (= última stop), bias estrecho 5 km centrado en la stop.
        Replica el patrón Spoke/Circuit: priorizar fuerte la ciudad/zona de la
        parada anterior cuando hay match exacto en otras ciudades del país."""
        client = MagicMock()
        captured = {}

        async def capture(url, json=None, headers=None, timeout=None):
            captured["json"] = json
            return MagicMock(status_code=200, json=lambda: {"suggestions": []})

        client.post = capture
        await places_v1.autocomplete_v1(
            client, "k", input="x", origin_lat=36.78, origin_lng=-6.35
        )
        # locationBias centered on origin (not GPS), radius 5km (not 30km)
        assert captured["json"]["locationBias"]["circle"]["center"]["latitude"] == 36.78
        assert captured["json"]["locationBias"]["circle"]["center"]["longitude"] == -6.35
        assert captured["json"]["locationBias"]["circle"]["radius"] == 5000.0
        # origin also propagated so Google returns distanceMeters per prediction
        assert captured["json"]["origin"]["latitude"] == 36.78
        assert captured["json"]["origin"]["longitude"] == -6.35

    @pytest.mark.asyncio
    async def test_origin_overrides_gps_lat_lng_for_bias(self):
        """Si llegan AMBOS gps (lat/lng) y origin, el bias se centra en
        origin (= última stop). El GPS se ignora para bias en este caso."""
        client = MagicMock()
        captured = {}

        async def capture(url, json=None, headers=None, timeout=None):
            captured["json"] = json
            return MagicMock(status_code=200, json=lambda: {"suggestions": []})

        client.post = capture
        await places_v1.autocomplete_v1(
            client, "k", input="x",
            lat=40.0, lng=-3.0,  # GPS Madrid
            origin_lat=36.78, origin_lng=-6.35,  # última stop Sanlúcar
        )
        # Bias debe ir a origin (Sanlúcar), no a GPS Madrid
        assert captured["json"]["locationBias"]["circle"]["center"]["latitude"] == 36.78
        assert captured["json"]["locationBias"]["circle"]["radius"] == 5000.0

    @pytest.mark.asyncio
    async def test_country_2letter_becomes_included_region_codes(self):
        client = MagicMock()
        captured = {}

        async def capture(url, json=None, headers=None, timeout=None):
            captured["json"] = json
            return MagicMock(status_code=200, json=lambda: {"suggestions": []})

        client.post = capture
        await places_v1.autocomplete_v1(client, "k", input="x", country="es")
        assert captured["json"]["includedRegionCodes"] == ["es"]

    @pytest.mark.asyncio
    async def test_invalid_country_string_ignored(self):
        client = MagicMock()
        captured = {}

        async def capture(url, json=None, headers=None, timeout=None):
            captured["json"] = json
            return MagicMock(status_code=200, json=lambda: {"suggestions": []})

        client.post = capture
        await places_v1.autocomplete_v1(client, "k", input="x", country="Spain")
        # "Spain" is not 2 letters → must be ignored (else Google rejects request)
        assert "includedRegionCodes" not in captured["json"]

    @pytest.mark.asyncio
    async def test_session_token_renamed_to_camel_case_for_v1(self):
        client = MagicMock()
        captured = {}

        async def capture(url, json=None, headers=None, timeout=None):
            captured["json"] = json
            return MagicMock(status_code=200, json=lambda: {"suggestions": []})

        client.post = capture
        await places_v1.autocomplete_v1(
            client, "k", input="x", sessiontoken="uuid-abc"
        )
        # v1 expects sessionToken (camelCase), legacy expected sessiontoken
        assert captured["json"]["sessionToken"] == "uuid-abc"
        assert "sessiontoken" not in captured["json"]

    @pytest.mark.asyncio
    async def test_field_mask_header_sent(self):
        client = MagicMock()
        captured = {}

        async def capture(url, json=None, headers=None, timeout=None):
            captured["headers"] = headers
            return MagicMock(status_code=200, json=lambda: {"suggestions": []})

        client.post = capture
        await places_v1.autocomplete_v1(client, "k", input="x")
        assert captured["headers"]["X-Goog-Api-Key"] == "k"
        assert "X-Goog-FieldMask" in captured["headers"]
        assert "distanceMeters" in captured["headers"]["X-Goog-FieldMask"]


# ──────────────────────── Handler branch by flag ───────────────────────────

@pytest.fixture(autouse=True)
def _reset_flag_memo():
    """Invalidate the 60s in-memory cache of places_api_version between tests."""
    import main
    main._places_api_version_fetched_at = 0.0
    main._places_api_version_value = "legacy"
    # Also reset cache_mode to keep the cache out of the way
    main._places_cache_mode_fetched_at = 0.0
    main._places_cache_mode_value = "off"
    yield
    main._places_api_version_fetched_at = 0.0
    main._places_api_version_value = "legacy"
    main._places_cache_mode_fetched_at = 0.0
    main._places_cache_mode_value = "off"


class TestHandlerBranchByFlag:
    """Verify the handler in main.py picks the right backend based on flag."""

    @pytest.mark.asyncio
    async def test_flag_legacy_does_not_call_v1_module(self, client):
        """With flag='legacy', the v1 module must NEVER be invoked."""
        mock_http = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "status": "OK", "predictions": [{"place_id": "x", "description": "y"}],
        }
        mock_http.get.return_value = mock_resp
        with patch("main._get_places_api_version", new=AsyncMock(return_value="legacy")), \
             patch("main.places_v1.autocomplete_v1", new=AsyncMock()) as mock_v1, \
             patch("main.google_maps_client", return_value=mock_http):
            r = await client.get("/places/autocomplete?input=test")
            assert r.status_code == 200
            mock_v1.assert_not_called()

    @pytest.mark.asyncio
    async def test_flag_v1_calls_v1_module_not_legacy(self, client):
        """With flag='v1', the v1 module is invoked and Legacy URL is NOT hit."""
        mock_http = AsyncMock()  # if Legacy is wrongly called, this'd be hit
        with patch("main._get_places_api_version", new=AsyncMock(return_value="v1")), \
             patch("main.places_v1.autocomplete_v1", new=AsyncMock(return_value={
                 "status": "OK", "predictions": [{"place_id": "v1", "description": "ok"}],
             })) as mock_v1, \
             patch("main.google_maps_client", return_value=mock_http):
            r = await client.get("/places/autocomplete?input=test")
            assert r.status_code == 200
            mock_v1.assert_called_once()
            # The Legacy fetch must NOT have happened.
            mock_http.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_details_flag_v1_calls_v1_module(self, client):
        mock_http = AsyncMock()
        with patch("main._get_places_api_version", new=AsyncMock(return_value="v1")), \
             patch("main.places_v1.details_v1", new=AsyncMock(return_value={
                 "status": "OK",
                 "result": {"geometry": {"location": {"lat": 1.0, "lng": 2.0}},
                            "address_components": [], "formatted_address": "X",
                            "types": [], "place_id": "ChIJ1"},
             })) as mock_v1, \
             patch("main.google_maps_client", return_value=mock_http):
            r = await client.get("/places/details?place_id=ChIJ1")
            assert r.status_code == 200
            mock_v1.assert_called_once()
            mock_http.get.assert_not_called()


class TestPlacesApiVersionFlag:
    """The flag helper itself."""

    @pytest.mark.asyncio
    async def test_invalid_value_defaults_to_legacy(self):
        """A garbage value in app_config falls back to 'legacy' safely."""
        import main
        main._places_api_version_fetched_at = 0.0
        with patch("main.asyncio.to_thread", new=AsyncMock(return_value="banana")):
            assert await main._get_places_api_version() == "legacy"

    @pytest.mark.asyncio
    async def test_v1_value_returned_when_set(self):
        import main
        main._places_api_version_fetched_at = 0.0
        with patch("main.asyncio.to_thread", new=AsyncMock(return_value="v1")):
            assert await main._get_places_api_version() == "v1"

    @pytest.mark.asyncio
    async def test_exception_defaults_to_legacy(self):
        import main
        main._places_api_version_fetched_at = 0.0
        with patch("main.asyncio.to_thread", new=AsyncMock(side_effect=Exception("db"))):
            assert await main._get_places_api_version() == "legacy"
