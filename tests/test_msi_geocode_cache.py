"""REGRESSION GUARDS (Miguel 21 may 2026): cache MSI geocoding in-memory.

Auditoría 10 puntos cumplida (ver `cambios_log_exhaustivo.md` 2026-05-21).

El flujo MSI llamaba Google Geocoding por CADA stop sin cache, incluso si el
mismo repartidor importaba 2 veces el mismo batch. Coste estimado ~$510/mes
hoy, ~$1200/mes a 100 paying.

Ahora `_msi_geocode_one` cachea por (country, postal_code, city, street, number)
con TTL 24h. Si alguien rompe el cache estos tests fallan ANTES de que el
bill se dispare.

Reglas críticas validadas:
- HIT: misma dirección 2x → 1 sola call Google
- MISS por country distinto: ES vs AR son keys distintas
- NO cachea round 2 (con bbox) — bbox altera resultado deliberadamente
- NO cachea errors / zero_results — esos se reintentan
- Stop sin street o sin (cp|city) → NO cachea (key ambigua)
- TTL expira tras 24h (cache_set + tiempo viejo → MISS)
"""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Import via main module to access globals correctly
import main


def _reset_cache():
    main._MSI_GEOCODE_CACHE.clear()


def _mock_response_ok(lat=40.0, lng=-3.0):
    resp = MagicMock()
    resp.json = MagicMock(return_value={
        "status": "OK",
        "results": [{
            "geometry": {
                "location": {"lat": lat, "lng": lng},
                "location_type": "ROOFTOP",
            },
            "formatted_address": "Calle Test 1, Madrid, España",
            "place_id": "pid_abc",
        }],
    })
    return resp


def _mock_response_zero():
    resp = MagicMock()
    resp.json = MagicMock(return_value={"status": "ZERO_RESULTS"})
    return resp


class TestMsiGeocodeCacheRegression:

    def setup_method(self):
        _reset_cache()

    @pytest.mark.asyncio
    async def test_cache_key_requires_street_and_locality(self):
        """Stops vagas (sin street o sin cp/city) NO deben generar key."""
        # Sin street
        assert main._msi_geocode_cache_key({"city": "Madrid"}, "ES") is None
        # Sin cp NI city
        assert main._msi_geocode_cache_key({"street": "Calle Mayor"}, "ES") is None
        # Street vacío
        assert main._msi_geocode_cache_key({"street": "  ", "city": "Madrid"}, "ES") is None
        # Válido: street + city
        key = main._msi_geocode_cache_key({"street": "Calle Mayor", "city": "Madrid"}, "ES")
        assert key == "ES||madrid|calle mayor|"
        # Válido: street + cp
        key2 = main._msi_geocode_cache_key({"street": "Calle Sol", "postal_code": "28013"}, "ES")
        assert key2 == "ES|28013||calle sol|"

    @pytest.mark.asyncio
    async def test_cache_hit_second_call_skips_google(self):
        """Llamada 1 → Google. Llamada 2 misma dirección → cache HIT, cero Google."""
        stop = {"street": "Calle Mayor", "number": "1", "postal_code": "28013", "city": "Madrid"}
        client = AsyncMock()
        client.get = AsyncMock(return_value=_mock_response_ok(40.0, -3.7))

        # Llamada 1: MISS → Google
        with patch.object(main, "GOOGLE_API_KEY", "fake_key"):
            r1 = await main._msi_geocode_one(client, stop, country="ES")
        assert r1["status"] == "ok"
        assert r1["lat"] == 40.0
        assert client.get.call_count == 1

        # Llamada 2: HIT → cero Google calls extra
        client2 = AsyncMock()
        client2.get = AsyncMock(side_effect=AssertionError(
            "CRÍTICO: cache MISS pero debería ser HIT. Esto regresa el "
            "$510/mes que el cache evita. Revisar [[cambios_log_exhaustivo]] "
            "2026-05-21 + _msi_geocode_one."
        ))
        with patch.object(main, "GOOGLE_API_KEY", "fake_key"):
            r2 = await main._msi_geocode_one(client2, stop, country="ES")
        assert r2["status"] == "ok"
        assert r2["lat"] == 40.0
        assert r2.get("_from_cache") is True
        client2.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_miss_when_country_differs(self):
        """Misma calle/cp/city pero country distinto → keys distintas, MISS."""
        stop = {"street": "Calle Mayor", "postal_code": "28013", "city": "Madrid"}
        client = AsyncMock()
        client.get = AsyncMock(return_value=_mock_response_ok(40.0, -3.7))

        with patch.object(main, "GOOGLE_API_KEY", "fake_key"):
            await main._msi_geocode_one(client, stop, country="ES")
        # Misma stop pero country AR → debe ir a Google de nuevo
        with patch.object(main, "GOOGLE_API_KEY", "fake_key"):
            await main._msi_geocode_one(client, stop, country="AR")

        assert client.get.call_count == 2

    @pytest.mark.asyncio
    async def test_cache_skipped_when_bbox_provided(self):
        """Round 2 con bbox → NO usa cache. Bbox altera resultado deliberadamente."""
        stop = {"street": "Calle Sol", "postal_code": "28013", "city": "Madrid"}
        bbox = {"sw_lat": 40.0, "sw_lng": -3.8, "ne_lat": 40.5, "ne_lng": -3.5}
        client = AsyncMock()
        client.get = AsyncMock(return_value=_mock_response_ok(40.4, -3.7))

        # Round 1: cachea
        with patch.object(main, "GOOGLE_API_KEY", "fake_key"):
            await main._msi_geocode_one(client, stop, country="ES")
        assert client.get.call_count == 1

        # Round 2 con bbox: debe ir a Google de nuevo (NO usa cache)
        with patch.object(main, "GOOGLE_API_KEY", "fake_key"):
            r2 = await main._msi_geocode_one(client, stop, country="ES", bbox=bbox)
        assert client.get.call_count == 2
        assert r2.get("_from_cache") is not True

    @pytest.mark.asyncio
    async def test_cache_does_not_store_zero_results(self):
        """status=zero_results NO se cachea — la próxima llamada reintenta."""
        stop = {"street": "Calle Inexistente", "postal_code": "99999", "city": "Madrid"}
        client = AsyncMock()
        client.get = AsyncMock(return_value=_mock_response_zero())

        with patch.object(main, "GOOGLE_API_KEY", "fake_key"):
            r1 = await main._msi_geocode_one(client, stop, country="ES")
        assert r1["status"] == "zero_results"
        # No debe haber entrada en cache
        key = main._msi_geocode_cache_key(stop, "ES")
        assert main._msi_geocode_cache_get(key) is None

        # 2da llamada también va a Google (no se cacheó el error)
        client.get = AsyncMock(return_value=_mock_response_ok(40.0, -3.7))
        with patch.object(main, "GOOGLE_API_KEY", "fake_key"):
            r2 = await main._msi_geocode_one(client, stop, country="ES")
        assert r2["status"] == "ok"

    @pytest.mark.asyncio
    async def test_cache_does_not_store_errors(self):
        """status=error NO se cachea — la próxima llamada reintenta."""
        stop = {"street": "Calle X", "postal_code": "28013", "city": "Madrid"}
        client = AsyncMock()
        client.get = AsyncMock(side_effect=Exception("network timeout"))

        with patch.object(main, "GOOGLE_API_KEY", "fake_key"):
            r1 = await main._msi_geocode_one(client, stop, country="ES")
        assert r1["status"] == "error"
        key = main._msi_geocode_cache_key(stop, "ES")
        assert main._msi_geocode_cache_get(key) is None

    @pytest.mark.asyncio
    async def test_cache_does_not_save_vague_stops(self):
        """Stop sin street → NO cachea aunque Google devuelva OK."""
        stop = {"city": "Madrid"}  # sin street
        client = AsyncMock()
        client.get = AsyncMock(return_value=_mock_response_ok(40.0, -3.7))

        with patch.object(main, "GOOGLE_API_KEY", "fake_key"):
            r1 = await main._msi_geocode_one(client, stop, country="ES")
        # Stop sin street + sin number devuelve zero_results sin pegar Google
        # porque address_query es solo "Madrid". Pero si llegara a Google,
        # tampoco se cachearía porque cache_key es None.
        assert main._msi_geocode_cache_key(stop, "ES") is None

    def test_cache_ttl_expires_after_24h(self):
        """Entry vieja (>24h) se considera expirada → MISS."""
        key = "ES|28013|madrid|calle mayor|1"
        # Insertar entrada falsamente con timestamp viejo (25h atrás)
        old_ts = time.time() - (25 * 3600)
        main._MSI_GEOCODE_CACHE[key] = (old_ts, {"status": "ok", "lat": 40.0, "lng": -3.7})

        # Get debe devolver None y limpiar la entrada
        assert main._msi_geocode_cache_get(key) is None
        assert key not in main._MSI_GEOCODE_CACHE

    def test_cache_eviction_when_max_reached(self):
        """Al alcanzar MAX, evict la entrada más vieja."""
        # Saturar cache (usamos un MAX pequeño temporal)
        original_max = main._MSI_GEOCODE_CACHE_MAX
        main._MSI_GEOCODE_CACHE_MAX = 3
        try:
            main._MSI_GEOCODE_CACHE.clear()
            # Insertar 3 con timestamps incrementales
            main._MSI_GEOCODE_CACHE["k1"] = (1000.0, {"v": 1})
            main._MSI_GEOCODE_CACHE["k2"] = (2000.0, {"v": 2})
            main._MSI_GEOCODE_CACHE["k3"] = (3000.0, {"v": 3})

            # Insertar k4 debe expulsar k1 (más vieja)
            main._msi_geocode_cache_set("k4", {"v": 4})
            assert "k1" not in main._MSI_GEOCODE_CACHE
            assert "k4" in main._MSI_GEOCODE_CACHE
            assert len(main._MSI_GEOCODE_CACHE) == 3
        finally:
            main._MSI_GEOCODE_CACHE_MAX = original_max
            main._MSI_GEOCODE_CACHE.clear()
