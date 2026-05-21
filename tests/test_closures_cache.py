"""REGRESSION GUARDS (Miguel 21 may 2026): cache en street_closures scraper.

Auditoría 10 puntos cumplida (ver `cambios_log_exhaustivo.md` 2026-05-21).

El scraper de Sanlúcar geocodificaba TODOS los cierres cada 30 min sin cache
= ~6000 calls Google/día = ~$900/mes. Ahora `geocode_record(existing=...)`
acepta un dict de cache (lo que está en BD) y skip Google calls si la
`localizacion` no cambió.

Si alguien rompe el cache en el futuro, estos tests fallan ANTES de que el
bill de Google se dispare otra vez.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from street_closures import ClosureRecord, geocode_record


def _make_record(**overrides) -> ClosureRecord:
    """Helper: crear ClosureRecord con defaults razonables."""
    from datetime import datetime, timezone
    defaults = {
        "source": "ayto_sanlucar",
        "source_url": "https://example.com/node/123",
        "city": "Sanlúcar de Barrameda",
        "street_name": "Calle Ancha",
        "starts_at": datetime(2026, 5, 21, 8, 0, tzinfo=timezone.utc),
        "ends_at": datetime(2026, 5, 21, 20, 0, tzinfo=timezone.utc),
        "raw_payload": {"localizacion": "Calle Ancha"},
    }
    defaults.update(overrides)
    return ClosureRecord(**defaults)


class TestClosuresCacheRegression:
    """Bloquea regresión: SIN cache = $900/mes Google waste."""

    @pytest.mark.asyncio
    async def test_cache_hit_when_localizacion_unchanged(self):
        """Si existing tiene lat/lng Y localizacion no cambió → NO llama Google."""
        record = _make_record(raw_payload={"localizacion": "Calle Ancha"})
        existing = {
            "source_url": record.source_url,
            "lat": 36.7720608,
            "lng": -6.3549026,
            "lat_from": None,
            "lng_from": None,
            "lat_to": None,
            "lng_to": None,
            "street_polyline": "encoded_poly_abc",
            "raw_payload": {"localizacion": "Calle Ancha"},
        }
        # Mock client que FALLARÍA si se llama (asegura zero Google calls)
        client = AsyncMock()
        client.get = AsyncMock(side_effect=AssertionError(
            "CRÍTICO: cache hit pero el código aún llamó Google. "
            "Esto es exactamente lo que el cache evitaba ($900/mes Sanlúcar). "
            "Revisar `geocode_record` y `[[cambios_log_exhaustivo]]` 2026-05-21."
        ))

        await geocode_record(client, "fake_key", record, existing=existing)

        # Coords vinieron del cache, no de Google
        assert record.lat == 36.7720608
        assert record.lng == -6.3549026
        assert record.street_polyline == "encoded_poly_abc"
        # Google NO se llamó (si se hubiera llamado, side_effect habría lanzado)
        client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_miss_when_no_existing_row(self):
        """Si no hay existing → llama Google como antes (flow original intacto)."""
        record = _make_record(
            segment_from=None,
            segment_to=None,
            raw_payload={"localizacion": "Calle Nueva"},
        )

        mock_resp = MagicMock()
        mock_resp.json = MagicMock(return_value={
            "status": "OK",
            "results": [{"geometry": {"location": {"lat": 36.0, "lng": -6.0}}}],
        })
        client = AsyncMock()
        client.get = AsyncMock(return_value=mock_resp)

        await geocode_record(client, "fake_key", record, existing=None)

        # Google SÍ se llamó (1 vez para centroid de calle)
        assert client.get.call_count == 1
        assert record.lat == 36.0
        assert record.lng == -6.0

    @pytest.mark.asyncio
    async def test_cache_invalidated_when_localizacion_changed(self):
        """Si existing.localizacion != record.localizacion (cierre se editó
        en la web) → re-geocodifica. Cache NO debe servir coords viejas para
        una calle que cambió."""
        # En BD teníamos otra localización (vieja)
        existing = {
            "source_url": "https://example.com/node/123",
            "lat": 36.0,
            "lng": -6.0,
            "raw_payload": {"localizacion": "Calle Vieja"},
        }
        # Ahora el scraper detecta nueva localización
        record = _make_record(
            segment_from=None,
            segment_to=None,
            raw_payload={"localizacion": "Calle Nueva — TRAMO CAMBIADO"},
        )

        mock_resp = MagicMock()
        mock_resp.json = MagicMock(return_value={
            "status": "OK",
            "results": [{"geometry": {"location": {"lat": 37.0, "lng": -7.0}}}],
        })
        client = AsyncMock()
        client.get = AsyncMock(return_value=mock_resp)

        await geocode_record(client, "fake_key", record, existing=existing)

        # Re-geocodificó porque localizacion cambió
        assert client.get.call_count == 1
        # Coords nuevas (no las viejas del cache)
        assert record.lat == 37.0
        assert record.lng == -7.0

    @pytest.mark.asyncio
    async def test_cache_skipped_when_existing_has_no_lat(self):
        """Si existing existe pero lat=NULL (geocoding falló en intento previo)
        → reintentar Google. NO servir cache vacío."""
        existing = {
            "source_url": "https://example.com/node/123",
            "lat": None,
            "lng": None,
            "raw_payload": {"localizacion": "Calle Ancha"},
        }
        record = _make_record(
            segment_from=None,
            segment_to=None,
            raw_payload={"localizacion": "Calle Ancha"},
        )

        mock_resp = MagicMock()
        mock_resp.json = MagicMock(return_value={
            "status": "OK",
            "results": [{"geometry": {"location": {"lat": 36.0, "lng": -6.0}}}],
        })
        client = AsyncMock()
        client.get = AsyncMock(return_value=mock_resp)

        await geocode_record(client, "fake_key", record, existing=existing)

        # Reintentó (no había lat válido en cache)
        assert client.get.call_count == 1
        assert record.lat == 36.0

    @pytest.mark.asyncio
    async def test_cache_preserves_segment_endpoints_and_polyline(self):
        """Si cache HIT con segmento (lat_from/to + polyline) → reusar todo,
        evitando 2 Geocoding + 1 Directions calls = $0.015/cierre."""
        record = _make_record(
            segment_from="Plaza Mayor",
            segment_to="Calle Sol",
            raw_payload={"localizacion": "Calle Ancha entre Plaza Mayor y Calle Sol"},
        )
        existing = {
            "source_url": record.source_url,
            "lat": 36.5,
            "lng": -6.4,
            "lat_from": 36.4,
            "lng_from": -6.5,
            "lat_to": 36.6,
            "lng_to": -6.3,
            "street_polyline": "encoded_segment_xyz",
            "raw_payload": {
                "localizacion": "Calle Ancha entre Plaza Mayor y Calle Sol"
            },
        }
        client = AsyncMock()
        client.get = AsyncMock(side_effect=AssertionError(
            "Cache HIT con segmento — NO debe llamar Geocoding ni Directions"
        ))

        await geocode_record(client, "fake_key", record, existing=existing)

        # Todos los campos restaurados del cache
        assert record.lat == 36.5
        assert record.lng == -6.4
        assert record.lat_from == 36.4
        assert record.lat_to == 36.6
        assert record.street_polyline == "encoded_segment_xyz"
        client.get.assert_not_called()
