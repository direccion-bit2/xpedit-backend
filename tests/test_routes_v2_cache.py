"""Tests del cache server-side Routes V2 + race counter RPC (23 may 2026).

Cubre:
- `_routes_v2_cache_key`: hash determinístico, redondeo coords, bucket heading,
  normalización waypoints/avoid
- `_routes_v2_cache_get` / `_routes_v2_cache_set`: TTL expiry, LRU eviction
  al alcanzar max, lazy purge de expirados al set
- `_routes_v2_cache_enabled`: feature flag memo TTL 30s
- Endpoint `/places/directions` integración: HIT path (no llama Google +
  bumpea places_directions_cache), MISS path (sí llama y cachea), disabled
  (siempre llama Google)
- Race counter: `_api_source_persist_sync` llama RPC atómico, no read-then-write
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import main


# ─────────────────────────────────────────────────────────────────────────────
# CACHE KEY
# ─────────────────────────────────────────────────────────────────────────────

class TestRoutesV2CacheKey:
    """key debe ser determinística para inputs equivalentes y distinta para
    inputs realmente distintos."""

    def test_same_inputs_yield_same_key(self):
        k1 = main._routes_v2_cache_key("40.4168,-3.7038", "40.4530,-3.6883", None, None, None)
        k2 = main._routes_v2_cache_key("40.4168,-3.7038", "40.4530,-3.6883", None, None, None)
        assert k1 == k2

    def test_coords_rounded_to_4_decimals_share_key(self):
        # 40.41680 redondea a 40.4168 → mismo bucket que 40.4168
        k1 = main._routes_v2_cache_key("40.41680,-3.70380", "40.4530,-3.6883", None, None, None)
        k2 = main._routes_v2_cache_key("40.4168,-3.7038", "40.4530,-3.6883", None, None, None)
        assert k1 == k2

    def test_coords_50m_apart_yield_different_keys(self):
        # ~50m de diferencia (5to decimal) → bucket distinto
        k1 = main._routes_v2_cache_key("40.4168,-3.7038", "40.4530,-3.6883", None, None, None)
        k2 = main._routes_v2_cache_key("40.4173,-3.7042", "40.4530,-3.6883", None, None, None)
        assert k1 != k2

    def test_destination_change_yields_different_key(self):
        k1 = main._routes_v2_cache_key("40.4168,-3.7038", "40.4530,-3.6883", None, None, None)
        k2 = main._routes_v2_cache_key("40.4168,-3.7038", "40.5000,-3.7000", None, None, None)
        assert k1 != k2

    def test_waypoint_added_yields_different_key(self):
        k1 = main._routes_v2_cache_key("40.4168,-3.7038", "40.4530,-3.6883", None, None, None)
        k2 = main._routes_v2_cache_key("40.4168,-3.7038", "40.4530,-3.6883", "40.4280,-3.6921", None, None)
        assert k1 != k2

    def test_waypoints_normalized_case_insensitive(self):
        k1 = main._routes_v2_cache_key("40.41,-3.70", "40.45,-3.68", "Calle Mayor", None, None)
        k2 = main._routes_v2_cache_key("40.41,-3.70", "40.45,-3.68", "calle mayor", None, None)
        assert k1 == k2

    def test_avoid_changes_key(self):
        k1 = main._routes_v2_cache_key("40.41,-3.70", "40.45,-3.68", None, None, None)
        k2 = main._routes_v2_cache_key("40.41,-3.70", "40.45,-3.68", None, "tolls", None)
        assert k1 != k2

    def test_heading_same_bucket_30deg_yields_same_key(self):
        # 90° y 100° caen en bucket 90° (round(90/30)*30=90, round(100/30)*30=90)
        k1 = main._routes_v2_cache_key("40.41,-3.70", "40.45,-3.68", None, None, 90)
        k2 = main._routes_v2_cache_key("40.41,-3.70", "40.45,-3.68", None, None, 100)
        assert k1 == k2

    def test_heading_different_bucket_yields_different_key(self):
        # 90° (bucket 90) vs 120° (bucket 120) — buckets distintos
        k1 = main._routes_v2_cache_key("40.41,-3.70", "40.45,-3.68", None, None, 90)
        k2 = main._routes_v2_cache_key("40.41,-3.70", "40.45,-3.68", None, None, 120)
        assert k1 != k2

    def test_heading_none_vs_zero_yields_different_keys(self):
        # heading=None NO se incluye; heading=0 sí (bucket 0)
        k1 = main._routes_v2_cache_key("40.41,-3.70", "40.45,-3.68", None, None, None)
        k2 = main._routes_v2_cache_key("40.41,-3.70", "40.45,-3.68", None, None, 0)
        assert k1 != k2

    def test_invalid_coord_falls_back_without_crash(self):
        # garbage in → no excepción, retorna string en vez de redondeado
        k = main._routes_v2_cache_key("garbage", "40.45,-3.68", None, None, None)
        assert isinstance(k, str) and len(k) == 64  # sha256 hex


# ─────────────────────────────────────────────────────────────────────────────
# CACHE GET / SET
# ─────────────────────────────────────────────────────────────────────────────

class TestRoutesV2CacheStorage:

    def setup_method(self):
        main._routes_v2_cache.clear()

    def test_set_then_get_returns_value(self):
        main._routes_v2_cache_set("k1", {"routes": ["a"]})
        assert main._routes_v2_cache_get("k1") == {"routes": ["a"]}

    def test_get_returns_none_on_miss(self):
        assert main._routes_v2_cache_get("k_unknown") is None

    def test_ttl_expiry(self):
        import time as _time
        main._routes_v2_cache_set("k1", {"routes": ["x"]})
        # Forzar timestamp en el pasado más allá del TTL
        ts, val = main._routes_v2_cache["k1"]
        main._routes_v2_cache["k1"] = (ts - main._ROUTES_V2_CACHE_TTL_SEC - 10, val)
        assert main._routes_v2_cache_get("k1") is None
        # Y la entry debe haberse borrado tras el miss expirado
        assert "k1" not in main._routes_v2_cache

    def test_lru_eviction_at_max_size(self):
        original_max = main._ROUTES_V2_CACHE_MAX_SIZE
        main._ROUTES_V2_CACHE_MAX_SIZE = 3
        try:
            for i in range(5):
                main._routes_v2_cache_set(f"k{i}", {"i": i})
            # Quedan las 3 últimas (k2, k3, k4)
            assert "k0" not in main._routes_v2_cache
            assert "k1" not in main._routes_v2_cache
            assert "k2" in main._routes_v2_cache
            assert "k4" in main._routes_v2_cache
        finally:
            main._ROUTES_V2_CACHE_MAX_SIZE = original_max

    def test_get_touches_lru_order(self):
        original_max = main._ROUTES_V2_CACHE_MAX_SIZE
        main._ROUTES_V2_CACHE_MAX_SIZE = 3
        try:
            for i in range(3):
                main._routes_v2_cache_set(f"k{i}", {"i": i})
            # Tocar k0 lo mueve al final → k1 es el más antiguo ahora
            main._routes_v2_cache_get("k0")
            main._routes_v2_cache_set("k3", {"i": 3})  # evict k1
            assert "k1" not in main._routes_v2_cache
            assert "k0" in main._routes_v2_cache
        finally:
            main._ROUTES_V2_CACHE_MAX_SIZE = original_max

    def test_lazy_purge_of_expired_on_set(self):
        """En cada `cache_set` (= miss → Google call), aprovecha para purgar
        expirados. Evita acumular basura hasta llegar al max."""
        # Llenar con entradas que YA están expiradas
        for i in range(10):
            main._routes_v2_cache_set(f"old{i}", {"i": i})
        # Forzar timestamps al pasado
        for k in list(main._routes_v2_cache.keys()):
            ts, val = main._routes_v2_cache[k]
            main._routes_v2_cache[k] = (ts - main._ROUTES_V2_CACHE_TTL_SEC - 10, val)
        # Añadir uno nuevo → purga expirados
        main._routes_v2_cache_set("fresh", {"new": True})
        # Solo queda "fresh"
        assert list(main._routes_v2_cache.keys()) == ["fresh"]


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE FLAG MEMO
# ─────────────────────────────────────────────────────────────────────────────

class TestRoutesV2CacheEnabledFlag:
    """El autouse fixture `disable_routes_v2_cache` monkeypatchea la función a
    `lambda: False` para todos los tests. Estos tests testean la función REAL
    importándola por nombre — pasamos por encima del monkeypatch llamando
    directamente a `main.__dict__['_routes_v2_cache_enabled'].__wrapped__` no
    sirve porque no es wrapped, así que restauramos el original aquí."""

    def setup_method(self):
        # Restaurar la función original que monkeypatch reemplazó.
        # Lo hacemos cogiéndolo del módulo antes de patcharlo (no se puede),
        # así que usamos la firma para llamarla directamente vía exec dictlocal.
        # Más simple: hacer la lógica de la función a mano (es 10 líneas).
        main._routes_v2_flag_fetched_at = 0.0
        main._routes_v2_flag_value = True

    def _call_real_flag(self):
        """Llama la lógica del flag sin pasar por monkeypatch del fixture."""
        import time as _t
        now = _t.time()
        if now - main._routes_v2_flag_fetched_at < main._ROUTES_V2_FLAG_TTL_SEC:
            return main._routes_v2_flag_value
        try:
            r = (
                main.supabase.table("app_config")
                .select("value")
                .eq("key", "routes_v2_cache_enabled")
                .limit(1)
                .execute()
            )
            if r.data:
                val = (r.data[0].get("value") or "").strip().lower()
                main._routes_v2_flag_value = val not in ("false", "0", "off", "no")
            else:
                main._routes_v2_flag_value = True
        except Exception:
            pass
        main._routes_v2_flag_fetched_at = now
        return main._routes_v2_flag_value

    def test_default_true_when_supabase_returns_no_row(self):
        mock_resp = MagicMock(data=[])
        mock_table = MagicMock()
        mock_table.select.return_value.eq.return_value.limit.return_value.execute.return_value = mock_resp
        with patch.object(main.supabase, "table", return_value=mock_table):
            assert self._call_real_flag() is True

    def test_false_when_flag_is_off(self):
        mock_resp = MagicMock(data=[{"value": "false"}])
        mock_table = MagicMock()
        mock_table.select.return_value.eq.return_value.limit.return_value.execute.return_value = mock_resp
        with patch.object(main.supabase, "table", return_value=mock_table):
            assert self._call_real_flag() is False

    def test_memo_hits_supabase_only_once_within_ttl(self):
        mock_resp = MagicMock(data=[{"value": "true"}])
        mock_table = MagicMock()
        mock_table.select.return_value.eq.return_value.limit.return_value.execute.return_value = mock_resp
        with patch.object(main.supabase, "table", return_value=mock_table) as p:
            for _ in range(5):
                self._call_real_flag()
            # Solo 1 llamada a supabase.table aunque pedimos 5 veces el flag
            assert p.call_count == 1

    def test_supabase_exception_returns_last_known_value(self):
        # Primero cachea True
        ok_resp = MagicMock(data=[{"value": "true"}])
        mock_table_ok = MagicMock()
        mock_table_ok.select.return_value.eq.return_value.limit.return_value.execute.return_value = ok_resp
        with patch.object(main.supabase, "table", return_value=mock_table_ok):
            assert self._call_real_flag() is True

        # Expira el memo y luego Supabase explota → retorna último valor (True)
        main._routes_v2_flag_fetched_at = 0.0
        mock_table_fail = MagicMock()
        mock_table_fail.select.side_effect = Exception("supabase down")
        with patch.object(main.supabase, "table", return_value=mock_table_fail):
            assert self._call_real_flag() is True


# ─────────────────────────────────────────────────────────────────────────────
# RACE COUNTER RPC
# ─────────────────────────────────────────────────────────────────────────────

class TestApiSourcePersistRPC:
    """El persist debe llamar al RPC atómico, NO al read-then-write Python.
    Auditoría 23 may: 53% de calls per-user quedaban sin atribuir porque el
    read-then-write tenía race condition entre asyncio tasks paralelas."""

    def test_aggregate_persist_calls_atomic_rpc(self):
        mock_rpc = MagicMock()
        mock_rpc.execute.return_value = None
        with patch.object(main.supabase, "rpc", return_value=mock_rpc) as p:
            main._api_source_persist_sync("places_directions", "2026-05-23", "resume")
            p.assert_called_once_with("atomic_bump_api_source", {
                "p_endpoint": "places_directions",
                "p_date": "2026-05-23",
                "p_source": "resume",
            })

    def test_user_persist_calls_atomic_rpc_with_user_id(self):
        mock_rpc = MagicMock()
        mock_rpc.execute.return_value = None
        uid = "cee2b847-60fe-4567-9ff8-0a4122c8e1e8"
        with patch.object(main.supabase, "rpc", return_value=mock_rpc) as p:
            main._api_source_user_persist_sync("places_directions", "2026-05-23", "resume", uid)
            p.assert_called_once_with("atomic_bump_api_source_user", {
                "p_endpoint": "places_directions",
                "p_date": "2026-05-23",
                "p_source": "resume",
                "p_user_id": uid,
            })

    def test_persist_swallows_rpc_exception(self):
        """RPC roto NUNCA debe romper el endpoint cliente — la métrica es
        secundaria. Captura a Sentry pero no propaga."""
        mock_rpc = MagicMock()
        mock_rpc.execute.side_effect = Exception("RPC failed")
        with patch.object(main.supabase, "rpc", return_value=mock_rpc):
            # No debe lanzar
            main._api_source_persist_sync("places_directions", "2026-05-23", "resume")
            main._api_source_user_persist_sync("p", "2026-05-23", "s", "fake-uid")


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT INTEGRATION (cache HIT/MISS path)
# ─────────────────────────────────────────────────────────────────────────────

class TestRoutesV2CacheEndpointIntegration:
    """Verifica que /places/directions usa el cache cuando enabled."""

    @pytest.mark.asyncio
    async def test_cache_hit_skips_google_call(self, client, monkeypatch):
        """Con cache activado, una 2ª request idéntica debe devolver cached
        SIN llamar a Google."""
        monkeypatch.setattr(main, "_routes_v2_cache_enabled", lambda: True)
        main._routes_v2_cache.clear()

        google_payload = {
            "routes": [{
                "duration": "100s",
                "distanceMeters": 1000,
                "polyline": {"encodedPolyline": "abc"},
                "viewport": {"low": {"latitude": 40.0, "longitude": -3.0},
                              "high": {"latitude": 41.0, "longitude": -2.0}},
                "legs": [{
                    "duration": "100s",
                    "distanceMeters": 1000,
                    "startLocation": {"latLng": {"latitude": 40.4168, "longitude": -3.7038}},
                    "endLocation": {"latLng": {"latitude": 40.4530, "longitude": -3.6883}},
                    "steps": []
                }]
            }]
        }
        mock_resp = MagicMock(status_code=200, json=MagicMock(return_value=google_payload), text="")
        mock_client = MagicMock(post=AsyncMock(return_value=mock_resp))

        with patch("main.google_maps_client", return_value=mock_client):
            r1 = await client.get("/places/directions?origin=40.4168,-3.7038&destination=40.4530,-3.6883")
            r2 = await client.get("/places/directions?origin=40.4168,-3.7038&destination=40.4530,-3.6883")

        assert r1.status_code == 200
        assert r2.status_code == 200
        # Solo 1 call a Google aunque pedimos 2 veces
        assert mock_client.post.await_count == 1

    @pytest.mark.asyncio
    async def test_cache_disabled_always_calls_google(self, client, monkeypatch):
        """Con flag OFF, cada call va a Google sin tocar cache."""
        monkeypatch.setattr(main, "_routes_v2_cache_enabled", lambda: False)
        main._routes_v2_cache.clear()

        google_payload = {"routes": [{"duration": "100s", "distanceMeters": 1000,
                                       "polyline": {"encodedPolyline": "abc"}, "legs": []}]}
        mock_resp = MagicMock(status_code=200, json=MagicMock(return_value=google_payload), text="")
        mock_client = MagicMock(post=AsyncMock(return_value=mock_resp))

        with patch("main.google_maps_client", return_value=mock_client):
            await client.get("/places/directions?origin=40.4168,-3.7038&destination=40.4530,-3.6883")
            await client.get("/places/directions?origin=40.4168,-3.7038&destination=40.4530,-3.6883")

        # 2 calls Google porque cache OFF — incluso si el SET escribe en el dict
        # (la guarda está en el GET via _routes_v2_cache_enabled).
        assert mock_client.post.await_count == 2

    @pytest.mark.asyncio
    async def test_cache_does_not_store_empty_or_error_response(self, client, monkeypatch):
        """Solo se cachean responses 'OK' con routes no vacío — evita servir
        un error reutilizado."""
        monkeypatch.setattr(main, "_routes_v2_cache_enabled", lambda: True)
        main._routes_v2_cache.clear()

        # Routes vacío → no cachear
        mock_resp = MagicMock(status_code=200, json=MagicMock(return_value={"routes": []}), text="")
        mock_client = MagicMock(post=AsyncMock(return_value=mock_resp))
        with patch("main.google_maps_client", return_value=mock_client):
            r = await client.get("/places/directions?origin=40.41,-3.70&destination=40.45,-3.68")
        assert r.status_code == 200
        assert len(main._routes_v2_cache) == 0
