"""Regression suite for places_autocomplete_cache (re-enabled 21 may 2026).

Covers root cause of incident 5 may + new feature flag behaviour:
- mode='off' → no cache lookup, no cache write, plain Google call
- mode='shadow' → write fire-and-forget, but Google response always served
- mode='on' → cache hit short-circuits Google, miss writes after Google
- cache lookup/write errors are SWALLOWED — never break autocomplete
- session token forwarding still intact
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _reset_cache_mode_memo():
    """Invalidate the 60s in-memory cache of the flag between tests."""
    import main
    main._places_cache_mode_fetched_at = 0.0
    main._places_cache_mode_value = "off"
    yield
    main._places_cache_mode_fetched_at = 0.0
    main._places_cache_mode_value = "off"


class TestPlacesCacheFlag:
    """`places_cache_mode` flag controls cache behaviour."""

    @pytest.mark.asyncio
    async def test_mode_off_skips_cache_calls_google(self, client):
        """flag='off' → no cache lookup, no cache write. Default safe behaviour."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "OK", "predictions": [{"description": "Madrid"}]}
        mock_http = AsyncMock()
        mock_http.get.return_value = mock_resp

        with patch("main._get_places_cache_mode", AsyncMock(return_value="off")), \
             patch("main._places_cache_lookup_sync") as mock_lookup, \
             patch("main._places_cache_write_sync") as mock_write, \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get("/places/autocomplete?input=Madrid")
        assert response.status_code == 200
        assert response.json()["status"] == "OK"
        mock_lookup.assert_not_called()
        mock_write.assert_not_called()

    @pytest.mark.asyncio
    async def test_mode_shadow_writes_but_serves_google(self, client):
        """flag='shadow' → cache write fire-and-forget, Google response served."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "OK", "predictions": [{"description": "Madrid"}]}
        mock_http = AsyncMock()
        mock_http.get.return_value = mock_resp

        with patch("main._get_places_cache_mode", AsyncMock(return_value="shadow")), \
             patch("main._places_cache_lookup_sync", return_value=None) as mock_lookup, \
             patch("main._places_cache_write_sync", return_value=True) as mock_write, \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get("/places/autocomplete?input=Madrid")
            # Yield once to let fire-and-forget task run.
            import asyncio
            await asyncio.sleep(0)
        assert response.status_code == 200
        body = response.json()
        # NEVER has source=cache in shadow (would mean we served cache, which we don't).
        assert body.get("source") != "cache"
        assert body["status"] == "OK"
        mock_lookup.assert_called_once()  # we DO lookup to compute shadow_would_hit
        mock_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_mode_on_cache_hit_skips_google(self, client):
        """flag='on' + hit → cached predictions served, Google NOT called."""
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=AssertionError("Google was called on cache hit"))

        cached = {"predictions": [{"description": "Madrid (cached)"}], "expires_at": "2099-01-01", "hits": 5}

        with patch("main._get_places_cache_mode", AsyncMock(return_value="on")), \
             patch("main._places_cache_lookup_sync", return_value=cached), \
             patch("main._places_cache_bump_sync") as mock_bump, \
             patch("main._places_cache_write_sync") as mock_write, \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get("/places/autocomplete?input=Madrid")
        assert response.status_code == 200
        body = response.json()
        assert body["source"] == "cache"
        assert body["predictions"][0]["description"] == "Madrid (cached)"
        mock_write.assert_not_called()  # hit doesn't write
        # bump may or may not have run yet (fire-and-forget) — don't assert

    @pytest.mark.asyncio
    async def test_mode_on_miss_calls_google_and_writes(self, client):
        """flag='on' + miss → Google called, cache write triggered."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "OK", "predictions": [{"description": "Cádiz"}]}
        mock_http = AsyncMock()
        mock_http.get.return_value = mock_resp

        with patch("main._get_places_cache_mode", AsyncMock(return_value="on")), \
             patch("main._places_cache_lookup_sync", return_value=None), \
             patch("main._places_cache_write_sync", return_value=True) as mock_write, \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get("/places/autocomplete?input=Cadiz")
            import asyncio
            await asyncio.sleep(0)
        assert response.status_code == 200
        assert response.json().get("source") != "cache"
        mock_write.assert_called_once()


class TestPlacesCacheResilience:
    """Cache layer NEVER breaks autocomplete — fail-open."""

    @pytest.mark.asyncio
    async def test_cache_lookup_raises_falls_through_to_google(self, client):
        """If _places_cache_lookup_sync raises, handler still serves Google response."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "OK", "predictions": [{"description": "Sevilla"}]}
        mock_http = AsyncMock()
        mock_http.get.return_value = mock_resp

        with patch("main._get_places_cache_mode", AsyncMock(return_value="on")), \
             patch("main._places_cache_lookup_sync", side_effect=RuntimeError("DB exploded")), \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get("/places/autocomplete?input=Sevilla")
        assert response.status_code == 200
        assert response.json()["status"] == "OK"
        assert response.json()["predictions"][0]["description"] == "Sevilla"

    @pytest.mark.asyncio
    async def test_flag_fetch_raises_defaults_to_off(self, client):
        """If app_config read fails, _get_places_cache_mode returns 'off' silently."""
        import main
        # Force a fresh fetch by zeroing the memo
        main._places_cache_mode_fetched_at = 0.0
        # Override the supabase.table chain to raise inside the helper
        with patch.object(main.supabase, "table", side_effect=RuntimeError("supabase down")):
            mode = await main._get_places_cache_mode()
        assert mode == "off"

    @pytest.mark.asyncio
    async def test_query_under_3_chars_skips_cache(self, client):
        """norm_query < 3 chars → cache_eligible=False (avoid wasting rows on 'ca')."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "OK", "predictions": []}
        mock_http = AsyncMock()
        mock_http.get.return_value = mock_resp

        with patch("main._get_places_cache_mode", AsyncMock(return_value="on")), \
             patch("main._places_cache_lookup_sync") as mock_lookup, \
             patch("main._places_cache_write_sync") as mock_write, \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get("/places/autocomplete?input=ca")
        assert response.status_code == 200
        mock_lookup.assert_not_called()
        mock_write.assert_not_called()


class TestPlacesCacheBackcompat:
    """Existing behaviour (session token, country bias, retries) unaffected."""

    @pytest.mark.asyncio
    async def test_sessiontoken_still_forwarded_with_cache_on(self, client):
        """Even when cache is enabled, the sessiontoken must reach Google on a miss."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "OK", "predictions": []}
        mock_http = AsyncMock()
        mock_http.get.return_value = mock_resp

        with patch("main._get_places_cache_mode", AsyncMock(return_value="on")), \
             patch("main._places_cache_lookup_sync", return_value=None), \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get(
                "/places/autocomplete?input=Madrid&sessiontoken=uuid-abc"
            )
        assert response.status_code == 200
        params = mock_http.get.call_args.kwargs.get("params", {})
        assert params.get("sessiontoken") == "uuid-abc"


class TestAcCacheKeyGranularity:
    """REGRESSION GUARD (22 may 2026 commit ab68af4): _ac_cache_key bias grid
    es 0.25° (~27km). Antes era 0.1° (~11km) y drivers en ciudades vecinas
    (Sanlúcar/Chipiona/Jerez/El Puerto, <30km) NO compartían cache → hit rate
    empírico <10%. Si alguien vuelve a round(lat,1), el bug regresa silencioso.
    Estos tests fallan al instante si se cambia la granularidad."""

    def test_bias_uses_quarter_degree_grid_lat(self):
        """lat redondeado a múltiplos de 0.25°."""
        from main import _ac_cache_key
        # lat 36.78 → round(36.78*4)/4 = round(147.12)/4 = 147/4 = 36.75
        _, bias = _ac_cache_key("calle ancha", 36.78, -6.35)
        # 36.78 cae más cerca de 36.75 que de 37.00
        assert bias.startswith("36.75,"), f"expected lat=36.75, got bias={bias!r}"

    def test_bias_uses_quarter_degree_grid_lng(self):
        """lng redondeado a múltiplos de 0.25°."""
        from main import _ac_cache_key
        # lng -6.35 → round(-6.35*4)/4 = round(-25.4)/4 = -25/4 = -6.25
        _, bias = _ac_cache_key("calle ancha", 36.78, -6.35)
        assert bias.endswith(",-6.25"), f"expected lng=-6.25, got bias={bias!r}"

    def test_bias_shared_across_27km_neighbors(self):
        """Drivers a ~10-20km de distancia comparten cache (mismo bias bucket).
        Sanlúcar (36.78,-6.35), Chipiona (36.74,-6.43): ambos → 36.75,-6.50/-6.25."""
        from main import _ac_cache_key
        _, sanlucar = _ac_cache_key("calle ancha", 36.78, -6.35)
        _, chipiona_a = _ac_cache_key("calle ancha", 36.79, -6.37)
        # Mismo bucket: ambos → 36.75,-6.25
        assert sanlucar == chipiona_a, (
            f"jitter 1km debería caer en mismo bucket — got {sanlucar!r} vs {chipiona_a!r}"
        )

    def test_bias_distinct_across_50km_cities(self):
        """Ciudades a >50km NO comparten bias (Cádiz vs Sevilla → buckets distintos)."""
        from main import _ac_cache_key
        _, cadiz = _ac_cache_key("calle ancha", 36.53, -6.30)   # Cádiz
        _, sevilla = _ac_cache_key("calle ancha", 37.39, -5.99)  # Sevilla (~100km NE)
        assert cadiz != sevilla, (
            f"Cádiz y Sevilla a 100km deberían tener bias distintos — got {cadiz!r} == {sevilla!r}"
        )

    def test_bias_empty_when_no_coords(self):
        """Sin lat/lng (query genérica) → bias vacío."""
        from main import _ac_cache_key
        _, bias = _ac_cache_key("calle ancha", None, None)
        assert bias == ""

    def test_query_normalize_strips_whitespace_and_lowercases(self):
        """query_normalized = lowercase + whitespace colapsado, max 200 chars."""
        from main import _ac_cache_key
        norm, _ = _ac_cache_key("  Calle   ANCHA  ", 36.78, -6.35)
        assert norm == "calle ancha"


class TestCompositeStreetPrefixLookup:
    """REGRESSION GUARD (22 may 2026): cuando cache exact MISS pero la query
    tiene número final (ej 'calle bolsa 32'), backend busca el prefix de calle
    ('calle bolsa') y filtra predictions por el número. Hit virtual sin gastar
    Google. Si alguien quita esta lógica, los tests fallan."""

    def test_extract_street_prefix_strips_final_number(self):
        from main import _extract_street_prefix
        assert _extract_street_prefix("calle bolsa 32") == "calle bolsa"

    def test_extract_street_prefix_handles_comma(self):
        from main import _extract_street_prefix
        assert _extract_street_prefix("calle de la cepa, 16") == "calle de la cepa"

    def test_extract_street_prefix_keeps_street_number_strips_portal(self):
        """En direcciones LATAM tipo 'carrera 39c, 84a-07' el primer número
        forma parte del nombre de calle, solo se quita el portal del final."""
        from main import _extract_street_prefix
        assert _extract_street_prefix("carrera 39c, 84a-07") == "carrera 39c"

    def test_extract_street_prefix_returns_none_when_no_number(self):
        from main import _extract_street_prefix
        assert _extract_street_prefix("pago zahora") is None

    def test_extract_street_prefix_returns_none_when_prefix_too_short(self):
        from main import _extract_street_prefix
        # "cl 67" → prefix sería "cl" (<5 chars) → None
        assert _extract_street_prefix("cl 67") is None

    def test_filter_predictions_matches_number_as_token(self):
        from main import _filter_predictions_containing_number
        preds = [
            {"description": "Calle Bolsa, 32, Madrid"},
            {"description": "Calle Bolsa, 45, Madrid"},
            {"description": "Calle Bolsa, 320, Madrid"},  # 320 contiene "32" pero NO como token
        ]
        result = _filter_predictions_containing_number(preds, "32")
        assert len(result) == 1
        assert result[0]["description"] == "Calle Bolsa, 32, Madrid"

    def test_filter_predictions_returns_empty_when_no_match(self):
        from main import _filter_predictions_containing_number
        preds = [{"description": "Calle Bolsa, 32, Madrid"}]
        assert _filter_predictions_containing_number(preds, "99") == []

    @pytest.mark.asyncio
    async def test_composite_prefix_hit_avoids_google_call(self, client):
        """Cache exact MISS + prefix HIT con número filtrado → devuelve cache, NO llama Google."""
        prefix_cache_row = {
            "predictions": [
                {"description": "Calle Bolsa, 15, Sevilla"},
                {"description": "Calle Bolsa, 32, Sevilla"},
                {"description": "Calle Bolsa, 67, Sevilla"},
            ],
            "hits": 5,
        }

        def lookup_side_effect(norm, bias):
            # Exact query MISS
            if norm == "calle bolsa 32":
                return None
            # Prefix HIT
            if norm == "calle bolsa":
                return prefix_cache_row
            return None

        mock_http = AsyncMock()
        # Si llegara a Google, falla el test
        mock_http.get.side_effect = AssertionError("Google should NOT be called when prefix hit")

        with patch("main._get_places_cache_mode", AsyncMock(return_value="on")), \
             patch("main._places_cache_lookup_sync", side_effect=lookup_side_effect), \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            response = await client.get("/places/autocomplete?input=calle%20bolsa%2032&lat=37.39&lng=-5.99")
        assert response.status_code == 200
        d = response.json()
        assert d["status"] == "OK"
        assert d.get("source") == "cache_prefix"
        # Solo debe devolver la prediction con "32", no las otras
        assert len(d["predictions"]) == 1
        assert "32" in d["predictions"][0]["description"]


class TestAdminCachePlacesStatsEndpoint:
    """REGRESSION GUARD (22 may 2026 commits d180646 + bb3a68a):
    /admin/cache/places-stats devuelve stats agregados del cache + pagina
    PostgREST cap 1000. Si rompemos shape o paginación, el panel admin
    muestra datos falsos (Miguel detectó 'Entradas 1000/1000' que era
    subestimación porque .limit(10000) silenciosamente capa a 1000)."""

    @pytest.mark.asyncio
    async def test_admin_cache_places_stats_returns_expected_shape(self, admin_client):
        """Verifica fields exactos del JSON response — si cambian, panel website /admin/costs se rompe."""
        # Mock: 3 entries, una expirada, 2 activas; total 5 hits.
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        future = (now + timedelta(days=5)).isoformat()
        past = (now - timedelta(days=1)).isoformat()
        recent = (now - timedelta(hours=2)).isoformat()
        old = (now - timedelta(days=10)).isoformat()
        mock_rows = [
            {"hits": 3, "created_at": recent, "last_used_at": recent, "expires_at": future},
            {"hits": 2, "created_at": old, "last_used_at": old, "expires_at": future},
            {"hits": 0, "created_at": old, "last_used_at": None, "expires_at": past},  # expirada
        ]
        mock_chunk = MagicMock(data=mock_rows)
        mock_empty = MagicMock(data=[])
        flag_row = MagicMock(data=[{"value": "on"}])
        top_q = MagicMock(data=[{"query_normalized": "calle ancha", "hits": 3, "bias_geohash5": "36.75,-6.25", "last_used_at": recent}])

        # supabase.table().select().X().Y().execute() chainable
        def make_chain(final):
            chain = MagicMock()
            chain.select.return_value = chain
            chain.eq.return_value = chain
            chain.limit.return_value = chain
            chain.range.side_effect = [chain, chain]
            chain.order.return_value = chain
            chain.execute.side_effect = [final, mock_empty]
            return chain

        def table_side_effect(name):
            if name == "app_config":
                c = MagicMock()
                c.select.return_value = c; c.eq.return_value = c; c.limit.return_value = c
                c.execute.return_value = flag_row
                return c
            if name == "places_autocomplete_cache":
                c = MagicMock()
                c.select.return_value = c
                # Primera vez (paginación): chunk con datos, 2ª vez: vacío
                # Segunda vez (top queries): top_q
                exec_calls = [mock_chunk, mock_empty, top_q]
                c.range.return_value = c
                c.order.return_value = c
                c.limit.return_value = c
                c.execute.side_effect = exec_calls
                return c
            return MagicMock()

        with patch("main.supabase.table", side_effect=table_side_effect):
            response = await admin_client.get("/admin/cache/places-stats")
        assert response.status_code == 200
        d = response.json()
        # Fields críticos que el panel website consume:
        for f in ("ok", "cache_mode", "total_entries", "active_entries", "total_hits",
                  "entries_added_7d", "hits_7d", "savings_total_usd", "savings_7d_usd",
                  "price_per_call_usd", "top_queries"):
            assert f in d, f"missing field {f!r} in response — panel admin/costs se romperá"
        assert d["cache_mode"] == "on"
        assert d["total_entries"] == 3
        assert d["total_hits"] == 5  # 3 + 2 + 0
        assert d["active_entries"] == 2  # las 2 con expires_at futuro

    @pytest.mark.asyncio
    async def test_admin_cache_places_stats_paginates_beyond_1000(self, admin_client):
        """REGRESSION (22 may 2026 commit bb3a68a): PostgREST Supabase Cloud
        capa silenciosamente a 1000 rows/request. .limit(10000) NO funciona.
        Si quitan el while-paginate, total_entries vuelve a subestimarse a 1000.
        Este test simula 1500 entries reales → debe devolver 1500."""
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        future = (now + timedelta(days=5)).isoformat()
        # 3 páginas de cache (1000 + 500 + vacía) + 1 query top_queries final.
        page1_data = [{"hits": 1, "created_at": now.isoformat(), "last_used_at": now.isoformat(), "expires_at": future} for _ in range(1000)]
        page2_data = [{"hits": 1, "created_at": now.isoformat(), "last_used_at": now.isoformat(), "expires_at": future} for _ in range(500)]
        # State compartido entre calls (el endpoint llama supabase.table() en cada iteración del loop)
        cache_call_counter = {"n": 0}
        cache_responses = [
            MagicMock(data=page1_data),
            MagicMock(data=page2_data),
            MagicMock(data=[]),         # 3ª llamada paginación = fin del loop
            MagicMock(data=[]),         # 4ª llamada = top_queries
        ]

        def table_side_effect(name):
            if name == "app_config":
                c = MagicMock()
                c.select.return_value = c; c.eq.return_value = c; c.limit.return_value = c
                c.execute.return_value = MagicMock(data=[{"value": "on"}])
                return c
            if name == "places_autocomplete_cache":
                c = MagicMock()
                c.select.return_value = c
                c.range.return_value = c
                c.order.return_value = c
                c.limit.return_value = c
                # Cada call de execute() devuelve la siguiente respuesta del state compartido
                def exec_next():
                    idx = cache_call_counter["n"]
                    cache_call_counter["n"] += 1
                    return cache_responses[min(idx, len(cache_responses) - 1)]
                c.execute.side_effect = exec_next
                return c
            return MagicMock()

        with patch("main.supabase.table", side_effect=table_side_effect):
            response = await admin_client.get("/admin/cache/places-stats")
        assert response.status_code == 200
        d = response.json()
        assert d["total_entries"] == 1500, (
            f"paginación rota: esperaba 1500 entries, got {d['total_entries']}. "
            "Si volvió a 1000, alguien quitó el while-paginate."
        )
        assert d["total_hits"] == 1500
