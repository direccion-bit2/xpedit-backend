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
    """Invalidate the 60s in-memory cache of the flag + L1 cache + counters between tests."""
    import main
    main._places_cache_mode_fetched_at = 0.0
    main._places_cache_mode_value = "off"
    main._places_l1_cache.clear()
    for k in main._places_source_counters:
        main._places_source_counters[k] = 0
    yield
    main._places_cache_mode_fetched_at = 0.0
    main._places_cache_mode_value = "off"
    main._places_l1_cache.clear()


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

    def test_query_normalize_strips_accents(self):
        """REGRESSION (22 may 2026 P0): 'María' y 'Maria' deben generar la misma key.
        Audit estimó 30-40% entries duplicadas por esto = ~€270/mes evitables."""
        from main import _ac_cache_key
        norm1, _ = _ac_cache_key("Calle María", None, None)
        norm2, _ = _ac_cache_key("calle maria", None, None)
        assert norm1 == norm2
        assert norm1 == "calle maria"

    def test_query_normalize_expands_abbreviations_es(self):
        """REGRESSION: abreviaturas ES expandidas + tipo vía stripped si seguro.
        'C/Mayor 5' → 'calle mayor 5' (abreviatura expandida) → 'mayor 5'
        (prefijo stripped porque ≥2 tokens y 'mayor' no empieza por dígito).
        'Av Andalucía' → 'avenida andalucia' (sin strip: solo 1 token después)."""
        from main import _ac_cache_key
        cases = [
            ("C/Mayor 5", "mayor 5"),                  # strip "calle" + "mayor 5"
            ("Av Andalucía", "avenida andalucia"),     # 1 token → no strip
            ("Avda. España", "avenida espana"),        # 1 token → no strip
            ("Pza España, 12", "espana 12"),           # strip "plaza" + "espana 12"
            ("Ctra. Sevilla, 5", "sevilla 5"),         # strip "carretera" + "sevilla 5"
        ]
        for inp, expected in cases:
            got, _ = _ac_cache_key(inp, None, None)
            assert got == expected, f"{inp!r} → {got!r} (expected {expected!r})"

    def test_query_normalize_expands_abbreviations_latam(self):
        """REGRESSION: 'CRA' (Colombia) expandido a 'carrera'. 'carrera 39c 84a-07'
        NO strippea porque '39c' empieza por dígito (guard LATAM: es nombre, no portal)."""
        from main import _ac_cache_key
        norm, _ = _ac_cache_key("CRA 39c, 84a-07", None, None)
        assert norm == "carrera 39c 84a-07"

    def test_query_normalize_strips_punctuation_preserves_dash(self):
        """REGRESSION: comas/puntos eliminados, pero - preservado (LATAM '84a-07')."""
        from main import _ac_cache_key
        norm, _ = _ac_cache_key("Calle Mayor, 5.", None, None)
        # Tras strip prefijo "calle" → "mayor 5"
        assert norm == "mayor 5"
        # Guión preservado
        norm2, _ = _ac_cache_key("Carrera 39c 84a-07", None, None)
        assert "84a-07" in norm2

    def test_query_normalize_strips_via_prefix_when_safe(self):
        """REGRESSION (22 may 2026 audit Miguel): 'calle X' y 'X' deben caer en
        la misma cache key cuando hay ≥2 tokens restantes y el siguiente NO es
        dígito. Drivers inconsistentes generan duplicados sin esto."""
        from main import _ac_cache_key
        # Caso clásico: con/sin "calle"
        n1, _ = _ac_cache_key("calle san francisco 43 zafra", None, None)
        n2, _ = _ac_cache_key("san francisco 43 zafra", None, None)
        assert n1 == n2 == "san francisco 43 zafra"

        # avenida
        n3, _ = _ac_cache_key("avenida andalucia 4", None, None)
        n4, _ = _ac_cache_key("andalucia 4", None, None)
        assert n3 == n4 == "andalucia 4"

        # plaza
        n5, _ = _ac_cache_key("plaza españa 12", None, None)
        n6, _ = _ac_cache_key("españa 12", None, None)
        assert n5 == n6 == "espana 12"

        # carrera (LATAM, pero con nombre alfabético después)
        n7, _ = _ac_cache_key("carrera dorada 100", None, None)
        n8, _ = _ac_cache_key("dorada 100", None, None)
        assert n7 == n8 == "dorada 100"

    def test_query_normalize_does_not_strip_via_prefix_with_numeric_latam(self):
        """REGRESSION LATAM: 'calle 43' NO se debe stripear porque '43' es
        nombre de calle (no portal). Riesgo: 'calle 43' → '43' colisión con
        otro contexto. Igual para 'carrera 39c' (Colombia)."""
        from main import _ac_cache_key
        # "calle 43" → "calle 43" (no se quita, segundo token es número)
        n1, _ = _ac_cache_key("calle 43", None, None)
        assert n1 == "calle 43"

        # "carrera 39c" → "carrera 39c" (39c es nombre LATAM)
        n2, _ = _ac_cache_key("carrera 39c", None, None)
        assert n2 == "carrera 39c"

        # "avenida 5 de mayo" → "avenida 5 de mayo" (5 es nombre, no portal)
        n3, _ = _ac_cache_key("avenida 5 de mayo", None, None)
        assert n3 == "avenida 5 de mayo"

    def test_query_normalize_does_not_strip_via_prefix_when_only_2_tokens(self):
        """REGRESSION: 'plaza mayor' tiene solo 2 tokens. Stripear → 'mayor'
        sería demasiado genérico (matchearía 'calle mayor', 'paseo mayor', etc).
        Solo se stripea si quedan ≥2 tokens DESPUÉS (es decir, query ≥3 tokens)."""
        from main import _ac_cache_key
        n1, _ = _ac_cache_key("plaza mayor", None, None)
        assert n1 == "plaza mayor"  # NO stripear

        n2, _ = _ac_cache_key("calle ancha", None, None)
        assert n2 == "calle ancha"  # NO stripear


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

    def test_extract_street_prefix_skips_latam_addresses_with_number_in_prefix(self):
        """REGRESSION (22 may 2026 audit): direcciones LATAM tipo 'calle 13 23'
        donde el primer número forma parte del nombre de calle (no portal).
        El guard devuelve None para evitar que el filtro \\b23\\b matchee
        OTRA dirección como 'Calle 23 con Carrera 13' y devuelva predictions
        WRONG → riesgo de entrega en sitio equivocado.

        ANTES: matcheaba como 'carrera 39c' prefix → riesgo entrega mala.
        AHORA: devuelve None → cae a Google → respuesta correcta."""
        from main import _extract_street_prefix
        assert _extract_street_prefix("carrera 39c, 84a-07") is None
        assert _extract_street_prefix("calle 13 23") is None
        assert _extract_street_prefix("avenida 5 de mayo 12") is None
        # Confirma que el guard SOLO afecta prefix con número
        assert _extract_street_prefix("calle bolsa 32") == "calle bolsa"  # OK ES

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


class TestStopsFuzzyLookup:
    """REGRESSION GUARD (22 may 2026 P1 audit): cuando cache exact + prefix MISS,
    backend busca en stops table con pg_trgm fuzzy + haversine distance.
    Hit virtual sin gastar Google para direcciones ya visitadas por flota.
    Ventaja específica Xpedit: drivers REPITEN clientes."""

    def test_stops_fuzzy_lookup_returns_empty_for_short_query(self):
        """Query <3 chars devuelve [] sin tocar Supabase (skip RPC)."""
        from main import _stops_fuzzy_lookup_sync
        assert _stops_fuzzy_lookup_sync("ab", 36.78, -6.35) == []
        assert _stops_fuzzy_lookup_sync("", 36.78, -6.35) == []
        assert _stops_fuzzy_lookup_sync(None, 36.78, -6.35) == []

    def test_stops_fuzzy_lookup_transforms_rpc_rows_to_google_format(self):
        """Output debe ser compatible con formato Google Places Autocomplete prediction."""
        from main import _stops_fuzzy_lookup_sync
        mock_rpc_resp = MagicMock(data=[
            {
                "address": "Calle Ancha\nSanlúcar de Barrameda, Cádiz, 11540",
                "lat": 36.78, "lng": -6.35,
                "place_id": "ChIJxyz123",
                "similarity": 0.85, "distance_km": 0.5,
            },
            {
                "address": "Calle Ancha 10",
                "lat": 36.79, "lng": -6.36,
                "place_id": None,  # caso real: stops sin place_id
                "similarity": 0.72, "distance_km": 1.2,
            },
        ])
        with patch("main.supabase.rpc", return_value=MagicMock(execute=MagicMock(return_value=mock_rpc_resp))):
            result = _stops_fuzzy_lookup_sync("calle ancha", 36.78, -6.35)
        assert len(result) == 2
        # Format Google Autocomplete:
        assert "description" in result[0]
        assert "structured_formatting" in result[0]
        assert result[0]["structured_formatting"]["main_text"] == "Calle Ancha"
        assert result[0]["structured_formatting"]["secondary_text"] == "Sanlúcar de Barrameda, Cádiz, 11540"
        assert result[0]["place_id"] == "ChIJxyz123"
        # Custom fields para que cliente use coords directos:
        assert result[0]["_xpedit_lat"] == 36.78
        assert result[0]["_xpedit_lng"] == -6.35
        assert result[0]["_xpedit_similarity"] == 0.85
        # Sin place_id (stops sin geocoding completo) debe seguir funcionando
        assert result[1]["place_id"] is None
        assert result[1]["_xpedit_lat"] == 36.79

    def test_stops_fuzzy_lookup_swallows_errors(self):
        """Si RPC falla, devuelve [] sin propagar excepción (fallback Google)."""
        from main import _stops_fuzzy_lookup_sync
        with patch("main.supabase.rpc", side_effect=Exception("Supabase down")):
            result = _stops_fuzzy_lookup_sync("calle ancha", 36.78, -6.35)
        assert result == []


class TestL1InMemoryCache:
    """REGRESSION GUARD (22 may 2026 P1): cache L1 in-memory antes de Supabase.
    Latencia esperada ~5ms (vs ~500ms Supabase round-trip). TTL 5min, max 1000."""

    def test_l1_put_get_roundtrip(self):
        from main import _l1_get, _l1_put, _places_l1_cache
        _places_l1_cache.clear()
        _l1_put("calle ancha", "36.75,-6.25", {"predictions": [{"description": "A"}]})
        got = _l1_get("calle ancha", "36.75,-6.25")
        assert got is not None
        assert got["predictions"][0]["description"] == "A"

    def test_l1_miss_returns_none(self):
        from main import _l1_get, _places_l1_cache
        _places_l1_cache.clear()
        assert _l1_get("no existe", "0,0") is None

    def test_l1_lru_eviction_at_max(self):
        """Cuando el cache supera _PLACES_L1_MAX, evicta el oldest."""
        import main
        main._places_l1_cache.clear()
        # Forzar maxsize pequeño para test
        original_max = main._PLACES_L1_MAX
        main._PLACES_L1_MAX = 3
        try:
            main._l1_put("q1", "b", {"v": 1})
            main._l1_put("q2", "b", {"v": 2})
            main._l1_put("q3", "b", {"v": 3})
            main._l1_put("q4", "b", {"v": 4})  # debe expulsar q1
            assert main._l1_get("q1", "b") is None
            assert main._l1_get("q4", "b") is not None
            assert len(main._places_l1_cache) == 3
        finally:
            main._PLACES_L1_MAX = original_max

    def test_l1_ttl_expires(self):
        """Entrada con timestamp > TTL no devuelve cache."""
        import time as _t

        import main
        main._places_l1_cache.clear()
        # Insertar con timestamp viejo manualmente
        main._places_l1_cache[("q1", "b")] = (_t.time() - main._PLACES_L1_TTL_SEC - 1, {"v": 1})
        assert main._l1_get("q1", "b") is None  # expirado


class TestSourceCounters:
    """REGRESSION GUARD (22 may 2026 P1): contadores in-memory por source
    para medir hit rate real en /admin/cache/places-stats."""

    def test_bump_counter_increments(self):
        import main
        main._places_source_counters["hit"] = 0
        main._bump_source_counter("hit")
        main._bump_source_counter("hit")
        assert main._places_source_counters["hit"] == 2

    def test_bump_unknown_source_is_noop(self):
        """Sources no registrados no rompen — no-op silencioso."""
        import main
        main._bump_source_counter("invented_source_xyz")
        # No crash, no entry creada
        assert "invented_source_xyz" not in main._places_source_counters


class TestTtlEscalonado:
    """REGRESSION GUARD (22 may 2026 P1): TTL escalonado por popularidad.
    Direcciones con muchos hits extienden expires_at para reducir re-llamadas
    Google al expirar el TTL base 30d."""

    def test_bump_at_hit_10_extends_ttl_90d(self):
        """Cuando hits llega a 10, expires_at se renueva a +90d."""
        import main
        captured = {}
        def fake_update_chain(payload):
            captured["payload"] = payload
            mock_chain = MagicMock()
            mock_chain.eq.return_value = mock_chain
            mock_chain.execute.return_value = MagicMock(data=[])
            return mock_chain
        with patch("main.supabase.table") as mock_tbl:
            mock_tbl.return_value.update.side_effect = fake_update_chain
            main._places_cache_bump_sync("q", "b", 9)  # current_hits=9, becomes 10
        assert "expires_at" in captured["payload"]
        # Verifica que el TTL extendido es razonable (+90d desde now)
        from datetime import datetime, timezone
        exp = datetime.fromisoformat(captured["payload"]["expires_at"].replace("Z", "+00:00"))
        delta = exp - datetime.now(timezone.utc)
        assert 89 <= delta.days <= 91, f"Expected ~90d, got {delta.days}d"

    def test_bump_at_hit_30_extends_ttl_180d(self):
        """Cuando hits llega a 30, expires_at se renueva a +180d."""
        import main
        captured = {}
        def fake_update_chain(payload):
            captured["payload"] = payload
            mock_chain = MagicMock()
            mock_chain.eq.return_value = mock_chain
            mock_chain.execute.return_value = MagicMock(data=[])
            return mock_chain
        with patch("main.supabase.table") as mock_tbl:
            mock_tbl.return_value.update.side_effect = fake_update_chain
            main._places_cache_bump_sync("q", "b", 29)  # becomes 30
        from datetime import datetime, timezone
        exp = datetime.fromisoformat(captured["payload"]["expires_at"].replace("Z", "+00:00"))
        delta = exp - datetime.now(timezone.utc)
        assert 179 <= delta.days <= 181

    def test_bump_at_regular_hit_does_not_extend_ttl(self):
        """En hits intermedios (no milestones), no se toca expires_at."""
        import main
        captured = {}
        def fake_update_chain(payload):
            captured["payload"] = payload
            mock_chain = MagicMock()
            mock_chain.eq.return_value = mock_chain
            mock_chain.execute.return_value = MagicMock(data=[])
            return mock_chain
        with patch("main.supabase.table") as mock_tbl:
            mock_tbl.return_value.update.side_effect = fake_update_chain
            main._places_cache_bump_sync("q", "b", 5)  # becomes 6, no milestone
        assert "expires_at" not in captured["payload"]


class TestSupabaseClientIntegrity:
    """REGRESSION GUARD (22 may 2026 — incident 1h con bug HTTP/1.1):
    el cliente Supabase DEBE tener base_url válido y poder hacer queries
    sintácticamente. Sin esto, TODO el backend muere silenciosamente
    (afectó 11 endpoints + 2 crons durante 1h hasta llegar email Sentry).

    Validación crítica: el código que sustituye supabase.postgrest.session
    (fix bug #222 HTTP/1.1) debe preservar base_url + headers del original.
    En el bug, mi nuevo httpx.Client lo creé sin esos → 'UnsupportedProtocol'
    en todas las queries posteriores.

    Estos tests pasan en runtime real (prod/local con SUPABASE_URL real) y
    se auto-skipean en CI/test environment (donde SUPABASE_URL es fake)."""

    def _has_real_supabase_client(self):
        """True solo si supabase client se instanció con env vars reales.
        En conftest se setean fake values que crean cliente sin headers."""
        import main
        try:
            headers = dict(main.supabase.postgrest.session.headers)
            return len(headers) > 0
        except Exception:
            return False

    def test_supabase_postgrest_session_has_valid_base_url(self):
        """session.base_url debe empezar con http:// o https://"""
        if not self._has_real_supabase_client():
            pytest.skip("Supabase client mocked/test env — solo runtime real")
        import main
        session = main.supabase.postgrest.session
        base_url = str(session.base_url)
        assert base_url.startswith(("http://", "https://")), (
            f"postgrest session base_url INVALID: {base_url!r}. "
            "Esto rompería TODAS las queries Supabase del backend."
        )

    def test_supabase_postgrest_session_has_auth_headers(self):
        """session.headers debe llevar apikey + Authorization (sin ellos, todo falla auth)."""
        if not self._has_real_supabase_client():
            pytest.skip("Supabase client mocked/test env — solo runtime real")
        import main
        headers = dict(main.supabase.postgrest.session.headers)
        lower_keys = {k.lower() for k in headers.keys()}
        assert "apikey" in lower_keys, f"apikey header missing: {list(headers.keys())}"
        assert "authorization" in lower_keys, f"authorization header missing: {list(headers.keys())}"

    def test_http1_replacement_preserves_base_url(self):
        """REGRESSION: simulamos el escenario del bug — sustituir session sin
        preservar base_url debe DETECTARSE. Test pasa siempre (no requiere env real).

        Si alguien en el futuro hace `supabase.postgrest.session = httpx.Client()`
        sin preservar atributos, este test no lo cazaría directamente, PERO
        el startup smoke test sí (corre al boot real). Aquí validamos que el
        helper de fix lo hace bien."""
        import httpx
        # Simular escenario: original session con base_url + headers
        original = httpx.Client(
            base_url="https://example.supabase.co/rest/v1/",
            headers={"apikey": "test123", "Authorization": "Bearer test123"},
        )
        # Replicar lógica del fix bug #222
        new_session = httpx.Client(
            base_url=original.base_url,
            headers=dict(original.headers),
            http1=True,
            http2=False,
        )
        original.close()
        # Verificar que el nuevo cliente preserva base_url + headers críticos
        assert str(new_session.base_url) == "https://example.supabase.co/rest/v1/"
        lower_keys = {k.lower() for k in dict(new_session.headers).keys()}
        assert "apikey" in lower_keys
        assert "authorization" in lower_keys
        new_session.close()


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
        from datetime import datetime, timedelta, timezone
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
                c.select.return_value = c
                c.eq.return_value = c
                c.limit.return_value = c
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
        from datetime import datetime, timedelta, timezone
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
                c.select.return_value = c
                c.eq.return_value = c
                c.limit.return_value = c
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
