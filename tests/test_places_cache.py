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
