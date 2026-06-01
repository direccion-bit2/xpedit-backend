"""POST /company/routes/import — caché de geocoding vía directorio de la empresa.

Cost-check: una dirección que ya está en customer_directory NO debe gastar una
llamada a Google Geocoding; solo las direcciones nuevas se geocodifican.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from main import app, get_current_user, normalize_address, require_admin_or_dispatcher

FAKE_COMPANY = "company-aaaa-0000-0000-000000000001"
TARGET_DRIVER = "driver-aaaa-0000-0000-000000000010"

CACHED_ADDR = "Calle Sol 1, Madrid"
NEW_ADDR = "Avenida Nueva 99, Madrid"


def _dispatcher():
    return {"id": "disp-1", "email": "d@c.com", "role": "company_admin", "company_id": FAKE_COMPANY}


@pytest_asyncio.fixture
async def client():
    async def _override():
        return _dispatcher()
    app.dependency_overrides[get_current_user] = _override
    app.dependency_overrides[require_admin_or_dispatcher] = _override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.clear()


class TestImportUsesDirectoryCache:
    @pytest.mark.asyncio
    async def test_cached_address_skips_google_geocoding(self, client):
        # El directorio ya tiene CACHED_ADDR geocodificada.
        directory_data = MagicMock()
        directory_data.data = [{
            "normalized_address": normalize_address(CACHED_ADDR),
            "lat": 40.41, "lng": -3.70, "phone": "600111222", "email": "cli@x.com",
        }]
        captured = {}

        def table_dispatch(name):
            chain = MagicMock()
            if name == "drivers":
                dl = MagicMock()
                dl.data = [{"company_id": FAKE_COMPANY}]
                chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = dl
            elif name == "customer_directory":
                chain.select.return_value.eq.return_value.execute.return_value = directory_data
            elif name == "routes":
                rr = MagicMock()
                rr.data = [{"id": "imported-route-1"}]
                chain.insert.return_value.execute.return_value = rr
            elif name == "stops":
                def cap(rows):
                    captured["stops"] = rows
                    m = MagicMock()
                    m.execute.return_value = MagicMock(data=[{"id": "s1"}, {"id": "s2"}])
                    return m
                chain.insert.side_effect = cap
            return chain

        geocode_mock = AsyncMock(return_value={"lat": 41.0, "lng": -3.0, "display_name": NEW_ADDR})

        with patch("main.verify_driver_access", new=AsyncMock(return_value=True)), \
             patch("main.notify_driver_route_assigned", new=AsyncMock()), \
             patch("main.log_audit"), \
             patch("main._geocode_address", geocode_mock), \
             patch("main.supabase") as mock_sb:
            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await client.post("/company/routes/import", json={
                "driver_id": TARGET_DRIVER,
                "name": "Import test",
                "country": "ES",
                "rows": [
                    {"address": CACHED_ADDR, "phone": None},
                    {"address": NEW_ADDR, "phone": "699"},
                ],
            })

        assert resp.status_code == 200, resp.text
        body = resp.json()
        # 1 desde caché (gratis), 1 geocodificada (coste)
        assert body["from_cache"] == 1
        assert body["geocoded"] == 1
        assert body["imported"] == 2
        # Google solo se llamó para la dirección NUEVA, nunca para la cacheada
        assert geocode_mock.await_count == 1
        assert geocode_mock.await_args.args[0] == NEW_ADDR
        # La parada cacheada heredó lat/lng y datos del directorio
        cached_stop = next(s for s in captured["stops"] if s["lat"] == 40.41)
        assert cached_stop["phone"] == "600111222"
        assert cached_stop["email"] == "cli@x.com"
