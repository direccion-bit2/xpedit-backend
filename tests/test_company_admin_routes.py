"""GET /routes para el dueño de empresa (role=company_admin).

Antes, el endpoint solo contemplaba 'dispatcher' y el dueño de empresa caía en
la rama de 'driver normal' → recibía [] y el panel de empresa no podía listar
rutas. Aquí fijamos que company_admin ve las rutas de los conductores de SU
empresa, igual que un dispatcher.
"""
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from main import app, get_current_user

FAKE_COMPANY = "company-aaaa-0000-0000-000000000001"
DRIVER_A = "driver-aaaa-0000-0000-000000000010"
DRIVER_B = "driver-aaaa-0000-0000-000000000011"


def _company_admin_user(company_id=FAKE_COMPANY):
    return {
        "id": "owner-0000-0000-0000-000000000001",
        "email": "owner@empresa.com",
        "role": "company_admin",
        "company_id": company_id,
    }


@pytest_asyncio.fixture
async def company_admin_client():
    async def _override():
        return _company_admin_user()

    app.dependency_overrides[get_current_user] = _override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.clear()


class TestCompanyAdminListsRoutes:
    @pytest.mark.asyncio
    async def test_company_admin_sees_company_routes(self, company_admin_client):
        captured = {}

        def table_dispatch(name):
            chain = MagicMock()
            if name == "drivers":
                dl = MagicMock()
                dl.data = [{"id": DRIVER_A}, {"id": DRIVER_B}]
                chain.select.return_value.eq.return_value.execute.return_value = dl
            elif name == "routes":
                routes_result = MagicMock()
                routes_result.data = [
                    {"id": "r1", "driver_id": DRIVER_A, "company_id": FAKE_COMPANY, "stops": []},
                ]

                def capture_in(col, ids):
                    captured["in_col"] = col
                    captured["in_ids"] = ids
                    nxt = MagicMock()
                    nxt.order.return_value.execute.return_value = routes_result
                    return nxt

                chain.select.return_value.in_.side_effect = capture_in
            return chain

        with patch("main.supabase") as mock_sb:
            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await company_admin_client.get("/routes")

        assert resp.status_code == 200
        assert resp.json()["routes"][0]["id"] == "r1"
        # Filtró por los conductores de SU empresa
        assert captured["in_col"] == "driver_id"
        assert set(captured["in_ids"]) == {DRIVER_A, DRIVER_B}

    @pytest.mark.asyncio
    async def test_company_admin_no_drivers_returns_empty(self, company_admin_client):
        def table_dispatch(name):
            chain = MagicMock()
            if name == "drivers":
                dl = MagicMock()
                dl.data = []
                chain.select.return_value.eq.return_value.execute.return_value = dl
            return chain

        with patch("main.supabase") as mock_sb:
            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await company_admin_client.get("/routes")

        assert resp.status_code == 200
        assert resp.json()["routes"] == []
