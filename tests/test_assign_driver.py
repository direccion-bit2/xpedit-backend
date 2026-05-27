"""
Tests for PATCH /routes/{route_id}/assign-driver (B2B V1 — asignación segura).

Sustituye el supabase.update({driver_id}) directo del dashboard, que NO validaba
empresa. El endpoint debe:
  - exigir rol admin/dispatcher (require_admin_or_dispatcher),
  - validar acceso a la RUTA (verify_route_access) y al CONDUCTOR (verify_driver_access),
  - persistir driver_id + company_id y registrar audit,
  - rechazar (403) asignar a un conductor/ruta de OTRA empresa.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from main import app, get_current_user, require_admin_or_dispatcher

FAKE_COMPANY_A = "company-aaaa-0000-0000-000000000001"
FAKE_ROUTE_ID = "route-0000-0000-0000-000000000001"
TARGET_DRIVER = "driver-aaaa-0000-0000-000000000010"


def _dispatcher_user(company_id=FAKE_COMPANY_A):
    return {
        "id": "dispatcher-0000-0000-0000-000000000001",
        "email": "dispatcher@company-a.com",
        "role": "dispatcher",
        "company_id": company_id,
    }


@pytest_asyncio.fixture
async def dispatcher_client():
    user = _dispatcher_user()

    async def _override():
        return user

    app.dependency_overrides[get_current_user] = _override
    app.dependency_overrides[require_admin_or_dispatcher] = _override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.clear()


class TestAssignRouteDriver:
    @pytest.mark.asyncio
    async def test_assign_success_persists_driver_and_company(self, dispatcher_client):
        """Dispatcher asigna una ruta de su empresa a un conductor de su empresa:
        200 + update con driver_id y company_id + audit registrado."""
        captured = {}

        def table_dispatch(name):
            chain = MagicMock()
            if name == "drivers":
                dl = MagicMock()
                dl.data = [{"company_id": FAKE_COMPANY_A}]
                chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = dl
            elif name == "routes":
                def capture_update(data):
                    captured["update"] = data
                    upd = MagicMock()
                    upd.data = [{"id": FAKE_ROUTE_ID, "driver_id": TARGET_DRIVER, "company_id": FAKE_COMPANY_A}]
                    m = MagicMock()
                    m.eq.return_value.execute.return_value = upd
                    return m
                chain.update.side_effect = capture_update
            return chain

        with patch("main.verify_route_access", new=AsyncMock(return_value={"id": FAKE_ROUTE_ID, "driver_id": "old"})), \
             patch("main.verify_driver_access", new=AsyncMock(return_value=True)), \
             patch("main.log_audit") as mock_audit, \
             patch("main.supabase") as mock_sb:
            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await dispatcher_client.patch(
                f"/routes/{FAKE_ROUTE_ID}/assign-driver", json={"driver_id": TARGET_DRIVER}
            )

        assert resp.status_code == 200
        assert captured["update"]["driver_id"] == TARGET_DRIVER
        assert captured["update"]["company_id"] == FAKE_COMPANY_A
        mock_audit.assert_called_once()

    @pytest.mark.asyncio
    async def test_unassign_sets_driver_null_without_driver_check(self, dispatcher_client):
        """driver_id=None desasigna la ruta: 200, update driver_id=None, sin validar
        conductor (verify_driver_access NO se llama) ni tocar company_id."""
        captured = {}

        def table_dispatch(name):
            chain = MagicMock()
            if name == "routes":
                def capture_update(data):
                    captured["update"] = data
                    upd = MagicMock()
                    upd.data = [{"id": FAKE_ROUTE_ID, "driver_id": None}]
                    m = MagicMock()
                    m.eq.return_value.execute.return_value = upd
                    return m
                chain.update.side_effect = capture_update
            return chain

        vda = AsyncMock(return_value=True)
        with patch("main.verify_route_access", new=AsyncMock(return_value={"id": FAKE_ROUTE_ID, "driver_id": "old"})), \
             patch("main.verify_driver_access", new=vda), \
             patch("main.log_audit"), \
             patch("main.supabase") as mock_sb:
            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await dispatcher_client.patch(
                f"/routes/{FAKE_ROUTE_ID}/assign-driver", json={"driver_id": None}
            )

        assert resp.status_code == 200
        assert captured["update"]["driver_id"] is None
        assert "company_id" not in captured["update"]
        vda.assert_not_called()

    @pytest.mark.asyncio
    async def test_assign_cross_company_driver_forbidden(self, dispatcher_client):
        """Asignar a un conductor de OTRA empresa: verify_driver_access lanza 403
        y NO se debe tocar la ruta."""
        routes_touched = {"update": False}

        def table_dispatch(name):
            chain = MagicMock()
            if name == "routes":
                def _upd(_data):
                    routes_touched["update"] = True
                    return MagicMock()
                chain.update.side_effect = _upd
            return chain

        with patch("main.verify_route_access", new=AsyncMock(return_value={"id": FAKE_ROUTE_ID, "driver_id": "old"})), \
             patch("main.verify_driver_access", new=AsyncMock(side_effect=HTTPException(status_code=403, detail="no"))), \
             patch("main.supabase") as mock_sb:
            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await dispatcher_client.patch(
                f"/routes/{FAKE_ROUTE_ID}/assign-driver", json={"driver_id": "driver-of-company-b"}
            )

        assert resp.status_code == 403
        assert routes_touched["update"] is False

    @pytest.mark.asyncio
    async def test_assign_route_other_company_forbidden(self, dispatcher_client):
        """Asignar una ruta que NO es de su empresa: verify_route_access lanza 403."""
        with patch("main.verify_route_access", new=AsyncMock(side_effect=HTTPException(status_code=403, detail="no"))), \
             patch("main.verify_driver_access", new=AsyncMock(return_value=True)), \
             patch("main.supabase"):
            resp = await dispatcher_client.patch(
                f"/routes/{FAKE_ROUTE_ID}/assign-driver", json={"driver_id": TARGET_DRIVER}
            )

        assert resp.status_code == 403


class TestDispatcherCreateRoute:
    """POST /routes: un dispatcher puede crear ruta para drivers de SU empresa (Fase 1)."""

    @pytest.mark.asyncio
    async def test_dispatcher_creates_route_for_company_driver(self, dispatcher_client):
        """Dispatcher de empresa A crea ruta para un driver de A → 200."""
        payload = {
            "driver_id": TARGET_DRIVER,
            "name": "Ruta dispatcher",
            "stops": [{"address": "Calle 1", "lat": 40.4, "lng": -3.7, "position": 0}],
            "total_distance_km": 1.0,
        }
        call = {"routes": 0}

        def table_dispatch(name):
            chain = MagicMock()
            if name == "drivers":
                dl = MagicMock()
                dl.data = [{"company_id": FAKE_COMPANY_A}]
                chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = dl
            elif name == "routes":
                call["routes"] += 1
                if call["routes"] <= 1:
                    ins = MagicMock()
                    ins.data = [{"id": "new-route-id", "driver_id": TARGET_DRIVER}]
                    chain.insert.return_value.execute.return_value = ins
                else:
                    fin = MagicMock()
                    fin.data = {"id": "new-route-id", "stops": []}
                    chain.select.return_value.eq.return_value.single.return_value.execute.return_value = fin
            elif name == "stops":
                st = MagicMock()
                st.data = [{"id": "s1"}]
                chain.insert.return_value.execute.return_value = st
            return chain

        with patch("main.verify_driver_access", new=AsyncMock(return_value=True)), \
             patch("main.supabase") as mock_sb:
            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await dispatcher_client.post("/routes", json=payload)

        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_dispatcher_cannot_create_for_other_company_driver(self, dispatcher_client):
        """Dispatcher intenta crear ruta para un driver de OTRA empresa → 403 (verify_driver_access)."""
        payload = {
            "driver_id": "driver-of-company-b",
            "name": "x",
            "stops": [{"address": "C", "lat": 40.4, "lng": -3.7, "position": 0}],
            "total_distance_km": 1.0,
        }
        with patch("main.verify_driver_access", new=AsyncMock(side_effect=HTTPException(status_code=403, detail="no"))), \
             patch("main.supabase"):
            resp = await dispatcher_client.post("/routes", json=payload)

        assert resp.status_code == 403
