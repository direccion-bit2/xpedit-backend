"""
Tests del lote seguridad/coste Tanda 2 (#80):
  S1 — GET /fleet/messages verifica acceso al driver (antes faltaba; el POST sí).
  S2 — /admin/users NO devuelve session_token/session_device/push_token.
  S3 — inputs Places acotados (Query) → basura rechazada con 422 antes de Google.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from main import app, require_admin_or_dispatcher


def _mock(data):
    m = MagicMock()
    m.data = data
    return m


class TestS1FleetMessagesAccess:
    @pytest.mark.asyncio
    async def test_get_messages_denies_driver_of_other_company(self, client, fake_user):
        # dispatcher autenticado pero verify_driver_access lo rechaza (otra empresa)
        from fastapi import HTTPException
        app.dependency_overrides[require_admin_or_dispatcher] = lambda: {**fake_user, "role": "dispatcher", "company_id": "A"}
        try:
            with patch("main.verify_driver_access", new=AsyncMock(side_effect=HTTPException(status_code=403, detail="no"))):
                resp = await client.get("/fleet/messages/driver-de-empresa-B")
            assert resp.status_code == 403
        finally:
            app.dependency_overrides.pop(require_admin_or_dispatcher, None)

    @pytest.mark.asyncio
    async def test_get_messages_allows_owner(self, client, fake_user):
        app.dependency_overrides[require_admin_or_dispatcher] = lambda: {**fake_user, "role": "dispatcher", "company_id": "A"}
        try:
            with patch("main.verify_driver_access", new=AsyncMock(return_value=True)), \
                 patch("main.supabase") as sb:
                chain = MagicMock()
                chain.select.return_value.or_.return_value.order.return_value.limit.return_value.execute.return_value = _mock([])
                chain.update.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value = _mock([])
                sb.table = MagicMock(return_value=chain)
                resp = await client.get("/fleet/messages/driver-propio")
            assert resp.status_code == 200
        finally:
            app.dependency_overrides.pop(require_admin_or_dispatcher, None)


class TestS2NoTokenLeak:
    @pytest.mark.asyncio
    async def test_admin_users_strips_session_and_push_tokens(self, admin_client):
        rows = [{"id": "d1", "email": "a@b.c", "name": "X", "promo_plan": None,
                 "session_token": "SECRET-SESSION", "session_device": "iPhone", "push_token": "ExponentPushToken[xxx]"}]
        with patch("main.fetch_all_rows", return_value=rows):
            resp = await admin_client.get("/admin/users")
        assert resp.status_code == 200
        u = resp.json()["users"][0]
        assert "session_token" not in u and "session_device" not in u and "push_token" not in u
        assert u["email"] == "a@b.c"  # los campos NO sensibles siguen


class TestS3PlacesInputBounds:
    @pytest.mark.asyncio
    async def test_snap_rejects_out_of_range_lat_before_google(self, client):
        # lat=200 es imposible → 422 del validador, NUNCA llega a Google (ahorro).
        resp = await client.get("/places/snap?lat=200&lng=0")
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_autocomplete_rejects_overlong_input(self, client):
        resp = await client.get("/places/autocomplete?input=" + ("x" * 5000))
        assert resp.status_code == 422
