"""
Tests for POST /stops/add — alta DURABLE de parada (#82).

Cubre la fuga de raíz: el INSERT directo del cliente fallaba bajo RLS
(can_insert_stop) y la parada se DESCARTABA en silencio. El endpoint:
  - service_role (bypassa RLS) → no más 42501 silencioso.
  - límite free 10/día como 402 LIMPIO (no drop) → upsell.
  - idempotente por (route_id, client_id) → reintento de cola no duplica.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import FAKE_DRIVER_ID


def _mock(data):
    m = MagicMock()
    m.data = data
    return m


BODY = {
    "route_id": "route-1", "client_id": "cid-1", "address": "Calle Test 1",
    "lat": 40.4, "lng": -3.7, "position": 0, "packageId": 5,
}


def _sb_for(existing=None, today_count=0, new_id="stop-new"):
    """Mock de supabase: drivers lookup + existing check + upsert + rpc."""
    mock_sb = MagicMock()

    def table_dispatch(name):
        chain = MagicMock()
        if name == "drivers":
            chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = _mock([{"id": FAKE_DRIVER_ID}])
        elif name == "stops":
            # existing check: select().eq().eq().is_().limit().execute()
            chain.select.return_value.eq.return_value.eq.return_value.is_.return_value.limit.return_value.execute.return_value = _mock(existing or [])
            # upsert(...).select().execute()
            chain.upsert.return_value.select.return_value.execute.return_value = _mock([{"id": new_id}])
        return chain

    mock_sb.table = MagicMock(side_effect=table_dispatch)
    mock_sb.rpc = MagicMock(return_value=MagicMock(execute=MagicMock(return_value=_mock(today_count))))
    return mock_sb


class TestStopAdd:
    @pytest.mark.asyncio
    async def test_new_stop_paid_user_inserts(self, client):
        with patch("main.supabase", _sb_for(existing=[], today_count=0)), \
             patch("main.verify_route_access", new=AsyncMock(return_value=None)), \
             patch("main._resolve_user_tier", return_value=("pro", FAKE_DRIVER_ID)):
            resp = await client.post("/stops/add", json=BODY)
        assert resp.status_code == 200, resp.text
        d = resp.json()
        assert d["success"] is True and d["existing"] is False and d["id"] == "stop-new"

    @pytest.mark.asyncio
    async def test_idempotent_existing_returns_same_no_reinsert(self, client):
        sb = _sb_for(existing=[{"id": "stop-existing"}])
        with patch("main.supabase", sb), \
             patch("main.verify_route_access", new=AsyncMock(return_value=None)), \
             patch("main._resolve_user_tier", return_value=("free", FAKE_DRIVER_ID)):
            resp = await client.post("/stops/add", json=BODY)
        assert resp.status_code == 200, resp.text
        d = resp.json()
        assert d["existing"] is True and d["id"] == "stop-existing"
        # idempotente: no se llama a la RPC del contador para una fila ya existente
        sb.rpc.assert_not_called()

    @pytest.mark.asyncio
    async def test_free_over_limit_returns_402_not_silent_drop(self, client):
        """El caso #82: free en el tope → 402 limpio con upsell, NO drop silencioso."""
        with patch("main.supabase", _sb_for(existing=[], today_count=10)), \
             patch("main.verify_route_access", new=AsyncMock(return_value=None)), \
             patch("main._resolve_user_tier", return_value=("free", FAKE_DRIVER_ID)):
            resp = await client.post("/stops/add", json=BODY)
        assert resp.status_code == 402, resp.text
        detail = resp.json()["detail"]
        assert detail["error"] == "free_daily_stop_limit" and detail["limit"] == 10

    @pytest.mark.asyncio
    async def test_free_under_limit_inserts_and_counts(self, client):
        sb = _sb_for(existing=[], today_count=3)
        with patch("main.supabase", sb), \
             patch("main.verify_route_access", new=AsyncMock(return_value=None)), \
             patch("main._resolve_user_tier", return_value=("free", FAKE_DRIVER_ID)):
            resp = await client.post("/stops/add", json=BODY)
        assert resp.status_code == 200, resp.text
        # contabiliza la parada del día (increment_daily_usage)
        rpc_calls = [c.args[0] for c in sb.rpc.call_args_list]
        assert "get_today_stop_count" in rpc_calls
        assert "increment_daily_usage" in rpc_calls

    @pytest.mark.asyncio
    async def test_route_not_owned_403(self, client):
        from fastapi import HTTPException
        with patch("main.supabase", _sb_for(existing=[])), \
             patch("main.verify_route_access", new=AsyncMock(side_effect=HTTPException(status_code=403, detail="no"))), \
             patch("main._resolve_user_tier", return_value=("pro", FAKE_DRIVER_ID)):
            resp = await client.post("/stops/add", json=BODY)
        assert resp.status_code == 403
