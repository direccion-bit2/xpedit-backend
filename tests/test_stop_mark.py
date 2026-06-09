"""
Tests for POST /stops/mark — marcado DURABLE de parada (completed/failed).

Garantía que cubrimos: un marcado de parada NUNCA se pierde.
  - Si se resuelve la fila → se aplica con service_role (applied=True).
  - Si NO se resuelve ahora (ruta sin sincronizar) → queda en stop_mutation_log
    (logged=True) para que el reconciliador la aplique luego.
  - Si NO se pudo ni loguear ni aplicar → 503, para que el cliente la mantenga
    en cola y reintente (jamás descartar un marcado sin guardarlo).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import FAKE_DRIVER_ID

MARKED_AT = "2026-06-08T20:00:00+00:00"


def _mock(data):
    m = MagicMock()
    m.data = data
    return m


def _driver_lookup():
    return _mock([{"id": FAKE_DRIVER_ID}])


class TestStopMark:
    @pytest.mark.asyncio
    async def test_mark_by_stop_id_applies(self, client):
        """dbId conocido → resuelve directo y aplica el UPDATE (applied=True)."""
        with patch("main.supabase") as mock_sb, \
             patch("main.verify_stop_access", new=AsyncMock(return_value=None)):
            log_insert = _mock([{"id": "log-1"}])
            log_update = _mock([{"id": "log-1"}])
            stop_update = _mock([{"id": "stop-1"}])

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = _driver_lookup()
                elif name == "stop_mutation_log":
                    chain.insert.return_value.execute.return_value = log_insert
                    chain.update.return_value.eq.return_value.execute.return_value = log_update
                elif name == "stops":
                    chain.update.return_value.eq.return_value.is_.return_value.execute.return_value = stop_update
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await client.post("/stops/mark", json={
                "action": "completed", "stop_id": "stop-1", "marked_at": MARKED_AT,
            })

        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["applied"] is True
        assert data["stop_id"] == "stop-1"

    @pytest.mark.asyncio
    async def test_mark_resolves_by_route_client_and_applies(self, client):
        """Sin dbId → resuelve por (route_id, client_id) y aplica. El caso del
        drain offline cuya parada se creó antes de confirmar el dbId."""
        with patch("main.supabase") as mock_sb, \
             patch("main.verify_stop_access", new=AsyncMock(return_value=None)):
            log_insert = _mock([{"id": "log-2"}])
            log_update = _mock([{"id": "log-2"}])
            found = _mock([{"id": "stop-resolved"}])
            stop_update = _mock([{"id": "stop-resolved"}])

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = _driver_lookup()
                elif name == "stop_mutation_log":
                    chain.insert.return_value.execute.return_value = log_insert
                    chain.update.return_value.eq.return_value.execute.return_value = log_update
                elif name == "stops":
                    chain.select.return_value.eq.return_value.eq.return_value.is_.return_value.limit.return_value.execute.return_value = found
                    chain.update.return_value.eq.return_value.is_.return_value.execute.return_value = stop_update
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await client.post("/stops/mark", json={
                "action": "completed", "route_id": "route-1", "client_id": "cli-1",
                "position": 3, "marked_at": MARKED_AT,
            })

        assert resp.status_code == 200
        data = resp.json()
        assert data["applied"] is True
        assert data["stop_id"] == "stop-resolved"

    @pytest.mark.asyncio
    async def test_mark_unresolvable_is_durable_logged(self, client):
        """No se resuelve (ruta aún sin sincronizar) → NO se pierde: queda en el
        log (logged=True) y el reconciliador la aplicará luego."""
        with patch("main.supabase") as mock_sb, \
             patch("main.verify_stop_access", new=AsyncMock(return_value=None)):
            log_insert = _mock([{"id": "log-3"}])
            not_found = _mock([])

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = _driver_lookup()
                elif name == "stop_mutation_log":
                    chain.insert.return_value.execute.return_value = log_insert
                elif name == "stops":
                    chain.select.return_value.eq.return_value.eq.return_value.is_.return_value.limit.return_value.execute.return_value = not_found
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await client.post("/stops/mark", json={
                "action": "completed", "route_id": "route-x", "client_id": "cli-x",
                "marked_at": MARKED_AT,
            })

        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["applied"] is False
        assert data["logged"] is True

    @pytest.mark.asyncio
    async def test_mark_invalid_action_rejected(self, client):
        """action distinto de completed|failed → 400 (no toca BD)."""
        resp = await client.post("/stops/mark", json={"action": "borrado", "stop_id": "s"})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_mark_not_durable_returns_503(self, client):
        """Si NO se pudo loguear (insert peta) NI resolver/aplicar → 503, para
        que el cliente la mantenga en cola y reintente. NUNCA se descarta."""
        with patch("main.supabase") as mock_sb, \
             patch("main.verify_stop_access", new=AsyncMock(return_value=None)):
            not_found = _mock([])

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = _driver_lookup()
                elif name == "stop_mutation_log":
                    chain.insert.return_value.execute.side_effect = Exception("log down")
                elif name == "stops":
                    chain.select.return_value.eq.return_value.eq.return_value.is_.return_value.limit.return_value.execute.return_value = not_found
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await client.post("/stops/mark", json={
                "action": "failed", "route_id": "route-z", "client_id": "cli-z",
                "marked_at": MARKED_AT,
            })

        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_mark_not_owner_is_logged_not_403(self, client):
        """Si la parada NO es del driver (verify_stop_access lanza 403) NO
        devolvemos 403 — un 403 haría que el cliente reintentara en bucle. El
        marcado queda en el log (forensic) y devolvemos success+logged → el
        cliente lo suelta sin bucle, sin aplicar a una parada ajena."""
        from fastapi import HTTPException
        with patch("main.supabase") as mock_sb, \
             patch("main.verify_stop_access", new=AsyncMock(side_effect=HTTPException(status_code=403, detail="no"))):
            log_insert = _mock([{"id": "log-9"}])
            log_update = _mock([{"id": "log-9"}])

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = _driver_lookup()
                elif name == "stop_mutation_log":
                    chain.insert.return_value.execute.return_value = log_insert
                    chain.update.return_value.eq.return_value.execute.return_value = log_update
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await client.post("/stops/mark", json={
                "action": "completed", "stop_id": "foreign-stop", "marked_at": MARKED_AT,
            })

        assert resp.status_code == 200
        data = resp.json()
        assert data["applied"] is False
        assert data["logged"] is True

    @pytest.mark.asyncio
    async def test_mark_failed_applies_both_timestamps(self, client):
        """action='failed' aplica status=failed + completed_at Y failed_at (la
        def canónica de 'trabajada' del dashboard = completed_at OR failed_at)."""
        captured = {}

        with patch("main.supabase") as mock_sb, \
             patch("main.verify_stop_access", new=AsyncMock(return_value=None)):
            log_insert = _mock([{"id": "log-f"}])
            log_update = _mock([{"id": "log-f"}])

            def _capture_update(payload):
                captured.update(payload)
                upd_chain = MagicMock()
                upd_chain.eq.return_value.is_.return_value.execute.return_value = _mock([{"id": "stop-f"}])
                return upd_chain

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = _driver_lookup()
                elif name == "stop_mutation_log":
                    chain.insert.return_value.execute.return_value = log_insert
                    chain.update.return_value.eq.return_value.execute.return_value = log_update
                elif name == "stops":
                    chain.update = MagicMock(side_effect=_capture_update)
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await client.post("/stops/mark", json={
                "action": "failed", "stop_id": "stop-f", "marked_at": MARKED_AT,
            })

        assert resp.status_code == 200
        data = resp.json()
        assert data["applied"] is True
        assert captured.get("status") == "failed"
        assert captured.get("completed_at") == MARKED_AT
        assert captured.get("failed_at") == MARKED_AT

    @pytest.mark.asyncio
    async def test_mark_resolves_by_route_position(self, client):
        """Sin stop_id ni client_id, solo (route_id, position) → resuelve por
        posición y aplica. Fallback del drain offline cuando el op no llevaba
        clientId (p.ej. tras reordenar)."""
        with patch("main.supabase") as mock_sb, \
             patch("main.verify_stop_access", new=AsyncMock(return_value=None)):
            log_insert = _mock([{"id": "log-p"}])
            log_update = _mock([{"id": "log-p"}])
            found = _mock([{"id": "stop-by-pos"}])
            stop_update = _mock([{"id": "stop-by-pos"}])

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = _driver_lookup()
                elif name == "stop_mutation_log":
                    chain.insert.return_value.execute.return_value = log_insert
                    chain.update.return_value.eq.return_value.execute.return_value = log_update
                elif name == "stops":
                    chain.select.return_value.eq.return_value.eq.return_value.is_.return_value.limit.return_value.execute.return_value = found
                    chain.update.return_value.eq.return_value.is_.return_value.execute.return_value = stop_update
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            resp = await client.post("/stops/mark", json={
                "action": "completed", "route_id": "route-1", "position": 5,
                "marked_at": MARKED_AT,
            })

        assert resp.status_code == 200
        data = resp.json()
        assert data["applied"] is True
        assert data["stop_id"] == "stop-by-pos"
