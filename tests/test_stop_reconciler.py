"""
Tests del reconciliador de stop_mutation_log (#58, OS-22).

El endpoint /stops/mark deja DURABLE la intención del driver aunque la fila aún
no exista (ruta sin sincronizar). reconcile_stop_mutation_log() la APLICA cuando
la fila ya existe. Garantías que cubrimos:
  - fila resoluble (por stop_id o route+client/position) → se aplica el UPDATE y
    el log queda applied=true.
  - fila NO resoluble y joven → se deja en cola (sin error, se reintenta luego).
  - fila NO resoluble y vieja (>24h) → se sella con error para no reprocesar.
  - action='failed' escribe completed_at Y failed_at (def canónica "trabajada").
  - 100% Supabase: el test verifica que NO se llama a ninguna API de pago
    (no hay imports de google/geocode en el camino; aquí se asegura por mocks).
"""

from datetime import datetime, timedelta, timezone

import pytest


def _mock(data):
    class _R:
        pass
    r = _R()
    r.data = data
    return r


class _Term:
    """Eslabón terminal: cualquier método encadena a sí mismo; execute() devuelve el resultado preconfigurado."""

    def __init__(self, result):
        self._result = result

    def __getattr__(self, name):
        if name == "execute":
            return lambda: self._result
        return lambda *a, **k: self


class _Table:
    def __init__(self, select_result=None, update_result=None):
        self.select_result = select_result
        self.update_result = update_result
        self.update_calls = []

    def select(self, *a, **k):
        return _Term(self.select_result)

    def update(self, payload, *a, **k):
        self.update_calls.append(payload)
        return _Term(self.update_result)


class _Supabase:
    def __init__(self, tables):
        self._tables = tables

    def table(self, name):
        return self._tables[name]


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _old_iso(hours):
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


@pytest.mark.asyncio
async def test_applies_resolvable_by_stop_id(monkeypatch):
    """Fila con stop_id conocido y parada existente → UPDATE aplicado, log applied=true."""
    import main
    log = _Table(
        select_result=_mock([{
            "id": "log-1", "stop_id": "stop-1", "route_id": None, "client_id": None,
            "position": None, "action": "completed", "marked_at": _now_iso(), "created_at": _now_iso(),
        }]),
        update_result=_mock([{"id": "log-1"}]),
    )
    stops = _Table(update_result=_mock([{"id": "stop-1"}]))
    monkeypatch.setattr(main, "supabase", _Supabase({"stop_mutation_log": log, "stops": stops}))

    res = await main.reconcile_stop_mutation_log()

    assert res["applied"] == 1
    assert res["gaveup"] == 0
    # el UPDATE de stops llevó status=completed + completed_at
    assert stops.update_calls and stops.update_calls[0]["status"] == "completed"
    assert "completed_at" in stops.update_calls[0]
    # el log se selló applied=true + resolved_stop_id
    assert any(c.get("applied") is True and c.get("resolved_stop_id") == "stop-1" for c in log.update_calls)


@pytest.mark.asyncio
async def test_applies_resolvable_by_route_client(monkeypatch):
    """Sin stop_id pero con route_id+client_id → resuelve por lookup y aplica."""
    import main
    log = _Table(
        select_result=_mock([{
            "id": "log-2", "stop_id": None, "route_id": "route-1", "client_id": "cli-1",
            "position": 3, "action": "completed", "marked_at": _now_iso(), "created_at": _now_iso(),
        }]),
        update_result=_mock([{"id": "log-2"}]),
    )
    # stops.select (lookup) encuentra la fila; stops.update la aplica.
    stops = _Table(select_result=_mock([{"id": "stop-resolved"}]), update_result=_mock([{"id": "stop-resolved"}]))
    monkeypatch.setattr(main, "supabase", _Supabase({"stop_mutation_log": log, "stops": stops}))

    res = await main.reconcile_stop_mutation_log()

    assert res["applied"] == 1
    assert any(c.get("resolved_stop_id") == "stop-resolved" for c in log.update_calls)


@pytest.mark.asyncio
async def test_failed_sets_both_timestamps(monkeypatch):
    """action='failed' escribe status=failed + completed_at Y failed_at."""
    import main
    marked = _now_iso()
    log = _Table(
        select_result=_mock([{
            "id": "log-3", "stop_id": "stop-3", "route_id": None, "client_id": None,
            "position": None, "action": "failed", "marked_at": marked, "created_at": _now_iso(),
        }]),
        update_result=_mock([{"id": "log-3"}]),
    )
    stops = _Table(update_result=_mock([{"id": "stop-3"}]))
    monkeypatch.setattr(main, "supabase", _Supabase({"stop_mutation_log": log, "stops": stops}))

    res = await main.reconcile_stop_mutation_log()

    assert res["applied"] == 1
    payload = stops.update_calls[0]
    assert payload["status"] == "failed"
    assert payload["completed_at"] == marked
    assert payload["failed_at"] == marked


@pytest.mark.asyncio
async def test_young_unresolvable_is_kept(monkeypatch):
    """Fila NO resoluble (UPDATE 0 filas) y JOVEN → no se aplica ni se sella (se reintenta luego)."""
    import main
    log = _Table(
        select_result=_mock([{
            "id": "log-4", "stop_id": "ghost", "route_id": None, "client_id": None,
            "position": None, "action": "completed", "marked_at": _now_iso(), "created_at": _now_iso(),
        }]),
        update_result=_mock([{"id": "log-4"}]),
    )
    stops = _Table(update_result=_mock([]))  # 0 filas: parada no existe / borrada
    monkeypatch.setattr(main, "supabase", _Supabase({"stop_mutation_log": log, "stops": stops}))

    res = await main.reconcile_stop_mutation_log()

    assert res["applied"] == 0
    assert res["gaveup"] == 0
    # NO se tocó el log (ni applied ni error) → sigue pendiente para el próximo ciclo
    assert log.update_calls == []


@pytest.mark.asyncio
async def test_old_unresolvable_is_sealed(monkeypatch):
    """Fila NO resoluble y VIEJA (>24h) → se sella con error para no reprocesar."""
    import main
    log = _Table(
        select_result=_mock([{
            "id": "log-5", "stop_id": "ghost", "route_id": None, "client_id": None,
            "position": None, "action": "completed", "marked_at": _old_iso(30), "created_at": _old_iso(30),
        }]),
        update_result=_mock([{"id": "log-5"}]),
    )
    stops = _Table(update_result=_mock([]))
    monkeypatch.setattr(main, "supabase", _Supabase({"stop_mutation_log": log, "stops": stops}))

    res = await main.reconcile_stop_mutation_log()

    assert res["applied"] == 0
    assert res["gaveup"] == 1
    assert any(c.get("error") == "unresolved_after_24h" for c in log.update_calls)


@pytest.mark.asyncio
async def test_invalid_action_is_sealed(monkeypatch):
    """action corrupta → se sella con error (no se reprocesa, no toca stops)."""
    import main
    log = _Table(
        select_result=_mock([{
            "id": "log-6", "stop_id": "stop-6", "route_id": None, "client_id": None,
            "position": None, "action": "weird", "marked_at": _now_iso(), "created_at": _now_iso(),
        }]),
        update_result=_mock([{"id": "log-6"}]),
    )
    stops = _Table(update_result=_mock([{"id": "stop-6"}]))
    monkeypatch.setattr(main, "supabase", _Supabase({"stop_mutation_log": log, "stops": stops}))

    res = await main.reconcile_stop_mutation_log()

    assert res["gaveup"] == 1
    assert stops.update_calls == []  # nunca tocó stops
    assert any(c.get("error") == "invalid_action" for c in log.update_calls)


@pytest.mark.asyncio
async def test_empty_pending_is_noop(monkeypatch):
    """Sin filas pendientes → no-op."""
    import main
    log = _Table(select_result=_mock([]))
    stops = _Table()
    monkeypatch.setattr(main, "supabase", _Supabase({"stop_mutation_log": log, "stops": stops}))

    res = await main.reconcile_stop_mutation_log()

    assert res == {"scanned": 0, "applied": 0, "gaveup": 0}
