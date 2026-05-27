"""Tests de INTEGRACIÓN contra Supabase STAGING real (vía PostgREST HTTP).

Por qué existe: los tests normales mockean la librería `supabase` (ver
conftest.py), y el mock NO conoce el esquema real → una query con una columna
inexistente (p.ej. `stops.driver_id`, que NO existe — stops tiene route_id)
PASA en el mock pero PETA en producción con 42703. Eso causó PYTHON-FASTAPI-R:
9 días enviando el email D-2 con "0 entregas" a todos los trials (25 may 2026).

Estos tests pegan directamente al endpoint PostgREST de STAGING con httpx
(NO usan la librería supabase mockeada por conftest) para detectar columnas
inexistentes ANTES de prod.

NO corren en CI normal (necesitan red + credenciales). Opt-in:

    XPEDIT_RUN_DB_INTEGRATION=1 \\
    XPEDIT_STG_SUPABASE_URL=https://ppxbmrzpogxtntsozggb.supabase.co \\
    XPEDIT_STG_KEY=<anon_o_service_key_staging> \\
    pytest tests/test_integration_db.py -v

Correr SIEMPRE antes de un deploy que toque queries de crons/emails.
"""
import os

import httpx
import pytest

_RUN = os.getenv("XPEDIT_RUN_DB_INTEGRATION") == "1"
_URL = os.getenv("XPEDIT_STG_SUPABASE_URL", "").rstrip("/")
_KEY = os.getenv("XPEDIT_STG_KEY", "") or os.getenv("XPEDIT_STG_SERVICE_KEY", "")

pytestmark = pytest.mark.skipif(
    not (_RUN and _URL and _KEY),
    reason="Integración DB opt-in: set XPEDIT_RUN_DB_INTEGRATION=1 + XPEDIT_STG_SUPABASE_URL + XPEDIT_STG_KEY",
)

_ZERO_UUID = "00000000-0000-0000-0000-000000000000"


def _q(path: str) -> httpx.Response:
    """GET a PostgREST de staging. El 42703 (columna inexistente) llega como
    HTTP 400 con el mensaje de Postgres en el body."""
    return httpx.get(
        f"{_URL}/rest/v1/{path}",
        headers={"apikey": _KEY, "Authorization": f"Bearer {_KEY}"},
        timeout=15.0,
    )


class TestStopsSchema:
    """stops NO tiene driver_id — tiene route_id. Estos tests fijan el contrato."""

    def test_stops_filtra_por_route_id_ok(self):
        # Query corregida de _compute_trial_kpis: stops por route_id.
        r = _q(f"stops?select=id&route_id=eq.{_ZERO_UUID}&status=eq.completed&deleted_at=is.null")
        assert r.status_code == 200, f"Query válida falló: {r.status_code} {r.text}"

    def test_stops_driver_id_NO_existe(self):
        # Contrato negativo: si alguien re-introduce stops.driver_id, esto debe
        # dar 42703. Detecta la regresión exacta que causó PYTHON-FASTAPI-R.
        r = _q(f"stops?select=id&driver_id=eq.{_ZERO_UUID}")
        assert r.status_code == 400, f"stops.driver_id NO debería existir (got {r.status_code})"
        assert "driver_id" in r.text or "42703" in r.text, r.text


class TestTrialKpiQueries:
    """Las queries reales de _compute_trial_kpis contra el esquema real."""

    def test_routes_por_driver_id_ok(self):
        # routes SÍ tiene driver_id (a diferencia de stops).
        r = _q(f"routes?select=id,total_distance_km,optimized_hash&driver_id=eq.{_ZERO_UUID}&deleted_at=is.null")
        assert r.status_code == 200, f"Query routes falló: {r.status_code} {r.text}"

    def test_columnas_routes_y_stops_existen(self):
        # Todas las columnas que leen el email D-2 + el feed admin deben existir.
        r1 = _q("routes?select=id,total_distance_km,optimized_hash,driver_id,deleted_at,created_at,status&limit=1")
        assert r1.status_code == 200, f"Columnas routes: {r1.status_code} {r1.text}"
        r2 = _q("stops?select=id,route_id,status,completed_at,failed_at,deleted_at,created_at&limit=1")
        assert r2.status_code == 200, f"Columnas stops: {r2.status_code} {r2.text}"
