"""
Tests for the B2B beta security hardening (P0):
  - verify_route_access: company operators reach UNASSIGNED routes (driver_id NULL)
    of their own company (the fix that lets a dispatcher assign an unassigned route),
    and are DENIED routes of another company.
  - verify_driver_access: 'company_admin' is recognised, and cross-company access is
    denied (the IDOR core behind /fleet/drivers/{id}/performance and /fleet/messages).
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

import main
from main import verify_driver_access, verify_route_access

COMPANY_A = "company-aaaa-0000-0000-000000000001"
COMPANY_B = "company-bbbb-0000-0000-000000000002"


def _dispatcher(company_id=COMPANY_A, role="dispatcher"):
    return {"id": "u-disp-A", "email": "d@a.com", "role": role, "company_id": company_id}


def _routes_table(route_row):
    """Mock supabase.table('routes')/('drivers') for verify_route_access."""
    def dispatch(name):
        chain = MagicMock()
        if name == "routes":
            res = MagicMock(); res.data = [route_row] if route_row else []
            chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = res
        elif name == "drivers":
            res = MagicMock(); res.data = [{"company_id": route_row.get("company_id")}] if route_row else []
            chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = res
        return chain
    return dispatch


@pytest.mark.asyncio
async def test_route_access_unassigned_route_of_own_company_allowed():
    """driver_id NULL + company_id == caller's company → allowed (NULL-driver fallback)."""
    route = {"id": "r1", "driver_id": None, "company_id": COMPANY_A}
    with patch.object(main, "supabase") as sb, \
         patch.object(main, "get_user_driver_id", new=AsyncMock(return_value="some-other-driver")):
        sb.table.side_effect = _routes_table(route)
        result = await verify_route_access("r1", _dispatcher())
    assert result["id"] == "r1"


@pytest.mark.asyncio
async def test_route_access_other_company_denied():
    """A route belonging to company B is denied to a dispatcher of company A."""
    route = {"id": "r2", "driver_id": None, "company_id": COMPANY_B}
    with patch.object(main, "supabase") as sb, \
         patch.object(main, "get_user_driver_id", new=AsyncMock(return_value="x")):
        sb.table.side_effect = _routes_table(route)
        with pytest.raises(HTTPException) as exc:
            await verify_route_access("r2", _dispatcher())
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_route_access_company_admin_role_recognised():
    """The 'company_admin' role gets the same company-scoped access as 'dispatcher'."""
    route = {"id": "r3", "driver_id": None, "company_id": COMPANY_A}
    with patch.object(main, "supabase") as sb, \
         patch.object(main, "get_user_driver_id", new=AsyncMock(return_value="x")):
        sb.table.side_effect = _routes_table(route)
        result = await verify_route_access("r3", _dispatcher(role="company_admin"))
    assert result["id"] == "r3"


def _drivers_table(driver_company):
    def dispatch(name):
        chain = MagicMock()
        res = MagicMock(); res.data = [{"company_id": driver_company}]
        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = res
        return chain
    return dispatch


@pytest.mark.asyncio
async def test_driver_access_cross_company_denied():
    """IDOR core: dispatcher of A cannot access a driver of company B."""
    with patch.object(main, "supabase") as sb, \
         patch.object(main, "get_user_driver_id", new=AsyncMock(return_value="my-driver")):
        sb.table.side_effect = _drivers_table(COMPANY_B)
        with pytest.raises(HTTPException) as exc:
            await verify_driver_access("driver-of-B", _dispatcher())
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_driver_access_same_company_allowed():
    """Dispatcher of A can access a driver of company A."""
    with patch.object(main, "supabase") as sb, \
         patch.object(main, "get_user_driver_id", new=AsyncMock(return_value="my-driver")):
        sb.table.side_effect = _drivers_table(COMPANY_A)
        assert await verify_driver_access("driver-of-A", _dispatcher()) is True


# --- Invite endpoint: privilege-escalation guard --------------------------------
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from main import app, get_current_user


@pytest_asyncio.fixture
async def dispatcher_client():
    user = {"id": "u-disp-A", "email": "d@a.com", "role": "dispatcher", "company_id": COMPANY_A}

    async def _override():
        return user

    app.dependency_overrides[get_current_user] = _override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_invite_dispatcher_cannot_mint_elevated_role(dispatcher_client):
    """A dispatcher requesting an elevated invite role (company_admin) is rejected 403,
    before ever touching the DB — prevents privilege escalation via the invite body."""
    res = await dispatcher_client.post(
        "/company/invites",
        json={"company_id": COMPANY_A, "role": "company_admin"},
    )
    assert res.status_code == 403
