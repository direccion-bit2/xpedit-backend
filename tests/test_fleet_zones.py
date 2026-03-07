"""
Tests for fleet zones IDOR protection:
  - GET /fleet/zones (filtered by company_id, 403 without company, admin bypass)
  - PUT /fleet/zones/{id} (ownership check, 403 cross-company, 404 non-existent)
  - DELETE /fleet/zones/{id} (ownership check, 403 cross-company)
"""

from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from main import app, get_current_user, require_admin_or_dispatcher


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

FAKE_COMPANY_A = "company-aaaa-0000-0000-000000000001"
FAKE_COMPANY_B = "company-bbbb-0000-0000-000000000002"
FAKE_ZONE_ID = "zone-0000-0000-0000-000000000001"


def _dispatcher_user(company_id=FAKE_COMPANY_A):
    """A dispatcher user with a company_id."""
    return {
        "id": "dispatcher-0000-0000-0000-000000000001",
        "email": "dispatcher@company-a.com",
        "role": "dispatcher",
        "company_id": company_id,
    }


def _dispatcher_no_company():
    """A dispatcher user WITHOUT a company_id (edge case)."""
    return {
        "id": "dispatcher-0000-0000-0000-000000000002",
        "email": "dispatcher@orphan.com",
        "role": "dispatcher",
        "company_id": None,
    }


def _admin_user():
    """An admin user (no company_id, global access)."""
    return {
        "id": "admin-0000-0000-0000-000000000099",
        "email": "admin@xpedit.es",
        "role": "admin",
        "company_id": None,
    }


# --------------------------------------------------------------------------
# Fixtures: clients with different user contexts
# --------------------------------------------------------------------------

@pytest_asyncio.fixture
async def dispatcher_client():
    """Client authenticated as a dispatcher with company_id."""
    user = _dispatcher_user()

    async def _override():
        return user

    app.dependency_overrides[get_current_user] = _override
    app.dependency_overrides[require_admin_or_dispatcher] = _override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def dispatcher_no_company_client():
    """Client authenticated as a dispatcher WITHOUT company_id."""
    user = _dispatcher_no_company()

    async def _override():
        return user

    app.dependency_overrides[get_current_user] = _override
    app.dependency_overrides[require_admin_or_dispatcher] = _override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def zone_admin_client():
    """Client authenticated as admin (no company_id, global access)."""
    user = _admin_user()

    async def _override():
        return user

    app.dependency_overrides[get_current_user] = _override
    app.dependency_overrides[require_admin_or_dispatcher] = _override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.clear()


# ==========================================================================
# GET /fleet/zones
# ==========================================================================

class TestListFleetZones:
    """Tests for GET /fleet/zones"""

    @pytest.mark.asyncio
    async def test_list_zones_filtered_by_company_id(self, dispatcher_client):
        """Dispatcher sees only zones belonging to their company."""
        with patch("main.supabase") as mock_sb:
            zone_data = [
                {"id": FAKE_ZONE_ID, "name": "Zone A", "company_id": FAKE_COMPANY_A},
            ]
            mock_result = MagicMock()
            mock_result.data = zone_data
            mock_sb.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = mock_result

            response = await dispatcher_client.get("/fleet/zones")

        assert response.status_code == 200
        data = response.json()
        assert "zones" in data
        assert len(data["zones"]) == 1
        assert data["zones"][0]["company_id"] == FAKE_COMPANY_A

    @pytest.mark.asyncio
    async def test_list_zones_returns_403_without_company_id(self, dispatcher_no_company_client):
        """Dispatcher without company_id gets 403."""
        response = await dispatcher_no_company_client.get("/fleet/zones")

        assert response.status_code == 403
        assert "empresa" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_list_zones_admin_sees_all(self, zone_admin_client):
        """Admin (no company_id) can list all zones without filtering."""
        with patch("main.supabase") as mock_sb:
            all_zones = [
                {"id": "z1", "name": "Zone 1", "company_id": FAKE_COMPANY_A},
                {"id": "z2", "name": "Zone 2", "company_id": FAKE_COMPANY_B},
            ]
            mock_result = MagicMock()
            mock_result.data = all_zones
            # Admin path: no .eq() call, goes directly to .order()
            mock_sb.table.return_value.select.return_value.order.return_value.execute.return_value = mock_result

            response = await zone_admin_client.get("/fleet/zones")

        assert response.status_code == 200
        data = response.json()
        assert len(data["zones"]) == 2


# ==========================================================================
# PUT /fleet/zones/{zone_id}
# ==========================================================================

class TestUpdateFleetZone:
    """Tests for PUT /fleet/zones/{zone_id} with IDOR protection."""

    @pytest.mark.asyncio
    async def test_update_zone_same_company_succeeds(self, dispatcher_client):
        """Dispatcher can update a zone that belongs to their company."""
        with patch("main.supabase") as mock_sb:
            # Ownership check: zone belongs to same company
            existing_result = MagicMock()
            existing_result.data = {"id": FAKE_ZONE_ID, "company_id": FAKE_COMPANY_A}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = existing_result

            # Update result
            updated = MagicMock()
            updated.data = [{"id": FAKE_ZONE_ID, "name": "Renamed Zone"}]
            mock_sb.table.return_value.update.return_value.eq.return_value.execute.return_value = updated

            response = await dispatcher_client.put(
                f"/fleet/zones/{FAKE_ZONE_ID}",
                json={"name": "Renamed Zone"},
            )

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_update_zone_different_company_returns_403(self, dispatcher_client):
        """Dispatcher cannot update a zone belonging to a different company (IDOR)."""
        with patch("main.supabase") as mock_sb:
            # Ownership check: zone belongs to DIFFERENT company
            existing_result = MagicMock()
            existing_result.data = {"id": FAKE_ZONE_ID, "company_id": FAKE_COMPANY_B}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = existing_result

            response = await dispatcher_client.put(
                f"/fleet/zones/{FAKE_ZONE_ID}",
                json={"name": "Hacked Zone"},
            )

        assert response.status_code == 403
        assert "acceso" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_update_zone_not_found_returns_404(self, dispatcher_client):
        """Updating a non-existent zone returns 404."""
        with patch("main.supabase") as mock_sb:
            # Ownership check: no zone found
            existing_result = MagicMock()
            existing_result.data = None
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = existing_result

            response = await dispatcher_client.put(
                f"/fleet/zones/{FAKE_ZONE_ID}",
                json={"name": "Ghost Zone"},
            )

        assert response.status_code == 404
        assert "no encontrada" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_update_zone_without_company_returns_403(self, dispatcher_no_company_client):
        """Dispatcher without company_id cannot update zones."""
        response = await dispatcher_no_company_client.put(
            f"/fleet/zones/{FAKE_ZONE_ID}",
            json={"name": "No Company Zone"},
        )

        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_update_zone_admin_bypasses_ownership(self, zone_admin_client):
        """Admin can update any zone regardless of company_id."""
        with patch("main.supabase") as mock_sb:
            # Admin path skips ownership check, goes straight to update
            updated = MagicMock()
            updated.data = [{"id": FAKE_ZONE_ID, "name": "Admin Updated"}]
            mock_sb.table.return_value.update.return_value.eq.return_value.execute.return_value = updated

            response = await zone_admin_client.put(
                f"/fleet/zones/{FAKE_ZONE_ID}",
                json={"name": "Admin Updated"},
            )

        assert response.status_code == 200


# ==========================================================================
# DELETE /fleet/zones/{zone_id}
# ==========================================================================

class TestDeleteFleetZone:
    """Tests for DELETE /fleet/zones/{zone_id} with IDOR protection."""

    @pytest.mark.asyncio
    async def test_delete_zone_same_company_succeeds(self, dispatcher_client):
        """Dispatcher can delete a zone belonging to their company."""
        with patch("main.supabase") as mock_sb:
            # Ownership check: zone belongs to same company
            existing_result = MagicMock()
            existing_result.data = {"id": FAKE_ZONE_ID, "company_id": FAKE_COMPANY_A}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = existing_result

            # Delete
            mock_sb.table.return_value.delete.return_value.eq.return_value.execute.return_value = MagicMock()

            response = await dispatcher_client.delete(f"/fleet/zones/{FAKE_ZONE_ID}")

        assert response.status_code == 200
        assert response.json()["ok"] is True

    @pytest.mark.asyncio
    async def test_delete_zone_different_company_returns_403(self, dispatcher_client):
        """Dispatcher cannot delete a zone belonging to a different company (IDOR)."""
        with patch("main.supabase") as mock_sb:
            # Ownership check: zone belongs to DIFFERENT company
            existing_result = MagicMock()
            existing_result.data = {"id": FAKE_ZONE_ID, "company_id": FAKE_COMPANY_B}
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = existing_result

            response = await dispatcher_client.delete(f"/fleet/zones/{FAKE_ZONE_ID}")

        assert response.status_code == 403
        assert "acceso" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_delete_zone_not_found_returns_404(self, dispatcher_client):
        """Deleting a non-existent zone returns 404."""
        with patch("main.supabase") as mock_sb:
            existing_result = MagicMock()
            existing_result.data = None
            mock_sb.table.return_value.select.return_value.eq.return_value.single.return_value.execute.return_value = existing_result

            response = await dispatcher_client.delete(f"/fleet/zones/{FAKE_ZONE_ID}")

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_zone_without_company_returns_403(self, dispatcher_no_company_client):
        """Dispatcher without company_id cannot delete zones."""
        response = await dispatcher_no_company_client.delete(f"/fleet/zones/{FAKE_ZONE_ID}")

        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_zone_admin_bypasses_ownership(self, zone_admin_client):
        """Admin can delete any zone regardless of company_id."""
        with patch("main.supabase") as mock_sb:
            # Admin path skips ownership check
            mock_sb.table.return_value.delete.return_value.eq.return_value.execute.return_value = MagicMock()

            response = await zone_admin_client.delete(f"/fleet/zones/{FAKE_ZONE_ID}")

        assert response.status_code == 200
        assert response.json()["ok"] is True
