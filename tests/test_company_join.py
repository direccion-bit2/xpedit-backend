"""Tests for POST /company/join.

The endpoint does 6 sequential writes without any rollback:
  users.update → drivers.update → drivers.select → links.insert →
  atomic_increment_uses (rpc) → invite_uses.insert

If any write after step 1 fails, the user is left in an inconsistent
state (company_id set on users/drivers but no active link row) without
any compensation. These tests guard the preconditions + happy path, and
specifically assert that a mid-sequence failure does NOT return 200.
"""

from unittest.mock import MagicMock, patch

import pytest


def _make_invite(active=True, max_uses=None, current_uses=0, expires_at=None):
    return {
        "id": "invite-1",
        "code": "JOIN10",
        "company_id": "company-1",
        "active": active,
        "max_uses": max_uses,
        "current_uses": current_uses,
        "expires_at": expires_at,
    }


class TestCompanyJoin:
    """POST /company/join"""

    @pytest.mark.asyncio
    async def test_invite_not_found_returns_404(self, client):
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
            response = await client.post("/company/join", json={"code": "NOPE", "user_id": "u1"})
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_inactive_invite_returns_400(self, client):
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[_make_invite(active=False)])
            response = await client.post("/company/join", json={"code": "JOIN10", "user_id": "u1"})
        assert response.status_code == 400
        assert "no longer active" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_max_uses_exhausted_returns_400(self, client):
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[_make_invite(max_uses=5, current_uses=5)])
            response = await client.post("/company/join", json={"code": "JOIN10", "user_id": "u1"})
        assert response.status_code == 400
        assert "maximum uses" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_user_already_in_company_returns_400(self, client):
        with patch("main.supabase") as mock_sb:
            def dispatch(table_name):
                mock = MagicMock()
                if table_name == "company_invites":
                    mock.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[_make_invite()])
                elif table_name == "users":
                    # User already has a company_id
                    result = MagicMock()
                    result.data = {"id": "u1", "company_id": "other-company"}
                    mock.select.return_value.eq.return_value.single.return_value.execute.return_value = result
                return mock
            mock_sb.table.side_effect = dispatch

            response = await client.post("/company/join", json={"code": "JOIN10", "user_id": "u1"})
        assert response.status_code == 400
        assert "already part of a company" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_happy_path_all_6_writes_fire(self, client):
        """Verifies the full chain executes: links.insert fires + rpc +
        invite_uses.insert. Guards against someone short-circuiting the
        sequence."""
        with patch("main.supabase") as mock_sb:
            links_insert_mock = MagicMock()
            invite_uses_insert_mock = MagicMock()

            def dispatch(table_name):
                mock = MagicMock()
                if table_name == "company_invites":
                    mock.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[_make_invite()])
                elif table_name == "users":
                    mock.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
                        data={"id": "u1", "company_id": None}
                    )
                    mock.update.return_value.eq.return_value.execute.return_value = MagicMock(data=[{"id": "u1"}])
                elif table_name == "drivers":
                    mock.update.return_value.eq.return_value.execute.return_value = MagicMock(data=[{"id": "d1"}])
                    mock.select.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(
                        data=[{"id": "d1", "promo_plan": "pro"}]
                    )
                elif table_name == "company_driver_links":
                    mock.insert = links_insert_mock
                    links_insert_mock.return_value.execute.return_value = MagicMock(data=[{"id": "link1"}])
                elif table_name == "company_invite_uses":
                    mock.insert = invite_uses_insert_mock
                    invite_uses_insert_mock.return_value.execute.return_value = MagicMock(data=[{"id": "use1"}])
                return mock
            mock_sb.table.side_effect = dispatch
            mock_sb.rpc.return_value.execute.return_value = MagicMock(data={"new_uses": 1})

            response = await client.post("/company/join", json={"code": "JOIN10", "user_id": "u1"})
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["company_id"] == "company-1"

        # Assert ALL the critical writes happened — links.insert is the step
        # whose failure left drivers with company_id but no link in audit.
        links_insert_mock.assert_called_once()
        # Verify driver_plan_at_link captured the PRE-join plan
        link_payload = links_insert_mock.call_args[0][0]
        assert link_payload["driver_plan_at_link"] == "pro"
        assert link_payload["active"] is True

        mock_sb.rpc.assert_called_once()
        invite_uses_insert_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_links_insert_failure_does_not_return_success(self, client):
        """CRITICAL: if company_driver_links.insert fails AFTER users/drivers
        were updated to the new company_id, the endpoint MUST NOT return
        200. Otherwise the driver sees "joined" but has no link row →
        fleet access silently broken, potential double billing when the
        real join finally happens."""
        with patch("main.supabase") as mock_sb:
            def dispatch(table_name):
                mock = MagicMock()
                if table_name == "company_invites":
                    mock.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[_make_invite()])
                elif table_name == "users":
                    mock.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
                        data={"id": "u1", "company_id": None}
                    )
                    mock.update.return_value.eq.return_value.execute.return_value = MagicMock(data=[{"id": "u1"}])
                elif table_name == "drivers":
                    mock.update.return_value.eq.return_value.execute.return_value = MagicMock(data=[{"id": "d1"}])
                    mock.select.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(
                        data=[{"id": "d1", "promo_plan": None}]
                    )
                elif table_name == "company_driver_links":
                    # This is the failure point
                    mock.insert.return_value.execute.side_effect = Exception("simulated link insert failure")
                return mock
            mock_sb.table.side_effect = dispatch

            response = await client.post("/company/join", json={"code": "JOIN10", "user_id": "u1"})
        # The endpoint's generic except block returns 500. What we want to
        # guarantee is simply: NOT a 200 success.
        assert response.status_code != 200, \
            "links.insert failure AFTER users/drivers updated must NOT report success"
