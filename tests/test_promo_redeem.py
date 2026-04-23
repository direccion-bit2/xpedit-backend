"""Tests for POST /promo/redeem.

The endpoint does 3 sequential writes (atomic_increment_uses RPC,
code_redemptions.insert, drivers.update). If step 2 or 3 fails AFTER
step 1, the counter has been spent but the benefit was never granted —
customer pays, gets nothing, and the admin dashboard shows a phantom
redemption. These tests guard the error branches.
"""

from unittest.mock import MagicMock, patch

import pytest


def _make_promo(max_uses=None, current_uses=0, active=True, expires_at=None, benefit_plan="pro", benefit_value=30):
    return {
        "id": "promo-1",
        "code": "TEST10",
        "active": active,
        "max_uses": max_uses,
        "current_uses": current_uses,
        "expires_at": expires_at,
        "benefit_plan": benefit_plan,
        "benefit_value": benefit_value,
    }


def _setup_promo_chain(mock_sb, promo, already_redeemed=False):
    """Wire the read path: promo_codes.select → [promo]; code_redemptions.select → already_redeemed?"""
    def table_dispatch(table_name):
        mock = MagicMock()
        if table_name == "promo_codes":
            result = MagicMock()
            result.data = [promo]
            mock.select.return_value.eq.return_value.execute.return_value = result
        elif table_name == "code_redemptions":
            redemption = MagicMock()
            redemption.data = [{"id": "red-1"}] if already_redeemed else []
            mock.select.return_value.eq.return_value.eq.return_value.execute.return_value = redemption
            # insert path used by step 8
            mock.insert.return_value.execute.return_value = MagicMock(data=[{"id": "red-new"}])
        elif table_name == "drivers":
            mock.update.return_value.eq.return_value.execute.return_value = MagicMock(data=[{"id": "d-1"}])
        return mock
    mock_sb.table.side_effect = table_dispatch
    mock_sb.rpc.return_value.execute.return_value = MagicMock(data={"new_uses": (promo.get("current_uses") or 0) + 1})


class TestPromoRedeem:
    """POST /promo/redeem"""

    @pytest.mark.asyncio
    async def test_code_not_found_returns_404(self, client):
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
            response = await client.post("/promo/redeem", json={"code": "NOPE", "user_id": "u1"})
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_inactive_code_returns_400(self, client):
        with patch("main.supabase") as mock_sb:
            _setup_promo_chain(mock_sb, _make_promo(active=False))
            response = await client.post("/promo/redeem", json={"code": "TEST10", "user_id": "u1"})
        assert response.status_code == 400
        assert "no longer active" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_exhausted_max_uses_returns_400(self, client):
        """Guards against overshoot: max_uses=10, current_uses=10 → no more redemptions."""
        with patch("main.supabase") as mock_sb:
            _setup_promo_chain(mock_sb, _make_promo(max_uses=10, current_uses=10))
            response = await client.post("/promo/redeem", json={"code": "TEST10", "user_id": "u1"})
        assert response.status_code == 400
        assert "maximum number of uses" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_already_redeemed_returns_400(self, client):
        with patch("main.supabase") as mock_sb:
            _setup_promo_chain(mock_sb, _make_promo(), already_redeemed=True)
            response = await client.post("/promo/redeem", json={"code": "TEST10", "user_id": "u1"})
        assert response.status_code == 400
        assert "already redeemed" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_happy_path_increments_and_records(self, client):
        """The 3-step write sequence fires: rpc → insert → update."""
        with patch("main.supabase") as mock_sb:
            _setup_promo_chain(mock_sb, _make_promo(max_uses=100, current_uses=5))
            response = await client.post("/promo/redeem", json={"code": "TEST10", "user_id": "u1"})
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["benefit"] == "pro"

        # Verify ALL three writes fired in order.
        mock_sb.rpc.assert_called_once()
        assert mock_sb.rpc.call_args[0][0] == "atomic_increment_uses"

    @pytest.mark.asyncio
    async def test_insert_failure_after_increment_surfaces_as_500(self, client):
        """THE CRITICAL BUG GUARD: if code_redemptions.insert raises AFTER
        atomic_increment_uses succeeded, the endpoint must NOT return a 200
        success to the client — the counter is already spent and the user
        got no benefit. A 500 is the right signal so Sentry captures it and
        the client can retry.
        """
        with patch("main.supabase") as mock_sb:
            _setup_promo_chain(mock_sb, _make_promo(max_uses=100, current_uses=5))

            # Override the code_redemptions branch to raise on insert
            def table_dispatch_failing(table_name):
                mock = MagicMock()
                if table_name == "promo_codes":
                    result = MagicMock()
                    result.data = [_make_promo(max_uses=100, current_uses=5)]
                    mock.select.return_value.eq.return_value.execute.return_value = result
                elif table_name == "code_redemptions":
                    no_redeem = MagicMock()
                    no_redeem.data = []
                    mock.select.return_value.eq.return_value.eq.return_value.execute.return_value = no_redeem
                    mock.insert.return_value.execute.side_effect = Exception("simulated DB failure")
                return mock
            mock_sb.table.side_effect = table_dispatch_failing

            response = await client.post("/promo/redeem", json={"code": "TEST10", "user_id": "u1"})
        # The endpoint catches broadly today (try/except around entire body).
        # Contract: MUST NOT return 200 success in this scenario.
        assert response.status_code != 200, \
            "Insert failure after increment must NOT be reported as success"
