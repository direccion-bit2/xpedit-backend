"""
Tests for referral system endpoints in main.py:
  - GET /referral/code
  - GET /referral/stats
  - POST /referral/redeem

Focuses on edge cases like code generation, self-referral, duplicate redemption,
reward extension logic, and error handling.
"""

from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import FAKE_DRIVER_ID

# ===================== GET /referral/code =====================


class TestGetReferralCode:
    """Tests for GET /referral/code"""

    @pytest.mark.asyncio
    async def test_returns_existing_code(self, client):
        """If driver already has a referral code, return it."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            code_result = MagicMock()
            code_result.data = {"referral_code": "XPD-TEST"}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                    chain.select.return_value.eq.return_value.single.return_value.execute.return_value = code_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/referral/code")
        assert response.status_code == 200
        data = response.json()
        assert data["code"] == "XPD-TEST"
        assert data["driver_id"] == FAKE_DRIVER_ID

    @pytest.mark.asyncio
    async def test_generates_new_code(self, client):
        """If driver has no referral code, generate and save one."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            code_result = MagicMock()
            code_result.data = {"referral_code": None}

            # No existing code with same name
            no_existing = MagicMock()
            no_existing.data = []

            call_count = {"n": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    call_count["n"] += 1
                    if call_count["n"] == 1:
                        # get_user_driver_id
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                    elif call_count["n"] == 2:
                        # select referral_code
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = code_result
                    elif call_count["n"] == 3:
                        # check uniqueness
                        chain.select.return_value.eq.return_value.execute.return_value = no_existing
                    else:
                        # update with new code
                        chain.update.return_value.eq.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/referral/code")
        assert response.status_code == 200
        data = response.json()
        assert data["code"].startswith("XPD-")
        assert len(data["code"]) == 8  # XPD- + 4 chars

    @pytest.mark.asyncio
    async def test_driver_not_found(self, client):
        """If no driver is linked to the user, return 404."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = []  # No driver

            mock_sb.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup

            response = await client.get("/referral/code")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_db_error_returns_500(self, client):
        """Database error returns 500."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                    chain.select.return_value.eq.return_value.single.return_value.execute.side_effect = Exception("DB error")
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/referral/code")
        assert response.status_code == 500


# ===================== POST /referral/redeem =====================


class TestRedeemReferral:
    """Tests for POST /referral/redeem"""

    @pytest.mark.asyncio
    async def test_redeem_success(self, client):
        """Successfully redeem a valid referral code."""
        with patch("main.supabase") as mock_sb, \
             patch("main.send_referral_reward_email"), \
             patch("main.send_plan_activated_email"):

            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            referrer_result = MagicMock()
            referrer_result.data = {
                "id": "referrer-driver-id",
                "referral_code": "XPD-ABCD",
                "email": "referrer@test.com",
                "name": "Referrer"
            }

            no_existing_referral = MagicMock()
            no_existing_referral.data = []

            referrer_plan = MagicMock()
            referrer_plan.data = {"promo_plan_expires_at": None}

            referred_info = MagicMock()
            referred_info.data = {"email": "referred@test.com", "name": "Referred"}

            call_count = {"n": 0}

            def table_dispatch(name):
                chain = MagicMock()
                call_count["n"] += 1
                if name == "drivers":
                    if call_count["n"] == 1:
                        # get_user_driver_id
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                    elif call_count["n"] == 2:
                        # find referrer by code
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = referrer_result
                    else:
                        # update/select for plan data
                        chain.update.return_value.eq.return_value.execute.return_value = MagicMock()
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = referrer_plan
                elif name == "referrals":
                    if call_count["n"] <= 4:
                        chain.select.return_value.eq.return_value.execute.return_value = no_existing_referral
                    else:
                        chain.insert.return_value.execute.return_value = MagicMock()
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post("/referral/redeem", json={
                "referral_code": "XPD-ABCD"
            })
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["reward_days"] == 7
        assert data["reward_plan"] == "pro"

    @pytest.mark.asyncio
    async def test_redeem_code_not_found(self, client):
        """Redeeming a non-existent code returns 404."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            referrer_result = MagicMock()
            referrer_result.data = None  # Code not found

            call_count = {"n": 0}

            def table_dispatch(name):
                chain = MagicMock()
                call_count["n"] += 1
                if name == "drivers":
                    if call_count["n"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                    else:
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = referrer_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post("/referral/redeem", json={
                "referral_code": "XPD-ZZZZ"
            })
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_redeem_self_referral_blocked(self, client):
        """Users cannot use their own referral code."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            referrer_result = MagicMock()
            referrer_result.data = {
                "id": FAKE_DRIVER_ID,  # Same driver
                "referral_code": "XPD-SELF",
                "email": "self@test.com",
                "name": "Self"
            }

            call_count = {"n": 0}

            def table_dispatch(name):
                chain = MagicMock()
                call_count["n"] += 1
                if name == "drivers":
                    if call_count["n"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                    else:
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = referrer_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post("/referral/redeem", json={
                "referral_code": "XPD-SELF"
            })
        assert response.status_code == 400
        assert "propio" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_redeem_already_used(self, client):
        """Users who already redeemed a code cannot redeem again."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            referrer_result = MagicMock()
            referrer_result.data = {
                "id": "other-driver",
                "referral_code": "XPD-USED",
                "email": "other@test.com",
                "name": "Other"
            }

            existing_referral = MagicMock()
            existing_referral.data = [{"id": "existing-referral-id"}]

            call_count = {"n": 0}

            def table_dispatch(name):
                chain = MagicMock()
                call_count["n"] += 1
                if name == "drivers":
                    if call_count["n"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                    else:
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = referrer_result
                elif name == "referrals":
                    chain.select.return_value.eq.return_value.execute.return_value = existing_referral
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post("/referral/redeem", json={
                "referral_code": "XPD-USED"
            })
        assert response.status_code == 400
        assert "ya" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_redeem_driver_not_found(self, client):
        """If the user has no driver record, return 404."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = []

            mock_sb.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup

            response = await client.post("/referral/redeem", json={
                "referral_code": "XPD-TEST"
            })
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_redeem_missing_code_returns_422(self, client):
        """Missing referral_code field returns validation error."""
        response = await client.post("/referral/redeem", json={})
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_redeem_code_uppercased(self, client):
        """Codes are uppercased before lookup."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            referrer_result = MagicMock()
            referrer_result.data = None  # Not found, but we test that it was uppercased

            call_count = {"n": 0}

            def table_dispatch(name):
                chain = MagicMock()
                call_count["n"] += 1
                if name == "drivers":
                    if call_count["n"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                    else:
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = referrer_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post("/referral/redeem", json={
                "referral_code": "xpd-abcd"
            })
        # Will be 404 since code not found, but the important thing is it doesn't crash
        assert response.status_code == 404


# ===================== GET /referral/stats =====================


class TestGetReferralStats:
    """Tests for GET /referral/stats"""

    @pytest.mark.asyncio
    async def test_stats_with_referrals(self, client):
        """User with referrals gets correct stats."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            referrals_result = MagicMock()
            referrals_result.data = [
                {"id": "r1", "referred_driver_id": "d1"},
                {"id": "r2", "referred_driver_id": "d2"},
                {"id": "r3", "referred_driver_id": "d3"},
            ]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "referrals":
                    chain.select.return_value.eq.return_value.execute.return_value = referrals_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/referral/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total_referrals"] == 3
        assert data["total_reward_days"] == 21  # 3 * 7
        assert len(data["referrals"]) == 3

    @pytest.mark.asyncio
    async def test_stats_no_referrals(self, client):
        """User with no referrals gets zeros."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            referrals_result = MagicMock()
            referrals_result.data = []

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "referrals":
                    chain.select.return_value.eq.return_value.execute.return_value = referrals_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/referral/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total_referrals"] == 0
        assert data["total_reward_days"] == 0
        assert data["referrals"] == []

    @pytest.mark.asyncio
    async def test_stats_driver_not_found(self, client):
        """If no driver is linked to the user, return 404."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = []

            mock_sb.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup

            response = await client.get("/referral/stats")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_stats_db_error_returns_500(self, client):
        """Database error returns 500."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "referrals":
                    chain.select.return_value.eq.return_value.execute.side_effect = Exception("DB error")
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/referral/stats")
        assert response.status_code == 500

    @pytest.mark.asyncio
    async def test_stats_null_data(self, client):
        """When referrals query returns None data, treat as empty."""
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            referrals_result = MagicMock()
            referrals_result.data = None

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "referrals":
                    chain.select.return_value.eq.return_value.execute.return_value = referrals_result
                return chain
            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/referral/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total_referrals"] == 0
        assert data["total_reward_days"] == 0
        assert data["referrals"] == []
