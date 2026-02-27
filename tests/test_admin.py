"""
Tests for admin endpoints:
  - GET /admin/users
  - PATCH /admin/users/{user_id}/grant
  - GET /admin/promo-codes
  - POST /admin/promo-codes
  - PATCH /admin/promo-codes/{code_id}
  - POST /admin/users/{user_id}/reset-password
  - POST /admin/companies
  - GET /admin/stats
  - GET /admin/companies (list)
  - Admin-only access control
"""

from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import FAKE_USER_ID


class TestAdminAccessControl:
    """Admin endpoints should reject non-admin users."""

    @pytest.mark.asyncio
    async def test_admin_users_requires_admin(self, client):
        """Regular driver should get 403 on admin endpoints."""
        response = await client.get("/admin/users")
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_promo_codes_requires_admin(self, client):
        """Regular driver should get 403 on promo codes listing."""
        response = await client.get("/admin/promo-codes")
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_grant_plan_requires_admin(self, client):
        """Regular driver should get 403 on grant plan."""
        response = await client.patch(
            f"/admin/users/{FAKE_USER_ID}/grant",
            json={"plan": "pro", "days": 30}
        )
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_broadcast_requires_admin(self, client):
        """Regular driver should get 403 on broadcast email."""
        response = await client.post("/admin/broadcast-email", json={
            "subject": "Test",
            "body": "<p>Test</p>",
            "target": "all"
        })
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_admin_create_company_requires_admin(self, client):
        """Regular driver should get 403 on create company."""
        response = await client.post("/admin/companies", json={
            "name": "Test Company"
        })
        assert response.status_code == 403


class TestAdminUsers:
    """Tests for GET /admin/users"""

    @pytest.mark.asyncio
    async def test_list_users_success(self, admin_client):
        """Admin should see all users/drivers."""
        with patch("main.supabase") as mock_sb:
            users_result = MagicMock()
            users_result.data = [
                {"id": "d1", "name": "Driver 1", "email": "d1@test.com", "promo_plan": None},
                {"id": "d2", "name": "Driver 2", "email": "d2@test.com", "promo_plan": "pro"},
            ]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.order.return_value.execute.return_value = users_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.get("/admin/users")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["users"]) == 2


class TestAdminGrantPlan:
    """Tests for PATCH /admin/users/{user_id}/grant"""

    @pytest.mark.asyncio
    async def test_grant_pro_plan(self, admin_client):
        """Admin should be able to grant Pro plan with days."""
        with patch("main.supabase") as mock_sb:
            update_result = MagicMock()
            update_result.data = [{"id": "d1", "promo_plan": "pro"}]

            driver_data = MagicMock()
            driver_data.data = {"email": "driver@test.com", "name": "Test Driver"}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.update.return_value.eq.return_value.execute.return_value = update_result
                    chain.select.return_value.eq.return_value.single.return_value.execute.return_value = driver_data
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            with patch("main.send_plan_activated_email", return_value={"success": True}):
                response = await admin_client.patch("/admin/users/d1/grant", json={
                    "plan": "pro",
                    "days": 30,
                })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["plan"] == "pro"
        assert data["days"] == 30
        assert data["expires_at"] is not None

    @pytest.mark.asyncio
    async def test_grant_permanent_plan(self, admin_client):
        """Admin should be able to grant permanent plan."""
        with patch("main.supabase") as mock_sb:
            update_result = MagicMock()
            update_result.data = [{"id": "d1", "promo_plan": "pro_plus"}]

            driver_data = MagicMock()
            driver_data.data = {"email": "driver@test.com", "name": "Test Driver"}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.update.return_value.eq.return_value.execute.return_value = update_result
                    chain.select.return_value.eq.return_value.single.return_value.execute.return_value = driver_data
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            with patch("main.send_plan_activated_email", return_value={"success": True}):
                response = await admin_client.patch("/admin/users/d1/grant", json={
                    "plan": "pro_plus",
                    "permanent": True,
                })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["permanent"] is True
        assert data["expires_at"] is None

    @pytest.mark.asyncio
    async def test_revoke_plan_set_free(self, admin_client):
        """Admin should be able to set a user back to free."""
        with patch("main.supabase") as mock_sb:
            update_result = MagicMock()
            update_result.data = [{"id": "d1", "promo_plan": None}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.update.return_value.eq.return_value.execute.return_value = update_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.patch("/admin/users/d1/grant", json={
                "plan": "free",
            })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["plan"] == "free"

    @pytest.mark.asyncio
    async def test_grant_temporary_requires_positive_days(self, admin_client):
        """Temporary plan with days <= 0 should return 400."""
        with patch("main.supabase"):
            response = await admin_client.patch("/admin/users/d1/grant", json={
                "plan": "pro",
                "days": 0,
            })

        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_grant_user_not_found(self, admin_client):
        """Granting plan to non-existent user should return 404."""
        with patch("main.supabase") as mock_sb:
            update_result = MagicMock()
            update_result.data = []  # No rows updated

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.update.return_value.eq.return_value.execute.return_value = update_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.patch("/admin/users/nonexistent/grant", json={
                "plan": "pro",
                "days": 30,
            })

        assert response.status_code == 404


class TestAdminPromoCodes:
    """Tests for promo code admin endpoints"""

    @pytest.mark.asyncio
    async def test_list_promo_codes(self, admin_client):
        """Admin should see all promo codes."""
        with patch("main.supabase") as mock_sb:
            codes_result = MagicMock()
            codes_result.data = [
                {"id": "pc1", "code": "TEST10", "active": True, "current_uses": 5},
                {"id": "pc2", "code": "PROMO20", "active": False, "current_uses": 20},
            ]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "promo_codes":
                    chain.select.return_value.order.return_value.execute.return_value = codes_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.get("/admin/promo-codes")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["promo_codes"]) == 2

    @pytest.mark.asyncio
    async def test_create_promo_code(self, admin_client):
        """Admin should be able to create a new promo code."""
        with patch("main.supabase") as mock_sb:
            # Check existing (no duplicate)
            existing_result = MagicMock()
            existing_result.data = []

            # Insert result
            insert_result = MagicMock()
            insert_result.data = [{
                "id": "new-pc",
                "code": "NEWCODE",
                "active": True,
                "benefit_plan": "pro_plus",
                "benefit_value": 30,
            }]

            call_count = {"promo_codes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "promo_codes":
                    call_count["promo_codes"] += 1
                    if call_count["promo_codes"] == 1:
                        # Check existing
                        chain.select.return_value.eq.return_value.execute.return_value = existing_result
                    else:
                        # Insert
                        chain.insert.return_value.execute.return_value = insert_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.post("/admin/promo-codes", json={
                "code": "NEWCODE",
                "benefit_value": 30,
                "benefit_plan": "pro_plus",
            })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["promo_code"]["code"] == "NEWCODE"

    @pytest.mark.asyncio
    async def test_create_duplicate_promo_code(self, admin_client):
        """Creating a duplicate promo code should return 400."""
        with patch("main.supabase") as mock_sb:
            existing_result = MagicMock()
            existing_result.data = [{"id": "existing-pc"}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "promo_codes":
                    chain.select.return_value.eq.return_value.execute.return_value = existing_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.post("/admin/promo-codes", json={
                "code": "EXISTING",
                "benefit_value": 30,
            })

        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_update_promo_code(self, admin_client):
        """Admin should be able to deactivate a promo code."""
        with patch("main.supabase") as mock_sb:
            update_result = MagicMock()
            update_result.data = [{"id": "pc1", "active": False}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "promo_codes":
                    chain.update.return_value.eq.return_value.execute.return_value = update_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.patch("/admin/promo-codes/pc1", json={
                "active": False,
            })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    @pytest.mark.asyncio
    async def test_update_promo_code_no_fields(self, admin_client):
        """Updating with no fields should return 400."""
        with patch("main.supabase"):
            response = await admin_client.patch("/admin/promo-codes/pc1", json={})

        assert response.status_code == 400


class TestAdminResetPassword:
    """Tests for POST /admin/users/{user_id}/reset-password"""

    @pytest.mark.asyncio
    async def test_reset_password_auto_generate(self, admin_client):
        """Reset password without providing one should auto-generate."""
        with patch("main.supabase") as mock_sb:
            mock_sb.auth.admin.update_user_by_id.return_value = MagicMock()

            driver_result = MagicMock()
            driver_result.data = [{"id": "d1"}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.execute.return_value = driver_result
                    chain.update.return_value.eq.return_value.execute.return_value = MagicMock()
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.post(
                f"/admin/users/{FAKE_USER_ID}/reset-password",
                json={"password": "AutoTest2026x"}
            )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    @pytest.mark.asyncio
    async def test_reset_password_custom(self, admin_client):
        """Reset with a custom password should use the provided password."""
        with patch("main.supabase") as mock_sb:
            mock_sb.auth.admin.update_user_by_id.return_value = MagicMock()

            driver_result = MagicMock()
            driver_result.data = [{"id": "d1"}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.execute.return_value = driver_result
                    chain.update.return_value.eq.return_value.execute.return_value = MagicMock()
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.post(
                f"/admin/users/{FAKE_USER_ID}/reset-password",
                json={"password": "NewPass123!"}
            )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "password" not in data

    @pytest.mark.asyncio
    async def test_reset_password_too_short(self, admin_client):
        """Password shorter than 8 chars should be rejected."""
        with patch("main.supabase") as mock_sb:
            mock_sb.auth.admin.update_user_by_id.return_value = MagicMock()

            response = await admin_client.post(
                f"/admin/users/{FAKE_USER_ID}/reset-password",
                json={"password": "Ab1"}
            )

        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_reset_password_no_uppercase(self, admin_client):
        """Password without uppercase should be rejected."""
        with patch("main.supabase") as mock_sb:
            mock_sb.auth.admin.update_user_by_id.return_value = MagicMock()

            response = await admin_client.post(
                f"/admin/users/{FAKE_USER_ID}/reset-password",
                json={"password": "alllowercase123"}
            )

        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_reset_password_no_digit(self, admin_client):
        """Password without digits should be rejected."""
        with patch("main.supabase") as mock_sb:
            mock_sb.auth.admin.update_user_by_id.return_value = MagicMock()

            response = await admin_client.post(
                f"/admin/users/{FAKE_USER_ID}/reset-password",
                json={"password": "NoDigitsHere!"}
            )

        assert response.status_code == 400


class TestAdminCreateCompany:
    """Tests for POST /admin/companies"""

    @pytest.mark.asyncio
    async def test_create_company_success(self, admin_client):
        """Admin should be able to create a company."""
        with patch("main.supabase") as mock_sb:
            company_result = MagicMock()
            company_result.data = [{
                "id": "company-1",
                "name": "Test Company",
                "active": True,
            }]

            sub_result = MagicMock()
            sub_result.data = [{"id": "sub-1"}]

            call_count = {"companies": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "companies":
                    chain.insert.return_value.execute.return_value = company_result
                elif name == "company_subscriptions":
                    chain.insert.return_value.execute.return_value = sub_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.post("/admin/companies", json={
                "name": "Test Company",
                "email": "company@test.com",
            })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["company"]["name"] == "Test Company"


class TestAdminStats:
    """Tests for GET /admin/stats"""

    @pytest.mark.asyncio
    async def test_stats_success(self, admin_client):
        """Admin should get global stats."""
        with patch("main.supabase") as mock_sb:
            # Build mock results for each query
            drivers_result = MagicMock()
            drivers_result.count = 50
            drivers_result.data = []

            routes_today_result = MagicMock()
            routes_today_result.count = 5
            routes_today_result.data = [
                {"driver_id": "d1"}, {"driver_id": "d2"}, {"driver_id": "d1"},
            ]

            routes_week_result = MagicMock()
            routes_week_result.count = 20
            routes_week_result.data = [
                {"driver_id": "d1"}, {"driver_id": "d2"}, {"driver_id": "d3"},
            ]

            new_month_result = MagicMock()
            new_month_result.count = 8
            new_month_result.data = []

            routes_total_result = MagicMock()
            routes_total_result.count = 200
            routes_total_result.data = []

            routes_month_result = MagicMock()
            routes_month_result.count = 40
            routes_month_result.data = []

            stops_total_result = MagicMock()
            stops_total_result.count = 1000
            stops_total_result.data = []

            stops_today_result = MagicMock()
            stops_today_result.count = 15
            stops_today_result.data = []

            stops_week_result = MagicMock()
            stops_week_result.count = 80
            stops_week_result.data = []

            stops_month_result = MagicMock()
            stops_month_result.count = 300
            stops_month_result.data = []

            failed_today_result = MagicMock()
            failed_today_result.count = 2
            failed_today_result.data = []

            failed_week_result = MagicMock()
            failed_week_result.count = 5
            failed_week_result.data = []

            call_index = {"drivers": 0, "routes": 0, "stops": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    call_index["drivers"] += 1
                    if call_index["drivers"] == 1:
                        chain.select.return_value.execute.return_value = drivers_result
                    else:
                        chain.select.return_value.gte.return_value.execute.return_value = new_month_result
                elif name == "routes":
                    call_index["routes"] += 1
                    if call_index["routes"] == 1:
                        chain.select.return_value.gte.return_value.execute.return_value = routes_today_result
                    elif call_index["routes"] == 2:
                        chain.select.return_value.gte.return_value.execute.return_value = routes_week_result
                    elif call_index["routes"] == 3:
                        chain.select.return_value.execute.return_value = routes_total_result
                    else:
                        chain.select.return_value.gte.return_value.execute.return_value = routes_month_result
                elif name == "stops":
                    call_index["stops"] += 1
                    if call_index["stops"] == 1:
                        chain.select.return_value.eq.return_value.execute.return_value = stops_total_result
                    elif call_index["stops"] == 2:
                        chain.select.return_value.eq.return_value.gte.return_value.execute.return_value = stops_today_result
                    elif call_index["stops"] == 3:
                        chain.select.return_value.eq.return_value.gte.return_value.execute.return_value = stops_week_result
                    elif call_index["stops"] == 4:
                        chain.select.return_value.eq.return_value.gte.return_value.execute.return_value = stops_month_result
                    elif call_index["stops"] == 5:
                        chain.select.return_value.eq.return_value.gte.return_value.execute.return_value = failed_today_result
                    else:
                        chain.select.return_value.eq.return_value.gte.return_value.execute.return_value = failed_week_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.get("/admin/stats")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        stats = data["stats"]
        assert "users" in stats
        assert "routes" in stats
        assert "deliveries" in stats
        assert "failed" in stats
        assert stats["users"]["total"] == 50
        assert stats["users"]["active_today"] == 2  # d1, d2 unique
        assert stats["users"]["active_week"] == 3  # d1, d2, d3 unique

    @pytest.mark.asyncio
    async def test_stats_requires_admin(self, client):
        """Regular driver should get 403 on stats."""
        response = await client.get("/admin/stats")
        assert response.status_code == 403


class TestAdminListCompanies:
    """Tests for GET /admin/companies (list)"""

    @pytest.mark.asyncio
    async def test_list_companies_success(self, admin_client):
        """Admin should see all companies with driver count and subscription."""
        with patch("main.supabase") as mock_sb:
            companies_result = MagicMock()
            companies_result.data = [
                {"id": "c1", "name": "Company A", "active": True},
                {"id": "c2", "name": "Company B", "active": False},
            ]

            links_c1 = MagicMock()
            links_c1.count = 5
            links_c1.data = []

            links_c2 = MagicMock()
            links_c2.count = 2
            links_c2.data = []

            sub_c1 = MagicMock()
            sub_c1.data = [{"id": "s1", "plan": "pro", "status": "active"}]

            sub_c2 = MagicMock()
            sub_c2.data = []

            call_index = {"links": 0, "subs": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "companies":
                    chain.select.return_value.order.return_value.execute.return_value = companies_result
                elif name == "company_driver_links":
                    call_index["links"] += 1
                    if call_index["links"] == 1:
                        chain.select.return_value.eq.return_value.execute.return_value = links_c1
                    else:
                        chain.select.return_value.eq.return_value.execute.return_value = links_c2
                elif name == "company_subscriptions":
                    call_index["subs"] += 1
                    if call_index["subs"] == 1:
                        chain.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = sub_c1
                    else:
                        chain.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = sub_c2
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await admin_client.get("/admin/companies")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["companies"]) == 2
        assert data["companies"][0]["driver_count"] == 5
        assert data["companies"][0]["subscription"]["plan"] == "pro"
        assert data["companies"][1]["driver_count"] == 2
        assert data["companies"][1]["subscription"] is None

    @pytest.mark.asyncio
    async def test_list_companies_requires_admin(self, client):
        """Regular driver should get 403 on list companies."""
        response = await client.get("/admin/companies")
        assert response.status_code == 403
