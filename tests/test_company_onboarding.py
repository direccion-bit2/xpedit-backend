"""Tests for B2B beta onboarding: max_drivers enforcement + onboarding emails.

Cubre las tres piezas de la beta de empresa:
 1) _company_seat_status — resolución de asientos usados/máx + valores por defecto.
 2) POST /company/join — rechaza (403) si la empresa ya está llena.
 3) POST /company/drivers — rechaza (403) si está llena SIN crear cuenta auth huérfana.
 4) send_welcome_company_email / send_driver_invite_email — HTML, escaping (XSS), éxito/error.
 5) POST /company/invites — manda el email solo si se aporta email válido (no-fatal).
"""

from unittest.mock import MagicMock, patch

import pytest

# =============== 1) _company_seat_status ===============

def _seat_mock(max_drivers, link_count=None, link_data=None):
    """Construye un mock de supabase para _company_seat_status."""
    def dispatch(name):
        m = MagicMock()
        if name == "company_subscriptions":
            data = [] if max_drivers is _NO_SUB else [{"max_drivers": max_drivers}]
            m.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = \
                MagicMock(data=data)
        elif name == "company_driver_links":
            m.select.return_value.eq.return_value.eq.return_value.execute.return_value = \
                MagicMock(count=link_count, data=link_data if link_data is not None else [])
        return m
    sb = MagicMock()
    sb.table = MagicMock(side_effect=dispatch)
    return sb


_NO_SUB = object()  # sentinel: no subscription row


class TestCompanySeatStatus:
    def test_returns_used_and_max(self):
        sb = _seat_mock(max_drivers=15, link_count=7)
        with patch("main.supabase", sb):
            from main import _company_seat_status
            used, mx = _company_seat_status("c1")
        assert used == 7
        assert mx == 15

    def test_null_max_defaults_to_15(self):
        sb = _seat_mock(max_drivers=None, link_count=3)
        with patch("main.supabase", sb):
            from main import DEFAULT_COMPANY_MAX_DRIVERS, _company_seat_status
            used, mx = _company_seat_status("c1")
        assert used == 3
        assert mx == DEFAULT_COMPANY_MAX_DRIVERS

    def test_no_subscription_row_defaults(self):
        sb = _seat_mock(max_drivers=_NO_SUB, link_count=0)
        with patch("main.supabase", sb):
            from main import DEFAULT_COMPANY_MAX_DRIVERS, _company_seat_status
            used, mx = _company_seat_status("c1")
        assert used == 0
        assert mx == DEFAULT_COMPANY_MAX_DRIVERS

    def test_zero_or_negative_max_defaults(self):
        sb = _seat_mock(max_drivers=0, link_count=1)
        with patch("main.supabase", sb):
            from main import DEFAULT_COMPANY_MAX_DRIVERS, _company_seat_status
            _used, mx = _company_seat_status("c1")
        assert mx == DEFAULT_COMPANY_MAX_DRIVERS

    def test_count_none_falls_back_to_len_data(self):
        sb = _seat_mock(max_drivers=10, link_count=None, link_data=[{"id": "a"}, {"id": "b"}])
        with patch("main.supabase", sb):
            from main import _company_seat_status
            used, mx = _company_seat_status("c1")
        assert used == 2
        assert mx == 10


# =============== 2) POST /company/join enforcement ===============

def _make_invite(company_id="company-1"):
    return {
        "id": "invite-1", "code": "JOIN10", "company_id": company_id,
        "active": True, "max_uses": None, "current_uses": 0, "expires_at": None,
        "role": "driver",
    }


class TestJoinSeatEnforcement:
    @pytest.mark.asyncio
    async def test_join_rejected_when_company_full(self, client):
        """Empresa con 2 asientos y 2 ocupados → el 3º conductor recibe 403."""
        with patch("main.supabase") as mock_sb:
            def dispatch(name):
                m = MagicMock()
                if name == "company_invites":
                    m.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[_make_invite()])
                elif name == "users":
                    m.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
                        data={"id": "u1", "company_id": None}
                    )
                elif name == "company_subscriptions":
                    m.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = \
                        MagicMock(data=[{"max_drivers": 2}])
                elif name == "company_driver_links":
                    m.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(count=2, data=[])
                return m
            mock_sb.table.side_effect = dispatch
            response = await client.post("/company/join", json={"code": "JOIN10", "user_id": "u1"})
        assert response.status_code == 403
        assert "límite" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_join_full_does_not_create_link(self, client):
        """Cuando está llena NO debe insertarse el link ni mutar al usuario."""
        link_insert = MagicMock()
        with patch("main.supabase") as mock_sb:
            def dispatch(name):
                m = MagicMock()
                if name == "company_invites":
                    m.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[_make_invite()])
                elif name == "users":
                    m.select.return_value.eq.return_value.single.return_value.execute.return_value = MagicMock(
                        data={"id": "u1", "company_id": None}
                    )
                elif name == "company_subscriptions":
                    m.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = \
                        MagicMock(data=[{"max_drivers": 1}])
                elif name == "company_driver_links":
                    m.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(count=1, data=[])
                    m.insert = link_insert
                return m
            mock_sb.table.side_effect = dispatch
            response = await client.post("/company/join", json={"code": "JOIN10", "user_id": "u1"})
        assert response.status_code == 403
        link_insert.assert_not_called()


# =============== 3) POST /company/drivers enforcement ===============

class TestCreateDriverSeatEnforcement:
    @pytest.mark.asyncio
    async def test_create_driver_rejected_when_full_no_auth_user(self, admin_client):
        """Empresa llena → 403 y NUNCA se llama a auth.admin.create_user
        (si no, quedaría una cuenta huérfana en auth.users)."""
        with patch("main.supabase") as mock_sb:
            def dispatch(name):
                m = MagicMock()
                if name == "company_subscriptions":
                    m.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = \
                        MagicMock(data=[{"max_drivers": 2}])
                elif name == "company_driver_links":
                    m.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(count=2, data=[])
                return m
            mock_sb.table.side_effect = dispatch
            response = await admin_client.post("/company/drivers", json={
                "company_id": "company-1", "email": "new@drv.com",
                "full_name": "New Driver", "password": "Secret123",
            })
        assert response.status_code == 403
        mock_sb.auth.admin.create_user.assert_not_called()


# =============== 4) Onboarding email functions ===============

class TestWelcomeCompanyEmail:
    def test_success_and_content(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "welcome_co"}
            from emails import send_welcome_company_email
            result = send_welcome_company_email("owner@acme.com", "ACME Logística", "Miguel")
            html = mock_resend.Emails.send.call_args[0][0]["html"]
        assert result["success"] is True
        assert "ACME Logística" in html
        assert "7 días" in html
        assert "/empresa/panel" in html

    def test_escapes_html_in_company_name(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "x"}
            from emails import send_welcome_company_email
            send_welcome_company_email("o@a.com", "<script>alert(1)</script>", None)
            html = mock_resend.Emails.send.call_args[0][0]["html"]
        assert "<script>alert(1)</script>" not in html
        assert "&lt;script&gt;" in html

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API down")
            from emails import send_welcome_company_email
            result = send_welcome_company_email("o@a.com", "ACME", None)
        assert result["success"] is False
        assert "error" in result


class TestDriverInviteEmail:
    def test_success_contains_code(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "inv_email"}
            from emails import send_driver_invite_email
            result = send_driver_invite_email("drv@x.com", "XPD-AB12", "ACME", "Pepe", "driver")
            payload = mock_resend.Emails.send.call_args[0][0]
        assert result["success"] is True
        assert "XPD-AB12" in payload["html"]
        assert "ACME" in payload["html"]
        assert "ACME" in payload["subject"]

    def test_escapes_code_and_company(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "x"}
            from emails import send_driver_invite_email
            send_driver_invite_email("d@x.com", "XPD-AB12", "<b>Evil</b>", "<i>Name</i>", "driver")
            html = mock_resend.Emails.send.call_args[0][0]["html"]
        assert "<b>Evil</b>" not in html
        assert "<i>Name</i>" not in html

    def test_role_label_dispatcher(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "x"}
            from emails import send_driver_invite_email
            send_driver_invite_email("d@x.com", "XPD-CD34", "ACME", None, "dispatcher")
            html = mock_resend.Emails.send.call_args[0][0]["html"]
        assert "coordinador" in html

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("boom")
            from emails import send_driver_invite_email
            result = send_driver_invite_email("d@x.com", "XPD-CD34", "ACME", None, "driver")
        assert result["success"] is False


# =============== 5) POST /company/invites sends email ===============

def _invites_dispatch(invite_data=None):
    def dispatch(name):
        m = MagicMock()
        if name == "company_invites":
            # code uniqueness check returns empty (first code is unique)
            m.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
            m.insert.return_value.execute.return_value = MagicMock(
                data=[invite_data or {"id": "inv1", "code": "XPD-AB12", "company_id": "company-1"}]
            )
        elif name == "companies":
            m.select.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(
                data=[{"name": "ACME"}]
            )
        return m
    return dispatch


class TestInviteEmailWiring:
    @pytest.mark.asyncio
    async def test_email_sent_when_email_provided(self, admin_client):
        with patch("main.supabase") as mock_sb, \
             patch("main.send_driver_invite_email", return_value={"success": True}) as mock_email:
            mock_sb.table.side_effect = _invites_dispatch()
            response = await admin_client.post("/company/invites", json={
                "company_id": "company-1", "role": "driver", "email": "drv@x.com", "name": "Pepe",
            })
        assert response.status_code == 200
        assert response.json()["email_sent"] is True
        mock_email.assert_called_once()
        # company_name resolved from companies table, recipient is the provided email
        args = mock_email.call_args[0]
        assert args[0] == "drv@x.com"
        assert args[2] == "ACME"

    @pytest.mark.asyncio
    async def test_no_email_when_not_provided(self, admin_client):
        with patch("main.supabase") as mock_sb, \
             patch("main.send_driver_invite_email") as mock_email:
            mock_sb.table.side_effect = _invites_dispatch()
            response = await admin_client.post("/company/invites", json={
                "company_id": "company-1", "role": "driver",
            })
        assert response.status_code == 200
        assert response.json()["email_sent"] is None
        mock_email.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_email_not_sent(self, admin_client):
        with patch("main.supabase") as mock_sb, \
             patch("main.send_driver_invite_email") as mock_email:
            mock_sb.table.side_effect = _invites_dispatch()
            response = await admin_client.post("/company/invites", json={
                "company_id": "company-1", "role": "driver", "email": "not-an-email",
            })
        assert response.status_code == 200
        assert response.json()["email_sent"] is None
        mock_email.assert_not_called()

    @pytest.mark.asyncio
    async def test_email_failure_is_non_fatal(self, admin_client):
        """Si el envío del email peta, el invite igual se crea (200)."""
        with patch("main.supabase") as mock_sb, \
             patch("main.send_driver_invite_email", side_effect=Exception("resend down")):
            mock_sb.table.side_effect = _invites_dispatch()
            response = await admin_client.post("/company/invites", json={
                "company_id": "company-1", "role": "driver", "email": "drv@x.com",
            })
        assert response.status_code == 200
        assert response.json()["success"] is True
        assert response.json()["email_sent"] is False
