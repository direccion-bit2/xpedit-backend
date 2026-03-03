"""
Tests for emails.py - ALL email sending functions.
Mocks resend.Emails.send to avoid real API calls.
"""

from unittest.mock import patch


class TestSendWelcomeEmail:
    """Tests for send_welcome_email"""

    def test_success(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_id_welcome"}
            from emails import send_welcome_email
            result = send_welcome_email("user@test.com", "Test User")
        assert result["success"] is True
        assert result["id"] == "test_id_welcome"

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_welcome_email
            result = send_welcome_email("user@test.com", "Test User")
        assert result["success"] is False
        assert "error" in result

    def test_empty_name(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_id_empty_name"}
            from emails import send_welcome_email
            result = send_welcome_email("user@test.com", "")
        assert result["success"] is True


class TestSendDeliveryStartedEmail:
    """Tests for send_delivery_started_email"""

    def test_success_with_all_params(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_delivery_started"}
            from emails import send_delivery_started_email
            result = send_delivery_started_email(
                "client@test.com", "Client Name", "Driver Name",
                "15 minutos", "https://track.example.com/abc"
            )
        assert result["success"] is True

    def test_success_without_optional_params(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_delivery_started_min"}
            from emails import send_delivery_started_email
            result = send_delivery_started_email(
                "client@test.com", "Client Name", "Driver Name"
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_delivery_started_email
            result = send_delivery_started_email(
                "client@test.com", "Client", "Driver"
            )
        assert result["success"] is False


class TestSendDeliveryCompletedEmail:
    """Tests for send_delivery_completed_email"""

    def test_success_with_all_params(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_completed"}
            from emails import send_delivery_completed_email
            result = send_delivery_completed_email(
                "client@test.com", "Client Name", "14:30",
                "https://photo.example.com/proof.jpg", "Recipient"
            )
        assert result["success"] is True

    def test_success_without_optional_params(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_completed_min"}
            from emails import send_delivery_completed_email
            result = send_delivery_completed_email(
                "client@test.com", "Client Name", "14:30"
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_delivery_completed_email
            result = send_delivery_completed_email(
                "client@test.com", "Client", "14:30"
            )
        assert result["success"] is False


class TestSendDeliveryFailedEmail:
    """Tests for send_delivery_failed_email"""

    def test_success_with_all_params(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_failed"}
            from emails import send_delivery_failed_email
            result = send_delivery_failed_email(
                "client@test.com", "Client Name",
                "No one home", "Tomorrow 10:00-14:00"
            )
        assert result["success"] is True

    def test_success_without_optional_params(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_failed_min"}
            from emails import send_delivery_failed_email
            result = send_delivery_failed_email(
                "client@test.com", "Client Name"
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_delivery_failed_email
            result = send_delivery_failed_email("client@test.com", "Client")
        assert result["success"] is False


class TestSendDailySummaryEmail:
    """Tests for send_daily_summary_email"""

    def test_success(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_summary"}
            from emails import send_daily_summary_email
            result = send_daily_summary_email(
                "dispatcher@test.com", "Dispatcher",
                "2026-03-01", 10, 50, 45, 5
            )
        assert result["success"] is True

    def test_zero_stats(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_summary_zero"}
            from emails import send_daily_summary_email
            result = send_daily_summary_email(
                "dispatcher@test.com", "Dispatcher",
                "2026-03-01", 0, 0, 0, 0
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_daily_summary_email
            result = send_daily_summary_email(
                "d@test.com", "D", "2026-01-01", 1, 1, 1, 0
            )
        assert result["success"] is False


class TestSendPlanActivatedEmail:
    """Tests for send_plan_activated_email"""

    def test_temporary_plan(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_plan"}
            from emails import send_plan_activated_email
            result = send_plan_activated_email(
                "user@test.com", "User Name", "Pro", days=30
            )
        assert result["success"] is True

    def test_permanent_plan(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_plan_perm"}
            from emails import send_plan_activated_email
            result = send_plan_activated_email(
                "user@test.com", "User Name", "Pro+", permanent=True
            )
        assert result["success"] is True

    def test_no_days_no_permanent(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_plan_nodur"}
            from emails import send_plan_activated_email
            result = send_plan_activated_email(
                "user@test.com", "User Name", "Pro"
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_plan_activated_email
            result = send_plan_activated_email(
                "user@test.com", "User", "Pro", days=7
            )
        assert result["success"] is False


class TestSendReferralRewardEmail:
    """Tests for send_referral_reward_email"""

    def test_success(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_referral"}
            from emails import send_referral_reward_email
            result = send_referral_reward_email(
                "referrer@test.com", "Referrer Name", "New User", 7
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_referral_reward_email
            result = send_referral_reward_email(
                "referrer@test.com", "Referrer", "New User", 7
            )
        assert result["success"] is False


class TestSendUpcomingEmail:
    """Tests for send_upcoming_email"""

    def test_success_with_tracking(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_upcoming"}
            from emails import send_upcoming_email
            result = send_upcoming_email(
                "client@test.com", "Client", "Driver", 3, "https://track.example.com"
            )
        assert result["success"] is True

    def test_success_without_tracking(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_upcoming_notrack"}
            from emails import send_upcoming_email
            result = send_upcoming_email(
                "client@test.com", "Client", "Driver", 2
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_upcoming_email
            result = send_upcoming_email(
                "client@test.com", "Client", "Driver", 1
            )
        assert result["success"] is False


class TestSendPasswordResetEmail:
    """Tests for send_password_reset_email"""

    def test_success(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_reset"}
            from emails import send_password_reset_email
            result = send_password_reset_email(
                "user@test.com", "User Name", "NewPass123!"
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_password_reset_email
            result = send_password_reset_email(
                "user@test.com", "User", "Pass123"
            )
        assert result["success"] is False


class TestSendCustomEmail:
    """Tests for send_custom_email"""

    def test_success(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_custom"}
            from emails import send_custom_email
            result = send_custom_email(
                "user@test.com", "Custom Subject", "<p>Hello</p>"
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_custom_email
            result = send_custom_email(
                "user@test.com", "Subject", "<p>Body</p>"
            )
        assert result["success"] is False


class TestSendAlertEmail:
    """Tests for send_alert_email"""

    def test_success(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_alert"}
            from emails import send_alert_email
            result = send_alert_email(
                "admin@test.com", "Server Down", "Details here"
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_alert_email
            result = send_alert_email(
                "admin@test.com", "Alert", "Details"
            )
        assert result["success"] is False


class TestSendBroadcastEmail:
    """Tests for send_broadcast_email"""

    def test_success_multiple_emails(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_broadcast"}
            from emails import send_broadcast_email
            result = send_broadcast_email(
                ["a@test.com", "b@test.com", "c@test.com"],
                "Broadcast Subject",
                "<p>Broadcast body</p>"
            )
        assert result["sent"] > 0

    def test_empty_list(self):
        with patch("emails.resend") as mock_resend:
            from emails import send_broadcast_email
            result = send_broadcast_email(
                [], "Subject", "<p>Body</p>"
            )
        assert result["sent"] == 0
        assert result["failed"] == 0

    def test_partial_failure(self):
        call_count = {"n": 0}

        def side_effect_fn(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise Exception("Partial failure")
            return {"id": f"test_{call_count['n']}"}

        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = side_effect_fn
            from emails import send_broadcast_email
            result = send_broadcast_email(
                ["a@test.com", "b@test.com", "c@test.com"],
                "Subject", "<p>Body</p>"
            )
        assert result["sent"] == 2
        assert result["failed"] == 1


class TestSendReengagementEmail:
    """Tests for send_reengagement_email"""

    def test_success(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_reengage"}
            from emails import send_reengagement_email
            result = send_reengagement_email("user@test.com", "User Name")
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_reengagement_email
            result = send_reengagement_email("user@test.com", "User")
        assert result["success"] is False


class TestSendReengagementBroadcast:
    """Tests for send_reengagement_broadcast"""

    def test_success(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_reengage_broadcast"}
            from emails import send_reengagement_broadcast
            targets = [
                {"email": "a@test.com", "name": "A"},
                {"email": "b@test.com", "name": "B"},
            ]
            result = send_reengagement_broadcast(targets)
        assert result["sent"] == 2
        assert result["failed"] == 0

    def test_empty_list(self):
        with patch("emails.resend") as mock_resend:
            from emails import send_reengagement_broadcast
            result = send_reengagement_broadcast([])
        assert result["sent"] == 0

    def test_partial_failure(self):
        call_count = {"n": 0}

        def side_effect_fn(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise Exception("Fail first")
            return {"id": f"test_{call_count['n']}"}

        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = side_effect_fn
            from emails import send_reengagement_broadcast
            targets = [
                {"email": "a@test.com", "name": "A"},
                {"email": "b@test.com", "name": "B"},
            ]
            result = send_reengagement_broadcast(targets)
        assert result["sent"] == 1
        assert result["failed"] == 1


class TestSendTrialExpiringEmail:
    """Tests for send_trial_expiring_email"""

    def test_success(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_trial_exp"}
            from emails import send_trial_expiring_email
            result = send_trial_expiring_email(
                "user@test.com", "User Name", "pro", 3
            )
        assert result["success"] is True

    def test_one_day_left(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_trial_1day"}
            from emails import send_trial_expiring_email
            result = send_trial_expiring_email(
                "user@test.com", "User Name", "pro_plus", 1
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_trial_expiring_email
            result = send_trial_expiring_email(
                "user@test.com", "User", "pro", 3
            )
        assert result["success"] is False


class TestSendTrialExpiredEmail:
    """Tests for send_trial_expired_email"""

    def test_success(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_trial_expired"}
            from emails import send_trial_expired_email
            result = send_trial_expired_email(
                "user@test.com", "User Name", "pro"
            )
        assert result["success"] is True

    def test_pro_plus_plan(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.return_value = {"id": "test_trial_expired_pp"}
            from emails import send_trial_expired_email
            result = send_trial_expired_email(
                "user@test.com", "User Name", "pro_plus"
            )
        assert result["success"] is True

    def test_error_handling(self):
        with patch("emails.resend") as mock_resend:
            mock_resend.Emails.send.side_effect = Exception("API error")
            from emails import send_trial_expired_email
            result = send_trial_expired_email(
                "user@test.com", "User", "pro"
            )
        assert result["success"] is False


class TestGetBaseTemplate:
    """Tests for get_base_template HTML generation"""

    def test_returns_html(self):
        from emails import get_base_template
        html = get_base_template("<p>Test content</p>", "Test Title")
        assert "<!DOCTYPE html>" in html
        assert "Test content" in html
        assert "Test Title" in html

    def test_default_title(self):
        from emails import get_base_template
        html = get_base_template("<p>Content</p>")
        assert "Xpedit" in html
