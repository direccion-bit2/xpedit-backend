"""
Tests for background/cron job functions in main.py:
  - backup_critical_tables (daily backup to Supabase Storage)
  - run_retention_cleanup (weekly data cleanup via Supabase RPC)
  - send_weekly_reengagement_push (Monday push to inactive drivers)
  - check_expiring_trials (daily trial expiry warnings)
  - degrade_expired_trials (daily downgrade expired trials to Free)
  - periodic_health_check (5-min health check, reports to Sentry)
  - monitor_website_health (15-min website ping, alert email)
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import make_mock_supabase


class _FixedResultChain:
    """A chainable mock where .execute() ALWAYS returns the same fixed result,
    regardless of how many chained methods are called before it.
    Every attribute access and every call returns self."""

    def __init__(self, data=None, count=0):
        self._data = data if data is not None else []
        self._count = count

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return self

    def __call__(self, *args, **kwargs):
        return self

    def execute(self):
        result = MagicMock()
        result.data = self._data
        result.count = self._count
        return result


class _ErrorChain:
    """A chainable mock where .execute() raises an exception."""

    def __init__(self, exc=None):
        self._exc = exc or Exception("DB error")

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return self

    def __call__(self, *args, **kwargs):
        return self

    def execute(self):
        raise self._exc


# ===================== BACKUP CRITICAL TABLES =====================

class TestBackupCriticalTables:
    """Tests for backup_critical_tables cron job."""

    @pytest.mark.asyncio
    async def test_backup_success(self):
        mock_sb = make_mock_supabase()

        def table_dispatch(name):
            return _FixedResultChain(data=[{"id": "1", "name": "test"}], count=1)

        mock_sb.table = MagicMock(side_effect=table_dispatch)

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", ""):
            from main import backup_critical_tables
            await backup_critical_tables()

        mock_sb.storage.from_.assert_called_with("backups")

    @pytest.mark.asyncio
    async def test_backup_with_sentry(self):
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain())

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry:
            from main import backup_critical_tables
            await backup_critical_tables()

        assert mock_sentry.capture_check_in.call_count >= 2

    @pytest.mark.asyncio
    async def test_backup_table_error_continues(self):
        """Even if one table fails, backup continues for others."""
        mock_sb = make_mock_supabase()

        def table_dispatch(name):
            if name == "routes":
                return _ErrorChain(Exception("DB error"))
            return _FixedResultChain()

        mock_sb.table = MagicMock(side_effect=table_dispatch)

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", ""):
            from main import backup_critical_tables
            await backup_critical_tables()

        mock_sb.storage.from_.assert_called_with("backups")

    @pytest.mark.asyncio
    async def test_backup_upload_error(self):
        """If storage upload fails, sentry reports error."""
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain())

        bucket = MagicMock()
        bucket.upload = MagicMock(side_effect=Exception("Upload failed"))
        mock_sb.storage.from_ = MagicMock(return_value=bucket)

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry:
            from main import backup_critical_tables
            await backup_critical_tables()

        mock_sentry.capture_exception.assert_called_once()


# ===================== RUN RETENTION CLEANUP =====================

class TestRunRetentionCleanup:
    """Tests for run_retention_cleanup cron job."""

    @pytest.mark.asyncio
    async def test_cleanup_success(self):
        mock_response = AsyncMock()
        mock_response.status_code = 200

        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("main.SENTRY_DSN", ""), \
             patch("main.SUPABASE_SERVICE_KEY", "fake-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            from main import run_retention_cleanup
            await run_retention_cleanup()

        assert mock_http.post.call_count == 2

    @pytest.mark.asyncio
    async def test_cleanup_with_sentry(self):
        mock_response = AsyncMock()
        mock_response.status_code = 200

        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry, \
             patch("main.SUPABASE_SERVICE_KEY", "fake-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            from main import run_retention_cleanup
            await run_retention_cleanup()

        assert mock_sentry.capture_check_in.call_count >= 2

    @pytest.mark.asyncio
    async def test_cleanup_error(self):
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(side_effect=Exception("Connection refused"))
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry, \
             patch("main.SUPABASE_SERVICE_KEY", "fake-key"), \
             patch("main.httpx.AsyncClient", return_value=mock_http):
            from main import run_retention_cleanup
            await run_retention_cleanup()

        mock_sentry.capture_exception.assert_called_once()


# ===================== SEND WEEKLY REENGAGEMENT PUSH =====================

class TestSendWeeklyReengagementPush:
    """Tests for send_weekly_reengagement_push cron job."""

    @pytest.mark.asyncio
    async def test_push_to_inactive_drivers(self):
        mock_sb = make_mock_supabase()
        call_count = {"n": 0}

        def table_dispatch(name):
            call_count["n"] += 1
            if name == "drivers":
                return _FixedResultChain(data=[
                    {"id": "d1", "name": "Driver 1", "push_token": "ExponentPushToken[abc]"},
                    {"id": "d2", "name": "Driver 2", "push_token": "ExponentPushToken[def]"},
                ])
            elif name == "routes":
                return _FixedResultChain(data=[{"driver_id": "d1"}])
            return _FixedResultChain()

        mock_sb.table = MagicMock(side_effect=table_dispatch)

        with patch("main.supabase", mock_sb), \
             patch("main.send_push_to_token", new_callable=AsyncMock, return_value=True) as mock_push:
            from main import send_weekly_reengagement_push
            await send_weekly_reengagement_push()

        # Only d2 is inactive, so push should be sent once
        mock_push.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_recent_drivers(self):
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain())

        with patch("main.supabase", mock_sb), \
             patch("main.send_push_to_token", new_callable=AsyncMock) as mock_push:
            from main import send_weekly_reengagement_push
            await send_weekly_reengagement_push()

        mock_push.assert_not_called()

    @pytest.mark.asyncio
    async def test_all_drivers_have_routes(self):
        mock_sb = make_mock_supabase()
        call_count = {"n": 0}

        def table_dispatch(name):
            call_count["n"] += 1
            if name == "drivers":
                return _FixedResultChain(data=[
                    {"id": "d1", "name": "Driver 1", "push_token": "ExponentPushToken[abc]"},
                ])
            elif name == "routes":
                return _FixedResultChain(data=[{"driver_id": "d1"}])
            return _FixedResultChain()

        mock_sb.table = MagicMock(side_effect=table_dispatch)

        with patch("main.supabase", mock_sb), \
             patch("main.send_push_to_token", new_callable=AsyncMock) as mock_push:
            from main import send_weekly_reengagement_push
            await send_weekly_reengagement_push()

        mock_push.assert_not_called()

    @pytest.mark.asyncio
    async def test_push_error_handled(self):
        mock_sb = make_mock_supabase()

        def table_dispatch(name):
            if name == "drivers":
                return _ErrorChain(Exception("DB error"))
            return _FixedResultChain()

        mock_sb.table = MagicMock(side_effect=table_dispatch)

        with patch("main.supabase", mock_sb):
            from main import send_weekly_reengagement_push
            # Should not raise
            await send_weekly_reengagement_push()


# ===================== CHECK EXPIRING TRIALS =====================

class TestCheckExpiringTrials:
    """Tests for check_expiring_trials cron job."""

    @pytest.mark.asyncio
    async def test_sends_email_for_expiring_trials(self):
        mock_sb = make_mock_supabase()
        expires_at = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()

        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain(data=[
            {
                "id": "driver-not-excluded",
                "email": "user@test.com",
                "name": "Test User",
                "promo_plan": "pro",
                "promo_plan_expires_at": expires_at,
            }
        ]))

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", ""), \
             patch("main.send_trial_expiring_email", return_value={"success": True}) as mock_email:
            from main import check_expiring_trials
            await check_expiring_trials()

        mock_email.assert_called_once()

    @pytest.mark.asyncio
    async def test_excludes_admin_and_test_ids(self):
        mock_sb = make_mock_supabase()
        expires_at = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()

        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain(data=[
            {
                "id": "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b",  # admin
                "email": "admin@xpedit.es",
                "name": "Admin",
                "promo_plan": "pro",
                "promo_plan_expires_at": expires_at,
            },
            {
                "id": "e481de53-bb8c-4b76-8b56-04a7d00f9c6f",  # test
                "email": "test@xpedit.es",
                "name": "Test",
                "promo_plan": "pro",
                "promo_plan_expires_at": expires_at,
            },
        ]))

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", ""), \
             patch("main.send_trial_expiring_email", return_value={"success": True}) as mock_email:
            from main import check_expiring_trials
            await check_expiring_trials()

        mock_email.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_expiring_trials(self):
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain())

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", ""), \
             patch("main.send_trial_expiring_email") as mock_email:
            from main import check_expiring_trials
            await check_expiring_trials()

        mock_email.assert_not_called()

    @pytest.mark.asyncio
    async def test_email_failure_counted(self):
        mock_sb = make_mock_supabase()
        expires_at = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()

        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain(data=[
            {
                "id": "driver-x",
                "email": "user@test.com",
                "name": "User",
                "promo_plan": "pro",
                "promo_plan_expires_at": expires_at,
            }
        ]))

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", ""), \
             patch("main.send_trial_expiring_email", return_value={"success": False, "error": "fail"}) as mock_email:
            from main import check_expiring_trials
            await check_expiring_trials()

        mock_email.assert_called_once()

    @pytest.mark.asyncio
    async def test_driver_without_email_skipped(self):
        mock_sb = make_mock_supabase()
        expires_at = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()

        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain(data=[
            {
                "id": "driver-no-email",
                "email": None,
                "name": "NoEmail",
                "promo_plan": "pro",
                "promo_plan_expires_at": expires_at,
            }
        ]))

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", ""), \
             patch("main.send_trial_expiring_email", return_value={"success": True}) as mock_email:
            from main import check_expiring_trials
            await check_expiring_trials()

        # No email because email is None
        mock_email.assert_not_called()

    @pytest.mark.asyncio
    async def test_db_error_with_sentry(self):
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _ErrorChain(Exception("DB failure")))

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry:
            from main import check_expiring_trials
            await check_expiring_trials()

        mock_sentry.capture_exception.assert_called_once()


# ===================== DEGRADE EXPIRED TRIALS =====================

class TestDegradeExpiredTrials:
    """Tests for degrade_expired_trials cron job."""

    @pytest.mark.asyncio
    async def test_downgrades_expired_trial(self):
        mock_sb = make_mock_supabase()
        call_count = {"n": 0}

        def table_dispatch(name):
            call_count["n"] += 1
            if name == "drivers" and call_count["n"] == 1:
                return _FixedResultChain(data=[
                    {
                        "id": "driver-expired",
                        "email": "user@test.com",
                        "name": "User",
                        "promo_plan": "pro",
                        "promo_plan_expires_at": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
                    }
                ])
            return _FixedResultChain()

        mock_sb.table = MagicMock(side_effect=table_dispatch)

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", ""), \
             patch("main.send_trial_expired_email", return_value={"success": True}) as mock_email:
            from main import degrade_expired_trials
            await degrade_expired_trials()

        mock_email.assert_called_once_with("user@test.com", "User", "pro")

    @pytest.mark.asyncio
    async def test_excludes_admin_ids(self):
        mock_sb = make_mock_supabase()

        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain(data=[
            {
                "id": "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b",  # excluded
                "email": "admin@xpedit.es",
                "name": "Admin",
                "promo_plan": "pro",
                "promo_plan_expires_at": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
            }
        ]))

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", ""), \
             patch("main.send_trial_expired_email") as mock_email:
            from main import degrade_expired_trials
            await degrade_expired_trials()

        mock_email.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_expired_trials(self):
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain())

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", ""), \
             patch("main.send_trial_expired_email") as mock_email:
            from main import degrade_expired_trials
            await degrade_expired_trials()

        mock_email.assert_not_called()

    @pytest.mark.asyncio
    async def test_driver_without_email_skips_notification(self):
        mock_sb = make_mock_supabase()
        call_count = {"n": 0}

        def table_dispatch(name):
            call_count["n"] += 1
            if name == "drivers" and call_count["n"] == 1:
                return _FixedResultChain(data=[
                    {
                        "id": "driver-no-email",
                        "email": None,
                        "name": "NoEmail",
                        "promo_plan": "pro_plus",
                        "promo_plan_expires_at": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
                    }
                ])
            return _FixedResultChain()

        mock_sb.table = MagicMock(side_effect=table_dispatch)

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", ""), \
             patch("main.send_trial_expired_email") as mock_email:
            from main import degrade_expired_trials
            await degrade_expired_trials()

        mock_email.assert_not_called()

    @pytest.mark.asyncio
    async def test_db_error_with_sentry(self):
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _ErrorChain(Exception("DB failure")))

        with patch("main.supabase", mock_sb), \
             patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry:
            from main import degrade_expired_trials
            await degrade_expired_trials()

        mock_sentry.capture_exception.assert_called_once()


# ===================== PERIODIC HEALTH CHECK =====================

class TestPeriodicHealthCheck:
    """Tests for periodic_health_check cron job."""

    @pytest.mark.asyncio
    async def test_healthy_status(self):
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain(count=5))

        mock_scheduler = MagicMock()
        mock_scheduler.running = True

        with patch("main.supabase", mock_sb), \
             patch("main.social_scheduler", mock_scheduler), \
             patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry:
            from main import periodic_health_check
            await periodic_health_check()

        mock_sentry.capture_check_in.assert_called_with(
            monitor_slug="backend-health-check", status="ok"
        )

    @pytest.mark.asyncio
    async def test_degraded_db(self):
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain(count=None))

        mock_scheduler = MagicMock()
        mock_scheduler.running = True

        with patch("main.supabase", mock_sb), \
             patch("main.social_scheduler", mock_scheduler), \
             patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry:
            from main import periodic_health_check
            await periodic_health_check()

        mock_sentry.capture_check_in.assert_called_with(
            monitor_slug="backend-health-check", status="error"
        )

    @pytest.mark.asyncio
    async def test_scheduler_not_running(self):
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain(count=5))

        mock_scheduler = MagicMock()
        mock_scheduler.running = False

        with patch("main.supabase", mock_sb), \
             patch("main.social_scheduler", mock_scheduler), \
             patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry:
            from main import periodic_health_check
            await periodic_health_check()

        mock_sentry.capture_check_in.assert_called_with(
            monitor_slug="backend-health-check", status="error"
        )

    @pytest.mark.asyncio
    async def test_db_exception(self):
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _ErrorChain(Exception("DB down")))

        with patch("main.supabase", mock_sb), \
             patch("main.social_scheduler", MagicMock(running=True)), \
             patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry:
            from main import periodic_health_check
            await periodic_health_check()

        mock_sentry.capture_exception.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_sentry(self):
        """When SENTRY_DSN is empty, health check still runs without error."""
        mock_sb = make_mock_supabase()
        mock_sb.table = MagicMock(side_effect=lambda name: _FixedResultChain(count=3))

        mock_scheduler = MagicMock()
        mock_scheduler.running = True

        with patch("main.supabase", mock_sb), \
             patch("main.social_scheduler", mock_scheduler), \
             patch("main.SENTRY_DSN", ""):
            from main import periodic_health_check
            await periodic_health_check()


# ===================== MONITOR WEBSITE HEALTH =====================

class TestMonitorWebsiteHealth:
    """Tests for monitor_website_health cron job."""

    @pytest.mark.asyncio
    async def test_website_healthy(self):
        mock_response = AsyncMock()
        mock_response.status_code = 200

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("main.httpx.AsyncClient", return_value=mock_http), \
             patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry:
            from main import monitor_website_health
            await monitor_website_health()

        mock_sentry.capture_check_in.assert_called_with(
            monitor_slug="website-health-monitor", status="ok"
        )

    @pytest.mark.asyncio
    async def test_website_degraded_sends_alert(self):
        mock_response = AsyncMock()
        mock_response.status_code = 500
        mock_response.json = MagicMock(return_value={"error": "internal"})
        mock_response.text = "Internal Server Error"

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("main.httpx.AsyncClient", return_value=mock_http), \
             patch("main.SENTRY_DSN", ""), \
             patch("main._last_website_alert", None), \
             patch("main.send_alert_email", return_value={"success": True}) as mock_alert:
            from main import monitor_website_health
            await monitor_website_health()

        mock_alert.assert_called_once()

    @pytest.mark.asyncio
    async def test_website_degraded_respects_cooldown(self):
        mock_response = AsyncMock()
        mock_response.status_code = 500
        mock_response.json = MagicMock(return_value={"error": "internal"})
        mock_response.text = "Internal Server Error"

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        recent = datetime.now(timezone.utc) - timedelta(minutes=30)

        with patch("main.httpx.AsyncClient", return_value=mock_http), \
             patch("main.SENTRY_DSN", ""), \
             patch("main._last_website_alert", recent), \
             patch("main.send_alert_email", return_value={"success": True}) as mock_alert:
            from main import monitor_website_health
            await monitor_website_health()

        mock_alert.assert_not_called()

    @pytest.mark.asyncio
    async def test_website_connection_error(self):
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=Exception("Connection refused"))
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("main.httpx.AsyncClient", return_value=mock_http), \
             patch("main.SENTRY_DSN", "https://sentry.io/fake"), \
             patch("main.sentry_sdk") as mock_sentry, \
             patch("main._last_website_alert", None), \
             patch("main.send_alert_email", return_value={"success": True}) as mock_alert:
            from main import monitor_website_health
            await monitor_website_health()

        mock_sentry.capture_exception.assert_called_once()
        mock_alert.assert_called_once()

    @pytest.mark.asyncio
    async def test_website_degraded_json_parse_error(self):
        """When response body is not valid JSON, falls back to raw text."""
        mock_response = AsyncMock()
        mock_response.status_code = 502
        mock_response.json = MagicMock(side_effect=Exception("Not JSON"))
        mock_response.text = "Bad Gateway"

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_response)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("main.httpx.AsyncClient", return_value=mock_http), \
             patch("main.SENTRY_DSN", ""), \
             patch("main._last_website_alert", None), \
             patch("main.send_alert_email", return_value={"success": True}) as mock_alert:
            from main import monitor_website_health
            await monitor_website_health()

        mock_alert.assert_called_once()
