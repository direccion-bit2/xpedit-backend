"""Tests for check_cost_alerts cron — blocks regressions like bug #266 (€1.6-4k/mo leak).

Covers 4 thresholds:
 1) Routes V2 €/day total (>€15 amber, >€25 red)
 2) Source individual calls/h (>30 amber, >80 red) — via in-memory counter
 3) Unknown calls/day per endpoint (>20 amber, >50 red)
 4) Driver individual €/day (>€5 amber, >€10 red)

Plus anti-spam: same alert_key within 4h must NOT re-fire.
"""

import time
from unittest.mock import MagicMock, patch

import pytest


class _FixedResultChain:
    """Chainable mock returning fixed data on .execute()."""
    def __init__(self, data=None):
        self._data = data if data is not None else []
    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return self
    def __call__(self, *args, **kwargs):
        return self
    def execute(self):
        result = MagicMock()
        result.data = self._data
        return result


def _setup_counters_under_threshold():
    """Reset in-memory counters so no spurious source-hour alerts fire."""
    import main
    main._api_source_counters.clear()
    main._api_counters_started_at = time.time()  # uptime <1h so source check runs


# =============== 1) Routes V2 €/day TOTAL ===============

class TestRoutesV2EurDayThreshold:
    def test_under_amber_does_not_fire(self):
        _setup_counters_under_threshold()
        # 1500 calls × 0.0073 = €10.95 < €15 amber
        rows = [{"count": 1500}]
        mock_sb = MagicMock()
        mock_sb.table = MagicMock(return_value=_FixedResultChain(rows))
        with patch("main.supabase", mock_sb), \
             patch("main.send_alert_email") as mock_send:
            from main import check_cost_alerts
            result = check_cost_alerts()
        assert mock_send.call_count == 0
        assert all(f["metric"] != "routes_v2_eur_day" for f in result["fired"])

    def test_amber_fires_amber(self):
        _setup_counters_under_threshold()
        # 2500 calls × 0.0073 = €18.25 > €15 amber, < €25 red
        rows = [{"count": 2500}]
        mock_sb = MagicMock()
        mock_sb.table = MagicMock(return_value=_FixedResultChain(rows))
        with patch("main.supabase", mock_sb), \
             patch("main.send_alert_email") as mock_send, \
             patch("main._cost_alert_already_sent_recently", return_value=False):
            from main import check_cost_alerts
            result = check_cost_alerts()
        amber_fired = [f for f in result["fired"] if f.get("metric") == "routes_v2_eur_day"]
        assert len(amber_fired) == 1
        assert amber_fired[0]["level"] == "amber"
        # send_alert_email puede haber sido invocado para otras alarmas también
        # (el mismo mock data dispara unknown también). Verifico que entre las
        # llamadas hay al menos UNA con AMBER del routes_v2.
        all_titles = [c[0][1] for c in mock_send.call_args_list]
        assert any("AMBER" in t and "Routes V2" in t for t in all_titles), all_titles

    def test_red_fires_red(self):
        _setup_counters_under_threshold()
        # 3500 × 0.0073 = €25.55 > €25 red
        rows = [{"count": 3500}]
        mock_sb = MagicMock()
        mock_sb.table = MagicMock(return_value=_FixedResultChain(rows))
        with patch("main.supabase", mock_sb), \
             patch("main.send_alert_email") as mock_send, \
             patch("main._cost_alert_already_sent_recently", return_value=False):
            from main import check_cost_alerts
            result = check_cost_alerts()
        red_fired = [f for f in result["fired"] if f.get("metric") == "routes_v2_eur_day"]
        assert len(red_fired) == 1
        assert red_fired[0]["level"] == "red"
        assert "RED" in mock_send.call_args[0][1]


# =============== 2) Source individual calls/hour ===============

class TestSourceCallsHourThreshold:
    def test_resume_under_amber(self):
        _setup_counters_under_threshold()
        import main
        main._api_source_counters["places_directions"] = {"resume": 25}  # <30 amber
        mock_sb = MagicMock()
        mock_sb.table = MagicMock(return_value=_FixedResultChain([]))
        with patch("main.supabase", mock_sb), \
             patch("main.send_alert_email") as mock_send, \
             patch("main._cost_alert_already_sent_recently", return_value=False):
            from main import check_cost_alerts
            result = check_cost_alerts()
        source_fired = [f for f in result["fired"] if "source_" in f.get("metric", "")]
        assert source_fired == []

    def test_resume_red_threshold_fires(self):
        """Caso bug #266: si resume vuelve a dispararse >80/h, alarma roja."""
        _setup_counters_under_threshold()
        import main
        main._api_source_counters["places_directions"] = {"resume": 90}  # >80 red
        mock_sb = MagicMock()
        mock_sb.table = MagicMock(return_value=_FixedResultChain([]))
        with patch("main.supabase", mock_sb), \
             patch("main.send_alert_email") as mock_send, \
             patch("main._cost_alert_already_sent_recently", return_value=False):
            from main import check_cost_alerts
            result = check_cost_alerts()
        source_fired = [f for f in result["fired"] if "source_places_directions_resume" in f.get("metric", "")]
        assert len(source_fired) == 1
        assert source_fired[0]["level"] == "red"


# =============== 3) Unknown calls/day ===============

class TestUnknownCallsDayThreshold:
    def test_unknown_red_fires(self):
        _setup_counters_under_threshold()
        # places_directions/source='unknown' = 60 calls > 50 red
        def table_dispatch(name):
            if name == "api_source_daily":
                # Will be called twice: once for routes_v2 total (empty), once for unknown
                return _FixedResultChain([{"endpoint": "places_directions", "count": 60}])
            return _FixedResultChain([])
        mock_sb = MagicMock()
        mock_sb.table = MagicMock(side_effect=table_dispatch)
        with patch("main.supabase", mock_sb), \
             patch("main.send_alert_email") as mock_send, \
             patch("main._cost_alert_already_sent_recently", return_value=False):
            from main import check_cost_alerts
            result = check_cost_alerts()
        unk_fired = [f for f in result["fired"] if "unknown_" in f.get("metric", "")]
        assert any(f["level"] == "red" for f in unk_fired)


# =============== 4) Driver individual €/day ===============

class TestDriverEurDayThreshold:
    def test_driver_red_fires(self):
        _setup_counters_under_threshold()
        # 1500 calls × 0.0073 = €10.95 > €10 red. La columna se llama user_id
        # desde 23 may 2026 (antes mal-nombrada driver_id pese a contener
        # auth.users.id). El email resuelve nombre vía JOIN drivers.user_id.
        rows = [{"user_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "endpoint": "places_directions", "count": 1500}]
        def table_dispatch(name):
            if name == "api_source_driver_daily":
                return _FixedResultChain(rows)
            if name == "drivers":
                # JOIN devuelve el driver real con su id, email, name
                return _FixedResultChain([{
                    "id": "drv-real-uuid", "user_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                    "email": "victor@example.com", "name": "Victor Tique",
                }])
            return _FixedResultChain([])
        mock_sb = MagicMock()
        mock_sb.table = MagicMock(side_effect=table_dispatch)
        with patch("main.supabase", mock_sb), \
             patch("main.send_alert_email") as mock_send, \
             patch("main._cost_alert_already_sent_recently", return_value=False):
            from main import check_cost_alerts
            result = check_cost_alerts()
        driver_fired = [f for f in result["fired"] if "driver_eur_" in f.get("metric", "")]
        assert len(driver_fired) == 1
        assert driver_fired[0]["level"] == "red"
        assert driver_fired[0]["user_id"] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        assert driver_fired[0]["driver_id"] == "drv-real-uuid"
        assert driver_fired[0]["label"] == "Victor Tique"


# =============== Anti-spam ===============

class TestAntiSpam:
    def test_repeated_call_does_not_resend(self):
        """Si _cost_alert_already_sent_recently devuelve True, NO se envía email."""
        _setup_counters_under_threshold()
        rows = [{"count": 3500}]  # red threshold
        mock_sb = MagicMock()
        mock_sb.table = MagicMock(return_value=_FixedResultChain(rows))
        with patch("main.supabase", mock_sb), \
             patch("main.send_alert_email") as mock_send, \
             patch("main._cost_alert_already_sent_recently", return_value=True):
            from main import check_cost_alerts
            check_cost_alerts()
        mock_send.assert_not_called()
