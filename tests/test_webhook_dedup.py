"""Tests for persistent webhook idempotency helpers.

The pre-2026-04-23 implementation used only an in-memory dict, which
reset on Railway restart. A Stripe/RevenueCat retry arriving after the
restart would be processed twice (double plan activation). These tests
guard the DB-backed fallback path.
"""

from unittest.mock import MagicMock, patch

import main


class TestWebhookDedup:
    def setup_method(self):
        main._processed_webhook_events.clear()

    def test_empty_event_id_returns_false(self):
        assert main._is_webhook_processed("", "stripe") is False

    def test_hits_memory_cache_first(self):
        main._processed_webhook_events["evt-memory"] = True
        # Even if DB returned empty, memory short-circuits.
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])
            assert main._is_webhook_processed("evt-memory", "stripe") is True
            # And DB was NOT consulted — in-memory short-circuit worked.
            mock_sb.table.assert_not_called()

    def test_falls_back_to_db_when_not_in_memory(self):
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(
                data=[{"event_id": "evt-db"}]
            )
            assert main._is_webhook_processed("evt-db", "stripe") is True
            # Populated the memory cache after finding it in DB.
            assert "evt-db" in main._processed_webhook_events

    def test_returns_false_when_event_not_seen_anywhere(self):
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])
            assert main._is_webhook_processed("evt-new", "stripe") is False

    def test_db_exception_does_not_raise(self):
        """A DB hiccup must NOT block webhook processing — it degrades
        back to in-memory behaviour."""
        with patch("main.supabase") as mock_sb:
            mock_sb.table.side_effect = Exception("DB timeout")
            # No exception raised; returns False so the event is processed.
            assert main._is_webhook_processed("evt-any", "stripe") is False

    def test_mark_writes_both_memory_and_db(self):
        with patch("main.supabase") as mock_sb:
            main._mark_webhook_processed("evt-to-mark", "revenuecat")
            assert "evt-to-mark" in main._processed_webhook_events
            mock_sb.table.assert_called_with("processed_webhooks")
            mock_sb.table.return_value.insert.assert_called_once()
            payload = mock_sb.table.return_value.insert.call_args[0][0]
            assert payload["event_id"] == "evt-to-mark"
            assert payload["provider"] == "revenuecat"

    def test_mark_insert_collision_does_not_raise(self):
        """PK collision from a parallel worker inserting the same event_id
        must be swallowed — idempotency still holds."""
        with patch("main.supabase") as mock_sb:
            mock_sb.table.return_value.insert.return_value.execute.side_effect = Exception("duplicate key")
            # Should not raise
            main._mark_webhook_processed("evt-collision", "stripe")
