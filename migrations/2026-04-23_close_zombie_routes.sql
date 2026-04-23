-- Migration: retroactive closure of zombie in_progress routes
-- Date: 2026-04-23
-- Context: April 2026 silent sync bug left many routes as "in_progress"
-- forever in the driver's history. This one-shot cleanup closes all
-- in_progress routes that have had no activity for >48h. We keep the
-- signal of why each was closed via a new nullable column.
--
-- Applied to: staging → prod
-- Applied via: Supabase MCP
--
-- Result: 90 routes updated in prod (45 with stop activity + 45 without).
-- pending routes (190 older than 2 days) left untouched — those are
-- drafts that were never started; user-deletable from the app.
--
-- ROLLBACK:
--   -- DATA rollback: impossible to restore perfectly because closure_reason
--   -- is how we know which routes we touched. To revert:
--   UPDATE public.routes
--   SET status = 'in_progress', closure_reason = NULL
--   WHERE closure_reason IN ('auto_closed_sync_bug_has_activity', 'auto_closed_inactive_no_activity');
--
--   -- SCHEMA rollback:
--   ALTER TABLE public.routes DROP COLUMN IF EXISTS closure_reason;

-- 1. Add closure_reason column (additive; NULL = driver closed normally)
ALTER TABLE public.routes
ADD COLUMN IF NOT EXISTS closure_reason TEXT;

COMMENT ON COLUMN public.routes.closure_reason IS
'Set when a route is closed programmatically (cron or manual backfill). Values: auto_closed_sync_bug_has_activity | auto_closed_inactive_no_activity | auto_closed_by_cron. NULL when the driver closed it naturally via the app.';

-- 2. Close zombies with stop activity (45 in prod). Likely sync bug victims.
UPDATE public.routes
SET status = 'completed', closure_reason = 'auto_closed_sync_bug_has_activity'
WHERE status = 'in_progress'
  AND created_at < NOW() - INTERVAL '2 days'
  AND EXISTS (SELECT 1 FROM stops s WHERE s.route_id = routes.id AND s.status IN ('completed','failed'));

-- 3. Close zombies with no activity (45 in prod). Probably started but never used.
UPDATE public.routes
SET status = 'completed', closure_reason = 'auto_closed_inactive_no_activity'
WHERE status = 'in_progress'
  AND created_at < NOW() - INTERVAL '2 days'
  AND NOT EXISTS (SELECT 1 FROM stops s WHERE s.route_id = routes.id AND s.status IN ('completed','failed'));

-- Verification query (not part of the migration):
-- SELECT status, COUNT(*) FROM routes WHERE created_at < NOW() - INTERVAL '2 days' AND status IN ('pending','in_progress') GROUP BY status;
-- expected: only 'pending' rows remain.
