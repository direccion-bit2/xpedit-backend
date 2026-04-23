-- Migration: audit log for stops.status changes
-- Date: 2026-04-23
-- Context: April 2026 silent sync bug left 93% of stops in pending state
-- while the app showed them completed. To detect regressions and support
-- driver disputes, every status change now leaves an immutable trail.
--
-- Applied to: staging → prod (after verify)
-- Applied via: Supabase MCP apply_migration
--
-- ROLLBACK:
--   DROP TRIGGER IF EXISTS trg_log_stop_status ON public.stops;
--   DROP FUNCTION IF EXISTS public.log_stop_status_change() CASCADE;
--   DROP TABLE IF EXISTS public.stop_status_events CASCADE;

CREATE TABLE IF NOT EXISTS public.stop_status_events (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  stop_id    UUID NOT NULL REFERENCES public.stops(id) ON DELETE CASCADE,
  driver_id  UUID NOT NULL REFERENCES public.drivers(id) ON DELETE CASCADE,
  route_id   UUID NOT NULL REFERENCES public.routes(id) ON DELETE CASCADE,
  old_status TEXT,
  new_status TEXT NOT NULL,
  source     TEXT NOT NULL DEFAULT 'db_trigger',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sse_stop      ON public.stop_status_events (stop_id, created_at);
CREATE INDEX IF NOT EXISTS idx_sse_driver    ON public.stop_status_events (driver_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sse_route     ON public.stop_status_events (route_id, created_at);

ALTER TABLE public.stop_status_events ENABLE ROW LEVEL SECURITY;

-- Driver can only read their own events. Writes are done by trigger under
-- SECURITY DEFINER so RLS doesn't block the INSERT.
DROP POLICY IF EXISTS stop_status_events_driver_read ON public.stop_status_events;
CREATE POLICY stop_status_events_driver_read
ON public.stop_status_events
FOR SELECT
USING (driver_id = (SELECT id FROM public.drivers WHERE user_id = auth.uid()));

-- Trigger function: runs under SECURITY DEFINER so the INSERT into
-- stop_status_events bypasses RLS. Idempotent: only fires when status
-- actually changes.
CREATE OR REPLACE FUNCTION public.log_stop_status_change()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_driver_id UUID;
BEGIN
  IF OLD.status IS NOT DISTINCT FROM NEW.status THEN
    RETURN NEW;
  END IF;

  SELECT driver_id INTO v_driver_id FROM public.routes WHERE id = NEW.route_id;
  IF v_driver_id IS NULL THEN
    RETURN NEW;
  END IF;

  INSERT INTO public.stop_status_events (stop_id, driver_id, route_id, old_status, new_status, source)
  VALUES (NEW.id, v_driver_id, NEW.route_id, OLD.status, NEW.status, 'db_trigger');

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_log_stop_status ON public.stops;
CREATE TRIGGER trg_log_stop_status
AFTER UPDATE OF status ON public.stops
FOR EACH ROW
EXECUTE FUNCTION public.log_stop_status_change();
