-- Migration: server-side validation of stops.completed_at
-- Date: 2026-04-23
-- Context: completed_at was set by the client (new Date().toISOString()),
-- which is subject to clock skew and manipulation. This trigger accepts
-- reasonable client-provided values (±5 min future, up to 7d past) and
-- falls back to NOW() otherwise. When status reverts to 'pending' it
-- clears completed_at.
--
-- Applied to: staging → prod (after verify)
-- Applied via: Supabase MCP apply_migration
--
-- ROLLBACK:
--   DROP TRIGGER IF EXISTS trg_validate_completed_at ON public.stops;
--   DROP FUNCTION IF EXISTS public.validate_completed_at() CASCADE;

CREATE OR REPLACE FUNCTION public.validate_completed_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.status IS NOT DISTINCT FROM NEW.status THEN
    RETURN NEW;
  END IF;

  IF NEW.status IN ('completed', 'failed') THEN
    -- Accept client timestamp only if plausible. Otherwise trust the server.
    IF NEW.completed_at IS NULL
       OR NEW.completed_at > NOW() + INTERVAL '5 minutes'  -- future clock skew
       OR NEW.completed_at < NOW() - INTERVAL '7 days'     -- unreasonably old (likely stale offline queue)
    THEN
      NEW.completed_at := NOW();
    END IF;
  ELSIF NEW.status = 'pending' THEN
    NEW.completed_at := NULL;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_validate_completed_at ON public.stops;
CREATE TRIGGER trg_validate_completed_at
BEFORE UPDATE OF status ON public.stops
FOR EACH ROW
EXECUTE FUNCTION public.validate_completed_at();
