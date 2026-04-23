-- Migration: explicit RLS policies for trial_claims
-- Date: 2026-04-23
-- Context: trial_claims had RLS enabled but NO policies. Any client-side
-- INSERT/SELECT returned { data: [], error: null } silently. The backend
-- uses service_role (bypasses RLS) so this didn't break anything in practice,
-- but the silent-drop is a footgun if any future code path writes from the
-- client. Adding explicit policies makes intent clear.
--
-- Applied to: staging → prod
-- Applied via: Supabase MCP
--
-- ROLLBACK:
--   DROP POLICY IF EXISTS trial_claims_driver_insert ON public.trial_claims;
--   DROP POLICY IF EXISTS trial_claims_driver_select ON public.trial_claims;

-- Driver can insert their own claim (driver_id matches their driver row)
DROP POLICY IF EXISTS trial_claims_driver_insert ON public.trial_claims;
CREATE POLICY trial_claims_driver_insert
ON public.trial_claims
FOR INSERT
WITH CHECK (
  driver_id IN (SELECT id FROM public.drivers WHERE user_id = auth.uid())
);

-- Driver can read their own claim (for debugging / "when did I claim?")
DROP POLICY IF EXISTS trial_claims_driver_select ON public.trial_claims;
CREATE POLICY trial_claims_driver_select
ON public.trial_claims
FOR SELECT
USING (
  driver_id IN (SELECT id FROM public.drivers WHERE user_id = auth.uid())
);
