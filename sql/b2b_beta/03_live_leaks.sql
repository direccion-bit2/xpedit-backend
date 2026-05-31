-- ============================================================================
-- B2B BETA — 03. LIVE DATA-LEAK FIXES (current-customer exposures, GDPR)
-- Three public-read exposures found in the audit. Treated with different risk:
--  A) company_invites  → SAFE to scope now (invites redeemed via backend service_role).
--  B) shared_routes     → ⚠️ DO NOT blind-drop: likely powers customer tracking links.
--                          Needs a SECURITY DEFINER code-lookup before tightening. Documented only.
--  C) proof-of-delivery → ⚠️ bucket public; making it private REQUIRES app/website to switch
--                          to signed URLs first, else POD images break. Coordinated change.
-- A) applied to STAGING 2026-05-29. B) and C) PREPARED, NOT applied (need code changes).
-- ============================================================================

-- A) company_invites: replace "read all" with company-scoped read. Redemption stays
--    backend-only (service_role). SAFE — applied to staging.
DROP POLICY IF EXISTS "invites_select" ON public.company_invites;                 -- staging name
DROP POLICY IF EXISTS "Anyone can read active invites" ON public.company_invites; -- prod name
CREATE POLICY "invites_select_company" ON public.company_invites FOR SELECT TO authenticated
USING (
  is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND company_id = get_my_company_id())
);

-- ----------------------------------------------------------------------------
-- B) shared_routes  ⚠️ NOT APPLIED — verify the customer tracking-link flow first.
--    The customer tracking page likely reads a shared route by its public CODE via the
--    anon client, relying on this `USING(true)` policy. Dropping it would break tracking.
--    SAFE FIX (do WITH a code change so the public page looks routes up via a SECURITY
--    DEFINER function by exact code, exposing only non-PII tracking fields):
--
--    DROP POLICY IF EXISTS "Anyone can read shared routes" ON public.shared_routes;
--    CREATE POLICY "shared_routes_owner_read" ON public.shared_routes FOR SELECT TO authenticated
--    USING (created_by = auth.uid());
--    -- + create get_shared_route_by_code(text) SECURITY DEFINER returning only safe columns,
--    --   and point the tracking page at it instead of a direct table read.
--
-- ----------------------------------------------------------------------------
-- C) proof-of-delivery storage bucket  ⚠️ NOT APPLIED — needs signed-URL code first.
--    Plan (apply SQL ONLY after app+website serve POD via signed URLs):
--
--    UPDATE storage.buckets SET public = false WHERE id = 'proof-of-delivery';
--    DROP POLICY IF EXISTS "Allow public read" ON storage.objects;
--    CREATE POLICY "pod_read_owner_or_company" ON storage.objects FOR SELECT TO authenticated
--    USING (
--      bucket_id = 'proof-of-delivery' AND (
--        is_platform_admin()
--        -- owner driver, or same-company dispatcher; path convention: <driver_id>/<route_id>/...
--      )
--    );
--    -- App/website must call supabase.storage.from('proof-of-delivery').createSignedUrl(path, ttl)
--    -- instead of getPublicUrl(). Code change tracked on the feature branches.
