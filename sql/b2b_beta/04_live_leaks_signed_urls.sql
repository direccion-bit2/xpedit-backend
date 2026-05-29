-- ============================================================================
-- B2B BETA — 04. LIVE-LEAK CLOSURE (signed URLs + code-lookup)  [supersedes 03 B/C]
-- Two public-read exposures still open in PROD as of 2026-05-29 (verified):
--   * shared_routes: policy "Anyone can read shared routes" USING(true) roles=public
--       → anon enumerates 53 rows of customer addresses/phones/notes.
--   * storage bucket proof-of-delivery: public=true + "Allow public read" roles=public
--       → anon lists/downloads every POD photo, signature, name, GPS (PII / GDPR).
--       + "Allow uploads" roles=public qual=NULL → anyone can WRITE to the bucket.
--
-- Fix shape (coordinated with code):
--   shared_routes  → SECURITY DEFINER claim-by-code fn (recipient ≠ owner, so owner-only
--                    RLS can't serve the receive flow); table reads locked to the owner.
--   POD bucket     → private + backend mints short-lived signed URLs (GET /pod/{id}/signed-urls)
--                    after authorizing via the proof's route. App+web stop using public URLs.
--
-- SECTIONS 1 & 2 are SAFE to apply now (no code dependency) — applied to STAGING first.
-- SECTION 3 (privatize bucket + drop public read) MUST be applied LAST, only AFTER the
-- app + dashboard are serving POD via signed URLs, or POD images break.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- SECTION 1 — shared_routes: claim-by-code function + lock down direct reads
-- ----------------------------------------------------------------------------

-- Atomic lookup+claim by exact code. Mirrors the app's existing receive flow
-- (load a shared route → mark it used). SECURITY DEFINER so it bypasses the
-- (now restrictive) table RLS; only returns data for an exact, valid, unclaimed
-- code, so it can't be used to enumerate the table.
CREATE OR REPLACE FUNCTION public.claim_shared_route(p_code text, p_driver_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  r public.shared_routes;
BEGIN
  SELECT * INTO r FROM public.shared_routes WHERE code = p_code LIMIT 1;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('status', 'not_found');
  END IF;
  IF r.expires_at IS NOT NULL AND r.expires_at < now() THEN
    RETURN jsonb_build_object('status', 'expired');
  END IF;
  IF r.used_by IS NOT NULL AND r.used_by <> p_driver_id THEN
    RETURN jsonb_build_object('status', 'already_used');
  END IF;
  IF r.used_by IS NULL THEN
    UPDATE public.shared_routes
       SET used_by = p_driver_id, used_at = now()
     WHERE id = r.id;
  END IF;
  RETURN jsonb_build_object(
    'status', 'ok',
    'route_data', r.route_data,
    'stops_count', r.stops_count
  );
END;
$$;

REVOKE ALL ON FUNCTION public.claim_shared_route(text, uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.claim_shared_route(text, uuid) TO authenticated;

-- Replace the public "read everything" policy with owner-only direct reads.
-- The receive flow no longer reads the table directly (it calls the function),
-- so locking SELECT to the creator breaks nothing and stops anon enumeration.
DROP POLICY IF EXISTS "Anyone can read shared routes" ON public.shared_routes;
DROP POLICY IF EXISTS "shared_routes_owner_select" ON public.shared_routes;
CREATE POLICY "shared_routes_owner_select" ON public.shared_routes
  FOR SELECT TO authenticated
  USING (created_by IN (SELECT id FROM public.drivers WHERE user_id = auth.uid()));


-- ----------------------------------------------------------------------------
-- SECTION 2 — POD bucket: stop anonymous WRITES (uploads stay, but auth-only)
-- The app uploads POD as an authenticated driver, so restricting INSERT to
-- 'authenticated' is safe and closes the "anyone can upload" hole.
-- ----------------------------------------------------------------------------
DROP POLICY IF EXISTS "Allow uploads" ON storage.objects;
DROP POLICY IF EXISTS "pod_insert_authenticated" ON storage.objects;
CREATE POLICY "pod_insert_authenticated" ON storage.objects
  FOR INSERT TO authenticated
  WITH CHECK (bucket_id = 'proof-of-delivery');


-- ----------------------------------------------------------------------------
-- SECTION 3 — ⚠️ APPLY LAST (after app+dashboard serve POD via signed URLs).
-- Privatize the bucket and remove anonymous read. Backend (service_role) keeps
-- minting signed URLs regardless of bucket visibility. Run ONLY once the signed-
-- URL code is live in staging/prod, or POD images break.
--
--   UPDATE storage.buckets SET public = false WHERE id = 'proof-of-delivery';
--   DROP POLICY IF EXISTS "Allow public read" ON storage.objects;
--   -- (No authenticated read policy needed: app/web read via backend signed URLs;
--   --  service_role bypasses RLS. Drivers never read storage directly anymore.)
-- ----------------------------------------------------------------------------
