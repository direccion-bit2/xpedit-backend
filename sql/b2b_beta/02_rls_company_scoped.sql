-- ============================================================================
-- B2B BETA — 02. COMPANY-SCOPED RLS REWRITE
-- Replaces the GLOBAL role bypass `get_my_role() IN ('admin','dispatcher')` (which
-- let ANY dispatcher/admin read+write EVERY tenant's data) with:
--    is_platform_admin()                         -- only Miguel/Inés (super-admin)
--  OR (role IN ('dispatcher','company_admin')    -- a company operator
--      AND <row belongs to caller's company>)
-- The single-driver path (driver_id IN own-drivers / own routes) is PRESERVED
-- verbatim in every policy, so the 1044 single drivers are unaffected.
-- Applied to STAGING (ppxbmrzpogxtntsozggb) 2026-05-29. NOT applied to PROD.
-- PREREQ for PROD: run 01_, then mark platform admins:
--    UPDATE users SET is_platform_admin=true WHERE email IN ('direccion@taespack.com','inesmpatrao@gmail.com');
-- ============================================================================

-- ROUTES -------------------------------------------------------------------
DROP POLICY IF EXISTS "Authenticated read routes" ON public.routes;
CREATE POLICY "Authenticated read routes" ON public.routes FOR SELECT TO public
USING (
  deleted_at IS NULL AND (
    driver_id IN (SELECT id FROM drivers WHERE user_id = auth.uid())
    OR is_platform_admin()
    OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND company_id = get_my_company_id())
  )
);

DROP POLICY IF EXISTS "Dispatchers can create routes" ON public.routes;
DROP POLICY IF EXISTS "Users insert routes" ON public.routes;
CREATE POLICY "Insert routes (own or company)" ON public.routes FOR INSERT TO authenticated
WITH CHECK (
  driver_id IN (SELECT id FROM drivers WHERE user_id = auth.uid())
  OR is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND company_id = get_my_company_id())
);

DROP POLICY IF EXISTS "Authenticated update routes" ON public.routes;
CREATE POLICY "Authenticated update routes" ON public.routes FOR UPDATE TO authenticated
USING (
  driver_id IN (SELECT id FROM drivers WHERE user_id = auth.uid())
  OR is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND company_id = get_my_company_id())
)
WITH CHECK (
  driver_id IN (SELECT id FROM drivers WHERE user_id = auth.uid())
  OR is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND company_id = get_my_company_id())
);

-- STOPS (scoped via the parent route's company) ----------------------------
DROP POLICY IF EXISTS "Users can view own stops" ON public.stops;
CREATE POLICY "Users can view own stops" ON public.stops FOR SELECT TO public
USING (
  deleted_at IS NULL AND (
    route_id IN (SELECT r.id FROM routes r JOIN drivers d ON r.driver_id = d.id WHERE d.user_id = (SELECT auth.uid()))
    OR is_platform_admin()
    OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND route_id IN (SELECT id FROM routes WHERE company_id = get_my_company_id()))
  )
);

DROP POLICY IF EXISTS "Users can insert own stops" ON public.stops;
CREATE POLICY "Users can insert own stops" ON public.stops FOR INSERT TO public
WITH CHECK (
  can_insert_stop(route_id)
  OR is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND route_id IN (SELECT id FROM routes WHERE company_id = get_my_company_id()))
);

DROP POLICY IF EXISTS "Users can update own stops" ON public.stops;
CREATE POLICY "Users can update own stops" ON public.stops FOR UPDATE TO public
USING (
  check_stop_ownership(route_id)
  OR is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND route_id IN (SELECT id FROM routes WHERE company_id = get_my_company_id()))
)
WITH CHECK (
  check_stop_ownership(route_id)
  OR is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND route_id IN (SELECT id FROM routes WHERE company_id = get_my_company_id()))
);

-- DRIVERS ------------------------------------------------------------------
DROP POLICY IF EXISTS "Authenticated read drivers" ON public.drivers;
CREATE POLICY "Authenticated read drivers" ON public.drivers FOR SELECT TO authenticated
USING (
  user_id = auth.uid()
  OR is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND company_id = get_my_company_id())
);
-- "Update own driver" policy left unchanged (own-only; dispatcher driver mgmt goes via backend/service_role).

-- DELIVERY_PROOFS (scoped via the driver's company) ------------------------
DROP POLICY IF EXISTS "Drivers can view own proofs" ON public.delivery_proofs;
CREATE POLICY "Drivers can view own proofs" ON public.delivery_proofs FOR SELECT TO public
USING (
  driver_id IN (SELECT id FROM drivers WHERE user_id = auth.uid())
  OR is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND driver_id IN (SELECT id FROM drivers WHERE company_id = get_my_company_id()))
);

DROP POLICY IF EXISTS "Drivers can insert proofs" ON public.delivery_proofs;
CREATE POLICY "Drivers can insert proofs" ON public.delivery_proofs FOR INSERT TO public
WITH CHECK (
  driver_id IN (SELECT id FROM drivers WHERE user_id = auth.uid())
  OR is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND driver_id IN (SELECT id FROM drivers WHERE company_id = get_my_company_id()))
);

-- LOCATION_HISTORY ---------------------------------------------------------
DROP POLICY IF EXISTS "Users can view own location history" ON public.location_history;
CREATE POLICY "Users can view own location history" ON public.location_history FOR SELECT TO public
USING (
  driver_id IN (SELECT id FROM drivers WHERE user_id = auth.uid())
  OR is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND driver_id IN (SELECT id FROM drivers WHERE company_id = get_my_company_id()))
);
-- INSERT policy left unchanged (own-only).

-- CUSTOMER_NOTIFICATIONS (add company-scoped dispatcher read) ---------------
DROP POLICY IF EXISTS "custnot_select_own" ON public.customer_notifications;
CREATE POLICY "custnot_select_own" ON public.customer_notifications FOR SELECT TO authenticated
USING (
  driver_id = auth.uid()
  OR is_platform_admin()
  OR (get_my_role() = ANY (ARRAY['dispatcher','company_admin']) AND route_id IN (SELECT id FROM routes WHERE company_id = get_my_company_id()))
);
-- INSERT policy (custnot_insert_own) left unchanged.
