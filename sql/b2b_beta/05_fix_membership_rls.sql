-- ============================================================================
-- B2B BETA — 05. FIX RLS de MEMBRESÍA (auth.uid() vs drivers.id)
-- Bug encontrado en el gap-analysis (29 may): las policies SELECT de
-- company_driver_links / companies / company_subscriptions filtraban
-- `driver_id = auth.uid()`, pero `driver_id` es FK a `drivers.id` (≠ auth.users.id,
-- el clásico nuestro [[feedback_auth_user_id_vs_driver_id]]). Resultado: un
-- conductor NO podía leer su propia empresa/suscripción vía PostgREST, y la
-- validación "A no ve B" sólo se había probado con el dispatcher (no con driver).
-- Fix: mapear por drivers.user_id + permitir operador (get_my_company_id) y
-- platform admin. APLICADO a STAGING 2026-05-29. Pendiente prod (Camino B).
-- ============================================================================

DROP POLICY IF EXISTS "cdl_select_own" ON public.company_driver_links;
CREATE POLICY "cdl_select_own" ON public.company_driver_links FOR SELECT TO authenticated
USING (
  is_platform_admin()
  OR driver_id IN (SELECT id FROM public.drivers WHERE user_id = auth.uid())
  OR company_id = get_my_company_id()
);

DROP POLICY IF EXISTS "companies_select_member" ON public.companies;
CREATE POLICY "companies_select_member" ON public.companies FOR SELECT TO authenticated
USING (
  is_platform_admin()
  OR id = get_my_company_id()
  OR id IN (
    SELECT company_id FROM public.company_driver_links
    WHERE driver_id IN (SELECT id FROM public.drivers WHERE user_id = auth.uid())
  )
);

DROP POLICY IF EXISTS "csub_select_member" ON public.company_subscriptions;
CREATE POLICY "csub_select_member" ON public.company_subscriptions FOR SELECT TO authenticated
USING (
  is_platform_admin()
  OR company_id = get_my_company_id()
  OR company_id IN (
    SELECT company_id FROM public.company_driver_links
    WHERE driver_id IN (SELECT id FROM public.drivers WHERE user_id = auth.uid())
  )
);
