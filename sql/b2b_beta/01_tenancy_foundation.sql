-- ============================================================================
-- B2B BETA — 01. TENANCY FOUNDATION (helpers, column, trigger, indexes, backfill)
-- Multi-tenant isolation foundation for Modo Empresa.
-- Applied to STAGING (ppxbmrzpogxtntsozggb) on 2026-05-29. NOT yet applied to PROD.
-- Safe to run on PROD later (idempotent): adds a column, hardens helpers, adds a
-- trigger + indexes, backfills routes.company_id. Does NOT change RLS (see 02_).
-- ============================================================================

-- 1. Platform super-admin marker. Only Miguel/Inés get true. Company owners must
--    NOT be global 'admin' anymore — tenant scope is enforced via this flag.
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS is_platform_admin boolean NOT NULL DEFAULT false;

-- 2. Harden helper functions: add SET search_path (advisor: function_search_path_mutable)
--    and add is_platform_admin(). Logic unchanged for the existing two.
CREATE OR REPLACE FUNCTION public.get_my_role()
  RETURNS text LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp
AS $$ SELECT role FROM public.users WHERE id = auth.uid(); $$;

CREATE OR REPLACE FUNCTION public.get_my_company_id()
  RETURNS uuid LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp
AS $$ SELECT company_id FROM public.users WHERE id = auth.uid(); $$;

CREATE OR REPLACE FUNCTION public.is_platform_admin()
  RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp
AS $$ SELECT COALESCE((SELECT is_platform_admin FROM public.users WHERE id = auth.uid()), false); $$;

-- 3. Indexes that the new company-scoped RLS will lean on.
CREATE INDEX IF NOT EXISTS idx_routes_company_id ON public.routes(company_id);
CREATE INDEX IF NOT EXISTS idx_drivers_company_id ON public.drivers(company_id);

-- 4. Trigger: when a route is created WITHOUT company_id but WITH a driver, derive
--    it from the driver's company. This is the ONLY way app-created routes (the RN
--    native insert omits company_id and can't be fixed via OTA) become tenant-visible.
--    Dispatcher/backend-created routes pass company_id explicitly and are untouched.
CREATE OR REPLACE FUNCTION public.set_route_company_id()
  RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp
AS $$
BEGIN
  IF NEW.company_id IS NULL AND NEW.driver_id IS NOT NULL THEN
    NEW.company_id := (SELECT company_id FROM public.drivers WHERE id = NEW.driver_id);
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_set_route_company_id ON public.routes;
CREATE TRIGGER trg_set_route_company_id
  BEFORE INSERT ON public.routes
  FOR EACH ROW EXECUTE FUNCTION public.set_route_company_id();

-- 5. Backfill existing routes.company_id from the driver's company (company drivers only;
--    single drivers have driver.company_id NULL → route stays NULL → single-driver path unaffected).
UPDATE public.routes r
   SET company_id = d.company_id
  FROM public.drivers d
 WHERE r.driver_id = d.id
   AND r.company_id IS NULL
   AND d.company_id IS NOT NULL;
