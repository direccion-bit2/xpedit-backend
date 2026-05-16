-- =============================================================================
-- Dispatcher isolation smoke test — staging only
-- =============================================================================
-- Purpose: validate that #263 (useAuth + filtros company_id) actually isolates
-- a dispatcher's view to ONE company, and that no peripheral query
-- (loadAnalytics, handleExport) leaks data from a sibling company.
--
-- Two companies are created on purpose:
--   A — the "own" company. The dispatcher logs in here and should see ALL of A.
--   B — the "control" company. None of B's drivers/routes/stops/proofs/notifs
--       should EVER appear in the dispatcher's dashboard, analytics, or export.
--
-- Run order:
--   1. Create the auth user manually first — see "STEP 1" below. SQL cannot
--      INSERT into auth.users safely (password hashing, identities table, etc.).
--      Use the Supabase Dashboard or `auth.admin.createUser` from a script.
--   2. Paste the resulting auth user UUID into the variable at the top.
--   3. Run this whole file in the staging SQL editor.
--   4. Log in to the dashboard at xpedit.es with the dispatcher email and
--      verify isolation. See "STEP 4" at the bottom for the queries to confirm.
--   5. When done, run the CLEANUP block at the very bottom.
--
-- Project: ppxbmrzpogxtntsozggb (staging) — do NOT run in prod.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- STEP 1 (manual, in Supabase Dashboard staging → Authentication → Users):
--   Create user with:
--     email:    dispatcher.test@xpedit.es
--     password: Dispatcher2026
--     auto-confirm: yes
--   Copy the resulting UUID and paste it below as DISPATCHER_AUTH_ID.
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- STEP 2 — fixed UUIDs (deterministic, makes cleanup trivial)
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  -- ⚠️ PASTE the auth.users.id of dispatcher.test@xpedit.es here:
  dispatcher_auth_id  uuid := '00000000-0000-0000-0000-000000000000';

  company_a_id        uuid := 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  company_b_id        uuid := 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';

  driver_a1_id        uuid := 'a1111111-1111-1111-1111-111111111111';
  driver_a2_id        uuid := 'a2222222-2222-2222-2222-222222222222';
  driver_b1_id        uuid := 'b1111111-1111-1111-1111-111111111111';

  route_a1_id         uuid := 'a1aa1aa1-1aa1-1aa1-1aa1-1aa11aa11aa1';
  route_a2_id         uuid := 'a2aa2aa2-2aa2-2aa2-2aa2-2aa22aa22aa2';
  route_b1_id         uuid := 'b1bb1bb1-1bb1-1bb1-1bb1-1bb11bb11bb1';
BEGIN
  IF dispatcher_auth_id = '00000000-0000-0000-0000-000000000000'::uuid THEN
    RAISE EXCEPTION
      'Paste the auth.users.id of dispatcher.test@xpedit.es into dispatcher_auth_id before running';
  END IF;

  -- ---------- companies ----------
  INSERT INTO companies (id, name, email, phone, address, owner_id, payment_model, active)
  VALUES
    (company_a_id, 'Test Empresa A (own)',     'a@xpedit-test.local', '+34 600 000 001', 'Calle A 1', dispatcher_auth_id, 'company_pays', true),
    (company_b_id, 'Test Empresa B (control)', 'b@xpedit-test.local', '+34 600 000 002', 'Calle B 1', NULL,               'company_pays', true)
  ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

  -- ---------- public.users — promote auth user to dispatcher of company A ----------
  INSERT INTO public.users (id, email, full_name, role, company_id, phone)
  VALUES (dispatcher_auth_id, 'dispatcher.test@xpedit.es', 'Dispatcher Test', 'dispatcher', company_a_id, '+34 600 100 100')
  ON CONFLICT (id) DO UPDATE
    SET role       = 'dispatcher',
        company_id = company_a_id,
        full_name  = 'Dispatcher Test';

  -- ---------- drivers ----------
  -- Two drivers on A (the dispatcher should see both).
  INSERT INTO drivers (id, name, email, phone, active, company_id, country)
  VALUES
    (driver_a1_id, 'Driver A1', 'a1@xpedit-test.local', '+34 611 000 001', true, company_a_id, 'ES'),
    (driver_a2_id, 'Driver A2', 'a2@xpedit-test.local', '+34 611 000 002', true, company_a_id, 'ES')
  ON CONFLICT (id) DO UPDATE
    SET company_id = EXCLUDED.company_id, active = true;

  -- One driver on B (the dispatcher should NEVER see this).
  INSERT INTO drivers (id, name, email, phone, active, company_id, country)
  VALUES
    (driver_b1_id, 'Driver B1 (control)', 'b1@xpedit-test.local', '+34 611 000 003', true, company_b_id, 'ES')
  ON CONFLICT (id) DO UPDATE
    SET company_id = EXCLUDED.company_id, active = true;

  -- ---------- routes ----------
  INSERT INTO routes (id, name, date, driver_id, status, company_id, total_stops, total_distance_km)
  VALUES
    (route_a1_id, 'Ruta A1 — hoy',     CURRENT_DATE,             driver_a1_id, 'in_progress', company_a_id, 3, 12.5),
    (route_a2_id, 'Ruta A2 — ayer',    CURRENT_DATE - INTERVAL '1 day', driver_a2_id, 'completed',   company_a_id, 2,  8.0),
    (route_b1_id, 'Ruta B1 (control)', CURRENT_DATE,             driver_b1_id, 'in_progress', company_b_id, 2,  5.0)
  ON CONFLICT (id) DO UPDATE
    SET company_id = EXCLUDED.company_id,
        driver_id  = EXCLUDED.driver_id,
        deleted_at = NULL;

  -- ---------- stops ----------
  -- Wipe and reinsert deterministically so re-runs stay clean.
  DELETE FROM stops WHERE route_id IN (route_a1_id, route_a2_id, route_b1_id);

  INSERT INTO stops (route_id, position, address, lat, lng, status, completed_at, package_id)
  VALUES
    -- Ruta A1: 1 completed, 1 failed, 1 pending
    (route_a1_id, 0, 'Calle Real 1, Sanlúcar',     36.7782, -6.3556, 'completed', now() - INTERVAL '2 hours', 1),
    (route_a1_id, 1, 'Calle Real 5, Sanlúcar',     36.7790, -6.3550, 'failed',    now() - INTERVAL '1 hour',  2),
    (route_a1_id, 2, 'Calle Ancha 12, Sanlúcar',   36.7800, -6.3540, 'pending',   NULL,                       3),
    -- Ruta A2: 2 completed
    (route_a2_id, 0, 'Av. de la Marina 8, Cádiz',  36.5298, -6.2924, 'completed', now() - INTERVAL '1 day',   1),
    (route_a2_id, 1, 'Av. de la Marina 22, Cádiz', 36.5310, -6.2900, 'completed', now() - INTERVAL '23 hours', 2),
    -- Ruta B1 (control): debe quedar invisible al dispatcher A
    (route_b1_id, 0, 'CONTROL — Calle Test 1, Madrid', 40.4168, -3.7038, 'completed', now() - INTERVAL '3 hours', 1),
    (route_b1_id, 1, 'CONTROL — Calle Test 9, Madrid', 40.4170, -3.7050, 'pending',   NULL,                       2);

  RAISE NOTICE 'Seed OK. Login as dispatcher.test@xpedit.es / Dispatcher2026 and run the verification queries below.';
END $$;


-- -----------------------------------------------------------------------------
-- STEP 4 — verification queries (run from SQL editor with anon JWT of the
-- dispatcher to validate RLS, OR just from the browser UI to validate the
-- application-layer filters added in #263).
-- -----------------------------------------------------------------------------

-- Quick sanity: counts as service_role (no RLS). Should show A=2 drivers, B=1.
SELECT 'A' AS company, COUNT(*) FROM drivers WHERE company_id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
UNION ALL
SELECT 'B', COUNT(*) FROM drivers WHERE company_id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';

-- And routes: A=2, B=1.
SELECT 'A' AS company, COUNT(*) FROM routes WHERE company_id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa' AND deleted_at IS NULL
UNION ALL
SELECT 'B', COUNT(*) FROM routes WHERE company_id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb' AND deleted_at IS NULL;

-- After logging in as the dispatcher in the browser, verify in the dashboard:
--   • Drivers panel shows exactly Driver A1 + Driver A2. NEVER B1.
--   • Routes list shows exactly Ruta A1 + Ruta A2. NEVER Ruta B1.
--   • Open Analytics ("Hoy"): activeDrivers/totalDrivers reflect only A.
--   • Export PDF/Excel "Hoy": appears only Ruta A1; "Mes" shows A1+A2; never B1.
--   • Open one of A's routes by URL — fine. Then try pasting B1's UUID into
--     the URL: it must NOT load (loadRouteDetails now filters by company_id).
--     B1's UUID is: b1bb1bb1-1bb1-1bb1-1bb1-1bb11bb11bb1


-- -----------------------------------------------------------------------------
-- STEP 5 — CLEANUP (uncomment to remove all seed data)
-- -----------------------------------------------------------------------------
-- DELETE FROM stops   WHERE route_id   IN ('a1aa1aa1-1aa1-1aa1-1aa1-1aa11aa11aa1', 'a2aa2aa2-2aa2-2aa2-2aa2-2aa22aa22aa2', 'b1bb1bb1-1bb1-1bb1-1bb1-1bb11bb11bb1');
-- DELETE FROM routes  WHERE id         IN ('a1aa1aa1-1aa1-1aa1-1aa1-1aa11aa11aa1', 'a2aa2aa2-2aa2-2aa2-2aa2-2aa22aa22aa2', 'b1bb1bb1-1bb1-1bb1-1bb1-1bb11bb11bb1');
-- DELETE FROM drivers WHERE id         IN ('a1111111-1111-1111-1111-111111111111', 'a2222222-2222-2222-2222-222222222222', 'b1111111-1111-1111-1111-111111111111');
-- UPDATE public.users SET role = 'driver', company_id = NULL WHERE email = 'dispatcher.test@xpedit.es';
-- DELETE FROM companies WHERE id IN ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb');
-- And finally remove the auth user via the Dashboard (Authentication → Users).
