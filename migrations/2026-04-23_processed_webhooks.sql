-- Migration: persistent webhook idempotency
-- Date: 2026-04-23
-- Context: webhook dedup used an in-memory dict _processed_webhook_events.
-- A Railway deploy/restart wiped it, so Stripe/RevenueCat retries that
-- arrived after the restart would be re-processed (double plan activation
-- or double expiry revocation). This table survives restarts.
--
-- Applied to: staging → prod via Supabase MCP
--
-- ROLLBACK:
--   DROP TABLE IF EXISTS public.processed_webhooks;

CREATE TABLE IF NOT EXISTS public.processed_webhooks (
  event_id   TEXT PRIMARY KEY,
  provider   TEXT NOT NULL,  -- 'stripe' | 'revenuecat' | 'supabase_auth' | 'resend'
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The table is write-only from the backend (service_role). No RLS needed
-- (no user-facing reads). Add RLS anyway as defence-in-depth so anon role
-- cannot insert spoofed rows to block real events.
ALTER TABLE public.processed_webhooks ENABLE ROW LEVEL SECURITY;

-- No policies = no access for anon/authenticated. service_role bypasses RLS.
-- Explicit note: leave policies empty so only the backend can write.

-- Index for cleanup cron (drop rows older than 30 days).
CREATE INDEX IF NOT EXISTS idx_processed_webhooks_processed_at
  ON public.processed_webhooks (processed_at);
