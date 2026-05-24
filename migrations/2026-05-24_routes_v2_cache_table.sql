-- routes_v2_cache: L2 persistente para Google Routes API v2 cache server-side.
-- Razón: el L1 in-memory se borra en cada restart Railway. Sábado 23 may 2026
-- tuvimos 16 deploys backend → 133 calls Routes V2 con 0% cache hit porque el
-- cache nunca tuvo tiempo de poblarse entre restarts. Con persistencia BD el
-- cache sobrevive deploys y solo expira por TTL natural (legal-safe 10 min).
--
-- ToS Google Maps Platform 2025-2026: lat/lng cacheable hasta 30 días, polyline
-- es "Maps Content" → "temporary cache" tolerado en minutos/horas. TTL 24h es
-- frontera "defendible" industria delivery (DoorDash/Uber estándar). Para >24h
-- migrar a OSRM self-hosted (proyecto Q3, patrón Strava/Komoot).

CREATE TABLE IF NOT EXISTS routes_v2_cache (
  key         TEXT PRIMARY KEY,         -- SHA256 hex de (origin, dest, waypoints, avoid, heading)
  value       JSONB NOT NULL,           -- shaped response (status + routes)
  expires_at  TIMESTAMPTZ NOT NULL,     -- now() + TTL_SEC
  hits        BIGINT NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_hit_at TIMESTAMPTZ
);

-- Index para purga eficiente de expirados (cron diario)
CREATE INDEX IF NOT EXISTS routes_v2_cache_expires_idx
  ON routes_v2_cache(expires_at);

-- Sin RLS: tabla operacional, no contiene datos de usuario. Solo accede
-- service_role desde backend. Bloqueada a cualquier rol no service.
ALTER TABLE routes_v2_cache ENABLE ROW LEVEL SECURITY;

-- Sin policies = nadie puede leer/escribir excepto service_role (que bypassa RLS).

-- Purga automática de expirados cada hora (pg_cron). Sin esto la tabla crece
-- indefinidamente porque el TTL de aplicación solo evita HITs servidos, no
-- borra filas. Schedule conservador: 1/h en vez de 1/15min porque la query
-- es full-scan del índice y queremos ahorrar I/O.
SELECT cron.schedule(
  'purge_routes_v2_cache_expired',
  '5 * * * *',  -- minuto 5 de cada hora
  $$DELETE FROM routes_v2_cache WHERE expires_at < NOW()$$
);

COMMENT ON TABLE routes_v2_cache IS
  'L2 cache para /places/directions (Google Routes API v2). Sobrevive restarts backend (vs _routes_v2_cache in-memory). TTL 10 min — legal-safe según ToS Google. Hit rate esperado 30-70% en flujo normal driver delivery.';
