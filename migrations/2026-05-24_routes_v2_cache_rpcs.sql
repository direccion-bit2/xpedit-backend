-- RPCs auxiliares para routes_v2_cache.
-- 1) increment_routes_v2_cache_hit: +1 hits + last_hit_at en una row, atómico.
--    Llamado fire-and-forget tras un L2 HIT — informativo, no crítico.
-- 2) sum_routes_v2_cache_hits: total hits acumulado para admin stats.
--    Permite ver "ahorro acumulado" cross-restart (a diferencia del L1 counter
--    in-memory que se borra en cada deploy).

CREATE OR REPLACE FUNCTION increment_routes_v2_cache_hit(p_key TEXT)
RETURNS VOID
LANGUAGE SQL
SECURITY DEFINER
SET search_path = public
AS $$
  UPDATE routes_v2_cache
  SET hits = hits + 1,
      last_hit_at = NOW()
  WHERE key = p_key;
$$;

CREATE OR REPLACE FUNCTION sum_routes_v2_cache_hits()
RETURNS BIGINT
LANGUAGE SQL
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT COALESCE(SUM(hits), 0)::BIGINT FROM routes_v2_cache;
$$;

-- Permisos: solo service_role puede invocar (anon/authenticated bloqueados).
REVOKE ALL ON FUNCTION increment_routes_v2_cache_hit(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION sum_routes_v2_cache_hits() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION increment_routes_v2_cache_hit(TEXT) TO service_role;
GRANT EXECUTE ON FUNCTION sum_routes_v2_cache_hits() TO service_role;
