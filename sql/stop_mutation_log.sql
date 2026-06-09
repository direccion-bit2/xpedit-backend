-- stop_mutation_log: registro append-only DURABLE de cada marcado de parada
-- (completed/failed) hecho por un repartidor. Garantiza que un marcado NUNCA se
-- pierde aunque la fila de `stops` aún no exista (ruta sin sincronizar) o el
-- UPDATE no se pueda aplicar en el momento. El reconciliador (cron) procesará
-- las entradas con applied=false cuando la fila exista.
-- Acceso SOLO backend (service_role); RLS habilitada SIN policies (drivers no tocan).
-- Usado por POST /stops/mark (#58, garantía "un marcado no se pierde nunca").

create table if not exists public.stop_mutation_log (
  id uuid primary key default gen_random_uuid(),
  driver_id uuid,
  stop_id uuid,
  route_id uuid,
  client_id uuid,
  position integer,
  action text not null check (action in ('completed','failed')),
  marked_at timestamptz not null,
  applied boolean not null default false,
  resolved_stop_id uuid,
  error text,
  created_at timestamptz not null default now()
);

create index if not exists idx_stop_mutation_log_unapplied
  on public.stop_mutation_log (marked_at)
  where applied = false;

create index if not exists idx_stop_mutation_log_route_client
  on public.stop_mutation_log (route_id, client_id);

alter table public.stop_mutation_log enable row level security;
-- sin policies a propósito: drivers NO acceden; solo service_role (backend) que bypassa RLS.
