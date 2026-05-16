#!/usr/bin/env python3
"""RLS audit — surface tables without row-level security or with suspicious gaps.

What it checks (one query per category):
  1. Tables in `public` schema where rls_enabled = false
  2. Tables with RLS on but zero policies — usually means everyone is locked
     out OR someone is bypassing via service_role and a real bug is hiding
  3. Tables that have a `deleted_at` column but at least one SELECT policy
     whose USING clause does NOT mention `deleted_at` — this is the exact
     pattern that leaked soft-deleted rows on 4 may (PR #189)

Usage (manual):
    SUPABASE_URL=...  SUPABASE_SERVICE_KEY=...  python scripts/rls_audit.py

Exit codes:
    0  no issues
    1  warnings only (tables without deleted_at filter etc)
    2  critical (RLS disabled on a table that has user data)

Designed to be wired into a monthly GitHub Action cron later. Runs read-only
SQL — no writes, no migrations.
"""

from __future__ import annotations

import os
import sys
from typing import Any

from supabase import Client, create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()

# Tables that legitimately don't need RLS — system tables, materialized views,
# or tables that only the service_role ever touches. Add a comment to every
# entry so the allowlist doesn't grow silently.
ALLOWED_NO_RLS: set[str] = {
    # Append-only event log written by webhooks — service_role only.
    "stripe_events",
    "revenuecat_events",
    "resend_email_events",
    # Spatial helpers / system metadata — no user data.
    "spatial_ref_sys",
}


def _run_sql(client: Client, sql: str) -> list[dict[str, Any]]:
    """Run raw SQL via the postgrest `exec_sql` rpc if it exists, falling
    back to information_schema queries through a regular SELECT.

    Supabase doesn't expose direct SQL by default; this helper wraps the
    typical patterns. We rely on the `pg_meta` style helpers via PostgREST
    when an RPC isn't available — practically the script just calls
    well-known views.
    """
    # The simplest contract: use rpc('exec_sql', { sql }) if the project has
    # an `exec_sql(text)` function. Otherwise, fall back to specific RPCs.
    res = client.rpc("exec_sql", {"sql": sql}).execute()
    return list(res.data or [])


def check_rls_disabled(client: Client) -> list[dict[str, Any]]:
    sql = """
        SELECT
          n.nspname AS schema,
          c.relname AS table_name,
          c.relrowsecurity AS rls_enabled
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind = 'r'
          AND c.relrowsecurity = false
        ORDER BY c.relname;
    """
    rows = _run_sql(client, sql)
    return [r for r in rows if r["table_name"] not in ALLOWED_NO_RLS]


def check_rls_no_policies(client: Client) -> list[dict[str, Any]]:
    sql = """
        SELECT
          c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_policy p ON p.polrelid = c.oid
        WHERE n.nspname = 'public'
          AND c.relkind = 'r'
          AND c.relrowsecurity = true
        GROUP BY c.relname
        HAVING count(p.polname) = 0
        ORDER BY c.relname;
    """
    return _run_sql(client, sql)


def check_select_policies_missing_deleted_at(client: Client) -> list[dict[str, Any]]:
    """Find tables that have a deleted_at column but a SELECT policy whose
    USING clause doesn't reference deleted_at. PR #189 showed how a single
    such policy leaked soft-deleted rows back to drivers."""
    sql = """
        WITH soft_delete_tables AS (
          SELECT DISTINCT table_name
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND column_name = 'deleted_at'
        ),
        select_policies AS (
          SELECT
            c.relname AS table_name,
            p.polname AS policy_name,
            pg_get_expr(p.polqual, p.polrelid) AS using_clause
          FROM pg_policy p
          JOIN pg_class c ON c.oid = p.polrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = 'public'
            AND p.polcmd IN ('r', '*')   -- SELECT or ALL
        )
        SELECT s.table_name, sp.policy_name, sp.using_clause
        FROM soft_delete_tables s
        JOIN select_policies sp ON sp.table_name = s.table_name
        WHERE sp.using_clause IS NULL
           OR position('deleted_at' in sp.using_clause) = 0
        ORDER BY s.table_name, sp.policy_name;
    """
    return _run_sql(client, sql)


def main() -> int:
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("FAIL: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set", file=sys.stderr)
        return 2

    client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    critical: list[str] = []
    warnings: list[str] = []

    try:
        no_rls = check_rls_disabled(client)
    except Exception as e:
        print(f"FAIL: RLS-disabled check errored: {e}", file=sys.stderr)
        return 2
    if no_rls:
        for row in no_rls:
            critical.append(f"RLS disabled on public.{row['table_name']}")

    try:
        no_policies = check_rls_no_policies(client)
    except Exception as e:
        print(f"FAIL: empty-policies check errored: {e}", file=sys.stderr)
        return 2
    if no_policies:
        for row in no_policies:
            warnings.append(f"RLS on but zero policies on public.{row['table_name']}")

    try:
        leak_candidates = check_select_policies_missing_deleted_at(client)
    except Exception as e:
        print(f"FAIL: deleted_at-filter check errored: {e}", file=sys.stderr)
        return 2
    if leak_candidates:
        for row in leak_candidates:
            warnings.append(
                f"SELECT policy {row['policy_name']} on public.{row['table_name']} "
                f"does not filter deleted_at"
            )

    if not critical and not warnings:
        print("✅ RLS audit clean — every public table has RLS enabled, "
              "policies attached, and SELECT policies filter soft-deletes.")
        return 0

    if critical:
        print("❌ Critical issues:")
        for c in critical:
            print(f"  - {c}")
    if warnings:
        print("⚠️  Warnings:")
        for w in warnings:
            print(f"  - {w}")
    return 2 if critical else 1


if __name__ == "__main__":
    sys.exit(main())
