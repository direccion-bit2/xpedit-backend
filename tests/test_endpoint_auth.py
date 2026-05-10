"""Catch endpoints accidentally shipped without auth.

Walks every APIRoute in main.app and checks the dependency tree for one of
the auth deps (get_current_user / require_admin / require_admin_or_dispatcher).
Public endpoints (health probes, webhooks, login flow) are explicitly
allowlisted below — anything new lands as a test failure forcing the dev
to either:
  1. Add the right Depends(get_current_user)
  2. Or, if the endpoint is genuinely public, add it to PUBLIC_ENDPOINTS
     with a comment explaining why.

This mirrors a real risk: in a 4 631-line main.py it's trivial to copy a
handler and forget the Depends — and `pytest tests/` won't catch it
because most tests mock the auth layer.
"""

from __future__ import annotations

from fastapi.routing import APIRoute

from main import app

# (method, path) tuples that are intentionally public.
# Every entry must have a comment explaining why; unexplained entries are
# a future incident waiting to happen.
PUBLIC_ENDPOINTS: set[tuple[str, str]] = {
    # Health & diagnostics — must be reachable without auth so probes work.
    ("GET", "/"),
    ("GET", "/health"),
    ("GET", "/health/loop"),
    ("GET", "/debug/sentry-test"),
    # Direct APK download for Android sideload — no auth gate by design.
    ("GET", "/download/apk"),
    # Trial feedback survey — emailed signed link, accessed without JWT.
    ("POST", "/feedback/trial"),
    # Fleet operator login — uses a separate credential flow, not JWT.
    ("POST", "/fleet/login"),
    # Webhooks — verify their own provider signature instead of JWT.
    ("POST", "/stripe/webhook"),
    ("POST", "/revenuecat/webhook"),
    ("POST", "/webhooks/resend"),
    ("POST", "/webhooks/supabase-auth"),
}

AUTH_DEP_NAMES = {
    "get_current_user",
    "require_admin",
    "require_admin_or_dispatcher",
    "verify_route_access",
    "verify_stop_access",
    "verify_driver_access",
}


def _route_has_auth(route: APIRoute) -> bool:
    """True if any dependant in the tree calls one of AUTH_DEP_NAMES."""
    queue = list(route.dependant.dependencies)
    while queue:
        d = queue.pop()
        name = getattr(d.call, "__name__", "")
        if name in AUTH_DEP_NAMES:
            return True
        queue.extend(d.dependencies)
    return False


def test_every_endpoint_has_auth_or_is_explicitly_public():
    """If this fails, either add Depends(get_current_user) or update
    PUBLIC_ENDPOINTS with a comment saying why the endpoint is public."""
    rogue: list[str] = []
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        for method in route.methods or set():
            if method in {"HEAD", "OPTIONS"}:
                continue
            key = (method, route.path)
            if key in PUBLIC_ENDPOINTS:
                continue
            if not _route_has_auth(route):
                rogue.append(f"{method} {route.path}")

    assert not rogue, (
        "Endpoints reachable without auth and not in PUBLIC_ENDPOINTS:\n  - "
        + "\n  - ".join(sorted(rogue))
        + "\n\nFix by adding `user=Depends(get_current_user)` (or admin variant) "
        "to the handler, or — if the endpoint truly is public — add it to "
        "PUBLIC_ENDPOINTS in tests/test_endpoint_auth.py with a comment "
        "explaining why."
    )


def test_public_allowlist_only_contains_real_routes():
    """Catch typos / dead entries in PUBLIC_ENDPOINTS so the allowlist
    doesn't quietly grow stale (an entry for a route that no longer exists
    masks future bugs because we'll never notice the test would have caught
    them)."""
    real: set[tuple[str, str]] = set()
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        for method in route.methods or set():
            real.add((method, route.path))
    stale = PUBLIC_ENDPOINTS - real
    assert not stale, (
        f"PUBLIC_ENDPOINTS references routes that don't exist: {sorted(stale)}. "
        "Remove them so the allowlist stays honest."
    )
