"""
Xpedit API - Backend de optimización de rutas
"""

import asyncio
import base64
import hashlib
import hmac as _hmac
import json
import logging
import math
import os
import random
import re
import time
import unicodedata
from datetime import date, datetime, timedelta, timezone
from typing import List, Literal, Optional
from zoneinfo import ZoneInfo

import httpx
import jwt as pyjwt
import sentry_sdk
from dotenv import load_dotenv


# Safe wrapper — capture_check_in doesn't exist in all sentry_sdk versions
def sentry_check_in(monitor_slug: str, status: str):
    try:
        sentry_sdk.capture_check_in(monitor_slug=monitor_slug, status=status)
    except AttributeError:
        pass  # sentry_sdk version doesn't support cron monitoring
from fastapi import Depends, FastAPI, File, Header, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from jwt import PyJWKClient
from pydantic import BaseModel, Field
from supabase import Client, create_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("xpedit")

# Sentry - Error monitoring
# 5 may 2026 incident: 0 events received in 30 days. Suspected silent SDK
# misconfiguration. Made integrations explicit, raised sample rate, and
# added a startup ping so we know whether Sentry is reachable from Railway.
SENTRY_DSN = os.getenv("SENTRY_DSN")
SENTRY_DEBUG = os.getenv("SENTRY_DEBUG", "false").lower() == "true"
if SENTRY_DSN:
    from sentry_sdk.integrations.asyncio import AsyncioIntegration
    from sentry_sdk.integrations.fastapi import FastApiIntegration
    from sentry_sdk.integrations.logging import LoggingIntegration
    from sentry_sdk.integrations.starlette import StarletteIntegration

    # Release SHA dinámico — Railway expone RAILWAY_GIT_COMMIT_SHA por deploy.
    # 12 may 2026 audit: el release estaba hardcoded a 1.1.4 desde hace 30 días,
    # eso rompe regression detection (Sentry no podía decir "qué commit introdujo
    # el bug"). SENTRY_RELEASE permite override manual desde Railway si hace falta.
    _release_sha = (
        os.getenv("SENTRY_RELEASE")
        or os.getenv("RAILWAY_GIT_COMMIT_SHA")
        or "unknown"
    )
    _release = f"xpedit-backend@{_release_sha[:12]}" if _release_sha != "unknown" else "xpedit-backend@unknown"

    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=0.1,  # 10% — ingestion estable desde may 2026, evita saturar cuota
        profiles_sample_rate=0.1,
        environment=os.getenv("SENTRY_ENVIRONMENT", "production"),
        release=_release,
        send_default_pii=False,
        debug=SENTRY_DEBUG,
        attach_stacktrace=True,
        integrations=[
            FastApiIntegration(transaction_style="endpoint"),
            StarletteIntegration(transaction_style="endpoint"),
            AsyncioIntegration(),
            # 12 may 2026 audit: event_level subido de ERROR a WARNING.
            # Miguel pidió "RUIDO a tope en Sentry, ya silenciaremos lo que sobre".
            # Esto captura los logger.warning de "Stripe webhook signature fail",
            # "Google Places fallback", "RevenueCat missing app_user_id", etc, que
            # antes se quedaban solo en Railway logs y nadie miraba.
            LoggingIntegration(level=logging.INFO, event_level=logging.WARNING),
        ],
    )
    logger.info(f"Sentry initialized (DSN configured, debug={SENTRY_DEBUG})")
    # Startup ping — if this never appears in the Sentry UI, the SDK is silently
    # dropping events (network blocked, DSN wrong, project disabled).
    try:
        sentry_sdk.capture_message(
            "Backend startup ping",
            level="info",
        )
    except Exception as e:
        logger.warning(f"Sentry startup ping failed: {e}")
else:
    # INFO en vez de WARNING (22 may 2026): solo informativo en local dev.
    # En prod SENTRY_DSN siempre está configurado, este branch no aplica.
    logger.info("SENTRY_DSN not configured — backend errors will NOT be reported (expected in local dev)")

from emails import (
    ACTIVE_FREE_PRO_INVITE_SUBJECT,
    TRIAL_EXPIRING_D1_SUBJECT,
    TRIAL_EXPIRING_D3_SUBJECT,
    TRIAL_VALUE_RECAP_SUBJECT,
    send_active_free_pro_invite_email,
    send_alert_email,
    send_broadcast_email,
    send_custom_email,
    send_daily_health_digest_email,
    send_daily_summary_email,
    send_delivery_completed_email,
    send_delivery_failed_email,
    send_delivery_started_email,
    send_plan_activated_email,
    send_reactivation_persistence_email,
    send_reengagement_broadcast,
    send_referral_reward_email,
    send_social_login_broadcast,
    send_survey_email,
    send_trial_expired_email,
    send_trial_expiring_email,
    send_trial_feedback_email,
    send_trial_last_day_email,
    send_trial_value_recap_email,
    send_upcoming_email,
    send_welcome_email,
)
from optimizer import (
    assign_drivers_to_zones,
    calculate_eta,
    calculate_route_etas,
    cluster_stops_by_zone,
    hybrid_optimize_route,
    optimize_multi_vehicle,
    optimize_route,
)

# Cargar variables de entorno
load_dotenv()


# ---------------------------------------------------------------------------
# Reusable HTTPX AsyncClient for Google Maps Platform.
# 5 may 2026 incident: every /places/* and /directions handler did
# `async with httpx.AsyncClient(timeout=N) as client:` — fresh TCP+TLS
# handshake on every call. Under real load (22 drivers FORCE-OTA reload
# at once) this saturated the event loop and pushed p99 past 17s.
#
# Note: an earlier attempt with a shared client at 10:24 today hit pool
# exhaustion under 22 concurrent users (max_connections=50, pool=2s was
# too tight). This version is more generous: 100/50/keepalive=30s, and
# the pool timeout is 10s so a small burst doesn't immediately error.
# Per-request timeout overrides are applied at each call site.
_google_maps_client: Optional["httpx.AsyncClient"] = None


def google_maps_client() -> "httpx.AsyncClient":
    """Lazy singleton. Re-creates if the previous one was closed."""
    global _google_maps_client
    if _google_maps_client is None or _google_maps_client.is_closed:
        _google_maps_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=3.0, read=10.0, write=3.0, pool=10.0),
            limits=httpx.Limits(
                max_keepalive_connections=50,
                max_connections=100,
                keepalive_expiry=30.0,
            ),
            headers={"User-Agent": "Xpedit/1.1.4 (+xpedit.es)"},
            http2=False,  # explicit: HTTP/1.1 keepalive is enough, avoids h2 dep
        )
    return _google_maps_client

# Inicializar Supabase (service role key para bypass RLS en servidor)
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY", ""))
supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    SUPABASE_SERVICE_KEY
)

# Fix bug #222 (22 may 2026): forzar HTTP/1.1 en el cliente httpx que PostgREST
# usa internamente. HTTP/2 con Supabase causa ráfagas "Server disconnected"
# cuando el stream pool se satura (visto en LATAM concurrente). HTTP/1.1
# reconnecta por request → no comparte pool problemático.
#
# IMPORTANTE (regresión 22 may 15:54): el nuevo httpx.Client DEBE preservar
# `base_url` y `headers` del cliente original que postgrest configuró con
# la URL/key de Supabase. Sin esto, requests con path relativo fallan con
# "Request URL is missing an 'http://' or 'https://' protocol".
# 20 events en 30s con drivers Bogotá afectados antes de detectarlo.
try:
    import httpx as _httpx_supabase
    if hasattr(supabase, "postgrest") and hasattr(supabase.postgrest, "session"):
        _orig_session = supabase.postgrest.session
        _orig_base_url = getattr(_orig_session, "base_url", None)
        _orig_headers = dict(getattr(_orig_session, "headers", {}) or {})
        if _orig_base_url and str(_orig_base_url).startswith(("http://", "https://")):
            _new_session = _httpx_supabase.Client(
                base_url=_orig_base_url,
                headers=_orig_headers,
                http1=True,
                http2=False,
                timeout=30.0,
                limits=_httpx_supabase.Limits(
                    max_keepalive_connections=20,
                    max_connections=100,
                    keepalive_expiry=30.0,
                ),
            )
            try:
                _orig_session.close()
            except Exception:
                pass
            supabase.postgrest.session = _new_session
            logger.info(f"Supabase httpx client forced to HTTP/1.1 (base_url={_orig_base_url})")
        else:
            logger.warning(
                f"Skip HTTP/1.1 fix — base_url invalid or missing on original session: {_orig_base_url!r}"
            )
except Exception as _e:
    logger.warning(f"Failed to force HTTP/1.1 on Supabase client (continuing with default): {_e}")

# Supabase JWT secret for token verification
_raw_jwt = os.getenv("SUPABASE_JWT_SECRET", "")
# Railway strips trailing '=' — restore base64 padding
SUPABASE_JWT_SECRET = _raw_jwt + "=" * ((4 - len(_raw_jwt) % 4) % 4) if _raw_jwt else ""


def safe_first(result) -> Optional[dict]:
    """Safely get first result from Supabase query, returns None if empty"""
    return result.data[0] if result.data else None


def mask_email(email: str) -> str:
    """Mask email for safe logging: jo***@gmail.com"""
    if not email or "@" not in email:
        return "***"
    local, domain = email.split("@", 1)
    return f"{local[:2]}***@{domain}"


async def send_push_to_token(token: str, title: str, body: str, data: dict = None) -> bool:
    """Send an Expo push notification to a single token. Returns True on success."""
    if not token or not token.startswith("ExponentPushToken["):
        return False
    payload = {"to": token, "title": title, "body": body, "sound": "default"}
    if data:
        payload["data"] = data
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://exp.host/--/api/v2/push/send",
                json=payload,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
            )
            result = resp.json()
            if result.get("data", {}).get("status") == "error":
                logger.warning(f"Push failed for token {token[:30]}...: {result['data'].get('message')}")
                return False
            return resp.status_code == 200
    except Exception as e:
        logger.error(f"Push send error: {e}")
        return False


# ========== ADDRESS NORMALIZATION ==========
_STREET_PREFIXES = {
    "c/": "calle", "cl": "calle", "cl.": "calle",
    "av": "avenida", "av.": "avenida", "avda": "avenida", "avda.": "avenida",
    "pz": "plaza", "pza": "plaza", "pza.": "plaza", "pl": "plaza", "pl.": "plaza",
    "ps": "paseo", "ps.": "paseo", "pso": "paseo", "pso.": "paseo",
    "ctra": "carretera", "ctra.": "carretera", "crta": "carretera",
    "rda": "ronda", "rda.": "ronda",
    "urb": "urbanizacion", "urb.": "urbanizacion",
    "pol": "poligono", "pol.": "poligono",
}


def normalize_address(address: str) -> str:
    """Normalize a Spanish address for matching."""
    text = unicodedata.normalize("NFD", address.lower())
    text = re.sub(r"[\u0300-\u036f]", "", text)
    text = re.sub(r"[,.\-/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    words = text.split()
    if words and words[0] in _STREET_PREFIXES:
        words[0] = _STREET_PREFIXES[words[0]]
    text = " ".join(words)
    text = re.sub(r"\b(de|del|la|las|los|el)\b", "", text)
    text = re.sub(r"\b\d{5}\b", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Distance in km between two lat/lng points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def enrich_stops_from_directory(company_id: str, stops: list[dict]) -> tuple[list[dict], int]:
    """Enrich stops with customer data from company directory. Returns (stops, match_count)."""
    if not company_id:
        return stops, 0
    directory = supabase.table("customer_directory").select(
        "normalized_address, lat, lng, phone, email, customer_name"
    ).eq("company_id", company_id).execute()
    if not directory.data:
        return stops, 0
    addr_lookup = {}
    geo_entries = []
    for entry in directory.data:
        addr_lookup[entry["normalized_address"]] = entry
        if entry.get("lat") and entry.get("lng"):
            geo_entries.append(entry)
    match_count = 0
    for stop in stops:
        if stop.get("phone") and stop.get("email"):
            continue
        norm = normalize_address(stop.get("address", ""))
        match = addr_lookup.get(norm)
        if not match and stop.get("lat") and stop.get("lng"):
            for entry in geo_entries:
                if _haversine_km(stop["lat"], stop["lng"], entry["lat"], entry["lng"]) <= 0.05:
                    match = entry
                    break
        if match:
            if not stop.get("phone") and match.get("phone"):
                stop["phone"] = match["phone"]
            if not stop.get("email") and match.get("email"):
                stop["email"] = match["email"]
            match_count += 1
    return stops, match_count


# Webhook idempotency. The in-memory dict stays as a fast-path cache
# (avoids a DB round-trip when the same event reaches us multiple times
# within one process lifetime). The persistent source of truth is the
# processed_webhooks table, which survives Railway restarts — before
# this table, a redeploy in the middle of a retry storm could re-process
# the same Stripe/RevenueCat event.
_processed_webhook_events: dict = {}


def _is_webhook_processed(event_id: str, provider: str) -> bool:
    """True if this event has already been handled (memory OR DB)."""
    if not event_id:
        return False
    if event_id in _processed_webhook_events:
        return True
    try:
        existing = supabase.table("processed_webhooks").select("event_id").eq("event_id", event_id).limit(1).execute()
        if existing.data:
            # Populate memory cache so subsequent in-process lookups skip DB.
            _processed_webhook_events[event_id] = True
            return True
    except Exception as e:
        # Don't block on a DB hiccup; fall back to in-memory behaviour.
        logger.warning(f"_is_webhook_processed DB check failed: {e}")
    return False


def _mark_webhook_processed(event_id: str, provider: str) -> None:
    """Record that we handled this event in both memory and DB."""
    if not event_id:
        return
    _processed_webhook_events[event_id] = True
    if len(_processed_webhook_events) > 10000:
        to_remove = list(_processed_webhook_events.keys())[:5000]
        for k in to_remove:
            del _processed_webhook_events[k]
    try:
        supabase.table("processed_webhooks").insert({
            "event_id": event_id,
            "provider": provider,
        }).execute()
    except Exception as e:
        # Insert can fail if another instance processed the same event in
        # parallel (PK collision). That's fine — idempotency holds.
        logger.info(f"_mark_webhook_processed insert note: {e}")

# Stripe webhook monitoring
_last_stripe_webhook_ok: Optional[datetime] = None
_last_stripe_webhook_error: Optional[datetime] = None

# Stripe
import stripe

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
RESEND_WEBHOOK_SECRET = os.getenv("RESEND_WEBHOOK_SECRET", "")
SUPABASE_WEBHOOK_SECRET = os.getenv("SUPABASE_WEBHOOK_SECRET", "")
REVENUECAT_WEBHOOK_SECRET = os.getenv("REVENUECAT_WEBHOOK_SECRET", "")

# Used to sign /feedback/trial links. Falls back to SUPABASE_JWT_SECRET so
# the feature works even before a dedicated env var is provisioned.
FEEDBACK_TOKEN_SECRET = os.getenv("FEEDBACK_TOKEN_SECRET", "") or os.getenv("SUPABASE_JWT_SECRET", "")


def _trial_feedback_token(driver_id: str, reason: str) -> str:
    """Stable HMAC binding (driver_id, reason) to FEEDBACK_TOKEN_SECRET.
    Truncated to 32 hex chars — enough entropy and short enough for URLs."""
    if not FEEDBACK_TOKEN_SECRET:
        return ""
    msg = f"{driver_id}:{reason}".encode()
    return _hmac.new(FEEDBACK_TOKEN_SECRET.encode(), msg, hashlib.sha256).hexdigest()[:32]
stripe.api_key = STRIPE_SECRET_KEY

STRIPE_PRICE_PRO = os.getenv("STRIPE_PRICE_PRO", "")
STRIPE_PRICE_PRO_PLUS = os.getenv("STRIPE_PRICE_PRO_PLUS", "")

STRIPE_PLANS = {
    "pro": {"name": "Xpedit Pro", "price_id": STRIPE_PRICE_PRO},
    "pro_plus": {"name": "Xpedit Pro+", "price_id": STRIPE_PRICE_PRO_PLUS},
}

# JWKS client for ES256 token verification (Supabase uses ES256)
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
_jwks_url = f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json"
_jwks_client = PyJWKClient(_jwks_url) if SUPABASE_URL else None

# In-process TTL cache for user profiles. 5 may 2026 incident: every single
# authenticated request hit `supabase.table("users").select(...).execute()`
# (sync supabase-py inside async handler) which blocked the event loop.
# Cache profile by user_id for 60s — JWT lifetime is much longer, role
# changes are rare, and a 60s window is still well within auth security
# tolerances. Reduces DB load ~60% under typical autocomplete bursts.
from cachetools import TTLCache as _TTLCache  # noqa: E402

_user_profile_cache: _TTLCache = _TTLCache(maxsize=10000, ttl=60)


def invalidate_user_cache(user_id: str) -> None:
    """Drop a single user profile from cache (call after role changes)."""
    _user_profile_cache.pop(user_id, None)


async def get_current_user(authorization: str = Header(default=None)):
    """Verify Supabase JWT token and return user info"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token requerido")

    token = authorization.replace("Bearer ", "")

    try:
        # Verify the JWT token - only allow HS256 and ES256
        ALLOWED_ALGORITHMS = ["HS256", "ES256"]
        header = pyjwt.get_unverified_header(token)
        alg = header.get("alg", "HS256")
        logger.debug(f"JWT alg={alg}, token_len={len(token)}")

        if alg not in ALLOWED_ALGORITHMS:
            raise HTTPException(status_code=401, detail=f"Algorithm {alg} not allowed")

        if alg == "ES256" and _jwks_client:
            # ES256: use JWKS public key from Supabase
            signing_key = _jwks_client.get_signing_key_from_jwt(token)
            key = signing_key.key
        else:
            # HS256 fallback: use symmetric secret
            key = SUPABASE_JWT_SECRET

        payload = pyjwt.decode(
            token,
            key,
            algorithms=ALLOWED_ALGORITHMS,
            audience="authenticated"
        )
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Token invalido")

        sentry_sdk.set_user({"id": user_id})

        # Cache hit path — skip DB roundtrip entirely.
        cached = _user_profile_cache.get(user_id)
        if cached is not None:
            return cached

        # Cache miss — fetch from DB and store. Sync call wrapped via thread
        # pool so a slow DB query doesn't block the event loop for other
        # in-flight requests (anyio default 40 → bumped to 200 in startup).
        result = await asyncio.to_thread(
            lambda: supabase.table("users").select("id, email, role, company_id").eq("id", user_id).single().execute()
        )
        if not result.data:
            raise HTTPException(status_code=401, detail="Usuario no encontrado")

        _user_profile_cache[user_id] = result.data
        return result.data
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado - cierra sesion y vuelve a entrar")
    except pyjwt.InvalidTokenError as e:
        logger.warning(f"InvalidTokenError: {e}")
        raise HTTPException(status_code=401, detail="Token invalido - cierra sesion y vuelve a entrar")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Auth unexpected error: {type(e).__name__}: {e}")
        raise HTTPException(status_code=401, detail="Error de autenticacion")


async def require_admin(user=Depends(get_current_user)):
    """Require admin role"""
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Acceso restringido a administradores")
    return user


def log_audit(admin_id: str, action: str, resource_type: str, resource_id: str = None, details: dict = None, ip_address: str = None):
    """Log an admin action to the audit_log table. Fire and forget."""
    try:
        supabase.table("audit_log").insert({
            "admin_id": admin_id,
            "action": action,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "details": details,
            "ip_address": ip_address,
        }).execute()
    except Exception as e:
        logger.warning(f"Audit log failed: {e}")


async def require_admin_or_dispatcher(user=Depends(get_current_user)):
    """Require admin or dispatcher role"""
    if user.get("role") not in ("admin", "dispatcher"):
        raise HTTPException(status_code=403, detail="Acceso restringido")
    return user


# === OWNERSHIP HELPERS ===

async def get_user_driver_id(user: dict) -> Optional[str]:
    """Get the driver_id for the authenticated user"""
    result = supabase.table("drivers").select("id, company_id").eq("user_id", user["id"]).limit(1).execute()
    if result.data:
        return result.data[0]["id"]
    return None


async def verify_route_access(route_id: str, user: dict):
    """Verify the user can access this route. Returns route data or raises 403."""
    route_result = supabase.table("routes").select("id, driver_id").eq("id", route_id).limit(1).execute()
    if not route_result.data:
        raise HTTPException(status_code=404, detail="Ruta no encontrada")
    route = route_result.data[0]
    if user["role"] == "admin":
        return route
    user_driver_id = await get_user_driver_id(user)
    if route["driver_id"] == user_driver_id:
        return route
    # Dispatcher can access routes from same company
    if user["role"] == "dispatcher" and user.get("company_id"):
        driver_result = supabase.table("drivers").select("company_id").eq("id", route["driver_id"]).limit(1).execute()
        if driver_result.data and driver_result.data[0].get("company_id") == user.get("company_id"):
            return route
    raise HTTPException(status_code=403, detail="No tienes acceso a esta ruta")


async def verify_stop_access(stop_id: str, user: dict):
    """Verify the user can access this stop via route ownership."""
    stop_result = supabase.table("stops").select("id, route_id").eq("id", stop_id).limit(1).execute()
    if not stop_result.data:
        raise HTTPException(status_code=404, detail="Parada no encontrada")
    await verify_route_access(stop_result.data[0]["route_id"], user)
    return stop_result.data[0]


async def verify_driver_access(driver_id: str, user: dict):
    """Verify the user can access this driver's data."""
    if user["role"] == "admin":
        return True
    user_driver_id = await get_user_driver_id(user)
    if driver_id == user_driver_id:
        return True
    if user["role"] == "dispatcher" and user.get("company_id"):
        driver_result = supabase.table("drivers").select("company_id").eq("id", driver_id).limit(1).execute()
        if driver_result.data and driver_result.data[0].get("company_id") == user.get("company_id"):
            return True
    raise HTTPException(status_code=403, detail="No tienes acceso a este conductor")


async def verify_company_management(user: dict, company_id: str = None):
    """Verify the user can manage this company (admin or dispatcher of the company)."""
    if user["role"] == "admin":
        return True
    if user["role"] == "dispatcher" and user.get("company_id"):
        if company_id is None or user.get("company_id") == company_id:
            return True
    raise HTTPException(status_code=403, detail="No tienes permisos para gestionar esta empresa")


tags_metadata = [
    {
        "name": "health",
        "description": "Estado del sistema, health checks y monitorización.",
    },
    {
        "name": "auth",
        "description": "Autenticación y gestión de cuentas de usuario.",
    },
    {
        "name": "optimize",
        "description": "Optimización de rutas, ETAs, clustering y asignación de conductores.",
    },
    {
        "name": "routes",
        "description": "Gestión de rutas: crear, listar, iniciar, completar y eliminar.",
    },
    {
        "name": "stops",
        "description": "Gestión de paradas dentro de las rutas.",
    },
    {
        "name": "drivers",
        "description": "Gestión de conductores y perfiles.",
    },
    {
        "name": "tracking",
        "description": "Seguimiento GPS en tiempo real de conductores.",
    },
    {
        "name": "company",
        "description": "Gestión de empresas, flotas, invitaciones y suscripciones.",
    },
    {
        "name": "promo",
        "description": "Códigos promocionales: canjear, verificar y administrar.",
    },
    {
        "name": "referral",
        "description": "Sistema de referidos entre usuarios.",
    },
    {
        "name": "email",
        "description": "Envío de emails transaccionales y notificaciones.",
    },
    {
        "name": "social",
        "description": "Gestión de redes sociales: publicaciones, calendario e IA generativa.",
    },
    {
        "name": "admin",
        "description": "Endpoints de administración (requiere rol admin).",
    },
    {
        "name": "download",
        "description": "Descarga de la APK y tracking de descargas.",
    },
    {
        "name": "stripe",
        "description": "Pagos con Stripe: checkout, webhooks y portal de cliente.",
    },
    {
        "name": "places",
        "description": "Proxy de Google Places: autocompletado, detalles y direcciones.",
    },
    {
        "name": "ocr",
        "description": "Reconocimiento óptico de etiquetas de envío (OCR).",
    },
    {
        "name": "webhooks",
        "description": "Webhooks entrantes de servicios externos (Stripe, Resend).",
    },
    {
        "name": "fleet",
        "description": "Fleet management: dashboard KPIs, driver performance, zones, chat, activity feed.",
    },
]

ADMIN_EXCLUDE_IDS = ["8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b", "e481de53-bb8c-4b76-8b56-04a7d00f9c6f"]

_is_production = os.getenv("SENTRY_ENVIRONMENT") == "production"

app = FastAPI(
    title="Xpedit API",
    description="API para la app de optimización de rutas Xpedit",
    version="1.1.4",
    docs_url=None if _is_production else "/docs",
    redoc_url=None if _is_production else "/redoc",
    openapi_url=None if _is_production else "/openapi.json",
    openapi_tags=tags_metadata,
)


# 5 may 2026 incident mitigation: FastAPI/Starlette default thread pool is
# 40 tokens (anyio). When `async def` handlers call sync supabase-py inside
# them, the call is NOT delegated to the thread pool — it blocks the event
# loop directly. But many internal `run_in_threadpool` paths (Starlette
# middleware, FastAPI sync routes) DO use this pool. With 22 concurrent
# drivers + a saturated event loop a 40-token pool is too small. Bumping
# to 200 buys us breathing room while we migrate sync calls to
# `await asyncio.to_thread(...)` one by one. See
# https://github.com/Kludex/fastapi-tips#5-bigger-applications-multiple-files
@app.on_event("startup")
async def _bump_anyio_thread_pool():
    try:
        import anyio
        anyio.to_thread.current_default_thread_limiter().total_tokens = 200
        logger.info("anyio thread pool limiter raised to 200 (was 40)")
    except Exception as e:
        logger.warning(f"Could not raise anyio thread limiter: {e}")


@app.on_event("startup")
async def _startup_smoke_test():
    """SMOKE TEST POST-DEPLOY (22 may 2026 — lessons learned bug HTTP/1.1):
    valida en arranque que las dependencias críticas funcionan ANTES de
    aceptar tráfico. Si falla, marca el backend como degraded en Sentry
    pero NO crashea (Railway puede reiniciar infinitamente lo que peor).

    Caso real bug ab88484: el cliente httpx Supabase quedó sin base_url tras
    refactor → TODO el backend rompió Supabase 1h sin que nada lo detectara
    hasta que un email Sentry llegó a Miguel. Este test lo habría cazado en
    el primer startup post-deploy.

    Validaciones:
    1. Supabase responde a query trivial (app_config)
    2. supabase.postgrest.session tiene base_url válido http(s)://
    3. GOOGLE_API_KEY env var configurada (si !places no funcionaría)
    4. JWT secret presente (auth rompería todo si falta)
    """
    failures: list[str] = []

    # 1. Supabase real query
    try:
        result = await asyncio.to_thread(
            lambda: supabase.table("app_config").select("key").limit(1).execute()
        )
        if not isinstance(result.data, list):
            failures.append(f"Supabase query returned non-list: {type(result.data)}")
        else:
            logger.info("smoke ✓ Supabase query OK")
    except Exception as e:
        failures.append(f"Supabase query failed: {type(e).__name__}: {e}")

    # 2. postgrest session base_url valid
    try:
        session = getattr(supabase, "postgrest", None)
        session = getattr(session, "session", None) if session else None
        base_url = str(getattr(session, "base_url", "")) if session else ""
        if not base_url.startswith(("http://", "https://")):
            failures.append(f"postgrest session base_url INVALID: {base_url!r}")
        else:
            logger.info(f"smoke ✓ postgrest base_url OK ({base_url[:50]})")
    except Exception as e:
        failures.append(f"postgrest base_url check failed: {e}")

    # 3. GOOGLE_API_KEY
    if not GOOGLE_API_KEY:
        failures.append("GOOGLE_API_KEY env var missing (places/* would not work)")
    else:
        logger.info("smoke ✓ GOOGLE_API_KEY set")

    # 4. JWT secret
    if not SUPABASE_JWT_SECRET:
        failures.append("SUPABASE_JWT_SECRET env var missing (all auth would break)")
    else:
        logger.info("smoke ✓ SUPABASE_JWT_SECRET set")

    if failures:
        msg = f"STARTUP SMOKE TEST FAILED ({len(failures)} checks): " + " | ".join(failures)
        logger.error(msg)
        if SENTRY_DSN:
            try:
                sentry_sdk.capture_message(msg, level="fatal")
            except Exception:
                pass
        # Marcamos health degraded para que Railway/admin lo vean. No
        # hacemos sys.exit — preferimos backend degradado a backend OFF.
        global _startup_smoke_failures
        _startup_smoke_failures = failures
    else:
        logger.info(f"STARTUP SMOKE TEST PASSED ({4} checks)")
        global _startup_smoke_ok
        _startup_smoke_ok = True


_startup_smoke_ok: bool = False
_startup_smoke_failures: list[str] = []

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://xpedit.es", "https://www.xpedit.es", "http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains; preload"
    if request.url.path.startswith("/admin"):
        response.headers["Cache-Control"] = "no-store, private"
    return response


# Rate limiting (in-memory, single instance)
from collections import defaultdict

_rate_limits: dict = defaultdict(list)
_rate_limits_last_cleanup = time.time()

def check_rate_limit(key: str, max_requests: int = 30, window_seconds: int = 60):
    """Simple in-memory rate limiter. Raises 429 if exceeded."""
    global _rate_limits_last_cleanup
    now = time.time()
    # Purge stale keys every 5 minutes to prevent unbounded growth
    if now - _rate_limits_last_cleanup > 300:
        stale = [k for k, v in _rate_limits.items() if not v or v[-1] < now - window_seconds]
        for k in stale:
            del _rate_limits[k]
        _rate_limits_last_cleanup = now
    _rate_limits[key] = [t for t in _rate_limits[key] if t > now - window_seconds]
    if len(_rate_limits[key]) >= max_requests:
        raise HTTPException(status_code=429, detail="Demasiadas solicitudes. Inténtalo en unos minutos.")
    _rate_limits[key].append(now)


# Daily OCR image quota per user/tier.
# IMPORTANT: this counts IMAGES, not requests. Day 12 may 2026 the MSI client
# was changed to chunk a 10-image import into 4 serial /ocr/screenshots-batch
# requests (3+3+3+1) to avoid gateway timeouts. A per-request limit would
# have shrunk the effective daily quota 4× without anyone noticing.
# Same bucket is shared by /ocr/label (label scan, 1 img/req) and
# /ocr/screenshots-batch (MSI, 1-3 img/req post-chunking) so an abuser can't
# bypass the limit by mixing endpoints. Limit is per driver_id (not IP) so
# device changes don't reset the quota either.
_OCR_DAILY_IMG_QUOTA = {
    # Limits set by Miguel 12 may 2026 14:29 CEST. Rationale: cost per
    # image ~$0.01 (Gemini + Geocoding). We don't size for worst-case
    # where every user maxes out every day; average use is much lower
    # and the heavy outlier is acceptable to lose money on as long as
    # the P50 stays profitable. If demand grows past sustainable, we
    # revisit pricing (either raise plan price or move MSI to an
    # add-on).  yearly users get the same daily quota as monthly —
    # Miguel: "ten en cuenta que no todos van a usar esa función
    # entonces no es realista calcular como si todos gastaran al
    # tope".
    "free": 0,         # Locked. Free users see the paywall.
    "trial": 30,       # 7-day trial: enough to feel the value.
    "pro": 30,         # Same as trial — Miguel decision 20 may: MSI for Pro
                       # accelerates the OCR learning flywheel. Pricing rises
                       # with QUALITY of MSI, not by gating who can use it.
    "pro_yearly": 30,  # Aligned with Pro monthly (was 20).
    "pro_plus": 50,    # Premium tier kept higher for future power-users.
    # `_verify_msi_access` returns 'pro_yearly' for any sub_period='yearly',
    # so a Pro+ yearly user also resolves to 'pro_yearly' here. That keeps
    # yearly users at 20/day across the board, which protects the lower
    # price-per-month of the yearly plan.
}
_OCR_QUOTA_WINDOW = 86400  # 24h rolling window.

# Internal testing accounts get unlimited daily quota while we iterate on
# MSI. Miguel staging + Miguel prod direccion@taespack. These are NOT
# customer accounts, they're the ones we use to smoke-test every OTA.
# Update this list when adding/removing dev devices, never expose it.
# IMPORTANT: include BOTH `drivers.id` AND `auth.users.id` for each account.
# When _resolve_user_tier fails for any reason, the code falls back to
# auth_user_id as the quota_key — so if only one of the two is in the set,
# bypass silently stops working in that failure path.
_OCR_QUOTA_TESTING_BYPASS = frozenset({
    # staging@xpedit.es (Miguel DEV staging)
    "9922cc2e-d88d-4e58-a5f5-8b3d7ca52e40",  # drivers.id
    "8f852b60-25a2-4180-a66f-8947a9325945",  # auth.users.id
    # direccion@taespack.com (Miguel prod)
    "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b",  # drivers.id
    "fe94de32-7f04-4f4f-83fc-b4264eedeaaa",  # auth.users.id
})

def get_ocr_quota_status(driver_id: str, tier: str) -> dict:
    """Returns the current OCR daily-image quota state for a driver.
    Used by GET /ocr/quota so the app can pre-check before sending a
    batch (Miguel report 12 may 15:13: bad UX when the limit alert
    fires AFTER the last image is processed). `used` and `remaining`
    refer to the current rolling 24h window."""
    if driver_id in _OCR_QUOTA_TESTING_BYPASS:
        return {"tier": tier, "used": 0, "limit": 9999, "remaining": 9999, "testing_bypass": True}
    now = time.time()
    key = f"ocr_imgs:{driver_id}:daily"
    used = len([t for t in _rate_limits.get(key, []) if t > now - _OCR_QUOTA_WINDOW])
    limit = _OCR_DAILY_IMG_QUOTA.get(tier, _OCR_DAILY_IMG_QUOTA["free"])
    return {
        "tier": tier,
        "used": used,
        "limit": limit,
        "remaining": max(0, limit - used),
        "testing_bypass": False,
    }


def _get_msi_bonus_today(driver_id: str) -> int:
    """Devuelve +10 si el driver contribuyó etiquetas vía /ocr/training-contribute
    AYER. Cambio Miguel 20 may 16:13: el bonus se entrega al DÍA SIGUIENTE
    (no el mismo día). Razón: si das bonus el mismo día, el driver puede
    haber gastado ya la quota y el regalo no se nota. Dándolo al día siguiente
    es un incentivo claro a volver a usar el importador mañana.

    Lógica:
    - Día N: driver contribuye → drivers.last_contribution_at = N
    - Día N+1: bonus activo (last_contribution_at = ayer UTC)
    - Día N+2 o posterior: bonus expirado, return 0
    """
    try:
        res = (
            supabase.table("drivers")
            .select("last_contribution_at")
            .eq("id", driver_id)
            .single()
            .execute()
        )
        last = (res.data or {}).get("last_contribution_at")
        if not last:
            return 0
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        last_date = last_dt.astimezone(timezone.utc).date()
        today = datetime.now(timezone.utc).date()
        # Bonus activo SOLO el día siguiente a la contribución.
        if (today - last_date).days == 1:
            return 10
        return 0
    except Exception:
        return 0


def check_ocr_image_quota(driver_id: str, tier: str, n_images: int):
    """Enforce a per-driver, per-day OCR image quota. Counts IMAGES, not requests.
    Raises 429 if accepting the new images would push the user over their tier's
    daily limit. Adds one timestamp per image so the rolling window math stays
    cheap (linear in current usage). Testing accounts bypass the gate."""
    if n_images <= 0:
        return
    if driver_id in _OCR_QUOTA_TESTING_BYPASS:
        return
    now = time.time()
    key = f"ocr_imgs:{driver_id}:daily"
    base_limit = _OCR_DAILY_IMG_QUOTA.get(tier, _OCR_DAILY_IMG_QUOTA["free"])
    bonus = _get_msi_bonus_today(driver_id)
    max_imgs = base_limit + bonus
    _rate_limits[key] = [t for t in _rate_limits[key] if t > now - _OCR_QUOTA_WINDOW]
    if len(_rate_limits[key]) + n_images > max_imgs:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "daily_image_quota_exceeded",
                "message": f"Límite diario alcanzado ({max_imgs} imágenes/día). Vuelve a probar mañana.",
                "tier": tier,
                "used": len(_rate_limits[key]),
                "limit": max_imgs,
            },
        )
    for _ in range(n_images):
        _rate_limits[key].append(now)


def _resolve_user_tier(auth_user_id: str) -> tuple[str, Optional[str]]:
    """Returns (tier, driver_id). Tier ∈ {'free','trial','pro','pro_yearly','pro_plus'}.
    Used for OCR quota enforcement on endpoints that don't require Pro+ gating
    (like /ocr/label, which free users can still hit a few times per day)."""
    try:
        d = supabase.table("drivers").select(
            "id, promo_plan, promo_plan_expires_at, subscription_source, subscription_period"
        ).eq("user_id", auth_user_id).single().execute()
        row = d.data or {}
    except Exception:
        return "free", None
    driver_id = row.get("id")
    promo = row.get("promo_plan")
    expires_raw = row.get("promo_plan_expires_at")
    sub_src = row.get("subscription_source")
    sub_period = row.get("subscription_period")
    if promo == "pro_plus" and sub_src in ("stripe", "revenuecat"):
        return "pro_plus", driver_id
    if sub_period == "yearly":
        return "pro_yearly", driver_id
    if promo == "pro" and sub_src in ("stripe", "revenuecat"):
        return "pro", driver_id
    # Trial: promo='pro'|'pro_plus' AND no paid subscription AND expires_at in future
    if promo in ("pro", "pro_plus") and sub_src is None and expires_raw:
        try:
            expires_at = datetime.fromisoformat(expires_raw.replace("Z", "+00:00"))
            if expires_at > datetime.now(timezone.utc):
                return "trial", driver_id
        except (ValueError, AttributeError):
            pass
    return "free", driver_id

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Apply rate limiting to sensitive endpoints"""
    path = request.url.path
    client_ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or (request.client.host if request.client else "unknown")
    try:
        if path.startswith("/admin"):
            check_rate_limit(f"admin:{client_ip}", max_requests=60, window_seconds=60)
        elif path.startswith("/auth") or path == "/promo/redeem":
            check_rate_limit(f"auth:{client_ip}", max_requests=20, window_seconds=60)
        elif path.startswith("/places"):
            check_rate_limit(f"places:{client_ip}", max_requests=30, window_seconds=60)
        elif path == "/optimize":
            check_rate_limit(f"optimize:{client_ip}", max_requests=10, window_seconds=60)
        elif path.startswith("/voice"):
            check_rate_limit(f"voice:{client_ip}", max_requests=30, window_seconds=60)
        elif path.startswith("/email"):
            check_rate_limit(f"email:{client_ip}", max_requests=20, window_seconds=60)
        elif path.startswith("/ocr"):
            # 5/min was set when /ocr only had /ocr/label (1 photo per
            # request). Now MSI fires up to 10 parallel /ocr/screenshots-batch
            # calls (one per photo with concurrency=4) so 5/min trips after
            # the first chunk wave. Per-user daily quota
            # (check_ocr_image_quota) is the real spend gate; this
            # middleware limit is just abuse-prevention. Bump it to a
            # number that comfortably fits a normal 10-photo import.
            check_rate_limit(f"ocr:{client_ip}", max_requests=60, window_seconds=60)
        elif path.startswith("/location"):
            check_rate_limit(f"location:{client_ip}", max_requests=60, window_seconds=60)
        elif path.startswith("/routes"):
            check_rate_limit(f"routes:{client_ip}", max_requests=30, window_seconds=60)
        elif path.startswith("/stops"):
            check_rate_limit(f"stops:{client_ip}", max_requests=30, window_seconds=60)
        elif path == "/fleet/login":
            check_rate_limit(f"fleet_login:{client_ip}", max_requests=5, window_seconds=60)
        elif path.startswith("/fleet"):
            check_rate_limit(f"fleet:{client_ip}", max_requests=30, window_seconds=60)
        elif path.startswith("/stripe"):
            check_rate_limit(f"stripe:{client_ip}", max_requests=20, window_seconds=60)
        elif path.startswith("/revenuecat"):
            check_rate_limit(f"revenuecat:{client_ip}", max_requests=20, window_seconds=60)
    except HTTPException as e:
        from starlette.responses import JSONResponse
        return JSONResponse(status_code=e.status_code, content={"detail": e.detail})
    return await call_next(request)


# === MODELOS ===

class Location(BaseModel):
    id: Optional[str] = None
    address: Optional[str] = None
    lat: float = Field(..., ge=-90, le=90)
    lng: float = Field(..., ge=-180, le=180)
    notes: Optional[str] = None
    phone: Optional[str] = None
    priority: Optional[int] = Field(default=0, ge=0, le=10)
    time_window_start: Optional[str] = None  # "HH:MM"
    time_window_end: Optional[str] = None    # "HH:MM"


class OptimizeRequest(BaseModel):
    locations: List[Location] = Field(..., min_length=1)
    start_index: Optional[int] = Field(default=0)
    round_trip: bool = Field(default=False, description="Deprecated: use strategy instead")
    strategy: Optional[str] = Field(default=None, description="nearest_first, farthest_first, businesses_first, round_trip")
    solver: Optional[str] = Field(default=None, description="Force solver: vroom, pyvrp, ortools")


class MultiVehicleOptimizeRequest(BaseModel):
    locations: List[Location] = Field(..., min_length=1)
    num_vehicles: int = Field(..., ge=1, le=50)
    depot_index: Optional[int] = Field(default=0)
    max_distance_per_vehicle_km: Optional[float] = None


class ClusterRequest(BaseModel):
    stops: List[Location] = Field(..., min_length=1)
    n_zones: Optional[int] = Field(default=None, ge=1, le=20)
    max_stops_per_zone: Optional[int] = Field(default=15, ge=5, le=50)


class ETARequest(BaseModel):
    current_lat: float
    current_lng: float
    destination_lat: float
    destination_lng: float
    avg_speed_kmh: Optional[float] = Field(default=30.0, ge=5, le=120)
    stop_time_minutes: Optional[float] = Field(default=5.0, ge=0, le=60)


class RouteETARequest(BaseModel):
    route: List[Location] = Field(..., min_length=1)
    start_lat: Optional[float] = None
    start_lng: Optional[float] = None
    avg_speed_kmh: Optional[float] = Field(default=30.0)
    stop_time_minutes: Optional[float] = Field(default=5.0)


class DriverInfo(BaseModel):
    id: str
    location: Optional[dict] = None  # {lat, lng}


class AssignDriversRequest(BaseModel):
    zones: List[dict]  # Output from cluster endpoint
    drivers: List[DriverInfo]
    driver_routes: Optional[dict] = {}  # driver_id -> pending_routes


class GeocodeRequest(BaseModel):
    address: str = Field(..., min_length=3)
    country: str | None = None  # ISO-2 (ES, MX, AR…) — biases search and adds components filter


class StopCreate(BaseModel):
    address: str
    lat: float
    lng: float
    position: int
    notes: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    time_window_start: Optional[str] = None
    time_window_end: Optional[str] = None
    packages: Optional[int] = None


class RouteCreate(BaseModel):
    driver_id: str
    name: Optional[str] = None
    stops: List[StopCreate]
    total_distance_km: Optional[float] = None


class LocationUpdate(BaseModel):
    driver_id: str
    route_id: Optional[str] = None
    lat: float
    lng: float
    speed: Optional[float] = None
    accuracy: Optional[float] = None


# -- Modelos de Email --

class WelcomeEmailRequest(BaseModel):
    to_email: str
    user_name: str


class DeliveryStartedEmailRequest(BaseModel):
    to_email: str
    client_name: str
    driver_name: str
    estimated_time: Optional[str] = None
    tracking_url: Optional[str] = None


class DeliveryCompletedEmailRequest(BaseModel):
    to_email: str
    client_name: str
    delivery_time: str
    photo_url: Optional[str] = None
    recipient_name: Optional[str] = None


class DeliveryFailedEmailRequest(BaseModel):
    to_email: str
    client_name: str
    reason: Optional[str] = None
    next_attempt: Optional[str] = None


class DailySummaryEmailRequest(BaseModel):
    to_email: str
    dispatcher_name: str
    date: str
    total_routes: int
    total_stops: int
    completed_stops: int
    failed_stops: int


class AdminSendEmailRequest(BaseModel):
    subject: str
    body: str  # HTML body


class AdminBroadcastEmailRequest(BaseModel):
    subject: str
    body: str  # HTML body
    target: str = "all"  # all, free, pro, pro_plus


class AdminPushBlastRequest(BaseModel):
    title: str
    body: str
    target: str = "inactive"  # "inactive" | "all"


class TrialFeedbackRequest(BaseModel):
    driver_id: str
    reason: Literal["price", "feature", "time", "competitor"]
    detail: Optional[str] = Field(default=None, max_length=2000)
    token: Optional[str] = Field(default=None, min_length=8, max_length=128)


class CustomerNotificationRequest(BaseModel):
    stop_id: Optional[str] = None
    route_id: Optional[str] = None
    alert_type: str  # upcoming, en_camino, entregado, failed
    customer_phone: Optional[str] = None
    customer_email: Optional[str] = None
    customer_name: Optional[str] = None
    driver_name: str
    stop_address: str
    tracking_url: Optional[str] = None
    eta_minutes: Optional[int] = None
    stops_away: Optional[int] = None
    photo_url: Optional[str] = None


# -- Modelos de respuesta --

class HealthCheckResponse(BaseModel):
    status: str = Field(..., description="Estado general: 'healthy' o 'degraded'")
    checks: dict = Field(..., description="Detalle de cada servicio verificado")


# -- Fleet Management Models --

class FleetZonePoint(BaseModel):
    lat: float
    lng: float

class FleetZoneCreate(BaseModel):
    name: str
    polygon: List[FleetZonePoint]
    color: str = "#8b5cf6"
    priority: int = 0

class FleetZoneUpdate(BaseModel):
    name: Optional[str] = None
    polygon: Optional[List[FleetZonePoint]] = None
    color: Optional[str] = None
    priority: Optional[int] = None
    active: Optional[bool] = None

class FleetMessageCreate(BaseModel):
    driver_id: str
    message: str


def _period_to_date_range(period: str) -> tuple[datetime, datetime]:
    """Convert period string to (start, end) datetime range."""
    now = datetime.now(timezone.utc)
    if period == "week":
        start = now - timedelta(days=7)
    elif period == "month":
        start = now - timedelta(days=30)
    else:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, now


async def _get_company_driver_ids(company_id: str) -> list[str]:
    """Get all driver IDs for a company, excluding admin/test accounts."""
    result = supabase.table("drivers").select("id").eq("company_id", company_id).eq("active", True).execute()
    return [d["id"] for d in (result.data or []) if d["id"] not in ADMIN_EXCLUDE_IDS]


# === ENDPOINTS BÁSICOS ===

@app.get("/", tags=["health"], summary="Estado del servicio")
async def root():
    """Devuelve el estado general del servicio."""
    return {
        "status": "ok",
        "service": "Xpedit API",
    }


APK_DOWNLOAD_URL = "https://github.com/direccion-bit2/xpedit-releases/releases/download/v1.1.4/xpedit-latest.apk"


@app.get("/download/apk", tags=["download"], summary="Descargar APK")
async def download_apk(request: Request):
    """Registra la descarga con un fingerprint único del dispositivo (IP+UA) y redirige al APK en GitHub Releases."""
    from fastapi.responses import RedirectResponse

    try:
        ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or (request.client.host if request.client else "unknown")
        ua = request.headers.get("user-agent", "unknown")
        fingerprint = hashlib.sha256(f"{ip}:{ua}".encode()).hexdigest()[:32]

        supabase.table("app_downloads").insert({
            "fingerprint": fingerprint,
            "ip_address": ip,
            "user_agent": ua[:500],
            "source": "web",
        }).execute()
    except Exception as e:
        logger.error(f"Download tracking error: {e}")
        sentry_sdk.capture_exception(e)

    return RedirectResponse(url=APK_DOWNLOAD_URL, status_code=302)


OSRM_URL = os.getenv("OSRM_URL", "http://router.project-osrm.org")
OSRM_MAX_RETRIES = 8  # Persist: better to wait than fall back to haversine
OSRM_RETRY_DELAYS = [1.5, 2.0, 2.0, 3.0, 3.0, 4.0, 5.0, 6.0]  # ~26s total wait max
OSRM_CHUNK_SIZE = 100  # Max sources per chunked request

# OpenRouteService — fallback regional para LATAM/US donde OSRM público no cubre.
# Free tier: 500 matrix req/día, max 50 ubicaciones por request.
# Se activa solo si ORS_API_KEY está set en env. Sin key → fallback Haversine.
# 18 may 2026: Daniel (MX, 100 stops Monterrey) reproduce OSRM 400 — los drivers
# LATAM llevan meses optimizando con Haversine sin calidad routing real.
ORS_API_KEY = os.getenv("ORS_API_KEY", "")
ORS_URL = "https://api.openrouteservice.org/v2/matrix/driving-car"
ORS_MAX_LOCATIONS = 50  # Free tier limit


async def _osrm_table_request(
    coords_str: str,
    sources_param: str = "",
    destinations_param: str = "",
    n_label: int = 0,
) -> dict | None:
    """Single OSRM table request with retries. Returns raw distances/durations or None."""
    import asyncio

    params = "annotations=duration,distance"
    if sources_param:
        params += f"&sources={sources_param}"
    if destinations_param:
        params += f"&destinations={destinations_param}"
    url = f"{OSRM_URL}/table/v1/driving/{coords_str}?{params}"

    for attempt in range(OSRM_MAX_RETRIES):
        try:
            if attempt > 0:
                delay = OSRM_RETRY_DELAYS[min(attempt, len(OSRM_RETRY_DELAYS) - 1)]
                logger.info(f"OSRM: waiting {delay}s before retry {attempt+1}/{OSRM_MAX_RETRIES} ({n_label} locs)")
                await asyncio.sleep(delay)

            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, timeout=30.0, headers={"User-Agent": "Xpedit/1.0"})

                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", 3.0))
                    logger.warning(f"OSRM rate limited (429), waiting {retry_after}s (attempt {attempt+1})")
                    await asyncio.sleep(retry_after)
                    continue

                if resp.status_code >= 500:
                    logger.warning(f"OSRM server error ({resp.status_code}), attempt {attempt+1}")
                    continue

                if resp.status_code >= 400:
                    logger.error(f"OSRM client error ({resp.status_code}) - cannot recover")
                    return None

                data = resp.json()
                if data.get("code") == "Ok" and data.get("distances") and data.get("durations"):
                    return data

                osrm_code = data.get("code", "Unknown")
                if osrm_code == "TooBig":
                    logger.warning(f"OSRM TooBig for {n_label} locs")
                    return None

                logger.warning(f"OSRM code '{osrm_code}', attempt {attempt+1}")
                continue

        except httpx.TimeoutException:
            logger.warning(f"OSRM timeout, attempt {attempt+1}")
        except Exception as e:
            logger.warning(f"OSRM error: {type(e).__name__}: {e}, attempt {attempt+1}")

    return None


async def _ors_matrix_request(locations: list) -> dict | None:
    """
    OpenRouteService Matrix API — fallback regional para zonas no cubiertas
    por OSRM público (LATAM, US, etc).

    Free tier: 500 matrix req/día, max 50 ubicaciones por request.
    Sin API key configurada → retorna None (caller cae a Haversine).

    18 may 2026: añadido tras incidente OSRM 400 con Daniel (MX, 100 stops
    Monterrey). LATAM llevaba meses con Haversine sin saberlo.
    """
    if not ORS_API_KEY:
        return None
    n = len(locations)
    if n > ORS_MAX_LOCATIONS:
        logger.info(f"ORS skipped: {n} locations > {ORS_MAX_LOCATIONS} free tier limit")
        return None

    body = {
        "locations": [[float(loc["lng"]), float(loc["lat"])] for loc in locations],
        "metrics": ["distance", "duration"],
        "units": "m",
    }
    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(ORS_URL, json=body, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                if "distances" in data and "durations" in data:
                    logger.info(f"ORS matrix OK: {n} locations")
                    return {
                        "distances": [[int(d) if d is not None else 999999 for d in row] for row in data["distances"]],
                        "durations": [[int(d) if d is not None else 999999 for d in row] for row in data["durations"]],
                    }
                logger.warning(f"ORS returned 200 but missing fields: {list(data.keys())}")
                return None
            if resp.status_code == 403:
                logger.warning("ORS 403 — quota exceeded or bad API key")
            elif resp.status_code == 429:
                logger.warning("ORS 429 rate limit")
            else:
                logger.warning(f"ORS {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        logger.warning(f"ORS request failed: {type(e).__name__}: {e}")
        return None


async def get_road_distance_matrix(locations: list) -> dict | None:
    """
    Obtiene matrices de distancias y duraciones reales por carretera.

    Flujo (18 may 2026):
    1. OSRM público — para España y Europa (cobertura full).
    2. ORS API (si OSRM falla y hay API key) — para LATAM/US y resto del mundo.
    3. None → caller (optimizer) cae a Haversine.

    La matriz de duraciones es ASIMÉTRICA (A→B ≠ B→A) por calles de un sentido.
    Retorna {"distances": [...], "durations": [...]} o None.
    """
    import asyncio

    n = len(locations)
    if n < 2 or n > 500:
        return None

    coords = ";".join(f"{loc['lng']},{loc['lat']}" for loc in locations)

    # Small enough for a single request
    if n <= OSRM_CHUNK_SIZE:
        data = await _osrm_table_request(coords, n_label=n)
        if not data:
            logger.warning(f"OSRM FAILED for {n} locations after all retries — trying ORS fallback")
            ors_data = await _ors_matrix_request(locations)
            if ors_data:
                return ors_data
            logger.error(f"Both OSRM and ORS failed for {n} locations — caller will fall back to Haversine")
            return None
        logger.info(f"OSRM road matrix OK: {n} locations")
        distances = [[int(d) if d is not None else 999999 for d in row] for row in data["distances"]]
        durations = [[int(d) if d is not None else 999999 for d in row] for row in data["durations"]]
        return {"distances": distances, "durations": durations}

    # Chunked: send all coords but request source rows in batches
    # OSRM sources/destinations params select which rows/columns to compute
    logger.info(f"OSRM chunked mode: {n} locations, chunk_size={OSRM_CHUNK_SIZE}")
    distances = [[0] * n for _ in range(n)]
    durations = [[0] * n for _ in range(n)]

    for chunk_start in range(0, n, OSRM_CHUNK_SIZE):
        chunk_end = min(chunk_start + OSRM_CHUNK_SIZE, n)
        sources_param = ";".join(str(i) for i in range(chunk_start, chunk_end))

        # Rate limit: wait 1.5s between chunks
        if chunk_start > 0:
            await asyncio.sleep(1.5)

        data = await _osrm_table_request(coords, sources_param=sources_param, n_label=n)
        if not data:
            logger.warning(f"OSRM chunk {chunk_start}-{chunk_end} FAILED for {n} locations")
            # ORS no soporta chunking en free tier (max 50 locations total) —
            # para rutas grandes en LATAM, caemos a Haversine. Caller maneja.
            return None

        # data["distances"] has (chunk_end - chunk_start) rows x n columns
        for local_i, row_dist in enumerate(data["distances"]):
            global_i = chunk_start + local_i
            distances[global_i] = [int(d) if d is not None else 999999 for d in row_dist]
        for local_i, row_dur in enumerate(data["durations"]):
            global_i = chunk_start + local_i
            durations[global_i] = [int(d) if d is not None else 999999 for d in row_dur]

    logger.info(f"OSRM chunked matrix OK: {n} locations in {(n + OSRM_CHUNK_SIZE - 1) // OSRM_CHUNK_SIZE} chunks")
    return {"distances": distances, "durations": durations}


@app.post("/optimize", tags=["optimize"], summary="Optimizar ruta")
async def optimize(request: OptimizeRequest, user=Depends(get_current_user)):
    """Calcula el orden óptimo de paradas para minimizar distancia/tiempo. Máximo 500 paradas."""
    if len(request.locations) > 500:
        raise HTTPException(status_code=400, detail="Máximo 500 paradas")

    locations_data = [loc.model_dump() for loc in request.locations]
    depot_index = request.start_index or 0

    # Determine strategy (new field takes precedence over deprecated round_trip)
    strategy = request.strategy or ("round_trip" if request.round_trip else "nearest_first")

    # Obtener distancias Y duraciones reales por carretera (OSRM)
    # La matriz de duraciones es ASIMÉTRICA: respeta calles de un sentido
    road_data = await get_road_distance_matrix(locations_data)
    road_matrix = road_data["distances"] if road_data else None
    duration_matrix = road_data["durations"] if road_data else None

    # For businesses_first: split into business and non-business stops, optimize separately
    if strategy == "businesses_first" and road_matrix:
        business_indices = [i for i, loc in enumerate(locations_data) if i != depot_index and loc.get("isBusiness")]
        non_business_indices = [i for i, loc in enumerate(locations_data) if i != depot_index and not loc.get("isBusiness")]

        if business_indices:
            # Phase 1: Optimize business stops (open-ended from depot)
            biz_locs = [locations_data[depot_index]] + [locations_data[i] for i in business_indices]
            biz_matrix_size = len(biz_locs)
            biz_idx_map = [depot_index] + business_indices
            biz_dist_matrix = [[road_matrix[biz_idx_map[r]][biz_idx_map[c]] for c in range(biz_matrix_size)] for r in range(biz_matrix_size)]
            biz_dur_matrix = [[duration_matrix[biz_idx_map[r]][biz_idx_map[c]] for c in range(biz_matrix_size)] for r in range(biz_matrix_size)]
            # Open-ended: zero return to depot
            for r in range(biz_matrix_size):
                biz_dist_matrix[r][0] = 0
                biz_dur_matrix[r][0] = 0
            biz_result = hybrid_optimize_route(biz_locs, 0, biz_dist_matrix, biz_dur_matrix)

            # Phase 2: Optimize non-business stops (open-ended from last business stop)
            if non_business_indices:
                last_biz = biz_result["route"][-1] if biz_result.get("success") and biz_result["route"] else locations_data[depot_index]
                last_biz_orig_idx = next((i for i, loc in enumerate(locations_data) if loc.get("id") == last_biz.get("id")), depot_index)
                non_biz_locs = [locations_data[last_biz_orig_idx]] + [locations_data[i] for i in non_business_indices]
                non_biz_size = len(non_biz_locs)
                non_biz_idx_map = [last_biz_orig_idx] + non_business_indices
                non_biz_dist_matrix = [[road_matrix[non_biz_idx_map[r]][non_biz_idx_map[c]] for c in range(non_biz_size)] for r in range(non_biz_size)]
                non_biz_dur_matrix = [[duration_matrix[non_biz_idx_map[r]][non_biz_idx_map[c]] for c in range(non_biz_size)] for r in range(non_biz_size)]
                for r in range(non_biz_size):
                    non_biz_dist_matrix[r][0] = 0
                    non_biz_dur_matrix[r][0] = 0
                non_biz_result = hybrid_optimize_route(non_biz_locs, 0, non_biz_dist_matrix, non_biz_dur_matrix)
                # Combine: depot + business route + non-business route (skip depot of each)
                combined_route = [locations_data[depot_index]]
                if biz_result.get("success"):
                    combined_route += biz_result["route"][1:]
                if non_biz_result.get("success"):
                    combined_route += non_biz_result["route"][1:]
                total_dist = (biz_result.get("total_distance_meters", 0) or 0) + (non_biz_result.get("total_distance_meters", 0) or 0)
                result = {
                    "success": True, "route": combined_route,
                    "total_distance_meters": total_dist,
                    "total_distance_km": round(total_dist / 1000, 2),
                    "num_stops": len(combined_route),
                    "solver": biz_result.get("solver", "pyvrp"),
                    "strategy": "businesses_first",
                    "has_time_windows": False,
                    "message": f"Negocios primero: {len(business_indices)} negocios + {len(non_business_indices)} particulares"
                }
            else:
                result = biz_result
                result["strategy"] = "businesses_first"
        else:
            strategy = "nearest_first"

    if strategy != "businesses_first":
        # Prepare effective matrices (zero return-to-depot for open-ended routes)
        eff_dist = road_matrix
        eff_dur = duration_matrix
        if strategy != "round_trip" and road_matrix:
            eff_dist = [row[:] for row in road_matrix]
            eff_dur = [row[:] for row in duration_matrix]
            for i in range(len(eff_dist)):
                eff_dist[i][depot_index] = 0
                eff_dur[i][depot_index] = 0

        # Allow forcing a specific solver for testing/comparison
        if request.solver == "vroom":
            from optimizer import solve_with_vroom
            result = solve_with_vroom(locations_data, depot_index, eff_dist, eff_dur)
        elif request.solver == "pyvrp":
            from optimizer import solve_with_pyvrp
            result = solve_with_pyvrp(locations_data, depot_index, eff_dist, eff_dur)
        elif request.solver == "ortools":
            result = optimize_route(locations_data, depot_index, distance_matrix=eff_dist, duration_matrix=eff_dur)
            result["solver"] = "ortools"
        else:
            result = hybrid_optimize_route(
                locations=locations_data,
                depot_index=depot_index,
                distance_matrix=eff_dist,
                duration_matrix=eff_dur,
            )

        # farthest_first: optimize then reverse (keep depot at start)
        if strategy == "farthest_first" and result.get("success") and len(result.get("route", [])) > 2:
            depot = result["route"][0]
            result["route"] = [depot] + list(reversed(result["route"][1:]))

        result["strategy"] = strategy

    if road_data:
        result["distance_source"] = "road"
    else:
        result["distance_source"] = "haversine"
        result["warning"] = "No se pudieron obtener distancias por carretera. La ruta es aproximada."

    return result


@app.post("/geocode", tags=["optimize"], summary="Geocodificar dirección")
async def geocode(request: GeocodeRequest, user=Depends(get_current_user)):
    """Convierte una dirección de texto en coordenadas (lat/lng) usando Google Geocoding API.

    Maneja business names (farmacias, hoteles), direcciones con sufijos raros
    y CPs parciales mucho mejor que Nominatim. Acepta `country` ISO-2 opcional
    para sesgar resultados al país del driver.
    """
    if not GOOGLE_API_KEY:
        raise HTTPException(status_code=503, detail="Geocoding service not configured")

    params = {
        "address": request.address,
        "language": "es",
        "key": GOOGLE_API_KEY,
    }
    if request.country:
        cc = request.country.strip().upper()
        if cc:
            params["region"] = cc.lower()
            params["components"] = f"country:{cc}"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                "https://maps.googleapis.com/maps/api/geocode/json",
                params=params,
                timeout=10.0,
            )
            data = response.json()
    except Exception as e:
        logger.error(f"Geocode error: {e}")
        raise HTTPException(status_code=502, detail="Geocoding service error")

    status = data.get("status")
    if status == "ZERO_RESULTS":
        return {"success": False, "error": "Dirección no encontrada"}
    if status != "OK" or not data.get("results"):
        logger.warning(f"Geocode returned status={status} for '{request.address[:80]}'")
        return {"success": False, "error": "Dirección no encontrada"}

    r = data["results"][0]
    geom = r.get("geometry", {}) or {}
    loc = geom.get("location", {}) or {}
    return {
        "success": True,
        "lat": loc.get("lat"),
        "lng": loc.get("lng"),
        "display_name": r.get("formatted_address", ""),
        "place_id": r.get("place_id", ""),
        "location_type": geom.get("location_type", ""),
    }


# === ENDPOINTS AVANZADOS DE OPTIMIZACIÓN ===

@app.post("/optimize-multi", tags=["optimize"], summary="Optimizar multi-vehículo")
async def optimize_multi(request: MultiVehicleOptimizeRequest, user=Depends(get_current_user)):
    """Optimiza rutas para múltiples vehículos (CVRP). Máximo 500 paradas y 50 vehículos."""
    if len(request.locations) > 500:
        raise HTTPException(status_code=400, detail="Máximo 500 paradas para multi-vehicle")

    locations_data = [loc.model_dump() for loc in request.locations]

    # Intentar obtener distancias reales por carretera (OSRM)
    road_matrix = await get_road_distance_matrix(locations_data)

    max_distance = None
    if request.max_distance_per_vehicle_km:
        max_distance = int(request.max_distance_per_vehicle_km * 1000)

    result = optimize_multi_vehicle(
        locations=locations_data,
        num_vehicles=request.num_vehicles,
        depot_index=request.depot_index or 0,
        max_distance_per_vehicle=max_distance,
        distance_matrix=road_matrix,
    )
    if road_matrix:
        result["distance_source"] = "road"
    else:
        result["distance_source"] = "haversine"
    return result


@app.post("/cluster-zones", tags=["optimize"], summary="Agrupar paradas en zonas")
async def cluster_zones(request: ClusterRequest, user=Depends(get_current_user)):
    """Agrupa paradas en zonas geográficas mediante clustering. Máximo 500 paradas."""
    if len(request.stops) > 500:
        raise HTTPException(status_code=400, detail="Máximo 500 paradas para clustering")

    stops_data = [stop.model_dump() for stop in request.stops]

    result = cluster_stops_by_zone(
        stops=stops_data,
        n_zones=request.n_zones,
        max_stops_per_zone=request.max_stops_per_zone or 15
    )
    return result


@app.post("/eta", tags=["optimize"], summary="Calcular ETA")
async def get_eta(request: ETARequest, user=Depends(get_current_user)):
    """Calcula el tiempo estimado de llegada (ETA) entre dos coordenadas."""
    result = calculate_eta(
        current_location=(request.current_lat, request.current_lng),
        destination=(request.destination_lat, request.destination_lng),
        avg_speed_kmh=request.avg_speed_kmh or 30.0,
        stop_time_minutes=request.stop_time_minutes or 5.0
    )
    return {"success": True, **result}


@app.post("/route-etas", tags=["optimize"], summary="ETAs de toda la ruta")
async def get_route_etas(request: RouteETARequest, user=Depends(get_current_user)):
    """Calcula ETAs acumuladas para todas las paradas de una ruta en orden."""
    route_data = [loc.model_dump() for loc in request.route]

    start_location = None
    if request.start_lat and request.start_lng:
        start_location = (request.start_lat, request.start_lng)

    result = calculate_route_etas(
        route=route_data,
        start_location=start_location,
        avg_speed_kmh=request.avg_speed_kmh or 30.0,
        stop_time_minutes=request.stop_time_minutes or 5.0
    )

    return {"success": True, "route": result, "num_stops": len(result)}


@app.post("/assign-drivers", tags=["optimize"], summary="Asignar conductores a zonas")
async def assign_drivers(request: AssignDriversRequest, user=Depends(get_current_user)):
    """Asigna conductores a zonas de forma inteligente basándose en ubicación y carga de trabajo."""
    drivers_data = [
        {
            "id": d.id,
            "location": d.location
        }
        for d in request.drivers
    ]

    result = assign_drivers_to_zones(
        zones=request.zones,
        drivers=drivers_data,
        driver_routes=request.driver_routes or {}
    )
    return {"success": True, **result}


@app.get("/stats/daily", tags=["routes"], summary="Estadísticas diarias")
async def get_daily_stats(company_id: Optional[str] = None, user=Depends(get_current_user)):
    """Obtiene estadísticas del día (rutas, paradas, distancia). Filtradas por permisos del usuario."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    try:
        # Obtener rutas filtradas por permisos
        query = supabase.table("routes").select("*, stops(*)")
        # Filter by today's date
        query = query.gte("created_at", f"{today}T00:00:00")
        if user["role"] == "admin":
            if company_id:
                query = query.eq("company_id", company_id)
        elif user["role"] == "dispatcher" and user.get("company_id"):
            company_drivers = supabase.table("drivers").select("id").eq("company_id", user.get("company_id")).execute()
            driver_ids = [d["id"] for d in (company_drivers.data or [])]
            if driver_ids:
                query = query.in_("driver_id", driver_ids)
            else:
                return {"success": True, "date": today, "routes": {"total": 0, "completed": 0, "pending": 0}, "stops": {"total": 0, "completed": 0, "failed": 0, "pending": 0}, "success_rate": 0, "total_distance_km": 0}
        else:
            user_driver_id = await get_user_driver_id(user)
            if not user_driver_id:
                return {"success": True, "date": today, "routes": {"total": 0, "completed": 0, "pending": 0}, "stops": {"total": 0, "completed": 0, "failed": 0, "pending": 0}, "success_rate": 0, "total_distance_km": 0}
            query = query.eq("driver_id", user_driver_id)

        routes_result = query.execute()
        routes = routes_result.data or []

        # Calcular estadísticas
        total_routes = len(routes)
        completed_routes = len([r for r in routes if r.get('status') == 'completed'])
        pending_routes = len([r for r in routes if r.get('status') != 'completed'])

        all_stops = []
        for route in routes:
            all_stops.extend(route.get('stops', []))

        total_stops = len(all_stops)
        completed_stops = len([s for s in all_stops if s.get('status') == 'completed'])
        failed_stops = len([s for s in all_stops if s.get('status') == 'failed'])
        pending_stops = total_stops - completed_stops - failed_stops

        success_rate = round((completed_stops / total_stops * 100) if total_stops > 0 else 0, 1)

        total_distance = sum(r.get('total_distance_km', 0) or 0 for r in routes)

        return {
            "success": True,
            "date": today,
            "routes": {
                "total": total_routes,
                "completed": completed_routes,
                "pending": pending_routes
            },
            "stops": {
                "total": total_stops,
                "completed": completed_stops,
                "failed": failed_stops,
                "pending": pending_stops
            },
            "success_rate": success_rate,
            "total_distance_km": round(total_distance, 1)
        }

    except Exception as e:
        logger.error(f"Stats error: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# === ENDPOINTS SUPABASE ===

# -- Conductores --

@app.get("/drivers", tags=["drivers"], summary="Listar conductores")
async def get_drivers(user=Depends(get_current_user)):
    """Lista conductores activos. Admin ve todos, dispatcher ve su empresa, driver ve solo él."""
    query = supabase.table("drivers").select("*").eq("active", True)
    if user["role"] == "admin":
        pass  # Admin sees all
    elif user["role"] == "dispatcher" and user.get("company_id"):
        query = query.eq("company_id", user.get("company_id"))
    else:
        query = query.eq("user_id", user["id"])
    result = query.execute()
    return {"drivers": result.data}


# Disposable email domains blocklist — prevent trial abuse with throwaway accounts
DISPOSABLE_EMAIL_DOMAINS = {
    "tempmail.com", "temp-mail.org", "guerrillamail.com", "guerrillamail.info",
    "guerrillamail.net", "guerrillamail.org", "sharklasers.com", "grr.la",
    "guerrillamailblock.com", "throwaway.email", "mailinator.com", "maildrop.cc",
    "dispostable.com", "yopmail.com", "yopmail.fr", "trashmail.com", "trashmail.net",
    "trashmail.me", "mailnesia.com", "mailnull.com", "tempail.com",
    "fakeinbox.com", "devnull.email", "discard.email", "discardmail.com",
    "emailondeck.com", "getnada.com", "harakirimail.com", "jetable.org",
    "mailcatch.com", "mailexpire.com", "mailforspam.com", "mohmal.com",
    "mytemp.email", "nomail.xl.cx", "spamgourmet.com", "tempmailaddress.com",
    "throwam.com", "tmpmail.net", "tmpmail.org", "trash-mail.com",
    "trashmail.ws", "uglymail.com", "wegwerfmail.de", "10minutemail.com",
    "20minutemail.com", "crazymailing.com", "disposableaddress.com",
    "emailisvalid.com", "inboxbear.com", "mailsac.com", "mintemail.com",
    "mt2015.com", "mx0.wwwnew.eu", "objectmail.com", "proxymail.eu",
    "rcpt.at", "rmqkr.net", "sharklasers.com", "spambox.us", "spamcero.com",
    "spamex.com", "spamspot.com", "superrito.com", "suremail.info",
    "thankyou2010.com", "thisisnotmyrealemail.com", "trashymail.com",
    "trashymail.net", "mailtemp.info", "tempinbox.com", "tempomail.fr",
    "temporarymail.org", "tempsky.com", "meltmail.com", "getairmail.com",
}


@app.post("/drivers/claim-trial", tags=["drivers"], summary="Reclamar trial gratuito")
async def claim_trial(request: Request, user=Depends(get_current_user)):
    """Grants 7-day Pro trial if device_id hasn't claimed one before.
    Checks device_id, IP abuse, and disposable email domains."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    device_id = (body.get("device_id") or "").strip()
    if not device_id or len(device_id) < 8:
        raise HTTPException(status_code=400, detail="Invalid device_id")

    # Get client IP (behind Railway proxy)
    client_ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or request.client.host if request.client else ""

    # Block disposable email domains
    user_email = user.get("email", "")
    email_domain = user_email.split("@")[-1].lower() if "@" in user_email else ""
    if email_domain in DISPOSABLE_EMAIL_DOMAINS:
        logger.warning(f"Trial denied: disposable email domain {email_domain} for user {user['id']}")
        return {"granted": False, "reason": "disposable_email"}

    # Get driver for this user
    driver_result = supabase.table("drivers").select("id, promo_plan").eq("user_id", user["id"]).single().execute()
    if not driver_result.data:
        raise HTTPException(status_code=404, detail="Driver not found")

    driver_id = driver_result.data["id"]

    # Already has a plan? Don't overwrite
    if driver_result.data.get("promo_plan"):
        return {"granted": False, "reason": "already_has_plan"}

    # Check if this device already claimed a trial
    existing = supabase.table("trial_claims").select("id, driver_id").eq("device_id", device_id).execute()
    if existing.data and len(existing.data) > 0:
        logger.info(f"Trial denied: device {device_id[:12]}... already claimed by driver {existing.data[0]['driver_id']}")
        return {"granted": False, "reason": "device_already_claimed"}

    # Check IP abuse: max 1 trial from same IP in 30 days
    if client_ip:
        thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        ip_claims = supabase.table("trial_claims").select("id").eq("ip", client_ip).gte("claimed_at", thirty_days_ago).execute()
        if ip_claims.data and len(ip_claims.data) >= 1:
            logger.warning(f"Trial denied: IP {client_ip} already has a claim in last 30 days")
            return {"granted": False, "reason": "ip_abuse_detected"}

    # Grant 7-day Pro trial
    expires_at = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
    supabase.table("drivers").update({
        "promo_plan": "pro",
        "promo_plan_expires_at": expires_at,
        "device_id": device_id,
    }).eq("id", driver_id).execute()

    # Record the claim with IP
    supabase.table("trial_claims").insert({
        "device_id": device_id,
        "driver_id": driver_id,
        "ip": client_ip or None,
    }).execute()

    # Also update users table
    supabase.table("users").update({
        "promo_plan": "pro",
        "promo_plan_expires_at": expires_at,
    }).eq("id", user["id"]).execute()

    logger.info(f"Trial granted: driver {driver_id}, device {device_id[:12]}..., IP {client_ip}, expires {expires_at}")
    return {"granted": True, "plan": "pro", "expires_at": expires_at}


@app.get("/drivers/{driver_id}", tags=["drivers"], summary="Obtener conductor")
async def get_driver(driver_id: str, user=Depends(get_current_user)):
    """Obtiene los datos de un conductor por ID. Verifica permisos de acceso."""
    await verify_driver_access(driver_id, user)
    result = supabase.table("drivers").select("*").eq("id", driver_id).single().execute()
    return result.data


# -- Rutas --

@app.get("/routes", tags=["routes"], summary="Listar rutas")
async def get_routes(driver_id: Optional[str] = None, date: Optional[str] = None, user=Depends(get_current_user)):
    """Lista rutas con sus paradas. Filtradas por propiedad del usuario y opcionalmente por conductor o fecha."""
    query = supabase.table("routes").select("*, stops(*)")

    if user["role"] == "admin":
        if driver_id:
            query = query.eq("driver_id", driver_id)
    elif user["role"] == "dispatcher" and user.get("company_id"):
        # Dispatcher: only routes from drivers in their company
        company_drivers = supabase.table("drivers").select("id").eq("company_id", user.get("company_id")).execute()
        company_driver_ids = [d["id"] for d in (company_drivers.data or [])]
        if driver_id:
            if driver_id not in company_driver_ids:
                raise HTTPException(status_code=403, detail="No tienes acceso a este conductor")
            query = query.eq("driver_id", driver_id)
        elif company_driver_ids:
            query = query.in_("driver_id", company_driver_ids)
        else:
            return {"routes": []}
    else:
        # Regular driver: only own routes
        user_driver_id = await get_user_driver_id(user)
        if not user_driver_id:
            return {"routes": []}
        if driver_id and driver_id != user_driver_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a estas rutas")
        query = query.eq("driver_id", user_driver_id)

    if date:
        query = query.eq("date", date)

    query = query.order("created_at", desc=True)
    result = query.execute()
    return {"routes": result.data}


@app.post("/routes", tags=["routes"], summary="Crear ruta")
async def create_route(route: RouteCreate, user=Depends(get_current_user)):
    """Crea una nueva ruta con sus paradas. El conductor debe ser el usuario autenticado (salvo admin)."""
    route_request = route  # Save original request before reassignment
    # Verify user can create route for this driver
    if user["role"] != "admin":
        user_driver_id = await get_user_driver_id(user)
        if route_request.driver_id != user_driver_id:
            raise HTTPException(status_code=403, detail="No puedes crear rutas para otro conductor")
    # Crear la ruta
    route_data = {
        "driver_id": route_request.driver_id,
        "name": route_request.name or f"Ruta {datetime.now(timezone.utc).strftime('%d/%m %H:%M')}",
        "total_distance_km": route_request.total_distance_km,
        "total_stops": len(route_request.stops),
        "status": "pending"
    }

    route_result = supabase.table("routes").insert(route_data).execute()
    route_row = safe_first(route_result)
    if not route_row:
        raise HTTPException(status_code=500, detail="Error al crear la ruta")
    route_id = route_row["id"]

    # Crear las paradas
    stops_data = [
        {
            "route_id": route_id,
            "address": stop.address,
            "lat": stop.lat,
            "lng": stop.lng,
            "position": stop.position,
            "notes": stop.notes,
            "phone": stop.phone,
            "email": stop.email,
            "time_window_start": stop.time_window_start,
            "time_window_end": stop.time_window_end,
            "packages": stop.packages,
        }
        for stop in route_request.stops
    ]

    # Enriquecer stops desde el directorio de clientes de la empresa
    try:
        driver_q = supabase.table("drivers").select("company_id").eq("id", route_request.driver_id).limit(1).execute()
        company_id = driver_q.data[0].get("company_id") if driver_q.data else None
        if company_id:
            stops_data, enriched = enrich_stops_from_directory(company_id, stops_data)
            if enriched:
                logger.info(f"Enriched {enriched} stops from customer directory")
    except Exception as e:
        logger.warning(f"Stop enrichment failed: {e}")
        sentry_sdk.capture_exception(e)

    stops_insert = supabase.table("stops").insert(stops_data).execute()
    if not stops_insert.data:
        logger.error(f"Failed to insert stops for route {route_id}")
        raise HTTPException(status_code=500, detail="Error al crear las paradas de la ruta")

    # Devolver ruta completa
    result = supabase.table("routes").select("*, stops(*)").eq("id", route_id).single().execute()
    return result.data


@app.get("/routes/{route_id}", tags=["routes"], summary="Obtener ruta")
async def get_route(route_id: str, user=Depends(get_current_user)):
    """Obtiene una ruta con todas sus paradas. Verifica permisos de acceso."""
    await verify_route_access(route_id, user)
    result = await asyncio.to_thread(
        lambda: supabase.table("routes").select("*, stops(*)").eq("id", route_id).single().execute()
    )
    return result.data


@app.patch("/routes/{route_id}/start", tags=["routes"], summary="Iniciar ruta")
async def start_route(route_id: str, user=Depends(get_current_user)):
    """Marca una ruta como 'in_progress' y registra la hora de inicio."""
    await verify_route_access(route_id, user)
    now_iso = datetime.now(timezone.utc).isoformat()
    result = await asyncio.to_thread(
        lambda: supabase.table("routes").update({
            "status": "in_progress",
            "started_at": now_iso,
        }).eq("id", route_id).execute()
    )
    route = safe_first(result)
    if not route:
        raise HTTPException(status_code=404, detail="Ruta no encontrada")
    return {"success": True, "route": route}


class ReconcileOptimizationBody(BaseModel):
    """Cliente envía los datos de optimización que tiene en local cuando
    detecta que BD los perdió (optimized_hash NULL en una ruta in_progress).
    Patrón rescate, similar a reconcileLocalVsRemoteStops.

    `force=True`: el cliente ACABA de optimizar y es la fuente de verdad
    fresca → saltar el guard hash_mismatch (autorizar sobrescritura).
    Solo lo manda el path post-optimize, nunca el rescate cold-start."""
    optimized_hash: str
    polyline_points: Optional[list] = None
    return_leg_polyline: Optional[list] = None
    snapped_waypoints: Optional[list] = None
    stops_order: Optional[list[dict]] = None  # [{stop_id, position}, ...]
    force: bool = False


@app.patch("/routes/{route_id}/reconcile-optimization", tags=["routes"], summary="Rescatar datos optimización desde cliente")
async def reconcile_route_optimization(
    route_id: str,
    body: ReconcileOptimizationBody,
    user=Depends(get_current_user),
):
    """Rescata datos de optimización que el cliente sí tiene en AsyncStorage
    pero la BD perdió (el UPDATE original entre `/optimize` cliente y el
    persist falló por crash, red, etc.). Idempotente — solo escribe si BD
    actualmente NO tiene optimized_hash. Si BD ya tiene un hash distinto,
    abortar (puede haber re-optimización más reciente).

    Limita el alcance a rutas in_progress o pending del propio driver
    para que el reconcile NO pueda alterar rutas completed/cancelled."""
    await verify_route_access(route_id, user)
    # Defensive: leer estado actual de la ruta
    existing = await asyncio.to_thread(
        lambda: supabase.table("routes")
        .select("id, status, optimized_hash")
        .eq("id", route_id)
        .limit(1)
        .single()
        .execute()
    )
    row = existing.data
    if not row:
        raise HTTPException(status_code=404, detail="Ruta no encontrada")
    if row.get("status") not in ("pending", "in_progress"):
        raise HTTPException(status_code=409, detail=f"Ruta en estado {row.get('status')}, no reconciliable")
    current_hash = row.get("optimized_hash")
    if current_hash and current_hash != body.optimized_hash and not body.force:
        return {"success": False, "reason": "hash_mismatch", "current_hash": current_hash}
    update_fields: dict = {"optimized_hash": body.optimized_hash}
    if body.polyline_points is not None:
        update_fields["polyline_points"] = body.polyline_points
    if body.return_leg_polyline is not None:
        update_fields["return_leg_polyline"] = body.return_leg_polyline
    if body.snapped_waypoints is not None:
        update_fields["snapped_waypoints"] = body.snapped_waypoints
    await asyncio.to_thread(
        lambda: supabase.table("routes").update(update_fields).eq("id", route_id).execute()
    )
    # Aplicar nuevo orden de stops si viene
    stops_updated = 0
    if body.stops_order:
        for item in body.stops_order:
            sid = item.get("stop_id")
            pos = item.get("position")
            if sid is None or pos is None:
                continue
            try:
                await asyncio.to_thread(
                    lambda s=sid, p=pos: supabase.table("stops").update({"position": p}).eq("id", s).eq("route_id", route_id).execute()
                )
                stops_updated += 1
            except Exception:
                continue
    return {"success": True, "stops_reordered": stops_updated, "polyline_persisted": body.polyline_points is not None}


@app.patch("/routes/{route_id}/complete", tags=["routes"], summary="Completar ruta")
async def complete_route(route_id: str, user=Depends(get_current_user)):
    """Marca una ruta como 'completed' y registra la hora de finalización.

    NO escribe deleted_at: una ruta finalizada debe aparecer en el
    historial del driver. El que la ruta no reaparezca como "activa" en
    cold start lo garantiza la app: loadSavedState valida la cache local
    exigiendo status IN ('pending','in_progress'), y loadActiveRouteFromCloud
    aplica el mismo filtro.

    Idempotente: si ya estaba completed, re-confirma estado y devuelve
    already_finalized=true. Diseñado server-side con service_role para
    evitar 42501 con JWT stale (Sentry REACT-NATIVE-30).
    """
    await verify_route_access(route_id, user)
    now_iso = datetime.now(timezone.utc).isoformat()
    result = await asyncio.to_thread(
        lambda: supabase.table("routes").update({
            "status": "completed",
            "completed_at": now_iso,
        }).eq("id", route_id).neq("status", "completed").execute()
    )
    route = safe_first(result)
    if not route:
        # Already completed (or doesn't exist). Idempotent path.
        existing = supabase.table("routes").select("id, status, completed_at, deleted_at").eq("id", route_id).limit(1).execute()
        if existing.data:
            return {"success": True, "route": existing.data[0], "already_finalized": True}
        raise HTTPException(status_code=404, detail="Ruta no encontrada")
    return {"success": True, "route": route}


@app.patch("/routes/{route_id}/clear", tags=["routes"], summary="Limpiar ruta (archivar sin marcarla completada)")
async def clear_route(route_id: str, user=Depends(get_current_user)):
    """Archiva una ruta sin marcarla como completada. Usado cuando el
    driver pulsa "limpiar ruta" para empezar de cero antes de terminar
    el reparto, o cuando descarta una ruta importada por error.

    Server-side soft-delete con service_role: evita 42501 cuando el JWT
    del cliente está stale. La cascada a stops la hace el trigger
    trg_soft_delete_route_stops automáticamente.

    Idempotente: si la ruta ya está archivada, devuelve 200 con
    `already_archived=true` para que la app limpie local-state sin error.
    """
    await verify_route_access(route_id, user)
    now_iso = datetime.now(timezone.utc).isoformat()
    result = await asyncio.to_thread(
        lambda: supabase.table("routes").update({
            "status": "cancelled",
            "deleted_at": now_iso,
        }).eq("id", route_id).is_("deleted_at", None).execute()
    )
    route = safe_first(result)
    if not route:
        existing = supabase.table("routes").select("id, status, deleted_at").eq("id", route_id).limit(1).execute()
        if existing.data:
            return {"success": True, "route": existing.data[0], "already_archived": True}
        raise HTTPException(status_code=404, detail="Ruta no encontrada")

    # Count stops cascaded by the trigger so the client can show a
    # confirmation toast ("ruta y 7 paradas archivadas").
    stops_count = supabase.table("stops").select("id", count="exact").eq("route_id", route_id).not_.is_("deleted_at", None).execute()
    return {
        "success": True,
        "route": route,
        "stops_cleared": stops_count.count or 0,
    }


@app.delete("/routes/{route_id}", tags=["routes"], summary="Eliminar ruta")
async def delete_route(route_id: str, user=Depends(get_current_user)):
    """Elimina una ruta y todas sus dependencias (paradas, tracking, pruebas de entrega)."""
    await verify_route_access(route_id, user)
    # Get stop IDs for this route
    stops_result = supabase.table("stops").select("id").eq("route_id", route_id).execute()
    stop_ids = [s["id"] for s in (stops_result.data or [])]

    if stop_ids:
        # Delete delivery proofs linked to these stops (batch)
        supabase.table("delivery_proofs").delete().in_("stop_id", stop_ids).execute()

        # Delete tracking links for this route
        supabase.table("tracking_links").delete().eq("route_id", route_id).execute()

        # Delete stops
        supabase.table("stops").delete().eq("route_id", route_id).execute()

    # Delete the route itself
    supabase.table("routes").delete().eq("id", route_id).execute()
    return {"success": True}


# -- Paradas --

@app.patch("/stops/{stop_id}/complete", tags=["stops"], summary="Completar parada")
async def complete_stop(stop_id: str, user=Depends(get_current_user)):
    """Marca una parada como 'completed' y registra la hora."""
    await verify_stop_access(stop_id, user)
    now_iso = datetime.now(timezone.utc).isoformat()
    result = await asyncio.to_thread(
        lambda: supabase.table("stops").update({
            "status": "completed",
            "completed_at": now_iso,
        }).eq("id", stop_id).execute()
    )
    stop = safe_first(result)
    if not stop:
        raise HTTPException(status_code=404, detail="Parada no encontrada")
    return {"success": True, "stop": stop}


@app.patch("/stops/{stop_id}/fail", tags=["stops"], summary="Marcar parada fallida")
async def fail_stop(stop_id: str, user=Depends(get_current_user)):
    """Marca una parada como 'failed' y registra la hora."""
    await verify_stop_access(stop_id, user)
    now_iso = datetime.now(timezone.utc).isoformat()
    result = await asyncio.to_thread(
        lambda: supabase.table("stops").update({
            "status": "failed",
            "completed_at": now_iso,
        }).eq("id", stop_id).execute()
    )
    stop = safe_first(result)
    if not stop:
        raise HTTPException(status_code=404, detail="Parada no encontrada")
    return {"success": True, "stop": stop}


class StopDeleteRequest(BaseModel):
    # Cliente puede mandar dbId conocido, o (route_id + client_id) cuando la
    # INSERT inicial aún no ha confirmado el dbId (offline queue drain path).
    stop_id: Optional[str] = None
    route_id: Optional[str] = None
    client_id: Optional[str] = None
    position: Optional[int] = None


@app.post("/stops/delete", tags=["stops"], summary="Soft-delete parada (bypass RLS)")
async def soft_delete_stop(body: StopDeleteRequest, user=Depends(get_current_user)):
    # Soft-delete con service_role: la RLS de stops rechaza el UPDATE de
    # deleted_at por cliente (la SELECT policy filtra deleted_at IS NULL y
    # Postgres la aplica al row nuevo). Aquí saltamos esa restricción y
    # validamos ownership manualmente vía verify_stop_access.
    resolved_id = body.stop_id
    if not resolved_id:
        if not body.route_id or not body.client_id:
            # Filosofía best-effort: si llega una op legacy sin identificadores
            # suficientes, NO devolvemos 400 (el cliente lo trataría como
            # PERMANENT_PG_CODE y dropearía la op sin guardarla en ningún sitio).
            # En su lugar devolvemos 200 + skipped:true para que el drain marque
            # la op como done y aprendamos del breadcrumb en Sentry. La op se
            # perdería igual con 400, pero al menos no llenamos Sentry de
            # excepciones para algo irrecuperable.
            return {"success": True, "skipped": True, "reason": "missing_identifiers"}
        # Resolver por (route_id, client_id) — UNIQUE en stops
        lookup = await asyncio.to_thread(
            lambda: supabase.table("stops").select("id")
                .eq("route_id", body.route_id)
                .eq("client_id", body.client_id)
                .is_("deleted_at", "null")
                .limit(1).execute()
        )
        row = safe_first(lookup)
        if not row and body.position is not None:
            lookup2 = await asyncio.to_thread(
                lambda: supabase.table("stops").select("id")
                    .eq("route_id", body.route_id)
                    .eq("position", body.position)
                    .is_("deleted_at", "null")
                    .limit(1).execute()
            )
            row = safe_first(lookup2)
        if not row:
            # Idempotencia: si la stop ya está borrada o nunca existió,
            # devolver success para que el drain marque la op como done.
            return {"success": True, "already_deleted": True}
        resolved_id = row["id"]

    await verify_stop_access(resolved_id, user)
    now_iso = datetime.now(timezone.utc).isoformat()
    result = await asyncio.to_thread(
        lambda: supabase.table("stops").update({"deleted_at": now_iso})
            .eq("id", resolved_id).is_("deleted_at", "null").execute()
    )
    stop = safe_first(result)
    if not stop:
        # Ya borrado por otra petición concurrente. Idempotente.
        return {"success": True, "already_deleted": True, "stop_id": resolved_id}
    return {"success": True, "stop_id": resolved_id}


# -- Push Token --

class PushTokenUpdate(BaseModel):
    push_token: str


@app.put("/drivers/{driver_id}/push-token", tags=["drivers"], summary="Save push notification token")
async def update_push_token(driver_id: str, body: PushTokenUpdate, user=Depends(get_current_user)):
    """Save Expo push token for a driver. Uses service role to bypass RLS."""
    # Verify the driver belongs to the authenticated user
    driver = supabase.table("drivers").select("id, user_id").eq("id", driver_id).execute()
    if not driver.data:
        raise HTTPException(status_code=404, detail="Driver not found")
    if driver.data[0]["user_id"] != user["id"]:
        raise HTTPException(status_code=403, detail="Not your driver profile")

    supabase.table("drivers").update({"push_token": body.push_token}).eq("id", driver_id).execute()
    return {"success": True}


# -- GPS Tracking --

@app.post("/location", tags=["tracking"], summary="Registrar ubicación")
async def update_location(location: LocationUpdate, user=Depends(get_current_user)):
    """Registra la ubicación GPS actual del conductor. Fuerza el driver_id del usuario autenticado.

    Best-effort: si el user autenticado no tiene fila en drivers todavía (race
    condition tras signup RC webhook, edge case auth), creamos la fila aquí
    con datos mínimos en lugar de devolver 400. Sin esto, drivers iOS recién
    sign-up que mandan GPS desde el primer minuto perderían tracking entero
    (cada ping → 400 → 0 location_history → admin no los ve nunca).
    """
    # Force driver_id to be the authenticated user's driver
    user_driver_id = await get_user_driver_id(user)
    if not user_driver_id:
        try:
            new_driver = await asyncio.to_thread(
                lambda: supabase.table("drivers").insert({
                    "user_id": user["id"],
                    "email": user.get("email"),
                    "name": (user.get("email") or "Driver").split("@")[0],
                }).execute()
            )
            row = safe_first(new_driver)
            if row:
                user_driver_id = row["id"]
                logger.warning(
                    "auto-created driver row for user_id=%s in POST /location (best-effort)",
                    user["id"],
                )
            else:
                raise HTTPException(status_code=500, detail="No se pudo crear perfil de conductor")
        except HTTPException:
            raise
        except Exception as e:
            # Race condition: otro POST /location del mismo user lo creó entre
            # nuestro SELECT y nuestro INSERT. Re-leer.
            user_driver_id = await get_user_driver_id(user)
            if not user_driver_id:
                logger.error("Failed auto-create driver for user_id=%s: %s", user["id"], e)
                raise HTTPException(status_code=500, detail="Error al asegurar perfil de conductor") from e
    if user["role"] != "admin" and location.driver_id != user_driver_id:
        raise HTTPException(status_code=403, detail="No puedes registrar ubicación de otro conductor")
    data = {
        "driver_id": user_driver_id if user["role"] != "admin" else location.driver_id,
        "route_id": location.route_id,
        "lat": location.lat,
        "lng": location.lng,
        "speed": location.speed,
        "accuracy": location.accuracy
    }

    # asyncio.to_thread: location pings llegan cada ~15s × N drivers — el endpoint
    # más caliente. Sync supabase aquí bloquea el event loop bajo carga.
    result = await asyncio.to_thread(
        lambda: supabase.table("location_history").insert(data).execute()
    )
    location = safe_first(result)
    if not location:
        raise HTTPException(status_code=500, detail="Error al registrar ubicación")
    return {"success": True, "id": location["id"]}


@app.get("/location/{driver_id}/latest", tags=["tracking"], summary="Última ubicación")
async def get_latest_location(driver_id: str, user=Depends(get_current_user)):
    """Obtiene la última ubicación GPS conocida de un conductor."""
    await verify_driver_access(driver_id, user)
    result = supabase.table("location_history")\
        .select("*")\
        .eq("driver_id", driver_id)\
        .order("recorded_at", desc=True)\
        .limit(1)\
        .execute()

    if not result.data:
        return {"success": False, "error": "Sin ubicación registrada"}

    return {"success": True, "location": result.data[0]}


@app.get("/location/{driver_id}/history", tags=["tracking"], summary="Historial de ubicaciones")
async def get_location_history(driver_id: str, route_id: Optional[str] = None, limit: int = Query(default=100, ge=1, le=1000), user=Depends(get_current_user)):
    """Obtiene el historial de ubicaciones GPS de un conductor. Se puede filtrar por ruta."""
    await verify_driver_access(driver_id, user)
    query = supabase.table("location_history")\
        .select("*")\
        .eq("driver_id", driver_id)

    if route_id:
        query = query.eq("route_id", route_id)

    result = query.order("recorded_at", desc=True).limit(limit).execute()
    return {"locations": result.data}


# === EMAILS ===

@app.post("/email/welcome", tags=["email"], summary="Email de bienvenida")
async def api_send_welcome_email(request: WelcomeEmailRequest, user=Depends(get_current_user)):
    """Envía email de bienvenida a nuevo usuario."""
    result = send_welcome_email(request.to_email, request.user_name)
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "Error enviando email"))
    return result


@app.post("/email/delivery-started", tags=["email"], summary="Email entrega en camino")
async def api_send_delivery_started_email(request: DeliveryStartedEmailRequest, user=Depends(get_current_user)):
    """Envía email al cliente notificando que su pedido está en camino."""
    result = send_delivery_started_email(
        request.to_email,
        request.client_name,
        request.driver_name,
        request.estimated_time,
        request.tracking_url
    )
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "Error enviando email"))
    return result


@app.post("/email/delivery-completed", tags=["email"], summary="Email entrega completada")
async def api_send_delivery_completed_email(request: DeliveryCompletedEmailRequest, user=Depends(get_current_user)):
    """Envía email de confirmación de entrega exitosa al cliente."""
    result = send_delivery_completed_email(
        request.to_email,
        request.client_name,
        request.delivery_time,
        request.photo_url,
        request.recipient_name
    )
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "Error enviando email"))
    return result


@app.post("/email/delivery-failed", tags=["email"], summary="Email entrega fallida")
async def api_send_delivery_failed_email(request: DeliveryFailedEmailRequest, user=Depends(get_current_user)):
    """Envía email al cliente notificando que la entrega ha fallado."""
    result = send_delivery_failed_email(
        request.to_email,
        request.client_name,
        request.reason,
        request.next_attempt
    )
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "Error enviando email"))
    return result


@app.post("/notifications/customer/send", tags=["notifications"], summary="Enviar notificacion al cliente")
async def api_send_customer_notification(request: CustomerNotificationRequest, user=Depends(get_current_user)):
    """Envía notificación automática al cliente (email). Registra en customer_notifications."""
    sent_via = []

    # Enviar email si hay email del cliente
    if request.customer_email:
        try:
            client_name = request.customer_name or ""
            if request.alert_type == "upcoming":
                result = send_upcoming_email(
                    request.customer_email, client_name, request.driver_name,
                    request.stops_away or 3, request.tracking_url
                )
            elif request.alert_type == "en_camino":
                eta_text = f"~{request.eta_minutes} minutos" if request.eta_minutes else None
                result = send_delivery_started_email(
                    request.customer_email, client_name, request.driver_name,
                    eta_text, request.tracking_url
                )
            elif request.alert_type == "entregado":
                result = send_delivery_completed_email(
                    request.customer_email, client_name,
                    datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M")
                )
            elif request.alert_type == "failed":
                result = send_delivery_failed_email(
                    request.customer_email, client_name
                )
            else:
                result = {"success": False, "error": f"Unknown alert_type: {request.alert_type}"}

            if result.get("success"):
                sent_via.append("email")
        except Exception as e:
            logger.warning(f"Failed to send customer email: {e}")
            sentry_sdk.capture_exception(e)

    # Registrar en customer_notifications
    notification_id = None
    if request.stop_id:
        try:
            driver_id = await get_user_driver_id(user)
            notif = supabase.table("customer_notifications").insert({
                "stop_id": request.stop_id,
                "route_id": request.route_id,
                "driver_id": driver_id,
                "alert_type": request.alert_type,
                "phone": request.customer_phone or "",
                "message": f"[{request.alert_type}] {request.stop_address}",
            }).execute()
            if notif.data:
                notification_id = notif.data[0].get("id")
        except Exception as e:
            logger.warning(f"Failed to log customer notification: {e}")
            sentry_sdk.capture_exception(e)

    return {"sent_via": sent_via, "notification_id": notification_id}


class EnrichExistingRequest(BaseModel):
    company_id: str


@app.post("/customer-directory/enrich-existing", tags=["directory"], summary="Enriquecer paradas existentes")
async def enrich_existing_stops(request: EnrichExistingRequest, user=Depends(get_current_user)):
    """Enriquece paradas de hoy con datos del directorio de clientes de la empresa."""
    if user["role"] not in ("admin", "dispatcher"):
        raise HTTPException(status_code=403, detail="Solo admin o dispatcher")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    drivers = supabase.table("drivers").select("id").eq("company_id", request.company_id).execute()
    driver_ids = [d["id"] for d in (drivers.data or [])]
    if not driver_ids:
        return {"enriched": 0}
    routes = supabase.table("routes").select("id").in_("driver_id", driver_ids).gte("created_at", today).execute()
    route_ids = [r["id"] for r in (routes.data or [])]
    if not route_ids:
        return {"enriched": 0}
    stops = supabase.table("stops").select("id, address, lat, lng, phone, email").in_("route_id", route_ids).execute()
    stops_data = stops.data or []
    enriched_stops, match_count = enrich_stops_from_directory(request.company_id, stops_data)
    # Group stops by the {phone,email} fields they need so we can do one UPDATE
    # per distinct field-set instead of one UPDATE per stop. Previous N+1 made
    # this endpoint take ~30s per 100 stops and hold the single uvicorn worker
    # the whole time. Same correctness, ~50× faster wall time.
    from collections import defaultdict
    groups: dict[tuple, list[str]] = defaultdict(list)
    for stop in enriched_stops:
        if not stop.get("id"):
            continue
        phone = stop.get("phone") or None
        email = stop.get("email") or None
        if not phone and not email:
            continue
        groups[(phone, email)].append(stop["id"])
    updated = 0
    for (phone, email), ids in groups.items():
        fields = {}
        if phone:
            fields["phone"] = phone
        if email:
            fields["email"] = email
        result = supabase.table("stops").update(fields).in_("id", ids).execute()
        updated += len(result.data or ids)
    return {"enriched": updated}


@app.post("/email/daily-summary", tags=["email"], summary="Email resumen diario")
async def api_send_daily_summary_email(request: DailySummaryEmailRequest, user=Depends(get_current_user)):
    """Envía resumen diario de actividad al dispatcher."""
    result = send_daily_summary_email(
        request.to_email,
        request.dispatcher_name,
        request.date,
        request.total_routes,
        request.total_stops,
        request.completed_stops,
        request.failed_stops
    )
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "Error enviando email"))
    return result


# --- Admin email endpoints ---

@app.post("/admin/users/{user_id}/send-email", tags=["admin", "email"], summary="Enviar email a usuario")
async def admin_send_email_to_user(user_id: str, request: AdminSendEmailRequest, user=Depends(require_admin)):
    """Envía un email personalizado a un usuario específico. Solo admin."""
    try:
        driver = supabase.table("drivers").select("email, name").eq("id", user_id).single().execute()
        if not driver.data:
            raise HTTPException(status_code=404, detail="User not found")

        result = send_custom_email(driver.data["email"], request.subject, request.body)
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "Error enviando email"))

        # Log email
        try:
            supabase.table("email_log").insert({
                "recipient_email": driver.data["email"],
                "recipient_name": driver.data.get("name"),
                "subject": request.subject,
                "body": request.body,
                "message_id": result.get("id"),
                "status": "sent",
            }).execute()
        except Exception:
            pass

        log_audit(user["id"], "send_email", "driver", user_id, {"subject": request.subject, "recipient": driver.data["email"]})
        return {"success": True, "email": driver.data["email"], "message_id": result.get("id")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error enviando email")


@app.post("/admin/broadcast-email", tags=["admin", "email"], summary="Broadcast email")
async def admin_broadcast_email(request: AdminBroadcastEmailRequest, user=Depends(require_admin)):
    """Envía un email masivo a todos los usuarios o filtrado por plan (free, pro, pro_plus). Solo admin."""
    try:
        query = supabase.table("drivers").select("email, name, promo_plan")

        if request.target == "free":
            query = query.is_("promo_plan", "null")
        elif request.target == "pro":
            query = query.eq("promo_plan", "pro")
        elif request.target == "pro_plus":
            query = query.eq("promo_plan", "pro_plus")

        drivers = query.execute()

        if not drivers.data:
            return {"success": True, "sent": 0, "failed": 0, "total": 0}

        emails = [d["email"] for d in drivers.data if d.get("email")]
        results = send_broadcast_email(emails, request.subject, request.body)

        # Log all emails in broadcast (batch insert)
        try:
            email_logs = [
                {
                    "recipient_email": d["email"],
                    "recipient_name": d.get("name"),
                    "subject": request.subject,
                    "body": request.body,
                    "sent_by": f"broadcast:{request.target}",
                    "status": "sent",
                }
                for d in drivers.data if d.get("email")
            ]
            if email_logs:
                supabase.table("email_log").insert(email_logs).execute()
        except Exception:
            pass

        log_audit(user["id"], "broadcast_email", "email", None, {"subject": request.subject, "target": request.target, "total": len(emails), "sent": results["sent"]})
        return {
            "success": True,
            "total": len(emails),
            "sent": results["sent"],
            "failed": results["failed"],
        }
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error enviando broadcast")


@app.post("/admin/reengagement-broadcast", tags=["admin", "email"], summary="Re-engagement broadcast")
async def admin_reengagement_broadcast(user=Depends(require_admin)):
    """Envía email de re-engagement a todos los usuarios con email, excluyendo admin/test."""
    EXCLUDED_IDS = [
        "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b",  # direccion@taespack.com
        "e481de53-bb8c-4b76-8b56-04a7d00f9c6f",  # migue995@gmail.com
    ]
    try:
        drivers = supabase.table("drivers").select("id, email, name").not_.is_("email", "null").execute()
        if not drivers.data:
            return {"success": True, "sent": 0, "failed": 0, "total": 0}

        targets = [d for d in drivers.data if d["id"] not in EXCLUDED_IDS and d.get("email")]
        results = send_reengagement_broadcast(targets)

        try:
            email_logs = [
                {
                    "recipient_email": d["email"],
                    "recipient_name": d.get("name"),
                    "subject": "¡Hemos mejorado Xpedit! Mira las novedades",
                    "body": "re-engagement broadcast",
                    "sent_by": "broadcast:reengagement",
                    "status": "sent",
                }
                for d in targets
            ]
            if email_logs:
                supabase.table("email_log").insert(email_logs).execute()
        except Exception:
            pass

        log_audit(user["id"], "reengagement_broadcast", "email", None, {"total": len(targets), "sent": results["sent"]})
        return {
            "success": True,
            "total": len(targets),
            "sent": results["sent"],
            "failed": results["failed"],
        }
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error enviando re-engagement broadcast")


@app.post("/admin/broadcast-social-login", tags=["admin", "email"], summary="Social login announcement broadcast")
async def admin_broadcast_social_login(user=Depends(require_admin)):
    """Envia email anunciando social login (Google + Apple) a todos los usuarios. Solo admin."""
    EXCLUDED_IDS = [
        "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b",  # direccion@taespack.com
        "e481de53-bb8c-4b76-8b56-04a7d00f9c6f",  # migue995@gmail.com
    ]
    try:
        drivers = supabase.table("drivers").select("id, email, name").not_.is_("email", "null").execute()
        if not drivers.data:
            return {"success": True, "sent": 0, "failed": 0, "total": 0}

        targets = [d for d in drivers.data if d["id"] not in EXCLUDED_IDS and d.get("email")]
        results = send_social_login_broadcast(targets)

        try:
            email_logs = [
                {
                    "recipient_email": d["email"],
                    "recipient_name": d.get("name"),
                    "subject": "Nuevo: inicia sesion con Google o Apple",
                    "body": "social-login broadcast",
                    "sent_by": "broadcast:social-login",
                    "status": "sent",
                }
                for d in targets
            ]
            if email_logs:
                supabase.table("email_log").insert(email_logs).execute()
        except Exception:
            pass

        log_audit(user["id"], "social_login_broadcast", "email", None, {"total": len(targets), "sent": results["sent"]})
        return {
            "success": True,
            "total": len(targets),
            "sent": results["sent"],
            "failed": results["failed"],
        }
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error enviando social login broadcast")


# ========== REACTIVATION CAMPAIGN (25 Apr 2026 — persistence-fix) ==========

REACTIVATION_CAMPAIGN_ID = "reactivation_25apr_persistence_fix"
REACTIVATION_INACTIVE_DAYS = 5  # users with last session >5 days ago
REACTIVATION_TEST_DRIVER_IDS = [
    "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b",  # direccion@taespack.com
    "e481de53-bb8c-4b76-8b56-04a7d00f9c6f",  # migue995@gmail.com
]
REACTIVATION_PUSH_TITLE = "Hemos arreglado lo de las paradas"
REACTIVATION_PUSH_BODY = "Vuelve a Xpedit. App rediseñada, mapas más rápidos y paradas que ya se guardan siempre."


def _reactivation_audience_query(days_inactive: int):
    """Base query for the reactivation audience. Returns Supabase result.

    Audience: drivers who used the app at some point, have email, and last session
    is older than days_inactive. Excludes internal accounts.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_inactive)).isoformat()
    return (
        supabase.table("drivers")
        .select("id, email, name, push_token, session_started_at, promo_plan, subscription_source")
        .not_.is_("email", "null")
        .not_.is_("session_started_at", "null")
        .lt("session_started_at", cutoff)
        .execute()
    )


def _reactivation_filter_internal(drivers: list) -> list:
    return [d for d in drivers if d["id"] not in REACTIVATION_TEST_DRIVER_IDS and d.get("email")]


def _reactivation_pick_channel(driver: dict, requested: str) -> str:
    """Decide channel for one driver. 'auto' picks push if token, email otherwise."""
    has_push = bool(driver.get("push_token") and str(driver["push_token"]).startswith("ExponentPushToken["))
    if requested == "push":
        return "push" if has_push else "skip"
    if requested == "email":
        return "email"
    # auto
    return "push" if has_push else "email"


class ReactivationPreviewRequest(BaseModel):
    days_inactive: int = Field(default=REACTIVATION_INACTIVE_DAYS, ge=1, le=365)


class ReactivationSendRequest(BaseModel):
    mode: Literal["test", "real"] = "test"
    channel: Literal["auto", "push", "email"] = "auto"
    days_inactive: int = Field(default=REACTIVATION_INACTIVE_DAYS, ge=1, le=365)
    limit: Optional[int] = Field(default=None, ge=1, le=10000)
    campaign: str = Field(default=REACTIVATION_CAMPAIGN_ID, max_length=64)
    dry_run: bool = False


@app.post("/admin/reactivation/preview", tags=["admin", "reactivation"], summary="Preview audience for the persistence-fix reactivation campaign")
async def admin_reactivation_preview(body: ReactivationPreviewRequest, user=Depends(require_admin)):
    """Returns audience count + breakdown without sending anything."""
    try:
        result = _reactivation_audience_query(body.days_inactive)
        targets = _reactivation_filter_internal(result.data or [])
        with_push = [d for d in targets if d.get("push_token") and str(d["push_token"]).startswith("ExponentPushToken[")]
        without_push = [d for d in targets if d not in with_push]
        sample = [{"name": d.get("name"), "email": d["email"], "channel": "push" if d in with_push else "email"} for d in targets[:5]]
        return {
            "total": len(targets),
            "with_push": len(with_push),
            "without_push": len(without_push),
            "campaign": REACTIVATION_CAMPAIGN_ID,
            "days_inactive_threshold": body.days_inactive,
            "sample": sample,
        }
    except Exception as e:
        logger.error(f"reactivation preview: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error generating preview")


@app.post("/admin/reactivation/send", tags=["admin", "reactivation"], summary="Send the persistence-fix reactivation campaign")
async def admin_reactivation_send(body: ReactivationSendRequest, user=Depends(require_admin)):
    """Send reactivation push or email. mode=test only sends to internal test drivers (Miguel)."""
    if body.mode not in ("test", "real"):
        raise HTTPException(status_code=400, detail="mode must be 'test' or 'real'")
    if body.channel not in ("auto", "push", "email"):
        raise HTTPException(status_code=400, detail="channel must be 'auto', 'push' or 'email'")

    try:
        if body.mode == "test":
            # Solo destinatarios de prueba (Miguel). Bypass exclusion.
            test_query = (
                supabase.table("drivers")
                .select("id, email, name, push_token, session_started_at")
                .in_("id", REACTIVATION_TEST_DRIVER_IDS)
                .execute()
            )
            targets = test_query.data or []
        else:
            result = _reactivation_audience_query(body.days_inactive)
            targets = _reactivation_filter_internal(result.data or [])

        if body.limit is not None and body.limit > 0:
            targets = targets[: body.limit]

        if body.dry_run:
            return {
                "dry_run": True,
                "would_send": len(targets),
                "campaign": body.campaign,
                "mode": body.mode,
            }

        sent_push = 0
        sent_email = 0
        failed = 0
        skipped = 0

        for d in targets:
            channel = _reactivation_pick_channel(d, body.channel)
            if channel == "skip":
                skipped += 1
                continue

            # Pre-insert log row (status=queued). UPSERT-style: skip if already sent.
            existing = (
                supabase.table("reactivation_log")
                .select("id, status")
                .eq("driver_id", d["id"]).eq("channel", channel).eq("campaign", body.campaign)
                .limit(1).execute()
            )
            if existing.data and existing.data[0].get("status") in ("sent", "opened"):
                skipped += 1
                continue

            log_row = {
                "driver_id": d["id"],
                "channel": channel,
                "campaign": body.campaign,
                "status": "queued",
                "session_at_send": d.get("session_started_at"),
            }

            ok = False
            error_msg = None
            resend_id = None

            if channel == "push":
                ok = await send_push_to_token(
                    d["push_token"],
                    REACTIVATION_PUSH_TITLE,
                    REACTIVATION_PUSH_BODY,
                    data={"campaign": body.campaign, "deeplink": "xpedit://"},
                )
                if not ok:
                    error_msg = "expo push returned error"
            else:  # email
                result = send_reactivation_persistence_email(d["email"], d.get("name") or "")
                ok = bool(result.get("success"))
                resend_id = result.get("id")
                if not ok:
                    error_msg = result.get("error", "unknown email error")

            log_row["status"] = "sent" if ok else "failed"
            log_row["error"] = error_msg
            log_row["resend_id"] = resend_id

            try:
                if existing.data:
                    supabase.table("reactivation_log").update(log_row).eq("id", existing.data[0]["id"]).execute()
                else:
                    supabase.table("reactivation_log").insert(log_row).execute()
            except Exception as log_err:
                logger.warning(f"reactivation_log write failed for driver {d['id']}: {log_err}")

            if ok:
                if channel == "push":
                    sent_push += 1
                else:
                    sent_email += 1
            else:
                failed += 1

        log_audit(
            user["id"],
            "reactivation_send",
            "campaign",
            body.campaign,
            {"mode": body.mode, "sent_push": sent_push, "sent_email": sent_email, "failed": failed, "skipped": skipped},
        )

        return {
            "success": True,
            "campaign": body.campaign,
            "mode": body.mode,
            "total_targets": len(targets),
            "sent_push": sent_push,
            "sent_email": sent_email,
            "failed": failed,
            "skipped": skipped,
        }
    except Exception as e:
        logger.error(f"reactivation send: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"Error en envío: {type(e).__name__}")


@app.post("/admin/push-blast")
async def admin_push_blast(request: AdminPushBlastRequest, user=Depends(require_admin)):
    """Send push notifications to drivers. target='inactive' = drivers with push_token but 0 routes, 'all' = all with push_token."""
    import asyncio

    try:
        # Get all drivers with push tokens
        drivers_result = supabase.table("drivers").select("id, name, push_token").not_.is_("push_token", "null").execute()
        if not drivers_result.data:
            return {"success": True, "sent": 0, "failed": 0, "total": 0, "message": "No drivers with push tokens"}

        targets = drivers_result.data

        if request.target == "inactive":
            # Filter to drivers with 0 routes - only fetch distinct driver_ids, not all rows
            target_ids = [d["id"] for d in targets]
            routes_result = supabase.table("routes").select("driver_id").in_("driver_id", target_ids).execute()
            drivers_with_routes = {r["driver_id"] for r in (routes_result.data or []) if r.get("driver_id")}
            targets = [d for d in targets if d["id"] not in drivers_with_routes]

        if not targets:
            return {"success": True, "sent": 0, "failed": 0, "total": 0, "message": "No matching drivers"}

        # Send pushes in parallel
        results = await asyncio.gather(*[
            send_push_to_token(d["push_token"], request.title, request.body)
            for d in targets
        ])

        sent = sum(1 for r in results if r)
        failed = sum(1 for r in results if not r)
        logger.info(f"Push blast ({request.target}): {sent} sent, {failed} failed out of {len(targets)}")

        log_audit(user["id"], "push_blast", "notification", None, {"target": request.target, "title": request.title, "sent": sent, "failed": failed})
        return {"success": True, "sent": sent, "failed": failed, "total": len(targets)}
    except Exception as e:
        logger.error(f"Push blast error: {e}")
        raise HTTPException(status_code=500, detail="Error enviando push blast")


# === SURVEY / ENGAGEMENT ENDPOINTS ===

class SurveyCampaignRequest(BaseModel):
    title: str
    description: Optional[str] = None
    questions: list = []
    target: str = "all"  # all, free, trial_expired, inactive, pro


class SurveySendRequest(BaseModel):
    campaign_id: str
    channels: list = ["push", "email"]  # which channels to use


@app.post("/admin/survey/create", tags=["admin", "survey"])
async def admin_create_survey(request: SurveyCampaignRequest, user=Depends(require_admin)):
    """Create a new survey campaign (draft status)."""
    result = supabase.table("survey_campaigns").insert({
        "title": request.title,
        "description": request.description,
        "questions": request.questions,
        "target": request.target,
        "status": "draft",
    }).execute()
    if not result.data:
        raise HTTPException(status_code=500, detail="Failed to create survey campaign")
    log_audit(user["id"], "survey_create", "survey", result.data[0]["id"], {"title": request.title})
    return {"success": True, "campaign": result.data[0]}


@app.post("/admin/survey/send", tags=["admin", "survey"])
async def admin_send_survey(request: SurveySendRequest, user=Depends(require_admin)):
    """Send push + email for a survey campaign to target drivers."""
    import asyncio

    # Get campaign
    campaign = supabase.table("survey_campaigns").select("*").eq("id", request.campaign_id).single().execute()
    if not campaign.data:
        raise HTTPException(status_code=404, detail="Campaign not found")

    target = campaign.data.get("target", "all")

    # Get target drivers
    query = supabase.table("drivers").select("id, name, email, push_token")
    if target == "free":
        query = query.is_("promo_plan", "null").is_("subscription_source", "null")
    elif target == "trial_expired":
        query = query.is_("promo_plan", "null").is_("subscription_source", "null")
    elif target == "pro":
        query = query.not_.is_("subscription_source", "null")
    drivers_result = query.execute()

    if not drivers_result.data:
        return {"success": True, "push_sent": 0, "email_sent": 0, "message": "No matching drivers"}

    drivers = drivers_result.data
    push_sent = 0
    push_failed = 0
    email_sent = 0
    email_failed = 0
    campaign_id = request.campaign_id
    survey_url = f"https://www.xpedit.es/survey/{campaign_id}"

    # Send pushes
    if "push" in request.channels:
        push_targets = [d for d in drivers if d.get("push_token")]
        if push_targets:
            results = await asyncio.gather(*[
                send_push_to_token(
                    d["push_token"],
                    "Xpedit se ha actualizado",
                    "Hemos corregido errores y mejorado el rendimiento. Cuentanos tu experiencia en 30 seg.",
                    data={"url": f"{survey_url}?driver={d['id']}"}
                )
                for d in push_targets
            ])
            push_sent = sum(1 for r in results if r)
            push_failed = sum(1 for r in results if not r)

    # Send emails
    if "email" in request.channels:
        email_targets = [d for d in drivers if d.get("email")]
        for d in email_targets:
            # Check email_log to avoid duplicates
            existing = supabase.table("email_log").select("id").eq("driver_id", d["id"]).eq("type", f"survey_{campaign_id}").execute()
            if existing.data:
                continue
            result = send_survey_email(d["email"], d.get("name", ""), campaign_id, d["id"])
            if result.get("success"):
                email_sent += 1
                supabase.table("email_log").insert({
                    "recipient": d["email"],
                    "subject": "Hemos mejorado Xpedit - queremos escucharte",
                    "type": f"survey_{campaign_id}",
                    "status": "sent",
                    "driver_id": d["id"],
                    "metadata": {"campaign_id": campaign_id},
                }).execute()
            else:
                email_failed += 1

    # Update campaign stats
    supabase.table("survey_campaigns").update({
        "status": "active",
        "push_sent_at": datetime.utcnow().isoformat() if "push" in request.channels else None,
        "email_sent_at": datetime.utcnow().isoformat() if "email" in request.channels else None,
        "push_sent_count": push_sent,
        "email_sent_count": email_sent,
    }).eq("id", campaign_id).execute()

    log_audit(user["id"], "survey_send", "survey", campaign_id, {
        "push_sent": push_sent, "push_failed": push_failed,
        "email_sent": email_sent, "email_failed": email_failed,
    })

    return {
        "success": True,
        "push_sent": push_sent, "push_failed": push_failed,
        "email_sent": email_sent, "email_failed": email_failed,
        "total_drivers": len(drivers),
    }


@app.get("/admin/survey/responses/{campaign_id}", tags=["admin", "survey"])
async def admin_get_survey_responses(campaign_id: str, user=Depends(require_admin)):
    """Get all responses for a survey campaign with driver info."""
    # Get campaign
    campaign = supabase.table("survey_campaigns").select("*").eq("id", campaign_id).single().execute()

    # Get responses with driver info
    responses = supabase.table("survey_responses").select("*").eq("campaign_id", campaign_id).order("created_at", desc=True).execute()

    # Enrich with driver info
    enriched = []
    driver_ids = [r["driver_id"] for r in (responses.data or []) if r.get("driver_id")]
    drivers_map = {}
    if driver_ids:
        drivers_result = supabase.table("drivers").select("id, name, email, promo_plan, subscription_source").in_("id", driver_ids).execute()
        drivers_map = {d["id"]: d for d in (drivers_result.data or [])}

    for r in (responses.data or []):
        driver = drivers_map.get(r.get("driver_id"), {})
        enriched.append({
            **r,
            "driver_name": driver.get("name", "Anonimo"),
            "driver_email": driver.get("email"),
            "driver_plan": driver.get("subscription_source") or driver.get("promo_plan") or "free",
        })

    # Aggregate stats
    answers = [r.get("answers", {}) for r in (responses.data or [])]
    q1_counts = {}
    q2_counts = {}
    ratings = []
    for a in answers:
        q1 = a.get("why_not_using")
        q2 = a.get("what_would_convert")
        if q1:
            q1_counts[q1] = q1_counts.get(q1, 0) + 1
        if q2:
            q2_counts[q2] = q2_counts.get(q2, 0) + 1
    for r in (responses.data or []):
        if r.get("rating"):
            ratings.append(r["rating"])

    return {
        "campaign": campaign.data,
        "responses": enriched,
        "stats": {
            "total_responses": len(responses.data or []),
            "avg_rating": round(sum(ratings) / len(ratings), 1) if ratings else None,
            "q1_breakdown": q1_counts,
            "q2_breakdown": q2_counts,
        },
    }


@app.get("/admin/survey/campaigns", tags=["admin", "survey"])
async def admin_list_campaigns(user=Depends(require_admin)):
    """List all survey campaigns with response counts."""
    campaigns = supabase.table("survey_campaigns").select("*").order("created_at", desc=True).execute()

    result = []
    for c in (campaigns.data or []):
        count = supabase.table("survey_responses").select("id", count="exact").eq("campaign_id", c["id"]).execute()
        result.append({**c, "response_count": count.count or 0})

    return {"campaigns": result}


# === PROMO CODE MODELS ===

class PromoRedeemRequest(BaseModel):
    code: str
    user_id: str


class PromoCodeCreateRequest(BaseModel):
    code: str
    description: Optional[str] = None
    benefit_type: str = "free_days"
    benefit_value: int = Field(..., ge=1)
    benefit_plan: str = "pro_plus"
    max_uses: Optional[int] = None
    expires_at: Optional[str] = None


class PromoCodeUpdateRequest(BaseModel):
    active: Optional[bool] = None
    max_uses: Optional[int] = None
    description: Optional[str] = None
    expires_at: Optional[str] = None


class AdminGrantRequest(BaseModel):
    plan: str
    days: int = Field(0, ge=0)
    permanent: bool = False


# === PROMO CODE ENDPOINTS ===

@app.post("/promo/redeem", tags=["promo"], summary="Canjear código promo")
async def redeem_promo_code(request: PromoRedeemRequest, user=Depends(get_current_user)):
    """Canjea un código promocional. Valida expiración, usos máximos y que no se haya canjeado antes."""
    try:
        # Use authenticated user's ID instead of request body
        user_id = user["id"]

        # 1. Find the promo code
        code_result = supabase.table("promo_codes")\
            .select("*")\
            .eq("code", request.code.strip().upper())\
            .execute()

        if not code_result.data:
            raise HTTPException(status_code=404, detail="Promo code not found")

        promo = code_result.data[0]

        # 2. Validate: is active
        if not promo.get("active", False):
            raise HTTPException(status_code=400, detail="This promo code is no longer active")

        # 3. Validate: not expired
        if promo.get("expires_at"):
            expires_at = datetime.fromisoformat(promo["expires_at"].replace("Z", "+00:00"))
            if datetime.now(timezone.utc) > expires_at:
                raise HTTPException(status_code=400, detail="This promo code has expired")

        # 4. Validate: max_uses not exceeded
        if promo.get("max_uses") is not None:
            if promo.get("current_uses", 0) >= promo["max_uses"]:
                raise HTTPException(status_code=400, detail="This promo code has reached its maximum number of uses")

        # 5. Validate: user hasn't already redeemed this code
        existing = supabase.table("code_redemptions")\
            .select("id")\
            .eq("code_id", promo["id"])\
            .eq("user_id", user_id)\
            .execute()

        if existing.data:
            raise HTTPException(status_code=400, detail="You have already redeemed this promo code")

        # 6. Calculate benefit expiration
        now = datetime.now(timezone.utc)
        benefit_expires_at = now + timedelta(days=promo["benefit_value"])
        benefit_expires_at_iso = benefit_expires_at.isoformat()

        # 7. Atomically increment current_uses (prevents race condition)
        supabase.rpc("atomic_increment_uses", {"p_table": "promo_codes", "p_id": promo["id"]}).execute()

        # 8. Create code_redemption record
        supabase.table("code_redemptions").insert({
            "code_id": promo["id"],
            "user_id": user_id,
            "redeemed_at": now.isoformat(),
            "benefit_expires_at": benefit_expires_at_iso
        }).execute()

        # 9. Update drivers table with promo plan
        supabase.table("drivers").update({
            "promo_plan": promo["benefit_plan"],
            "promo_plan_expires_at": benefit_expires_at_iso
        }).eq("user_id", user_id).execute()

        return {
            "success": True,
            "benefit": promo["benefit_plan"],
            "expires_at": benefit_expires_at_iso,
            "message": f"Promo code redeemed! You have {promo['benefit_plan']} for {promo['benefit_value']} days."
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.get("/promo/check/{driver_id}", tags=["promo"], summary="Verificar beneficio promo")
async def check_promo_benefit(driver_id: str, user=Depends(get_current_user)):
    """Verifica si un conductor tiene un beneficio promo activo. Solo datos propios o admin."""
    # Verify ownership: look up driver and check user_id matches authenticated user
    driver_check = supabase.table("drivers").select("user_id").eq("id", driver_id).single().execute()
    if not driver_check.data:
        raise HTTPException(status_code=404, detail="Driver no encontrado")
    if user["role"] != "admin" and user["id"] != driver_check.data["user_id"]:
        raise HTTPException(status_code=403, detail="No tienes acceso a estos datos")
    try:
        result = supabase.table("drivers")\
            .select("promo_plan, promo_plan_expires_at, is_ambassador")\
            .eq("id", driver_id)\
            .single()\
            .execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="User not found")

        driver = result.data
        promo_plan = driver.get("promo_plan")
        expires_at_str = driver.get("promo_plan_expires_at")

        is_ambassador = driver.get("is_ambassador", False)

        if not promo_plan:
            return {
                "has_promo": False,
                "plan": None,
                "expires_at": None,
                "days_remaining": 0,
                "permanent": False,
                "is_ambassador": is_ambassador
            }

        # Permanent plan (no expiration date)
        if not expires_at_str:
            return {
                "has_promo": True,
                "plan": promo_plan,
                "expires_at": None,
                "days_remaining": -1,
                "permanent": True,
                "is_ambassador": is_ambassador
            }

        expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        remaining = expires_at - now
        days_remaining = max(0, remaining.days)

        has_promo = days_remaining > 0

        return {
            "has_promo": has_promo,
            "plan": promo_plan if has_promo else None,
            "expires_at": expires_at_str if has_promo else None,
            "days_remaining": days_remaining,
            "permanent": False,
            "is_ambassador": is_ambassador
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# === ADMIN ENDPOINTS ===

@app.get("/admin/promo-codes", tags=["admin", "promo"], summary="Listar códigos promo")
async def list_promo_codes(user=Depends(require_admin)):
    """Lista todos los códigos promocionales con estadísticas de uso. Solo admin."""
    try:
        result = supabase.table("promo_codes")\
            .select("*")\
            .order("created_at", desc=True)\
            .execute()

        return {"success": True, "promo_codes": result.data}

    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.post("/admin/promo-codes", tags=["admin", "promo"], summary="Crear código promo")
async def create_promo_code(request: PromoCodeCreateRequest, user=Depends(require_admin)):
    """Crea un nuevo código promocional con beneficio, usos máximos y expiración. Solo admin."""
    try:
        # Check if code already exists
        existing = supabase.table("promo_codes")\
            .select("id")\
            .eq("code", request.code.strip().upper())\
            .execute()

        if existing.data:
            raise HTTPException(status_code=400, detail="A promo code with this code already exists")

        data = {
            "code": request.code.strip().upper(),
            "description": request.description,
            "benefit_type": request.benefit_type,
            "benefit_value": request.benefit_value,
            "benefit_plan": request.benefit_plan,
            "max_uses": request.max_uses,
            "expires_at": request.expires_at,
            "active": True,
            "current_uses": 0
        }

        result = supabase.table("promo_codes").insert(data).execute()
        promo_code = safe_first(result)
        if not promo_code:
            raise HTTPException(status_code=500, detail="Error al crear promo code")

        log_audit(user["id"], "create_promo_code", "promo_code", promo_code.get("id"), {"code": request.code.strip().upper(), "plan": request.benefit_plan, "value": request.benefit_value})
        return {"success": True, "promo_code": promo_code}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.patch("/admin/promo-codes/{code_id}", tags=["admin", "promo"], summary="Actualizar código promo")
async def update_promo_code(code_id: str, request: PromoCodeUpdateRequest, user=Depends(require_admin)):
    """Actualiza un código promocional existente (activo, usos máximos, descripción, expiración). Solo admin."""
    try:
        # Build update dict with only provided fields
        update_data = {}
        if request.active is not None:
            update_data["active"] = request.active
        if request.max_uses is not None:
            update_data["max_uses"] = request.max_uses
        if request.description is not None:
            update_data["description"] = request.description
        if request.expires_at is not None:
            update_data["expires_at"] = request.expires_at

        if not update_data:
            raise HTTPException(status_code=400, detail="No fields to update")

        result = supabase.table("promo_codes")\
            .update(update_data)\
            .eq("id", code_id)\
            .execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="Promo code not found")

        log_audit(user["id"], "update_promo_code", "promo_code", code_id, update_data)
        return {"success": True, "promo_code": safe_first(result)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.get("/admin/users", tags=["admin"], summary="Listar usuarios")
async def list_admin_users(user=Depends(require_admin)):
    """Lista todos los usuarios/conductores con su estado de plan promo. Solo admin."""
    try:
        result = supabase.table("drivers")\
            .select("*")\
            .order("created_at", desc=True)\
            .execute()

        return {"success": True, "users": result.data}

    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.patch("/admin/users/{user_id}/grant", tags=["admin"], summary="Otorgar plan a usuario")
async def grant_plan(user_id: str, request: AdminGrantRequest, user=Depends(require_admin)):
    """Otorga un plan (pro/pro_plus) a un usuario, temporal o permanente. Envía email de notificación. Solo admin."""
    try:
        if request.plan == "free":
            # Remove plan
            update_data = {"promo_plan": None, "promo_plan_expires_at": None}
            message = "Plan removed, set to free."
            expires_at_iso = None
        elif request.permanent:
            # Permanent plan - no expiration
            update_data = {"promo_plan": request.plan, "promo_plan_expires_at": None}
            message = f"Granted permanent {request.plan} to user."
            expires_at_iso = None
        else:
            # Temporary plan with days
            if request.days <= 0:
                raise HTTPException(status_code=400, detail="Days must be > 0 for temporary plans")
            now = datetime.now(timezone.utc)
            expires_at = now + timedelta(days=request.days)
            expires_at_iso = expires_at.isoformat()
            update_data = {"promo_plan": request.plan, "promo_plan_expires_at": expires_at_iso}
            message = f"Granted {request.days} days of {request.plan} to user."

        result = supabase.table("drivers").update(update_data).eq("id", user_id).execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="User not found")

        # Send email notification (fire and forget)
        if request.plan != "free":
            try:
                # Fetch driver email/name (update result may not include them)
                driver_data = supabase.table("drivers").select("email, name").eq("id", user_id).single().execute()
                driver_email = driver_data.data.get("email") if driver_data.data else None
                driver_name = driver_data.data.get("name", "Usuario") if driver_data.data else "Usuario"

                if driver_email:
                    plan_label = "Pro+" if request.plan == "pro_plus" else "Pro"
                    email_result = send_plan_activated_email(
                        driver_email,
                        driver_name,
                        plan_label,
                        days=request.days if not request.permanent else None,
                        permanent=request.permanent
                    )
                    # Log email
                    if email_result.get("success"):
                        try:
                            supabase.table("email_log").insert({
                                "recipient_email": driver_email,
                                "recipient_name": driver_name,
                                "subject": f"Plan {plan_label} activado",
                                "body": f"Plan {plan_label} {'permanente' if request.permanent else f'{request.days} dias'}",
                                "message_id": email_result.get("id"),
                                "status": "sent",
                            }).execute()
                        except Exception:
                            pass
            except Exception as e:
                logger.warning(f"Could not send plan email: {e}")
                sentry_sdk.capture_exception(e)

        log_audit(user["id"], "grant_plan", "driver", user_id, {"plan": request.plan, "days": request.days, "permanent": request.permanent})
        return {
            "success": True,
            "user_id": user_id,
            "plan": request.plan,
            "permanent": request.permanent,
            "expires_at": expires_at_iso,
            "days": request.days,
            "message": message
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


class AdminResetPasswordRequest(BaseModel):
    password: Optional[str] = None  # If None, generate random


@app.post("/admin/users/{user_id}/reset-password", tags=["admin"], summary="Resetear contraseña")
async def admin_reset_password(user_id: str, request: AdminResetPasswordRequest, user=Depends(require_admin)):
    """Resetea la contraseña de un usuario. Genera una aleatoria si no se proporciona. Solo admin."""
    try:
        # Generate random password if not provided
        chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789!@#"
        new_password = request.password or "".join(random.choices(chars, k=12))

        if len(new_password) < 8:
            raise HTTPException(status_code=400, detail="La contraseña debe tener al menos 8 caracteres")
        if not any(c.isupper() for c in new_password):
            raise HTTPException(status_code=400, detail="La contraseña debe incluir al menos una mayúscula")
        if not any(c.isdigit() for c in new_password):
            raise HTTPException(status_code=400, detail="La contraseña debe incluir al menos un número")

        # Update password via Supabase Admin API
        result = supabase.auth.admin.update_user_by_id(user_id, {"password": new_password})

        if not result:
            raise HTTPException(status_code=404, detail="User not found")

        # Mark user to force password change on next login
        driver = supabase.table("drivers").select("id, email, name").eq("user_id", user_id).execute()
        if driver.data:
            supabase.table("drivers").update({"must_change_password": True}).eq("id", driver.data[0]["id"]).execute()

        # Send email with new password (best-effort)
        email_sent = False
        if driver.data and driver.data[0].get("email"):
            try:
                from emails import send_password_reset_email
                email_result = send_password_reset_email(
                    driver.data[0]["email"],
                    driver.data[0].get("name", ""),
                    new_password
                )
                email_sent = email_result.get("success", False)
            except Exception as email_err:
                logger.warning(f"Failed to send password reset email: {email_err}")
                sentry_sdk.capture_exception(email_err)

        driver_name = driver.data[0].get("name", "") if driver.data else ""
        driver_email = driver.data[0].get("email", "") if driver.data else ""
        log_audit(user["id"], "reset_password", "user", user_id, {"target_name": driver_name, "target_email": driver_email, "email_sent": email_sent})
        return {
            "success": True,
            "user_id": user_id,
            "email_sent": email_sent,
            "message": "Password reset successfully. The new password was sent by email." if email_sent else "Password reset successfully. Email could not be sent — check the user's email address.",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


class AdminCreateCompanyRequest(BaseModel):
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    payment_model: str = "driver_pays"


@app.post("/admin/companies", tags=["admin", "company"], summary="Crear empresa (admin)")
async def admin_create_company(request: AdminCreateCompanyRequest, user=Depends(require_admin)):
    """Crea una empresa desde el panel de admin con suscripción trial de 7 días."""
    try:
        result = supabase.table("companies").insert({
            "name": request.name,
            "email": request.email,
            "phone": request.phone,
            "payment_model": request.payment_model,
            "active": True,
        }).execute()

        company = safe_first(result)
        if not company:
            raise HTTPException(status_code=500, detail="Failed to create company")

        # Create trial subscription
        supabase.table("company_subscriptions").insert({
            "company_id": company["id"],
            "plan": "free",
            "max_drivers": 15,
            "price_per_month": 0,
            "status": "trialing",
            "trial_ends_at": (datetime.now(timezone.utc) + timedelta(days=7)).isoformat(),
            "current_period_start": datetime.now(timezone.utc).isoformat(),
            "current_period_end": (datetime.now(timezone.utc) + timedelta(days=7)).isoformat(),
        }).execute()

        log_audit(user["id"], "create_company", "company", company["id"], {"name": request.name, "payment_model": request.payment_model})
        return {"success": True, "company": company}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


class CompanyToggleRequest(BaseModel):
    active: Optional[bool] = None


@app.patch("/admin/companies/{company_id}", tags=["admin", "company"], summary="Activar/desactivar empresa")
async def admin_toggle_company(company_id: str, request: CompanyToggleRequest, user=Depends(require_admin)):
    """Activa o desactiva una empresa. Solo admin."""
    try:
        update_data = {}
        if request.active is not None:
            update_data["active"] = request.active

        if not update_data:
            raise HTTPException(status_code=400, detail="No fields to update")

        result = supabase.table("companies").update(update_data).eq("id", company_id).execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="Company not found")

        log_audit(user["id"], "toggle_company", "company", company_id, update_data)
        return {"success": True, "company": safe_first(result)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


class DriverFeatureToggleRequest(BaseModel):
    voice_assistant_enabled: Optional[bool] = None
    is_ambassador: Optional[bool] = None
    closures_alerts_enabled: Optional[bool] = None  # Pro+ feature, opt-in for early access


@app.patch("/admin/drivers/{driver_id}/features", tags=["admin"], summary="Toggle feature flags de un driver")
async def admin_toggle_driver_features(driver_id: str, request: DriverFeatureToggleRequest, user=Depends(require_admin)):
    """Activa o desactiva feature flags de un driver. Solo admin."""
    try:
        update_data = {}
        if request.voice_assistant_enabled is not None:
            update_data["voice_assistant_enabled"] = request.voice_assistant_enabled

        if request.is_ambassador is not None:
            update_data["is_ambassador"] = request.is_ambassador
            if request.is_ambassador:
                update_data["promo_plan"] = "pro"
                update_data["promo_plan_expires_at"] = None
            else:
                update_data["promo_plan"] = None
                update_data["promo_plan_expires_at"] = None

        if request.closures_alerts_enabled is not None:
            update_data["closures_alerts_enabled"] = request.closures_alerts_enabled

        if not update_data:
            raise HTTPException(status_code=400, detail="No fields to update")

        result = supabase.table("drivers").update(update_data).eq("id", driver_id).execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="Driver not found")

        log_audit(user["id"], "toggle_features", "driver", driver_id, update_data)
        return {"success": True, "driver": safe_first(result)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.get("/admin/audit-log", tags=["admin"], summary="Audit log")
async def get_audit_log(limit: int = 100, offset: int = 0, user=Depends(require_admin)):
    """Devuelve el historial de acciones admin, enriquecido con nombres."""
    try:
        result = supabase.table("audit_log")\
            .select("*")\
            .order("created_at", desc=True)\
            .range(offset, offset + limit - 1)\
            .execute()

        count_result = supabase.table("audit_log").select("*", count="exact", head=True).execute()

        logs = result.data or []

        # Enrich: resolve admin_id and resource_id to human-readable names
        admin_ids = list({log["admin_id"] for log in logs if log.get("admin_id")})
        resource_user_ids = list({log["resource_id"] for log in logs if log.get("resource_id") and log.get("resource_type") in ("user", "driver")})
        resource_company_ids = list({log["resource_id"] for log in logs if log.get("resource_id") and log.get("resource_type") == "company"})

        # Resolve admin names from drivers table (admin_id = user_id in auth)
        admin_map = {}
        if admin_ids:
            admins = supabase.table("drivers").select("user_id, name, email").in_("user_id", admin_ids).execute()
            for a in (admins.data or []):
                admin_map[a["user_id"]] = {"name": a.get("name", ""), "email": a.get("email", "")}

        # Resolve resource names for users/drivers
        resource_user_map = {}
        if resource_user_ids:
            # resource_id for "user" type is the auth user_id
            users = supabase.table("drivers").select("user_id, name, email").in_("user_id", resource_user_ids).execute()
            for u in (users.data or []):
                resource_user_map[u["user_id"]] = {"name": u.get("name", ""), "email": u.get("email", "")}
            # Also try by driver id directly
            drivers = supabase.table("drivers").select("id, name, email").in_("id", resource_user_ids).execute()
            for d in (drivers.data or []):
                if d["id"] not in resource_user_map:
                    resource_user_map[d["id"]] = {"name": d.get("name", ""), "email": d.get("email", "")}

        # Resolve company names
        resource_company_map = {}
        if resource_company_ids:
            companies = supabase.table("companies").select("id, name").in_("id", resource_company_ids).execute()
            for c in (companies.data or []):
                resource_company_map[c["id"]] = c.get("name", "")

        # Enrich logs
        for log in logs:
            admin_info = admin_map.get(log.get("admin_id"), {})
            log["admin_name"] = admin_info.get("name", "")
            log["admin_email"] = admin_info.get("email", "")

            rid = log.get("resource_id")
            rtype = log.get("resource_type")
            if rid and rtype in ("user", "driver"):
                rinfo = resource_user_map.get(rid, {})
                log["resource_name"] = rinfo.get("name", "")
                log["resource_email"] = rinfo.get("email", "")
            elif rid and rtype == "company":
                log["resource_name"] = resource_company_map.get(rid, "")

        return {"success": True, "logs": logs, "total": count_result.count or 0}
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo audit log")


@app.get("/admin/stats", tags=["admin"], summary="Estadísticas globales")
async def admin_stats(user=Depends(require_admin)):
    """Estadísticas globales: usuarios, rutas, entregas, fallos. Solo admin."""
    try:
        now_madrid = datetime.now(ZoneInfo("Europe/Madrid"))
        today_start = now_madrid.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
        week_start = (now_madrid - timedelta(days=now_madrid.weekday())).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
        month_start = now_madrid.replace(day=1, hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()

        # Total drivers
        drivers_total = supabase.table("drivers").select("id", count="exact").execute()

        # Active today (have routes created today)
        routes_today = supabase.table("routes").select("driver_id", count="exact").gte("created_at", today_start).execute()
        active_today_ids = {r["driver_id"] for r in (routes_today.data or []) if r.get("driver_id")}

        # Active this week
        routes_week = supabase.table("routes").select("driver_id", count="exact").gte("created_at", week_start).execute()
        active_week_ids = {r["driver_id"] for r in (routes_week.data or []) if r.get("driver_id")}

        # New drivers this month
        new_month = supabase.table("drivers").select("id", count="exact").gte("created_at", month_start).execute()

        # Routes counts
        routes_total = supabase.table("routes").select("id", count="exact").execute()
        routes_month = supabase.table("routes").select("id", count="exact").gte("created_at", month_start).execute()

        # Stops delivered/failed
        stops_total = supabase.table("stops").select("id", count="exact").eq("status", "completed").execute()
        stops_today = supabase.table("stops").select("id", count="exact").eq("status", "completed").gte("completed_at", today_start).execute()
        stops_week = supabase.table("stops").select("id", count="exact").eq("status", "completed").gte("completed_at", week_start).execute()
        stops_month = supabase.table("stops").select("id", count="exact").eq("status", "completed").gte("completed_at", month_start).execute()
        failed_today = supabase.table("stops").select("id", count="exact").eq("status", "failed").gte("created_at", today_start).execute()
        failed_week = supabase.table("stops").select("id", count="exact").eq("status", "failed").gte("created_at", week_start).execute()

        return {
            "success": True,
            "stats": {
                "users": {
                    "total": drivers_total.count or 0,
                    "active_today": len(active_today_ids),
                    "active_week": len(active_week_ids),
                    "new_month": new_month.count or 0,
                },
                "routes": {
                    "today": routes_today.count or 0,
                    "week": routes_week.count or 0,
                    "month": routes_month.count or 0,
                    "total": routes_total.count or 0,
                },
                "deliveries": {
                    "today": stops_today.count or 0,
                    "week": stops_week.count or 0,
                    "month": stops_month.count or 0,
                    "total": stops_total.count or 0,
                },
                "failed": {
                    "today": failed_today.count or 0,
                    "week": failed_week.count or 0,
                },
            },
        }
    except Exception as e:
        logger.error(f"Admin stats error: {e}")
        raise HTTPException(status_code=500, detail="Error obteniendo estadísticas")


@app.get("/admin/companies", tags=["admin", "company"], summary="Listar empresas")
async def admin_list_companies(user=Depends(require_admin)):
    """Lista todas las empresas con conteo de drivers y suscripción. Solo admin."""
    try:
        companies = supabase.table("companies").select("*").order("created_at", desc=True).execute()
        company_ids = [c["id"] for c in (companies.data or [])]

        # Batch fetch: all driver links and subscriptions in 2 queries instead of 2*N
        all_links = supabase.table("company_driver_links").select("company_id", count="exact").in_("company_id", company_ids).execute() if company_ids else None
        all_subs = supabase.table("company_subscriptions").select("*").in_("company_id", company_ids).order("created_at", desc=True).execute() if company_ids else None

        # Build lookup maps
        driver_counts: dict = {}
        for link in (all_links.data if all_links else []):
            cid = link["company_id"]
            driver_counts[cid] = driver_counts.get(cid, 0) + 1

        sub_map: dict = {}
        for sub in (all_subs.data if all_subs else []):
            cid = sub["company_id"]
            if cid not in sub_map:  # first = most recent (ordered desc)
                sub_map[cid] = sub

        result = []
        for company in (companies.data or []):
            result.append({
                **company,
                "driver_count": driver_counts.get(company["id"], 0),
                "subscription": sub_map.get(company["id"]),
            })

        return {"success": True, "companies": result}
    except Exception as e:
        logger.error(f"Admin companies error: {e}")
        raise HTTPException(status_code=500, detail="Error listando empresas")


# === REFERRAL SYSTEM ===

INVITE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


class ReferralRedeemRequest(BaseModel):
    referral_code: str


@app.get("/referral/code", tags=["referral"], summary="Obtener código de referido")
async def get_referral_code(user=Depends(get_current_user)):
    """Obtiene o genera el código de referido del usuario (formato XPD-XXXX)."""
    try:
        driver_id = await get_user_driver_id(user)
        if not driver_id:
            raise HTTPException(status_code=404, detail="Driver not found")

        result = supabase.table("drivers").select("referral_code").eq("id", driver_id).single().execute()

        if result.data and result.data.get("referral_code"):
            return {"code": result.data["referral_code"], "driver_id": driver_id}

        # Generate unique code
        for _ in range(10):
            code = "XPD-" + "".join(random.choices(INVITE_CHARS, k=4))
            existing = supabase.table("drivers").select("id").eq("referral_code", code).execute()
            if not existing.data:
                break

        supabase.table("drivers").update({"referral_code": code}).eq("id", driver_id).execute()
        return {"code": code, "driver_id": driver_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.post("/referral/redeem", tags=["referral"], summary="Canjear código de referido")
async def redeem_referral(request: ReferralRedeemRequest, user=Depends(get_current_user)):
    """Canjea un código de referido. El nuevo usuario y el referidor reciben 7 días de Pro gratis."""
    try:
        referred_driver_id = await get_user_driver_id(user)
        if not referred_driver_id:
            raise HTTPException(status_code=404, detail="Driver not found")

        code = request.referral_code.strip().upper()

        # Find referrer (include email and name for notification)
        referrer = supabase.table("drivers").select("id, referral_code, email, name").eq("referral_code", code).single().execute()
        if not referrer.data:
            raise HTTPException(status_code=404, detail="Codigo de referido no encontrado")

        referrer_id = referrer.data["id"]

        # No self-referral
        if referrer_id == referred_driver_id:
            raise HTTPException(status_code=400, detail="No puedes usar tu propio codigo")

        # Check if already referred
        existing = supabase.table("referrals").select("id").eq("referred_driver_id", referred_driver_id).execute()
        if existing.data:
            raise HTTPException(status_code=400, detail="Ya has usado un codigo de referido")

        REWARD_DAYS = 7
        REWARD_PLAN = "pro"
        now = datetime.now(timezone.utc)
        expires_at = (now + timedelta(days=REWARD_DAYS)).isoformat()

        # Grant reward to referred (new user)
        supabase.table("drivers").update({
            "promo_plan": REWARD_PLAN,
            "promo_plan_expires_at": expires_at,
        }).eq("id", referred_driver_id).execute()

        # Grant reward to referrer (extend or set)
        referrer_data = supabase.table("drivers").select("promo_plan_expires_at").eq("id", referrer_id).single().execute()
        ref_expires = referrer_data.data.get("promo_plan_expires_at") if referrer_data.data else None

        if ref_expires:
            try:
                current_exp = datetime.fromisoformat(ref_expires.replace("Z", "+00:00"))
                if current_exp.tzinfo:
                    current_exp = current_exp.replace(tzinfo=None)
                if current_exp > now:
                    new_exp = (current_exp + timedelta(days=REWARD_DAYS)).isoformat()
                else:
                    new_exp = expires_at
            except Exception:
                new_exp = expires_at
        else:
            new_exp = expires_at

        supabase.table("drivers").update({
            "promo_plan": REWARD_PLAN,
            "promo_plan_expires_at": new_exp,
        }).eq("id", referrer_id).execute()

        # Record referral
        supabase.table("referrals").insert({
            "referrer_driver_id": referrer_id,
            "referred_driver_id": referred_driver_id,
            "referral_code": code,
            "reward_given": True,
        }).execute()

        # Send email notifications (fire and forget)
        try:
            referrer_email = referrer.data.get("email")
            referrer_name = referrer.data.get("name") or "Usuario"
            referred = supabase.table("drivers").select("email, name").eq("id", referred_driver_id).single().execute()
            referred_email = referred.data.get("email") if referred.data else None
            referred_name = referred.data.get("name") if referred.data else "Usuario"

            if referrer_email:
                send_referral_reward_email(referrer_email, referrer_name, referred_name, REWARD_DAYS)
            if referred_email:
                send_plan_activated_email(referred_email, referred_name, "Pro", REWARD_DAYS, False)
        except Exception as email_err:
            sentry_sdk.capture_exception(email_err)  # Don't fail the referral if email fails

        return {
            "success": True,
            "reward_days": REWARD_DAYS,
            "reward_plan": REWARD_PLAN,
            "message": f"Codigo canjeado. {REWARD_DAYS} dias de {REWARD_PLAN} para ti y para quien te invito."
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.get("/referral/stats", tags=["referral"], summary="Estadísticas de referidos")
async def get_referral_stats(user=Depends(get_current_user)):
    """Obtiene las estadísticas de referidos del usuario: total, días ganados y lista de referidos."""
    try:
        driver_id = await get_user_driver_id(user)
        if not driver_id:
            raise HTTPException(status_code=404, detail="Driver not found")

        result = supabase.table("referrals").select("*").eq("referrer_driver_id", driver_id).execute()

        return {
            "total_referrals": len(result.data) if result.data else 0,
            "total_reward_days": len(result.data) * 7 if result.data else 0,
            "referrals": result.data or [],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# === COMPANY / FLEET MANAGEMENT MODELS ===

DRIVER_PLAN_PRICES = {
    "free": 0,
    "pro": 4.99,
    "pro_plus": 9.99,
}

FLEET_RATE_PER_DRIVER = 18.0


class CompanyRegisterRequest(BaseModel):
    name: str
    email: str
    phone: Optional[str] = None
    address: Optional[str] = None
    owner_user_id: str


class CompanyUpdateRequest(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    payment_model: Optional[str] = None


class CompanyInviteRequest(BaseModel):
    company_id: str
    role: str = "driver"
    max_uses: Optional[int] = None
    expires_hours: int = 168  # 7 days


class CompanyJoinRequest(BaseModel):
    code: str
    user_id: str


class CompanyLeaveRequest(BaseModel):
    user_id: str


class CompanyCreateDriverRequest(BaseModel):
    company_id: str
    email: str
    full_name: str
    phone: Optional[str] = None
    password: str


class CompanyDriverModeRequest(BaseModel):
    mode: str  # 'driver_pays', 'company_pays', 'company_complete'


# === COMPANY / FLEET MANAGEMENT ENDPOINTS ===


def _generate_invite_code() -> str:
    """Generate a random invite code in the format XPD-XXXX"""
    suffix = "".join(random.choice(INVITE_CHARS) for _ in range(4))
    return f"XPD-{suffix}"


# 1. POST /company/register
@app.post("/company/register", tags=["company"], summary="Registrar empresa")
async def register_company(request: CompanyRegisterRequest, user=Depends(get_current_user)):
    """Registra una nueva empresa, configura al propietario como admin y crea suscripción trial de 7 días."""
    # SECURITY: owner_user_id must be the authenticated user (prevent privilege escalation)
    if request.owner_user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Solo puedes registrar una empresa para tu propia cuenta")
    # Validate email format
    if not request.email or "@" not in request.email or "." not in request.email.split("@")[-1]:
        raise HTTPException(status_code=400, detail="Email no valido")
    try:
        # Create company
        company_data = {
            "name": request.name,
            "email": request.email,
            "phone": request.phone,
            "address": request.address,
            "owner_id": user["id"],
            "payment_model": "driver_pays",
            "active": True,
        }
        company_result = supabase.table("companies").insert(company_data).execute()

        company = safe_first(company_result)
        if not company:
            raise HTTPException(status_code=500, detail="Failed to create company")

        company_id = company["id"]

        # Update owner's role to admin in users table
        supabase.table("users").update({
            "role": "admin",
            "company_id": company_id,
        }).eq("id", user["id"]).execute()

        # Update owner's company_id in drivers table
        supabase.table("drivers").update({
            "company_id": company_id,
        }).eq("user_id", user["id"]).execute()

        # Create subscription with 7-day trial
        now = datetime.now(timezone.utc)
        trial_end = now + timedelta(days=7)
        subscription_data = {
            "company_id": company_id,
            "plan": "free",
            "max_drivers": 15,
            "price_per_month": 0,
            "status": "trialing",
            "trial_ends_at": trial_end.isoformat(),
            "current_period_start": now.isoformat(),
            "current_period_end": trial_end.isoformat(),
        }
        supabase.table("company_subscriptions").insert(subscription_data).execute()

        return {"success": True, "company": company}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 15. GET /company/check-access/{driver_id}
# NOTE: Defined before /company/{company_id} to avoid route shadowing
@app.get("/company/check-access/{driver_id}", tags=["company"], summary="Verificar acceso empresa")
async def check_company_access(driver_id: str, user=Depends(get_current_user)):
    """Verifica si un conductor tiene acceso pagado por empresa (company_pays o company_complete)."""
    # Verify ownership: look up driver and check user_id matches authenticated user
    driver_check = supabase.table("drivers").select("user_id").eq("id", driver_id).single().execute()
    if not driver_check.data:
        raise HTTPException(status_code=404, detail="Driver no encontrado")
    owner_user_id = driver_check.data["user_id"]
    if user["role"] != "admin" and user["id"] != owner_user_id:
        raise HTTPException(status_code=403, detail="No tienes acceso a estos datos")
    try:
        # Look up active company_driver_links for this driver's user
        link_result = supabase.table("company_driver_links")\
            .select("*")\
            .eq("user_id", owner_user_id)\
            .eq("active", True)\
            .limit(1)\
            .execute()

        link = safe_first(link_result)
        if not link:
            return {"has_access": False}

        mode = link.get("mode", "driver_pays")

        if mode in ("company_pays", "company_complete"):
            # Get company name
            company_result = supabase.table("companies")\
                .select("name")\
                .eq("id", link["company_id"])\
                .limit(1)\
                .execute()

            company_row = safe_first(company_result)
            company_name = company_row["name"] if company_row else None

            return {
                "has_access": True,
                "plan": "pro_plus",
                "company_name": company_name,
            }

        return {"has_access": False}

    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 2. GET /company/{company_id}
@app.get("/company/{company_id}", tags=["company"], summary="Obtener empresa")
async def get_company(company_id: str, user=Depends(get_current_user)):
    """Obtiene los datos de una empresa con información de suscripción."""
    # Authorization: user must belong to this company or be admin
    if user.get("company_id") != company_id and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="No tienes acceso a esta empresa")

    try:
        company_result = supabase.table("companies")\
            .select("*")\
            .eq("id", company_id)\
            .single()\
            .execute()

        if not company_result.data:
            raise HTTPException(status_code=404, detail="Company not found")

        sub_result = supabase.table("company_subscriptions")\
            .select("*")\
            .eq("company_id", company_id)\
            .order("created_at", desc=True)\
            .limit(1)\
            .execute()

        subscription = safe_first(sub_result)

        return {
            "success": True,
            "company": company_result.data,
            "subscription": subscription,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 3. PATCH /company/{company_id}
@app.patch("/company/{company_id}", tags=["company"], summary="Actualizar empresa")
async def update_company(company_id: str, request: CompanyUpdateRequest, user=Depends(get_current_user)):
    """Actualiza los datos de una empresa (nombre, email, teléfono, dirección, modelo de pago)."""
    # Authorization: user must belong to this company or be admin
    if user.get("company_id") != company_id and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="No tienes acceso a esta empresa")

    try:
        update_data = {}
        if request.name is not None:
            update_data["name"] = request.name
        if request.email is not None:
            update_data["email"] = request.email
        if request.phone is not None:
            update_data["phone"] = request.phone
        if request.address is not None:
            update_data["address"] = request.address
        if request.payment_model is not None:
            if request.payment_model not in ("driver_pays", "company_pays", "company_complete"):
                raise HTTPException(status_code=400, detail="Invalid payment_model")
            update_data["payment_model"] = request.payment_model

        if not update_data:
            raise HTTPException(status_code=400, detail="No fields to update")

        update_data["updated_at"] = datetime.now(timezone.utc).isoformat()

        result = supabase.table("companies")\
            .update(update_data)\
            .eq("id", company_id)\
            .execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="Company not found")

        return {"success": True, "company": safe_first(result)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 4. GET /company/{company_id}/drivers
@app.get("/company/{company_id}/drivers", tags=["company"], summary="Conductores de empresa")
async def get_company_drivers(company_id: str, user=Depends(get_current_user)):
    """Lista todos los conductores de una empresa con modo de pago, coste y plan."""
    # Authorization: user must belong to this company or be admin
    if user.get("company_id") != company_id and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="No tienes acceso a esta empresa")

    try:
        # Get all driver links for this company (including inactive)
        links_result = supabase.table("company_driver_links")\
            .select("*")\
            .eq("company_id", company_id)\
            .execute()

        links = links_result.data or []
        if not links:
            return {"success": True, "drivers": [], "total": 0, "active_count": 0}

        # Batch fetch all drivers and users in 2 queries instead of 2*N
        user_ids = [link["user_id"] for link in links if link.get("user_id")]
        all_drivers = supabase.table("drivers").select("*").in_("user_id", user_ids).execute() if user_ids else None
        all_users = supabase.table("users").select("id, email, full_name, phone, role").in_("id", user_ids).execute() if user_ids else None

        driver_map = {d["user_id"]: d for d in (all_drivers.data if all_drivers else [])}
        user_map = {u["id"]: u for u in (all_users.data if all_users else [])}

        drivers_list = []
        for link in links:
            uid = link.get("user_id")
            driver_data = driver_map.get(uid, {})
            user_data = user_map.get(uid, {})

            drivers_list.append({
                "link_id": link["id"],
                "user_id": uid,
                "driver_id": link.get("driver_id"),
                "mode": link.get("mode", "driver_pays"),
                "company_cost": link.get("company_cost"),
                "joined_at": link.get("joined_at"),
                "driver_plan_at_link": link.get("driver_plan_at_link"),
                "active": link.get("active", True),
                "email": user_data.get("email"),
                "full_name": user_data.get("full_name"),
                "phone": user_data.get("phone"),
                "promo_plan": driver_data.get("promo_plan"),
                "promo_plan_expires_at": driver_data.get("promo_plan_expires_at"),
            })

        active_count = sum(1 for d in drivers_list if d["active"])
        return {"success": True, "drivers": drivers_list, "total": len(drivers_list), "active_count": active_count}

    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 5. GET /company/{company_id}/stats
@app.get("/company/{company_id}/stats", tags=["company"], summary="Estadísticas de flota")
async def get_company_stats(company_id: str, user=Depends(get_current_user)):
    """Estadísticas de la flota: conductores totales, activos hoy, rutas/paradas/entregas del día."""
    # Authorization: user must belong to this company or be admin
    if user.get("company_id") != company_id and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="No tienes acceso a esta empresa")

    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Total drivers in company
        links_result = supabase.table("company_driver_links")\
            .select("user_id")\
            .eq("company_id", company_id)\
            .eq("active", True)\
            .execute()
        total_drivers = len(links_result.data or [])
        driver_user_ids = [link["user_id"] for link in (links_result.data or [])]

        # Get driver IDs from drivers table for these users
        active_today = 0
        routes_today = 0
        stops_today = 0
        deliveries_today = 0

        if driver_user_ids:
            # Get driver records
            drivers_result = supabase.table("drivers")\
                .select("id, user_id")\
                .in_("user_id", driver_user_ids)\
                .execute()

            driver_ids = [d["id"] for d in (drivers_result.data or [])]

            if driver_ids:
                # Routes today for these drivers
                routes_result = supabase.table("routes")\
                    .select("*, stops(*)")\
                    .in_("driver_id", driver_ids)\
                    .gte("created_at", f"{today}T00:00:00")\
                    .lte("created_at", f"{today}T23:59:59")\
                    .execute()

                routes = routes_result.data or []
                routes_today = len(routes)

                # Unique drivers who have routes today
                active_driver_ids = set(r["driver_id"] for r in routes)
                active_today = len(active_driver_ids)

                # Count stops and completed deliveries
                for route in routes:
                    route_stops = route.get("stops", [])
                    stops_today += len(route_stops)
                    deliveries_today += len([
                        s for s in route_stops if s.get("status") == "completed"
                    ])

        return {
            "success": True,
            "total_drivers": total_drivers,
            "active_today": active_today,
            "routes_today": routes_today,
            "stops_today": stops_today,
            "deliveries_today": deliveries_today,
        }

    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 6. POST /company/invites
@app.post("/company/invites", tags=["company"], summary="Crear invitación empresa")
async def create_company_invite(request: CompanyInviteRequest, user=Depends(get_current_user)):
    """Genera un código de invitación para unirse a la empresa. Solo admin/dispatcher."""
    await verify_company_management(user, request.company_id)
    try:
        # Generate unique code
        for _ in range(10):
            code = _generate_invite_code()
            existing = supabase.table("company_invites")\
                .select("id")\
                .eq("code", code)\
                .execute()
            if not existing.data:
                break
        else:
            raise HTTPException(status_code=500, detail="Failed to generate unique invite code")

        expires_at = (datetime.now(timezone.utc) + timedelta(hours=request.expires_hours)).isoformat()

        invite_data = {
            "code": code,
            "company_id": request.company_id,
            "role": request.role,
            "max_uses": request.max_uses,
            "current_uses": 0,
            "active": True,
            "expires_at": expires_at,
        }

        result = supabase.table("company_invites").insert(invite_data).execute()

        invite = safe_first(result)
        if not invite:
            raise HTTPException(status_code=500, detail="Failed to create invite")

        return {"success": True, "invite": invite}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 7. GET /company/{company_id}/invites
@app.get("/company/{company_id}/invites", tags=["company"], summary="Listar invitaciones")
async def get_company_invites(company_id: str, user=Depends(get_current_user)):
    """Lista los códigos de invitación de una empresa."""
    # Authorization: user must belong to this company or be admin
    if user.get("company_id") != company_id and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="No tienes acceso a esta empresa")

    try:
        result = supabase.table("company_invites")\
            .select("*")\
            .eq("company_id", company_id)\
            .order("created_at", desc=True)\
            .execute()

        return {"success": True, "invites": result.data or []}

    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 8. DELETE /company/invites/{invite_id}
@app.delete("/company/invites/{invite_id}", tags=["company"], summary="Desactivar invitación")
async def deactivate_company_invite(invite_id: str, user=Depends(get_current_user)):
    """Desactiva un código de invitación. Verifica propiedad de la empresa."""
    try:
        # Verify invite belongs to user's company
        invite_check = supabase.table("company_invites").select("company_id").eq("id", invite_id).limit(1).execute()
        invite_row = safe_first(invite_check)
        if invite_row:
            await verify_company_management(user, invite_row["company_id"])
        result = supabase.table("company_invites")\
            .update({"active": False})\
            .eq("id", invite_id)\
            .execute()

        invite = safe_first(result)
        if not invite:
            raise HTTPException(status_code=404, detail="Invite not found")

        return {"success": True, "invite": invite}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 9. POST /company/join
@app.post("/company/join", tags=["company"], summary="Unirse a empresa")
async def join_company(request: CompanyJoinRequest, user=Depends(get_current_user)):
    """Un conductor se une a una empresa usando un código de invitación."""
    try:
        # Use authenticated user's ID instead of request body
        user_id = user["id"]
        code = request.code.strip().upper()

        # Find the invite
        invite_result = supabase.table("company_invites")\
            .select("*")\
            .eq("code", code)\
            .execute()

        invite = safe_first(invite_result)
        if not invite:
            raise HTTPException(status_code=404, detail="Invite code not found")

        # Validate: active
        if not invite.get("active", False):
            raise HTTPException(status_code=400, detail="This invite code is no longer active")

        # Validate: not expired
        if invite.get("expires_at"):
            expires_at = datetime.fromisoformat(invite["expires_at"].replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            if now > expires_at:
                raise HTTPException(status_code=400, detail="This invite code has expired")

        # Validate: max uses
        if invite.get("max_uses") is not None:
            if invite.get("current_uses", 0) >= invite["max_uses"]:
                raise HTTPException(status_code=400, detail="This invite code has reached its maximum uses")

        # Check driver is not already in a company
        user_result = supabase.table("users")\
            .select("id, company_id")\
            .eq("id", user_id)\
            .single()\
            .execute()

        if not user_result.data:
            raise HTTPException(status_code=404, detail="User not found")

        if user_result.data.get("company_id"):
            raise HTTPException(status_code=400, detail="User is already part of a company")

        company_id = invite["company_id"]

        # Update users table
        supabase.table("users").update({
            "company_id": company_id,
        }).eq("id", user_id).execute()

        # Update drivers table
        supabase.table("drivers").update({
            "company_id": company_id,
        }).eq("user_id", user_id).execute()

        # Get driver record for driver_id
        driver_result = supabase.table("drivers")\
            .select("id, promo_plan")\
            .eq("user_id", user_id)\
            .limit(1)\
            .execute()

        driver_row = safe_first(driver_result)
        driver_id = driver_row["id"] if driver_row else None
        driver_plan = driver_row.get("promo_plan") if driver_row else None

        # Create company_driver_links entry
        link_data = {
            "company_id": company_id,
            "driver_id": driver_id,
            "user_id": user_id,
            "mode": "driver_pays",
            "company_cost": None,
            "driver_plan_at_link": driver_plan,
            "active": True,
        }
        supabase.table("company_driver_links").insert(link_data).execute()

        # Increment current_uses
        # Atomically increment current_uses (prevents race condition)
        supabase.rpc("atomic_increment_uses", {"p_table": "company_invites", "p_id": invite["id"]}).execute()

        # Record in company_invite_uses
        supabase.table("company_invite_uses").insert({
            "invite_id": invite["id"],
            "user_id": user_id,
        }).execute()

        return {
            "success": True,
            "company_id": company_id,
            "message": "Successfully joined the company",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 10. POST /company/leave
@app.post("/company/leave", tags=["company"], summary="Salir de empresa")
async def leave_company(request: CompanyLeaveRequest, user=Depends(get_current_user)):
    """Un conductor abandona su empresa. Si tenía acceso pagado por la empresa, se revoca."""
    try:
        # Use authenticated user's ID instead of request body
        user_id = user["id"]

        # Get current driver link to check mode
        link_result = supabase.table("company_driver_links")\
            .select("*")\
            .eq("user_id", user_id)\
            .eq("active", True)\
            .limit(1)\
            .execute()

        link = safe_first(link_result)
        if not link:
            raise HTTPException(status_code=404, detail="User is not linked to any company")

        mode = link.get("mode", "driver_pays")

        # Remove company_id from users
        supabase.table("users").update({
            "company_id": None,
        }).eq("id", user_id).execute()

        # Remove company_id from drivers
        supabase.table("drivers").update({
            "company_id": None,
        }).eq("user_id", user_id).execute()

        # Deactivate driver link
        supabase.table("company_driver_links").update({
            "active": False,
        }).eq("id", link["id"]).execute()

        # If was company_pays or company_complete, remove promo benefits
        if mode in ("company_pays", "company_complete"):
            supabase.table("drivers").update({
                "promo_plan": None,
                "promo_plan_expires_at": None,
            }).eq("user_id", user_id).execute()

        return {"success": True, "message": "Successfully left the company"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 11. POST /company/drivers
@app.post("/company/drivers", tags=["company"], summary="Crear conductor en empresa")
async def create_company_driver(request: CompanyCreateDriverRequest, user=Depends(get_current_user)):
    """Crea una cuenta de conductor directamente en la empresa. Solo admin/dispatcher."""
    await verify_company_management(user, request.company_id)
    try:
        # Use supabase admin auth to create a new user
        auth_response = supabase.auth.admin.create_user({
            "email": request.email,
            "password": request.password,
            "email_confirm": True,
            "user_metadata": {
                "full_name": request.full_name,
                "phone": request.phone,
            },
        })

        new_user_id = auth_response.user.id

        # Wait briefly for database triggers to fire (users + drivers auto-created)
        import asyncio
        await asyncio.sleep(2)

        # Update company_id in users
        supabase.table("users").update({
            "company_id": request.company_id,
            "full_name": request.full_name,
            "phone": request.phone,
        }).eq("id", str(new_user_id)).execute()

        # Update company_id in drivers
        supabase.table("drivers").update({
            "company_id": request.company_id,
        }).eq("user_id", str(new_user_id)).execute()

        # Get driver record
        driver_result = supabase.table("drivers")\
            .select("id")\
            .eq("user_id", str(new_user_id))\
            .limit(1)\
            .execute()

        driver_row = safe_first(driver_result)
        driver_id = driver_row["id"] if driver_row else None

        # Create company_driver_links entry
        link_data = {
            "company_id": request.company_id,
            "driver_id": driver_id,
            "user_id": str(new_user_id),
            "mode": "driver_pays",
            "active": True,
        }
        supabase.table("company_driver_links").insert(link_data).execute()

        return {
            "success": True,
            "user_id": str(new_user_id),
            "driver_id": driver_id,
            "email": request.email,
            "message": "Driver account created and added to company",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 12. DELETE /company/drivers/{user_id}
@app.delete("/company/drivers/{user_id}", tags=["company"], summary="Eliminar conductor de empresa")
async def remove_company_driver(user_id: str, user=Depends(get_current_user)):
    """Elimina un conductor de la empresa. Si tenía acceso pagado, se revoca. Solo admin/dispatcher."""
    try:
        # Get current driver link to check mode and verify company ownership
        link_result = supabase.table("company_driver_links")\
            .select("*")\
            .eq("user_id", user_id)\
            .eq("active", True)\
            .limit(1)\
            .execute()

        link = safe_first(link_result)
        if not link:
            raise HTTPException(status_code=404, detail="Driver is not linked to any company")

        await verify_company_management(user, link["company_id"])
        mode = link.get("mode", "driver_pays")

        # Remove company_id from users
        supabase.table("users").update({
            "company_id": None,
        }).eq("id", user_id).execute()

        # Remove company_id from drivers
        supabase.table("drivers").update({
            "company_id": None,
        }).eq("user_id", user_id).execute()

        # Deactivate driver link
        supabase.table("company_driver_links").update({
            "active": False,
        }).eq("id", link["id"]).execute()

        # If was company_pays or company_complete, remove promo benefits
        if mode in ("company_pays", "company_complete"):
            supabase.table("drivers").update({
                "promo_plan": None,
                "promo_plan_expires_at": None,
            }).eq("user_id", user_id).execute()

        return {"success": True, "message": "Driver removed from company"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 13b. PATCH /company/drivers/{user_id}/active - toggle driver active/inactive
@app.patch("/company/drivers/{user_id}/active", tags=["company"], summary="Activar/desactivar conductor")
async def toggle_driver_active(user_id: str, user=Depends(get_current_user)):
    """Activa o desactiva un conductor en la empresa. Gestiona beneficios de plan automáticamente."""
    try:
        # Get current driver link
        link_result = supabase.table("company_driver_links")\
            .select("*")\
            .eq("user_id", user_id)\
            .limit(1)\
            .execute()

        link = safe_first(link_result)
        if not link:
            raise HTTPException(status_code=404, detail="Driver link not found")

        await verify_company_management(user, link["company_id"])
        new_active = not link.get("active", True)
        mode = link.get("mode", "driver_pays")

        # Update link active status
        supabase.table("company_driver_links")\
            .update({"active": new_active})\
            .eq("id", link["id"])\
            .execute()

        # If deactivating and was company_pays/company_complete, remove promo benefits
        if not new_active and mode in ("company_pays", "company_complete"):
            supabase.table("drivers").update({
                "promo_plan": None,
                "promo_plan_expires_at": None,
            }).eq("user_id", user_id).execute()

        # If reactivating and mode is company_pays/company_complete, restore promo benefits
        if new_active and mode in ("company_pays", "company_complete"):
            company_id = link.get("company_id")
            sub_result = supabase.table("company_subscriptions")\
                .select("current_period_end")\
                .eq("company_id", company_id)\
                .order("created_at", desc=True)\
                .limit(1)\
                .execute()
            sub_row = safe_first(sub_result)
            period_end = sub_row.get("current_period_end") if sub_row else None

            supabase.table("drivers").update({
                "promo_plan": "pro_plus",
                "promo_plan_expires_at": period_end,
            }).eq("user_id", user_id).execute()

        return {
            "success": True,
            "active": new_active,
            "message": f"Driver {'activated' if new_active else 'deactivated'}",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 13. PATCH /company/drivers/{user_id}/mode
@app.patch("/company/drivers/{user_id}/mode", tags=["company"], summary="Cambiar modo de pago conductor")
async def change_driver_mode(user_id: str, request: CompanyDriverModeRequest, user=Depends(get_current_user)):
    """Cambia el modo de pago de un conductor (driver_pays, company_pays, company_complete). Solo admin/dispatcher."""
    try:
        if request.mode not in ("driver_pays", "company_pays", "company_complete"):
            raise HTTPException(status_code=400, detail="Invalid mode. Must be driver_pays, company_pays, or company_complete")

        # Get active driver link
        link_result = supabase.table("company_driver_links")\
            .select("*")\
            .eq("user_id", user_id)\
            .eq("active", True)\
            .limit(1)\
            .execute()

        link = safe_first(link_result)
        if not link:
            raise HTTPException(status_code=404, detail="Driver is not linked to any company")

        await verify_company_management(user, link["company_id"])
        company_id = link["company_id"]

        # Get subscription for period end date
        sub_result = supabase.table("company_subscriptions")\
            .select("*")\
            .eq("company_id", company_id)\
            .order("created_at", desc=True)\
            .limit(1)\
            .execute()

        subscription = safe_first(sub_result)
        period_end = subscription.get("current_period_end") if subscription else None

        link_update = {"mode": request.mode}
        driver_update = {}

        if request.mode == "company_pays":
            # Company pays: grant pro_plus to driver
            driver_update["promo_plan"] = "pro_plus"
            driver_update["promo_plan_expires_at"] = period_end
            link_update["company_cost"] = FLEET_RATE_PER_DRIVER

        elif request.mode == "company_complete":
            # Company complete: grant pro_plus and calculate cost
            driver_update["promo_plan"] = "pro_plus"
            driver_update["promo_plan_expires_at"] = period_end

            # Get driver's current plan to calculate company_cost
            driver_result = supabase.table("drivers")\
                .select("promo_plan")\
                .eq("user_id", user_id)\
                .limit(1)\
                .execute()

            current_plan = link.get("driver_plan_at_link") or "free"
            driver_price = DRIVER_PLAN_PRICES.get(current_plan, 0)
            company_cost = FLEET_RATE_PER_DRIVER - driver_price
            link_update["company_cost"] = round(company_cost, 2)

        elif request.mode == "driver_pays":
            # Driver pays: remove promo benefits
            driver_update["promo_plan"] = None
            driver_update["promo_plan_expires_at"] = None
            link_update["company_cost"] = None

        # Update driver link
        supabase.table("company_driver_links")\
            .update(link_update)\
            .eq("id", link["id"])\
            .execute()

        # Update driver record
        if driver_update:
            supabase.table("drivers")\
                .update(driver_update)\
                .eq("user_id", user_id)\
                .execute()

        return {
            "success": True,
            "mode": request.mode,
            "company_cost": link_update.get("company_cost"),
            "message": f"Driver mode changed to {request.mode}",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 14. GET /company/{company_id}/subscription
@app.get("/company/{company_id}/subscription", tags=["company"], summary="Suscripción de empresa")
async def get_company_subscription(company_id: str, user=Depends(get_current_user)):
    """Obtiene los detalles de la suscripción de una empresa."""
    # Authorization: user must belong to this company or be admin
    if user.get("company_id") != company_id and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="No tienes acceso a esta empresa")

    try:
        result = supabase.table("company_subscriptions")\
            .select("*")\
            .eq("company_id", company_id)\
            .order("created_at", desc=True)\
            .limit(1)\
            .execute()

        subscription = safe_first(result)
        if not subscription:
            raise HTTPException(status_code=404, detail="No subscription found for this company")

        return {"success": True, "subscription": subscription}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# === OCR PROXY ===
#
# Single-label extractor (one shipping label photo → 5 fields). Uses Gemini
# 2.5 Flash with structured JSON output. Migrated from Anthropic Claude
# Haiku 10 may 2026 (#244) for cost (~3-5x cheaper) and to consolidate AI
# stack (MSI already on Gemini, removes the Anthropic API key dependency).
#
# Response schema is intentionally identical to the previous one:
# `{success, content}` where `content` is a JSON string. The `data` field is
# new (parsed dict, same content) so newer clients can skip the JSON.parse.

# Modelo gemini-2.5-pro (no Flash) tras incidente 14 may PYTHON-FASTAPI-K
# donde Flash truncaba JSON con response_schema enforced. Ver memoria
# feedback_gemini_pro_for_ocr.md.
_OCR_LABEL_MODEL = "gemini-2.5-pro"

_OCR_LABEL_PROMPT = """Esta es una foto de una etiqueta de envío de paquetería (iMile, Shein, etc.).
IMPORTANTE: La imagen puede estar ROTADA 90°, 180° o 270°. Analiza la orientación del texto primero.

Busca la sección "TO" o destinatario que contiene:
- Nombre del destinatario (persona)
- Dirección: calle y número
- Ciudad (ej: Arcos De La Frontera)
- Código postal (5 dígitos, ej: 11630)
- Provincia (ej: Cádiz)

CRÍTICO: Lee TODA la etiqueta cuidadosamente aunque esté rotada. Si un campo no es legible, devuelve string vacío para ese campo, nunca inventes datos."""

_OCR_LABEL_SCHEMA = {
    "type": "object",
    "properties": {
        "name":       {"type": "string", "description": "Nombre completo del destinatario"},
        "street":     {"type": "string", "description": "Calle y número exacto"},
        "city":       {"type": "string", "description": "Ciudad/Localidad"},
        "postalCode": {"type": "string", "description": "Código postal de 5 dígitos"},
        "province":   {"type": "string", "description": "Provincia"},
    },
    "required": ["name", "street", "city", "postalCode", "province"],
}


class OCRLabelRequest(BaseModel):
    image_base64: str = Field(..., max_length=10_000_000)  # ~7.5MB max image
    media_type: Literal["image/jpeg", "image/png", "image/gif", "image/webp"] = "image/jpeg"
    # When True the request authorizes the backend to keep the image + the
    # model/user pair in `ocr_corrections` for OCR improvement (Day-2
    # learning loop). Default False = no capture, no storage. The app shows
    # the toggle in Settings and only forwards the True value when explicit
    # consent has been recorded.
    consent_to_training: bool = False


def _ocr_label_with_gemini(image_base64: str, media_type: str) -> dict:
    """Synchronous Gemini call for label OCR. Wrap in asyncio.to_thread() from
    the async caller — google-genai SDK is blocking. Returns a dict that
    matches `_OCR_LABEL_SCHEMA`. Raises HTTPException on Gemini errors.

    Uses Vertex AI (europe-west4) so the label image — which contains the
    recipient's full name + address — never leaves the EU and falls under
    the GCP DPA instead of AI Studio terms."""
    from google.genai import types

    client = get_gemini_vertex_client()
    if not client:
        raise HTTPException(status_code=503, detail="OCR service not configured")

    parts = [
        types.Part.from_text(text=_OCR_LABEL_PROMPT),
        types.Part.from_bytes(
            data=base64.b64decode(image_base64),
            mime_type=media_type,
        ),
    ]

    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=_OCR_LABEL_SCHEMA,
        temperature=0.1,
        # 1024 tokens (no 500) tras incidente 14 may donde Gemini Flash
        # truncaba el JSON con direcciones largas. Mantenemos 1024 con Pro.
        max_output_tokens=1024,
    )

    try:
        response = client.models.generate_content(
            model=_OCR_LABEL_MODEL,
            contents=[types.Content(role="user", parts=parts)],
            config=config,
        )
    except Exception as e:
        logger.error(f"Gemini OCR error: {type(e).__name__}: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=502, detail=f"OCR API error: {str(e)[:200]}")

    text = (response.text or "").strip()
    if not text:
        logger.warning("OCR Gemini returned empty text")
        return {"name": "", "street": "", "city": "", "postalCode": "", "province": ""}

    # Fast path: clean JSON straight from response_schema.
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    # Tolerant fallback: Gemini occasionally wraps the JSON in ```json...```
    # fences or prefixes a one-line apology. Strip those and try again.
    cleaned = text
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```", 2)[1] if "```" in cleaned[3:] else cleaned[3:]
        if cleaned.lstrip().lower().startswith("json"):
            cleaned = cleaned.lstrip()[4:]
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        try:
            parsed = json.loads(match.group(0))
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    # Worst case: log to Sentry and return empty fields so the UI doesn't
    # explode on a hard label. The app shows the user "couldn't read, try
    # again" instead of a generic 502.
    logger.error(f"OCR Gemini unparseable response, text[:300]={text[:300]}")
    sentry_sdk.capture_message(
        f"OCR Gemini unparseable response: {text[:200]}",
        level="warning",
    )
    return {"name": "", "street": "", "city": "", "postalCode": "", "province": ""}


@app.get("/ocr/quota", tags=["ocr"], summary="Current daily OCR image quota for this driver")
async def ocr_quota(user=Depends(get_current_user)):
    """Returns the caller's current OCR daily image quota state.
    Used by the app to pre-check before sending a batch — without this
    pre-check the limit alert fires AFTER the last image is processed
    (which still costs Gemini money). Miguel report 12 may 15:13 CEST.
    """
    tier, driver_id = _resolve_user_tier(user["id"])
    return get_ocr_quota_status(driver_id or user["id"], tier)


class OCRContributeImage(BaseModel):
    image_base64: str = Field(..., max_length=12_000_000)  # ~9MB post-base64
    media_type: Literal["image/jpeg", "image/png", "image/webp"] = "image/jpeg"


class OCRContributeRequest(BaseModel):
    images: List[OCRContributeImage] = Field(..., min_length=1, max_length=10)


@app.post("/ocr/training-contribute", tags=["ocr"], summary="Subir etiquetas para entrenar el OCR")
async def ocr_training_contribute(
    request: OCRContributeRequest,
    user=Depends(get_current_user),
):
    """Drivers suben 1-10 etiquetas de su galería para entrenar el OCR.

    No pasa por Gemini (no consume Gemini quota ni MSI daily quota). Solo
    sube las imágenes al bucket `ocr-training` con `source='contribution'`
    y deja una fila pendiente en `ocr_corrections` para que el admin las
    revise + promueva a golden manualmente.

    Recompensa (decisión Miguel 20 may 15:50): +10 imágenes bonus MSI
    quota ese día — registrado vía `drivers.last_contribution_at = NOW()`.
    `_get_msi_bonus_today` lee ese campo y, si == hoy, suma 10 al límite
    diario base.

    Rate limit: 1 batch/día/driver (vía same `last_contribution_at` check).
    Consentimiento: implícito al pulsar el botón en la app (el banner deja
    claro que las imágenes se usan para mejorar el escáner).
    """
    # get_user_driver_id espera el dict completo (no user["id"]) — la función
    # internamente hace user["id"] sobre el dict. Pasar string causa TypeError
    # "string indices must be integers" (PYTHON-FASTAPI-11 20 may 16:53).
    driver_id = await get_user_driver_id(user)
    if not driver_id:
        raise HTTPException(status_code=403, detail={"error": "driver_not_found"})

    # Decisión Miguel 20 may 17:28: eliminado el rate limit "1 batch/día".
    # Si el driver quiere subir 5 batches el mismo día (60 fotos), bienvenido
    # — cada batch trae más seed real para entrenar el OCR. El bonus +10 imgs
    # MSI mañana se sigue dando con cualquier `last_contribution_at` reciente
    # (no se multiplica con N batches porque _get_msi_bonus_today devuelve 10
    # fijo si fue ayer). Mantenemos el SELECT solo para leer country.
    try:
        d = (
            supabase.table("drivers")
            .select("country")
            .eq("id", driver_id)
            .single()
            .execute()
        )
        row = d.data or {}
    except Exception as e:
        logger.warning(f"training-contribute drivers lookup failed: {e}")
        raise HTTPException(status_code=500, detail={"error": "lookup_failed"}) from e

    country_iso = row.get("country")

    # Upload + insert rows. Si falla alguna, seguimos con las demás (best-effort)
    # y reportamos el conteo final al cliente.
    import base64
    import uuid as _uuid
    inserted = 0
    failed = 0
    for img in request.images:
        try:
            img_bytes = base64.b64decode(img.image_base64, validate=False)
            uid = _uuid.uuid4().hex
            storage_path = f"contribution/{driver_id}/{uid}.jpg"
            supabase.storage.from_("ocr-training").upload(
                storage_path,
                img_bytes,
                {"content-type": "image/jpeg", "upsert": "true"},
            )
            supabase.table("ocr_corrections").insert({
                "source": "contribution",
                "driver_id": driver_id,
                "country_iso": country_iso,
                "image_storage_path": storage_path,
                "user_action": "pending",
                "user_consented_training": True,
                "is_golden_example": False,
                "prompt_version": "contribution_v1",
                "notes": f"contribución voluntaria 20may driver={driver_id[:8]}",
            }).execute()
            inserted += 1
        except Exception as e:
            failed += 1
            try:
                sentry_sdk.capture_exception(e)
            except Exception:
                pass

    if inserted == 0:
        raise HTTPException(status_code=500, detail={"error": "all_uploads_failed"})

    # Marca contribution date para activar el bonus +10 MSI quota hoy
    try:
        supabase.table("drivers").update(
            {"last_contribution_at": datetime.now(timezone.utc).isoformat()}
        ).eq("id", driver_id).execute()
    except Exception as e:
        logger.warning(f"training-contribute last_contribution_at update failed: {e}")

    return {
        "ok": True,
        "uploaded": inserted,
        "failed": failed,
        "bonus_quota_msi_tomorrow": 10,
        "message": f"¡Gracias! Subiste {inserted} etiquetas. Mañana tendrás +10 imágenes extra para el importador de pantallazos.",
    }


@app.post("/ocr/label", tags=["ocr"], summary="OCR de etiqueta de envío")
async def ocr_label(request: OCRLabelRequest, user=Depends(get_current_user)):
    """Extrae datos de una etiqueta de envío (nombre, dirección, ciudad, CP,
    provincia) con Gemini Vision. La API key se mantiene en el servidor.

    If `consent_to_training` is true, the source image is uploaded to the
    `ocr-training` bucket and an `ocr_corrections` row is created. The id
    is returned as `correction_id` so the app can PATCH it later with the
    user's accepted/edited answer.
    """
    if not get_gemini_vertex_client():
        raise HTTPException(status_code=503, detail="OCR service not configured")

    # Per-user daily image quota, shared with /ocr/screenshots-batch. Free
    # tier gets a handful of scans/day; trial and Pro+ get a much higher
    # cap. Counts 1 image per call (this endpoint is single-image only).
    tier, driver_id = _resolve_user_tier(user["id"])
    quota_key = driver_id or user["id"]
    check_ocr_image_quota(quota_key, tier, 1)

    import time
    t0 = time.perf_counter()
    try:
        data = await asyncio.to_thread(
            _ocr_label_with_gemini,
            request.image_base64,
            request.media_type,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    latency_ms = int((time.perf_counter() - t0) * 1000)

    correction_id: Optional[str] = None
    if request.consent_to_training:
        driver_id = _resolve_driver_id_from_user(user["id"])
        if driver_id:
            try:
                image_bytes = base64.b64decode(request.image_base64)
                storage_path = await asyncio.to_thread(
                    _upload_ocr_image_sync, driver_id, image_bytes, request.media_type, "label_scan"
                )
            except Exception as e:
                logger.warning(f"label_scan image decode/upload failed: {e}")
                sentry_sdk.capture_exception(e)
                storage_path = None
            parts = data if isinstance(data, dict) else {}
            # Compose a flat address string for fast diff/search later.
            extracted_address = ", ".join(
                p for p in [
                    parts.get("street"), parts.get("city"),
                    parts.get("postalCode"), parts.get("province"),
                ] if p
            ) or None
            correction_id = await asyncio.to_thread(
                _create_ocr_correction_row,
                driver_id=driver_id,
                source="label_scan",
                image_storage_path=storage_path,
                model_name=_OCR_LABEL_MODEL,
                model_extracted_address=extracted_address,
                model_extracted_parts=parts or None,
                model_confidence=None,
                carrier_hint=None,
                country_iso=None,
                model_latency_ms=latency_ms,
                consent=True,
            )

    return {
        "success": True,
        "content": json.dumps(data, ensure_ascii=False),
        "data": data,
        "correction_id": correction_id,
    }


# === MULTI-SCREENSHOT IMPORTER (Pro+ killer feature) ===
#
# Driver sends 1-10 screenshots of their carrier app (CTT, MRW, Seur, GLS,
# NACEX, Correos Express, …) or a generic stop list and we extract structured
# stops with Gemini 2.5 Pro. Day 1 returns raw extraction; Day 2 will add
# normalization + Google Geocoding with ES anchors.
#
# Gate: any paying user (Pro / Pro yearly / Pro+) OR active Pro trial.
# 20 may decision (Miguel): MSI no longer gated to Pro+ — opening it to Pro
# monthly maximizes the learning flywheel (more corrections → better OCR →
# higher willingness-to-pay later). Rate limit per tier in _OCR_DAILY_IMG_QUOTA.

_MSI_MAX_IMAGES = 10
_MSI_MAX_IMAGE_B64 = 12_000_000  # ~9 MB per image post-base64
_MSI_MODEL = "gemini-2.5-pro"


class MSIScreenshotImage(BaseModel):
    image_base64: str = Field(..., max_length=_MSI_MAX_IMAGE_B64)
    media_type: Literal["image/jpeg", "image/png", "image/webp"] = "image/jpeg"


class MSIRouteContext(BaseModel):
    depot_lat: Optional[float] = Field(default=None, ge=-90, le=90)
    depot_lng: Optional[float] = Field(default=None, ge=-180, le=180)
    country: Optional[str] = Field(default=None, max_length=2)  # ISO-2
    language: Optional[str] = Field(default="es", max_length=5)


class MSIBatchRequest(BaseModel):
    images: List[MSIScreenshotImage] = Field(..., min_length=1, max_length=_MSI_MAX_IMAGES)
    carrier_hint: Optional[Literal[
        "ctt", "mrw", "seur", "gls", "nacex", "correos_express", "tipsa",
        # Añadidos 20 may 2026 — Gemini ahora reconoce estos carriers tras
        # 50+ ejemplos seed/golden. Antes caían silenciosamente a 'generic'
        # y el few-shot dinámico nunca encontraba ejemplos del carrier real.
        "sending", "paack", "ups", "zeleris",
        "generic"
    ]] = None
    route_context: Optional[MSIRouteContext] = None
    # Opt-in flag for the OCR learning loop. When True the backend uploads
    # the screenshots to the `ocr-training` bucket and creates one row per
    # extracted stop in `ocr_corrections`, returning correction_ids that
    # the app uses to PATCH the user's final answer after review.
    consent_to_training: bool = False


def _verify_msi_access(auth_user_id: str) -> dict:
    """Gate for /ocr/screenshots-batch. Returns {tier, is_eligible, trial_eligible}.

    Eligible (decision Miguel 20 may — abrir MSI a Pro para acelerar el
    flywheel de aprendizaje):
      - Pro+ paid: promo_plan='pro_plus' AND subscription_source IN ('stripe','revenuecat')
      - Pro paid (monthly o yearly): promo_plan='pro' AND subscription_source IN ('stripe','revenuecat')
      - Pro yearly por sub_period: subscription_period='yearly' (any plan name)
      - Active trial: promo_plan IN ('pro','pro_plus') AND expires > NOW() AND source IS NULL

    Solo `free` (sin trial activo y sin pago) cae al paywall. La quota diaria
    por tier la cubre `_OCR_DAILY_IMG_QUOTA` (trial=pro=30, pro_plus=50).

    Raises HTTPException(403) si no eligible. Detail incluye `trial_eligible`
    para que la app decida mostrar "Start trial" vs "Upgrade".
    """
    try:
        d = supabase.table("drivers").select(
            "promo_plan, promo_plan_expires_at, subscription_source, subscription_period"
        ).eq("user_id", auth_user_id).single().execute()
        row = d.data or {}
    except Exception as e:
        logger.warning(f"MSI access check failed: {e}")
        raise HTTPException(status_code=403, detail={"error": "verification_failed"})

    promo = row.get("promo_plan")
    expires_raw = row.get("promo_plan_expires_at")
    sub_src = row.get("subscription_source")
    sub_period = row.get("subscription_period")

    is_pro_plus_paid = promo == "pro_plus" and sub_src in ("stripe", "revenuecat")
    is_pro_paid = promo == "pro" and sub_src in ("stripe", "revenuecat")
    is_pro_yearly = sub_period == "yearly"

    is_trial = False
    if promo in ("pro", "pro_plus") and sub_src is None and expires_raw:
        try:
            expires_at = datetime.fromisoformat(expires_raw.replace("Z", "+00:00"))
            is_trial = expires_at > datetime.now(timezone.utc)
        except (ValueError, AttributeError):
            is_trial = False

    is_eligible = is_pro_plus_paid or is_pro_paid or is_pro_yearly or is_trial

    # Tier resolution: pro_plus wins over yearly wins over pro wins over trial.
    if is_pro_plus_paid:
        tier = "pro_plus"
    elif is_pro_yearly:
        tier = "pro_yearly"
    elif is_pro_paid:
        tier = "pro"
    elif is_trial:
        tier = "trial"
    else:
        tier = "none"

    if not is_eligible:
        trial_eligible = sub_src is None and not is_trial
        raise HTTPException(
            status_code=403,
            detail={
                # Code kept as 'pro_plus_required' for backwards compat with
                # the app (4 sites in MultiScreenshotImport / screenshotImport).
                # After the 20 may decision MSI is open to Pro too — this 403
                # only fires for users with NO subscription nor active trial.
                "error": "pro_plus_required",
                "message": "El importador de pantallazos requiere suscripción. Activa la prueba o suscríbete.",
                "trial_eligible": trial_eligible,
            },
        )

    return {"tier": tier, "is_eligible": True, "trial_eligible": False}


def _msi_gemini_response_schema() -> dict:
    """JSON Schema (subset Google supports) for Gemini structured output."""
    return {
        "type": "OBJECT",
        "properties": {
            "carrier_detected": {
                "type": "STRING",
                "description": "Detected carrier from app UI hints (logo, colors, layout). Use 'generic' if uncertain.",
                "enum": [
                    "ctt", "mrw", "seur", "gls", "nacex", "correos_express", "tipsa",
                    "sending", "paack", "ups", "zeleris",
                    "generic",
                ],
            },
            "language": {"type": "STRING", "description": "Detected language (ISO-639-1). Usually 'es'."},
            "stops": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "raw_text": {"type": "STRING", "description": "Original text block from screenshot"},
                        "name": {"type": "STRING", "description": "Recipient name if visible"},
                        "street": {"type": "STRING", "description": "Street name only — NOT including floor/etc."},
                        "number": {"type": "STRING", "description": "Street number (5, 5B, 5-7, s/n)"},
                        "floor_etc": {"type": "STRING", "description": "Floor/portal/staircase: '4B', 'Esc 2', 'Pta 3'. Save for delivery instructions, NEVER send to geocoder."},
                        "postal_code": {"type": "STRING", "description": "Spanish 5-digit postal code"},
                        "city": {"type": "STRING", "description": "City / municipality"},
                        "province": {"type": "STRING", "description": "Spanish province"},
                        "phone": {"type": "STRING"},
                        "tracking_number": {"type": "STRING", "description": "Carrier tracking ID if visible"},
                        "notes": {"type": "STRING", "description": "Extra delivery instructions"},
                        "confidence_per_field": {
                            "type": "OBJECT",
                            "description": "Per-field confidence 0..1. Lower if inferred from context.",
                            "properties": {
                                "street": {"type": "NUMBER"},
                                "number": {"type": "NUMBER"},
                                "city": {"type": "NUMBER"},
                                "postal_code": {"type": "NUMBER"},
                                "province": {"type": "NUMBER"},
                            },
                        },
                        "source_image_idx": {"type": "INTEGER", "description": "0-based index of the source image in the batch"},
                        "context_inferred_fields": {
                            "type": "ARRAY",
                            "items": {"type": "STRING"},
                            "description": "Field names that were inferred from sibling stops, not explicitly visible.",
                        },
                    },
                    "required": ["street", "source_image_idx", "confidence_per_field"],
                },
            },
            "global_inference_notes": {
                "type": "STRING",
                "description": "Free-form notes about overall extraction (e.g. 'all stops appear to be in Sevilla based on visible header').",
            },
        },
        "required": ["carrier_detected", "stops"],
    }


def _msi_load_dynamic_examples(
    carrier_hint: Optional[str],
    country_iso: Optional[str] = None,
    limit: int = 3,
) -> str:
    """Lee golden examples desde ocr_corrections para inyectar como few-shot
    dinámico en el prompt de Gemini.

    Historial:
    - 20 may v1: añadido filtro carrier_hint con ilike case-insensitive.
    - 20 may v2: si carrier='generic'/None, mezcla ejemplos de varios carriers.
    - 20 may v3 (lección Miguel): añadido filtro country_iso para NO contaminar
      OCR de LATAM con seeds ES. Los 113 golden actuales son TODOS de ES — si
      llega una etiqueta CO/MX/AR/CL/PE/EC y le metemos ejemplos con "C/ Mayor 5
      28013 Madrid" como pista, el modelo aprende patrones ES (calles "C/",
      CP 5 dígitos, "Avenida", "Plaza") y los aplica mal a la dirección LATAM.
      Mejor sin ejemplos (modelo razona desde foto) que con ejemplos del país
      equivocado. Las primeras correcciones reales LATAM cuando se promuevan a
      golden alimentarán el few-shot para su propio país.

    Prefiere admin_corrected_at descendente (los más recientes son ground truth
    más fiable).
    """
    try:
        q = supabase.table("ocr_corrections").select(
            "model_extracted_address, user_final_address, admin_corrected_parts, "
            "model_extracted_parts, carrier_hint, country_iso"
        ).eq("is_golden_example", True)
        if carrier_hint and carrier_hint != "generic":
            # ilike case-insensitive — tolera 'sending'/'Sending'/'SENDING'.
            q = q.ilike("carrier_hint", carrier_hint)
        if country_iso:
            # Filtro por país DESTINO. Sin este filtro, drivers LATAM recibían
            # patrones ES como ejemplo y aprendían formatos equivocados.
            q = q.ilike("country_iso", country_iso)
        res = q.order("admin_corrected_at", desc=True).limit(limit).execute()
        rows = res.data or []
    except Exception as e:
        # Sentry capture pero NO romper el OCR si BD está mala
        try:
            sentry_sdk.capture_exception(e)
        except Exception:
            pass
        return ""

    if not rows:
        return ""

    if carrier_hint and carrier_hint != "generic":
        header_label = f"de {carrier_hint.upper()}"
    else:
        header_label = "(de varios carriers — usa el patrón general)"
    lines = [
        f"\n\n15. **EJEMPLOS REALES {header_label} corregidos por drivers/admin** (aprende estos patrones específicos):\n"
    ]
    for i, r in enumerate(rows, 1):
        model_addr = (r.get("model_extracted_address") or "").strip()
        final_addr = (r.get("user_final_address") or "").strip()
        truth_parts = r.get("admin_corrected_parts") or {}
        lines.append(f"EJEMPLO REAL #{i}:")
        if model_addr and final_addr and model_addr != final_addr:
            lines.append(f'- Modelo extrajo: "{model_addr[:140]}"')
            lines.append(f'- Resultado correcto: "{final_addr[:140]}"')
        elif final_addr:
            lines.append(f'- Dirección correcta: "{final_addr[:140]}"')
        if isinstance(truth_parts, dict) and truth_parts:
            kvs = ", ".join(f"{k}={v}" for k, v in truth_parts.items() if v)[:240]
            if kvs:
                lines.append(f"- Campos ground-truth: {kvs}")
        lines.append("")
    return "\n".join(lines)


def _msi_build_prompt(carrier_hint: Optional[str], route_context: Optional[MSIRouteContext]) -> str:
    """Builds the system prompt for screenshot extraction. Injects per-carrier
    few-shot examples desde ocr_corrections (TODO #256 cerrado 20 may 2026)."""
    carrier_line = ""
    if carrier_hint and carrier_hint != "generic":
        carrier_line = f"\nEl usuario indica que las pantallas son de la app del courier: {carrier_hint.upper()}. Usa ese contexto para localizar campos."
    else:
        carrier_line = "\nDetecta el courier (Zeleris, CTT, MRW, Seur, GLS, NACEX, Correos Express, TIPSA, PAACK, Sending, UPS) por logo, colores o disposición. Si no estás seguro, devuelve 'generic'.\n\n**Señales clave por carrier**:\n- **Zeleris**: logo verde con puntitos al final ('zeleris' o 'Zeleris logística'). Banner verde-lima horizontal con texto 'ZLR DIA SIGUIENTE 0XX-0YY-CIUDAD' o 'ZELERIS 14'. Variante 'VINOSELECCION' con formato Origen/Consignatario/Direccion. Zona reparto tipo '11CONIL', '28MOS-1', '24LEON'. RTEs frecuentes a IGNORAR: KUEHNE Y NAGEL, TME FUSION SAP, TME GRAN PUBLICO, VINOSELECCION, SOLAZZIA, WELEDA, SANYMEDICAL ARAGON, DMYTRO KURAKULOV.\n- **GLS**: logo 'GLS.' con punto (negrita) o 'ASM GLS ES' en esquina. Texto 'BusinessParcel(NN)' o 'COURIER GLS BUSINESS PARCEL24' indica servicio. Etiquetas frecuentemente rotadas 90° (texto vertical, hay que orientarlas mentalmente). Código zona tipo 'S79' grande + 'DEPOT TROCADERO 11160' (depot Cádiz) o equivalente. URL 'gls-spain.es' al pie. Abreviaturas observadas: 'BDA.' = Barriada, 'TES.' = Test/Tienda/Establecimiento (puede ir como name junto a nombre fantasía). Tracking 'Alb. Cli.' + 'Ref. Cli.'. RTEs típicos: Oxford University Press, Zara, ALDIA Reforma, polígonos industriales.\n- **PAACK**: logo 'paack' con flecha verde/morada. Badges esquina 'NT4' (Nike sender), 'CFA' (Anbo China sender), 'ECI' (El Corte Inglés con campo 'Datos de Envío'). Variante co-branded Tiendanimal/Amazon con badge 'paack' arriba derecha.\n- **Sending**: logo 'sending transporte urgente' (figura humana corriendo). Códigos 'XXX LEON', 'XXX MADRID', servicios 'SEND ECOMM', 'SEND MASIVO'.\n- **UPS**: marca clásica 'UPS' escudo marrón/dorado, banner 'UPS STANDARD', tracking '1Z...'.\n- **TIPSA**: logo TIPSA naranja/azul, formato 'ARABA 30', 'XX HORAS' (servicio horario)."

    ctx_line = ""
    if route_context:
        bits = []
        if route_context.depot_lat and route_context.depot_lng:
            bits.append(f"el depósito del repartidor está en lat={route_context.depot_lat}, lng={route_context.depot_lng}")
        if route_context.country:
            bits.append(f"país={route_context.country}")
        if bits:
            ctx_line = "\nContexto del repartidor: " + "; ".join(bits) + "."

    return f"""Eres un extractor experto de listas de paradas de reparto desde fotos de etiquetas físicas Y pantallazos de apps de paquetería españolas.{carrier_line}{ctx_line}

Recibes 1-10 imágenes que pueden mostrar la MISMA lista (scrolleada en distintas posiciones), etiquetas físicas individuales de paquetes, o listas distintas. Tu tarea:

1. Detecta cada parada/envío único. Si la misma parada aparece en 2 imágenes (porque el usuario hizo scroll), inclúyela UNA sola vez (con `source_image_idx` = la imagen donde se ve más completa).

2. Extrae los campos: name (destinatario, persona física), street (solo nombre de calle), number (número), floor_etc (piso/escalera/portal/puerta — NUNCA juntar con street), postal_code (5 dígitos), city, province, phone, tracking_number, notes.

3. Para cada campo extraído anota un `confidence_per_field` entre 0 y 1 (1 = totalmente legible y seguro).

4. **CRÍTICO — distinguir REMITENTE vs DESTINATARIO en etiquetas físicas**:
   - El "RTE." o "Rte." o "Remitente" al inicio de la etiqueta = QUIÉN ENVÍA. **JAMÁS extraigas el remitente como destinatario**. Ej: si la etiqueta dice "RTE: TME GRAN PUBLICO" arriba, ignóralo. "TME" no es la dirección.
   - El destinatario suele aparecer DESPUÉS del banner del servicio (ej. "ZLR DIA SIGUIENTE"), normalmente con el nombre de la persona en negrita y la dirección debajo.
   - Si solo ves "MADRID" o el nombre del remitente al inicio = NO es la dirección de entrega.

5. **CRÍTICO — patrón ZELERIS (logo verde con puntitos al final + banner verde-lima)**:
   - El banner verde-lima dice "ZLR DIA SIGUIENTE / 011 CADIZ" o "011-001-CADIZ". Eso es el TIPO DE SERVICIO de Zeleris, NO el destino real.
   - El destinatario real (persona + calle + CP + ciudad) aparece DEBAJO del banner verde, en bloque blanco, en este orden:
       línea 1: Nombre persona (ej. "OLGA GARCIA ESTEVE")
       línea 2: Calle + número (ej. "Avenida De Dolores Ibarruri")
       línea 3: CP + ciudad (ej. "11140 Conil De La Frontera")
   - La "ZONA DE REPARTO" (ej. "11140 / 11CONIL") confirma el CP y zona del destinatario.
   - Carrier real = Zeleris (no TME, no el remitente del banner superior).

6. **Inferencia contextual**: si una parada no muestra ciudad/provincia pero el resto de la lista sí, infiere usándolas. Marca esos campos en `context_inferred_fields` y baja su confidence a ≤0.7.

7. **NUNCA inventes**. Si no puedes leer un campo y NO hay contexto suficiente, deja el campo vacío (string vacía).

8. Para `floor_etc` extrae expresiones como "4ºB", "Esc 2", "Pta 3", "Portal C", "Pl Bajo", "1º derecha". Estas NUNCA van junto a la calle, van separadas para añadirlas a las notas del repartidor.

8.b **Normalizaciones específicas observadas en etiquetas reales** (lecciones de 25+ ejemplos seed, 20 may 2026):
   - "n12", "Nº 12", "número 12", "núm. 12" → number="12" (solo el dígito).
   - "Pdo" manuscrito junto al número → floor_etc="Puerta" (abreviatura común León/Castilla).
   - "Pta No Aplica" → IGNORAR (no es información útil, no rellenar floor_etc).
   - "P.Bajo", "Pl Bajo", "Bajo L", "Bajo" → floor_etc="Planta Bajo" (o "Planta Bajo (xxx)" si lleva apellido como "Bajo L Mapfre", "Bajo D").
   - "Esc Única", "Esc Izquierda", "Escalera 2" → floor_etc="Esc <X>".
   - "Pl 2 Pta A", "Planta 3 Puerta B" → floor_etc="Pl <X> Pta <Y>".
   - "Chalet 58", "Casa 12", "Nave 17" en urbanizaciones → number="58"/"12"/"17", floor_etc="Chalet"/"Casa"/"Nave" (la urbanización va en street).
   - "Carril", "Cortijo", "Pago de", "Camino" son prefijos VÁLIDOS de vía rural (Cádiz, Castilla, Galicia) — no los confundas con texto suelto.
   - PAACK NT4 normaliza "Len" impreso → "León" (typo conocido).
   - Cuando la etiqueta lleva "Adjuntar al pedido X", "Entrega a partir de las HH:MMh", "Entregar en el Hotel X" → va a `notes`, no a otros campos.

8.c **🎯 business_name (CAMPO CRÍTICO PARA GEOCODING)**: cuando la etiqueta lleva un nombre de empresa, comercio, local o establecimiento (sufijo SL/SA/SLU/SAU, palabra FARMACIA/HOTEL/RESTAURANTE/SUPERMERCADO/CLÍNICA/HOSPITAL/COLEGIO, marca conocida como MAPFRE/MERCADONA/EL CORTE INGLÉS, o nombre de fantasía como "Konilcity SL"/"Hotel Arena House"/"Bar Manolo"), **extráelo SIEMPRE en el campo `business_name`** del JSON, NO en floor_etc ni en name.

   Por qué importa: el cliente downstream puede buscarlo en Google Places **directamente** ("Konilcity SL Conil") y Google devuelve la dirección EXACTA del local — muchísimo más fiable que geocodificar una calle sola que a veces es vía rural sin número (Carril Los Limas) o una avenida larga (Avda Dolores Ibárruri). Si la calle viene con la empresa, ambas se quedan: street para fallback, business_name para Place Search prioritario.

   Reglas:
   - Si ves "Mapfre" junto a "Avda Suero de Quiñones 4 P.Bajo" → business_name="Mapfre", floor_etc="Planta Bajo", street/number SE QUEDAN.
   - Si ves "Konilcity SL" junto a "Carril Los Limas" → business_name="Konilcity SL", street="Carril Los Limas", number="S/N" si no hay (la empresa GANA al geocodificar).
   - Si ves "Farmacia" junto a "Avda Dolores Ibárruri" → business_name="Farmacia", street se queda. NO meter "Farmacia" en floor_etc.
   - Distingue: business_name es el LOCAL DE ENTREGA. name es la PERSONA DESTINATARIA. Pueden coexistir: name="Olga García", business_name="Farmacia".
   - Si no hay name persona pero hay empresa → business_name está, name puede quedar vacío.

   Añade además `geocoding_hint` (texto libre) con la sugerencia de búsqueda si tu olfato indica que la empresa es muy buscable ("Buscar 'Konilcity SL Conil' en Google Places — devuelve dirección exacta").

9. España: provincias con tilde correctamente ("Cádiz", "Córdoba", "Almería"). Códigos postales 5 dígitos.

10. **Texto rotado 90°/180°/270°**: las etiquetas físicas frecuentemente se pegan rotadas sobre el paquete. Lee la imagen mentalmente desde TODAS las orientaciones lógicas. Si lees el texto principal del barcode "al revés" → la etiqueta está rotada 180° y debes rotar mentalmente para extraer todo. NUNCA descartes una etiqueta por estar rotada — siempre tienen contenido extraíble.

11. **Nunca dejes street vacío** si la imagen tiene CP+ciudad legibles y SI hay alguna línea con patrón de dirección. Busca "Avda", "Av.", "Calle", "C/", "C.", "Plaza", "Pza", "Camino", "Carretera", "Ctra", "Rúa", "Carrer", "Cuesta", "Travesía", "Paseo", "Polígono", "Carril", "Cortijo", "Urbanización", "Urb.", "Barriada", "Bda.", "Diseminado", "Pago de" seguidos opcionalmente de número.

11.b **CRÍTICO — NUNCA dejes `number` ni `postal_code` vacíos si están visibles en la imagen**. Patrón observado 20 may: en un lote de 10 etiquetas de Móstoles, el modelo extrajo correctamente la calle ("CALLE URANO", "BERLIN", "REYES CATOLICOS", etc.) pero devolvió `number=""` y `postal_code=""` en TODAS aunque las etiquetas físicas los llevaban impresos. ANTES de devolver un campo vacío:
   - Vuelve a inspeccionar la imagen buscando explícitamente: dígitos sueltos cerca del nombre de la calle (ej. "C/NIZA 12", "BERLIN, 4"), dígitos al final de la línea de dirección, números separados por coma/guion, números pegados a "nº", "núm", "#".
   - Para `postal_code`: busca 5 dígitos juntos junto a la ciudad ("28937 Móstoles", "28938 MOSTOLES"), o solos en otra línea.
   - Si tras releer SIGUEN sin estar visibles, devuelve "" y baja `confidence_per_field` de ese campo a 0.2. Pero **el comportamiento por defecto debe ser EXTRAER, no omitir**.
   - Para CEIPs/colegios/empresas/comercios: la etiqueta de mensajería SIEMPRE lleva número de portal (el repartidor no entregaría sin él). Si no lo ves, está rotado, en pequeño, o tapado — sigue buscando antes de rendirte.

12. Si una imagen no contiene una lista de paradas Y tampoco una etiqueta de envío (foto random sin texto reconocible), simplemente no añadas paradas de ahí.

13. **EJEMPLOS RESUELTOS** (etiquetas físicas Zeleris reales de Conil/Cádiz — aprende estos patrones):

EJEMPLO A — Etiqueta rotada 180° + CP 11580 ≠ Conil:
- Vista superficial: parece rotada, banner "ZLR DIA SIGUIENTE 011 001 - CADIZ", RTE arriba "TME GRAN PUBLICO - MADRID".
- Extracción correcta:
  name: "Elena Maria Salas Perdigones"
  street: "Avenida La Independencia"  number: "55"
  postal_code: "11580"  city: "San Jose del Valle"  province: "Cádiz"
- LECCIÓN: CP=11580 es San José del Valle (pedanía), NO Conil. NUNCA asumir Conil solo por ver "CADIZ" en el banner. El CP manda. El RTE "TME GRAN PUBLICO" se IGNORA.

EJEMPLO B — Destinatario es COMERCIO + prefijo rural "Carril":
- Vista superficial: nombre del destinatario en mayúsculas tipo razón social, calle empieza por "Carril".
- Extracción correcta:
  name: "CADIZFORNIA"  (es un comercio, mantener mayúsculas)
  street: "Carril Guerrero"  number: "4"  floor_etc: "Casa de Postas"
  postal_code: "11140"  city: "Conil de la Frontera"  province: "Cádiz"
- LECCIÓN: el destinatario puede ser una empresa/comercio (mantén el nombre como aparece). "Carril" es prefijo VÁLIDO de vía rural en Conil — NO descartar como ruido. "Casa de Postas" es el nombre del local → va en floor_etc, no en street.

EJEMPLO C — Vivienda con nombre + confusión RTE/destino:
- Vista superficial: arriba "PODENCO ACTIVE - TOLEDO" (RTE), banner ZLR, abajo destinatario.
- Extracción correcta:
  name: "LYDIA GASKELL"
  street: "Carretera Fuente del Gallo"  number: "728"  floor_etc: "Casa Alondra"
  postal_code: "11140"  city: "Conil de la Frontera"  province: "Cádiz"
- LECCIÓN: "PODENCO ACTIVE" es el RTE (toledano), NUNCA lo extraigas como destino. "Casa Alondra" es el nombre de la vivienda (típico zonas rurales Conil) → floor_etc. Carretera es prefijo VÁLIDO.

14. **Patrón PAACK (logo "paack" + flecha verde/morada arriba — variantes CFA, NT4, ECI, co-branded)**:
    - PAACK NO imprime su propia info de origen; reusa la del cliente final (Anbo/Shein desde "Prologis (Anbo) Logistics Center, Datang Town, 528143 FO SHAN, CN" / Nike Europe desde "Carrer Camí de la Font Freda 1, 08110 Montcada i Reixac" / Amazon.es desde "Avenida de la Astronomía 24, 28830 San Fernando de Henares" / Tiendanimal vía DHL desde Ontigola). **JAMÁS extraigas ninguna de esas direcciones como destino** — son el ORIGEN.
    - El destinatario real (persona + calle + CP + ciudad ES) aparece en el bloque CENTRAL de la etiqueta, normalmente DEBAJO del bloque del sender.
    - El número grande arriba a la derecha (ej. 24391, 24350) suele ser el **wave/oleada de PAACK**, NO el postal_code. El CP real está en el bloque del destinatario y suele coincidir pero NO siempre.
    - Badges `CFA`, `NT4`, `ECI` en esquinas son tipo de servicio PAACK, NO ciudad ni CP.
    - Variante `ECI` (El Corte Inglés): layout en formato "Datos de Envío" con campos Operación venta + Pedido + Bulto + Oleada. Texto literal "PAACK" en medio inferior.
    - Variante `co-branded Tiendanimal`: layout completo de Tiendanimal con badge paack arriba derecha + texto "Next-day delivery / Timeslot available - X hours".
    - Variante `Amazon-Paack`: layout Amazon estándar con badge paack arriba derecha + códigos sortation (MAD4, A266, etc.).
    - Etiquetas rotadas 90/180/270° muy frecuentes en CFA (sobre bolsas Anbo).
    - Normalización: ciudad impresa como "Len" debe normalizarse a "León" (typo recurrente en generador NT4).
    - `n12` significa "número 12" — extraer solo el dígito (`12`) en number.
    - Pedanías de León típicas: Carbajal de la Legua, Aldea de la Valdoncina, San Miguel del Camino, Villarejo de Orbigo, Espinosa de la Ribera, Sariegos.

EJEMPLO D — Paack CFA con sender Anbo China rotada:
- Vista superficial: bolsa con etiqueta rotada 270°, logo paack arriba, badge CFA, sender "Prologis (Anbo) Logistics Center, 528143 FO SHAN, CN" arriba.
- Extracción correcta:
  name: "Camino Fernandez Gutierrez"
  street: "Camino Quintana"  number: "15"  floor_etc: ""
  postal_code: "24391"  city: "San Miguel del Camino"  province: "León"
- LECCIÓN: ignorar el sender chino. El destinatario está en el bloque central debajo. Rotación 270° → rotar mentalmente para leer.

EJEMPLO E — Paack NT4 con typo "Len" y dirección con piso:
- Vista superficial: etiqueta rotada 90°, logo paack + badge NT4, sender Nike Europe Montcada, ciudad impresa como "Len".
- Extracción correcta:
  name: "Álvarez Villa M. Cristina"
  street: "Calle el Fuero"  number: "15"  floor_etc: "4C"
  postal_code: "24001"  city: "León"  province: "León"
- LECCIÓN: "Len" → "León" (typo conocido NT4). "15 4C" se separa: number="15", floor_etc="4C". Sender Nike es ORIGEN, no destino.

EJEMPLO F — Paack con dirección manuscrita sobre impresa tachada:
- Vista superficial: dirección impresa "Avenida de Portugal 7" con un trazo a bolígrafo que la tacha + escrito a mano al lado "Rep Argentina 31 Pdo".
- Extracción correcta (la manuscrita gana):
  name: "Adriana Escalona"
  street: "Calle República Argentina"  number: "31"  floor_etc: "Puerta"
  postal_code: "24009"  city: "León"  province: "León"
- LECCIÓN: cuando hay manuscrita superpuesta sobre dirección impresa tachada, la MANUSCRITA es la dirección REAL (re-direccionado). "Pdo" en este contexto = "Puerta" (abreviatura manuscrita común en León/Castilla). Expandir abreviaturas conocidas: "Pdo" → Puerta, "Pl" → Planta, "Esc" → Escalera, "Pta" → Puerta.
{_msi_load_dynamic_examples(carrier_hint, country_iso=(route_context.country if route_context and route_context.country else None))}
Responde EXCLUSIVAMENTE con un JSON válido siguiendo el schema indicado. Sin texto adicional, sin markdown."""


def _msi_extract_stops_with_gemini(
    images: List[MSIScreenshotImage],
    carrier_hint: Optional[str],
    route_context: Optional[MSIRouteContext],
) -> dict:
    """Synchronous helper that calls Gemini 2.5 Pro with multimodal request +
    structured response schema. Returns the parsed JSON dict. Wrap in
    asyncio.to_thread() from async caller — google-genai SDK is blocking.

    Uses Vertex AI (europe-west4) so the screenshots — which expose lists of
    recipients with names and addresses — stay inside the EU and are
    covered by the Google Cloud DPA, not the AI Studio terms."""
    from google.genai import types

    client = get_gemini_vertex_client()
    if not client:
        raise HTTPException(status_code=503, detail="Gemini AI no configurado")

    parts: list = [types.Part.from_text(text=_msi_build_prompt(carrier_hint, route_context))]
    for idx, img in enumerate(images):
        parts.append(types.Part.from_text(text=f"[Imagen #{idx}]"))
        parts.append(
            types.Part.from_bytes(
                data=base64.b64decode(img.image_base64),
                mime_type=img.media_type,
            )
        )

    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=_msi_gemini_response_schema(),
        temperature=0.1,
        max_output_tokens=8192,
    )

    response = client.models.generate_content(
        model=_MSI_MODEL,
        contents=[types.Content(role="user", parts=parts)],
        config=config,
    )
    text = (response.text or "").strip()
    if not text:
        raise HTTPException(status_code=502, detail="Empty response from Gemini")
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"MSI Gemini returned invalid JSON: {e}; text[:200]={text[:200]}")
        raise HTTPException(status_code=502, detail="Invalid JSON from Gemini")


# --- Day 2: normalization, geocoding, confidence ---
#
# Gemini returns raw text fields. To make the stops actually usable we:
#   1) Normalize each stop (clean fields, defensively re-split floor_etc from
#      street if the model leaked it through).
#   2) Geocode each stop with Google Geocoding using country:ES +
#      postal_code anchor + (round 2) centroid bounds for stops that failed
#      round 1.
#   3) Compute a HIGH/MEDIUM/LOW confidence per stop combining extraction
#      confidence, geocoder location_type, and presence of inferred fields.
#   4) For LOW, fetch up to 3 autocomplete candidates so the user can pick.

# Spanish 5-digit postal codes: first 2 digits → province. Static map (52
# provinces inc. Ceuta/Melilla). Used as a defensive anchor when Gemini
# extracts CP but no province (or wrong province).
_MSI_CP_PROVINCE_MAP = {
    "01": "Álava",         "02": "Albacete",      "03": "Alicante",
    "04": "Almería",       "05": "Ávila",         "06": "Badajoz",
    "07": "Baleares",      "08": "Barcelona",     "09": "Burgos",
    "10": "Cáceres",       "11": "Cádiz",         "12": "Castellón",
    "13": "Ciudad Real",   "14": "Córdoba",       "15": "A Coruña",
    "16": "Cuenca",        "17": "Girona",        "18": "Granada",
    "19": "Guadalajara",   "20": "Guipúzcoa",     "21": "Huelva",
    "22": "Huesca",        "23": "Jaén",          "24": "León",
    "25": "Lleida",        "26": "La Rioja",      "27": "Lugo",
    "28": "Madrid",        "29": "Málaga",        "30": "Murcia",
    "31": "Navarra",       "32": "Ourense",       "33": "Asturias",
    "34": "Palencia",      "35": "Las Palmas",    "36": "Pontevedra",
    "37": "Salamanca",     "38": "Santa Cruz de Tenerife",
    "39": "Cantabria",     "40": "Segovia",       "41": "Sevilla",
    "42": "Soria",         "43": "Tarragona",     "44": "Teruel",
    "45": "Toledo",        "46": "Valencia",      "47": "Valladolid",
    "48": "Vizcaya",       "49": "Zamora",        "50": "Zaragoza",
    "51": "Ceuta",         "52": "Melilla",
}

# Regex patterns to detect floor/portal/staircase fragments that occasionally
# leak into `street`. Word-bounded so they never match substrings inside a
# longer word (e.g. "esc" must not match the "esc" inside "Desconocida").
_MSI_FLOOR_RE = re.compile(
    r"(?:,\s*|\s+)(?P<frag>"
    r"(?:\bpiso\b|\bpta\b|\bpuerta\b|\besc\b|\bescalera\b|\bportal\b|\bbloque\b|\bblq\b|"
    r"\d+\s*[ºªo]\s*[A-Za-zÀ-ÿ]?(?:\s+(?:izda|dcha|izquierda|derecha))?|"
    r"\b\d+[A-Za-z]\b"
    r")(?:[^,]*?))\s*$",
    re.IGNORECASE,
)

# Detects whether a string contains a Spanish/LATAM street-type prefix.
# Used to tell "Calle Real" (deliverable) from "San José del Valle"
# (just a town the model misclassified into the street field). Covers
# common ES forms + Catalan (Carrer) and Galician (Rúa). Word-boundary
# escapes guard against false positives inside other words.
_MSI_STREET_TYPE_RE = re.compile(
    r"\b("
    r"av(?:\.|enida)?|"
    r"c\/|c\.|calle|"
    r"plaza|pza|plza|"
    r"camino|cami|"
    r"carretera|ctra|crta|cra|"
    r"traves(?:i|í)a|trav|"
    r"paseo|p\.º|po|"
    r"pol(?:\.|í|i)?gono|pol|pg|"
    r"urbanizaci(?:o|ó)n|urb|"
    r"r(?:ú|u)a|carrer|"
    r"v(?:i|í)a|ronda|cuesta|"
    r"callej(?:o|ó)n|"
    r"gran\s+v(?:i|í)a|"
    r"bulevar|bvar|blvd|"
    r"glorieta|glta|"
    r"rambla|"
    r"cl|av\b"
    r")\b\.?\s",
    re.IGNORECASE,
)


def _msi_postal_code_to_province(cp: Optional[str]) -> Optional[str]:
    """Map Spanish 5-digit postal code first 2 digits to province name."""
    if not cp:
        return None
    digits = re.sub(r"\D", "", cp)
    if len(digits) >= 2:
        return _MSI_CP_PROVINCE_MAP.get(digits[:2])
    return None


def _msi_normalize_extracted_stop(stop: dict) -> dict:
    """Defensive cleanup of one stop dict from Gemini. Returns mutated dict.

    - Trim whitespace on string fields
    - Reject garbage placeholders FIRST so they don't survive later steps
    - Strip non-digits from postal_code, take first 5
    - If `street` ends with a floor/portal fragment that Gemini missed,
      move it to `floor_etc` so it never reaches the geocoder
    - If province is missing but postal_code is valid Spanish CP, infer it
    """
    # Trim everything
    for k in ("name", "street", "number", "floor_etc", "city",
              "province", "phone", "tracking_number", "notes"):
        if isinstance(stop.get(k), str):
            stop[k] = stop[k].strip()

    # Reject placeholder/garbage values BEFORE other transforms so they
    # never reach the floor splitter or the geocoder.
    GARBAGE = {
        "DIRECCIÓN DESCONOCIDA", "DIRECCION DESCONOCIDA", "TBD", "S/I", "N/A",
        "PENDIENTE", "—", "-", "?", "??", "DESCONOCIDA", "DESCONOCIDO",
    }
    for fld in ("street", "city", "province"):
        v = (stop.get(fld) or "").upper().strip()
        if v in GARBAGE:
            stop[fld] = ""

    # Postal code: keep only digits, take first 5
    if stop.get("postal_code"):
        digits = re.sub(r"\D", "", stop["postal_code"])
        stop["postal_code"] = digits[:5] if len(digits) >= 5 else ""

    # Defensive: re-split floor_etc from street if Gemini missed it
    street = stop.get("street") or ""
    if street and not stop.get("floor_etc"):
        m = _MSI_FLOOR_RE.search(street)
        if m:
            frag = m.group("frag").strip(", ").strip()
            # Only split when the fragment is plausibly a floor/portal — not
            # the whole street (e.g. "4ºB" alone shouldn't replace "Mayor").
            remaining = street[: m.start()].strip(" ,")
            if remaining and len(remaining) >= 3:
                stop["street"] = remaining
                stop["floor_etc"] = frag

    # Anchor province from CP if missing. Use direct dict access (not
    # `setdefault(...) or []`) — when the existing list is empty it is
    # falsy and `or` would shadow the real list with a fresh one,
    # silently dropping the append.
    if not stop.get("province"):
        inferred = _msi_postal_code_to_province(stop.get("postal_code"))
        if inferred:
            stop["province"] = inferred
            cif = stop.get("context_inferred_fields")
            if not isinstance(cif, list):
                cif = []
                stop["context_inferred_fields"] = cif
            if "province" not in cif:
                cif.append("province")
            cpf = stop.get("confidence_per_field")
            if not isinstance(cpf, dict):
                cpf = {}
                stop["confidence_per_field"] = cpf
            cpf["province"] = max(0.85, cpf.get("province", 0.0))

    return stop


def _msi_compute_centroid_bbox(coords: list) -> Optional[dict]:
    """Given list of {lat, lng}, return a bounding box dict {ne_lat, ne_lng,
    sw_lat, sw_lng} centered on the centroid with ~30 km padding. Returns
    None if fewer than 2 valid coords (insufficient signal)."""
    valid = [c for c in coords if c and c.get("lat") is not None and c.get("lng") is not None]
    if len(valid) < 2:
        return None
    avg_lat = sum(c["lat"] for c in valid) / len(valid)
    avg_lng = sum(c["lng"] for c in valid) / len(valid)
    # ~30km bbox: 0.27° lat, 0.40° lng at lat=40
    lat_pad = 0.27
    lng_pad = 0.40 / max(math.cos(math.radians(avg_lat)), 1e-6)
    return {
        "sw_lat": avg_lat - lat_pad, "sw_lng": avg_lng - lng_pad,
        "ne_lat": avg_lat + lat_pad, "ne_lng": avg_lng + lng_pad,
    }


# ===== MSI Geocoding cache (Miguel 21 may 2026) =====
# Saves ~$510/mes hoy, ~$1200/mes a 100 paying. Repartidores típicos
# importan las mismas zonas repetidamente — hit rate alto.
# TTL 24h: conservador vs Google ToS (max 30d para lat/lng).
# Max 5000 entradas: ~500KB memoria (~100 bytes/entry).
# In-memory por proceso: restart Railway lo borra (acceptable).
# Solo cachea status=ok — errores se reintentan en próxima llamada.
# Solo cachea round 1 (sin bbox) — round 2 con bbox altera deliberadamente.
_MSI_GEOCODE_CACHE: dict[str, tuple[float, dict]] = {}
_MSI_GEOCODE_CACHE_MAX = 5000
_MSI_GEOCODE_CACHE_TTL_S = 24 * 3600  # 24h


def _msi_geocode_cache_key(stop: dict, country: str) -> Optional[str]:
    """Build cache key. Returns None if stop too vague to safely cache.
    Requires street + (cp OR city) to avoid collisions / wrong results.
    """
    street = (stop.get("street") or "").strip().lower()
    number = (stop.get("number") or "").strip().lower()
    cp = (stop.get("postal_code") or "").strip().lower()
    city = (stop.get("city") or "").strip().lower()
    country_n = country.upper()
    if not street or not (cp or city):
        return None
    return f"{country_n}|{cp}|{city}|{street}|{number}"


def _msi_geocode_cache_get(key: str) -> Optional[dict]:
    import time as _time
    entry = _MSI_GEOCODE_CACHE.get(key)
    if not entry:
        return None
    cached_at, value = entry
    if _time.time() - cached_at > _MSI_GEOCODE_CACHE_TTL_S:
        _MSI_GEOCODE_CACHE.pop(key, None)
        return None
    return value


def _msi_geocode_cache_set(key: str, value: dict) -> None:
    import time as _time
    if len(_MSI_GEOCODE_CACHE) >= _MSI_GEOCODE_CACHE_MAX:
        # Eviction LRU: borrar la entrada más antigua
        oldest_key = min(_MSI_GEOCODE_CACHE.items(), key=lambda kv: kv[1][0])[0]
        _MSI_GEOCODE_CACHE.pop(oldest_key, None)
    _MSI_GEOCODE_CACHE[key] = (_time.time(), value)


async def _msi_geocode_one(
    client: "httpx.AsyncClient",
    stop: dict,
    country: str = "ES",
    bbox: Optional[dict] = None,
) -> dict:
    """Single async call to Google Geocoding for one stop.

    Returns a dict augmenting the stop:
      { lat, lng, formatted_address, location_type, place_id, status }
    where status ∈ {"ok", "zero_results", "error"}.

    NEVER includes floor_etc in the geocoded address — that's preserved
    separately for the driver's delivery_instructions.

    CACHE (Miguel 21 may 2026): si NO hay bbox (= round 1) Y la stop tiene
    street + (cp OR city), buscar primero en `_MSI_GEOCODE_CACHE`. Si HIT con
    TTL válido (<24h), devolver directamente. Si MISS, geocodificar y cachear
    solo si status=ok. Saves ~$510/mes.
    """
    if not GOOGLE_API_KEY:
        return {"status": "error", "error": "no_api_key"}

    # CACHE lookup (solo round 1: sin bbox)
    cache_key: Optional[str] = None
    if bbox is None:
        cache_key = _msi_geocode_cache_key(stop, country)
        if cache_key:
            cached = _msi_geocode_cache_get(cache_key)
            if cached is not None:
                return {**cached, "_from_cache": True}

    parts = []
    if stop.get("street"):
        parts.append(stop["street"])
    if stop.get("number"):
        parts.append(stop["number"])
    if stop.get("city"):
        parts.append(stop["city"])
    if stop.get("province"):
        parts.append(stop["province"])
    address_query = ", ".join(p for p in parts if p)

    if not address_query.strip():
        return {"status": "zero_results"}

    components_parts = [f"country:{country}"]
    if stop.get("postal_code"):
        components_parts.append(f"postal_code:{stop['postal_code']}")

    params = {
        "address": address_query,
        "components": "|".join(components_parts),
        "language": "es",
        "region": "es",
        "key": GOOGLE_API_KEY,
    }
    if bbox:
        params["bounds"] = f"{bbox['sw_lat']},{bbox['sw_lng']}|{bbox['ne_lat']},{bbox['ne_lng']}"

    try:
        resp = await client.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params=params,
            timeout=10.0,
        )
        data = resp.json()
    except Exception as e:
        return {"status": "error", "error": str(e)[:100]}

    status = data.get("status")
    if status == "ZERO_RESULTS":
        return {"status": "zero_results"}
    if status != "OK" or not data.get("results"):
        return {"status": "error", "error": status or "unknown"}

    r = data["results"][0]
    geom = r.get("geometry", {})
    result = {
        "status": "ok",
        "lat": geom.get("location", {}).get("lat"),
        "lng": geom.get("location", {}).get("lng"),
        "formatted_address": r.get("formatted_address", ""),
        "location_type": geom.get("location_type", ""),  # ROOFTOP|RANGE_INTERPOLATED|GEOMETRIC_CENTER|APPROXIMATE
        "place_id": r.get("place_id", ""),
    }
    # CACHE: guardar SOLO si tenemos key válida (round 1, stop específica)
    if cache_key:
        _msi_geocode_cache_set(cache_key, result)
    return result


def _msi_classify_confidence(stop: dict, geo: dict) -> str:
    """Combine Gemini extraction confidence + Google location_type + inferred
    fields → discrete bucket. Used to color the UI chip and decide whether to
    auto-add or force user confirmation.

    HIGH (≥0.85)  : ROOFTOP/RANGE_INTERPOLATED, no critical inferred fields,
                    extraction confidence on street ≥ 0.85.
    MEDIUM (0.60-0.85) : GEOMETRIC_CENTER with CP match, OR inferred city,
                    OR slightly low extraction confidence.
    LOW (<0.60)   : APPROXIMATE, geocoder failed, or low extraction confidence.
    """
    if geo.get("status") != "ok":
        return "low"
    location_type = geo.get("location_type", "")
    cif = stop.get("context_inferred_fields") or []
    cpf = stop.get("confidence_per_field") or {}
    street_conf = cpf.get("street", 0.5)
    city_conf = cpf.get("city", 0.5)

    if location_type in ("ROOFTOP", "RANGE_INTERPOLATED"):
        if street_conf >= 0.85 and "street" not in cif:
            return "high"
        return "medium"

    if location_type == "GEOMETRIC_CENTER" and stop.get("postal_code"):
        if street_conf >= 0.7 and city_conf >= 0.5:
            return "medium"

    return "low"


async def _msi_get_candidates_for_stop(
    client: "httpx.AsyncClient",
    stop: dict,
    bbox: Optional[dict] = None,
    max_candidates: int = 3,
) -> list:
    """Last resort for LOW-confidence stops: hit Google Places Autocomplete
    biased to the centroid bbox so the user can pick from real options.

    Returns a list of {description, place_id} dicts (max `max_candidates`).
    """
    if not GOOGLE_API_KEY:
        return []
    query_bits = [stop.get("street") or "", stop.get("number") or "", stop.get("city") or ""]
    query = " ".join(b for b in query_bits if b).strip()
    if not query:
        return []
    params = {"input": query, "language": "es", "key": GOOGLE_API_KEY,
              "components": "country:es"}
    if bbox:
        avg_lat = (bbox["sw_lat"] + bbox["ne_lat"]) / 2
        avg_lng = (bbox["sw_lng"] + bbox["ne_lng"]) / 2
        params["location"] = f"{avg_lat},{avg_lng}"
        params["radius"] = "30000"
    try:
        resp = await client.get(
            "https://maps.googleapis.com/maps/api/place/autocomplete/json",
            params=params,
            timeout=8.0,
        )
        data = resp.json()
    except Exception:
        return []
    if data.get("status") != "OK":
        return []
    out = []
    for p in (data.get("predictions") or [])[:max_candidates]:
        out.append({"description": p.get("description", ""), "place_id": p.get("place_id", "")})
    return out


async def _msi_normalize_and_geocode(
    stops_raw: list,
    route_context: Optional[MSIRouteContext],
) -> list:
    """Full pipeline: normalize each stop, geocode in 2 rounds with anchors,
    compute confidence, fetch candidates for LOW. Returns enriched stops list."""
    country = (route_context.country if route_context and route_context.country else "ES").upper()

    # Step 1: normalize all stops
    stops = [_msi_normalize_extracted_stop(dict(s)) for s in stops_raw]

    if not stops:
        return []

    client = google_maps_client()

    # Step 2: round 1 — geocode all in parallel with country + CP anchors
    round1_results = await asyncio.gather(
        *[_msi_geocode_one(client, s, country=country) for s in stops],
        return_exceptions=True,
    )

    coords_for_centroid = []
    for s, geo in zip(stops, round1_results):
        if isinstance(geo, dict) and geo.get("status") == "ok":
            s["_geo"] = geo
            coords_for_centroid.append({"lat": geo["lat"], "lng": geo["lng"]})
        else:
            s["_geo"] = {"status": (geo.get("status") if isinstance(geo, dict) else "error")}

    bbox = _msi_compute_centroid_bbox(coords_for_centroid)

    # Step 3: round 2 — re-geocode failures using centroid bbox bias
    if bbox:
        retry_indices = [i for i, s in enumerate(stops) if s["_geo"].get("status") != "ok"]
        if retry_indices:
            retry_results = await asyncio.gather(
                *[_msi_geocode_one(client, stops[i], country=country, bbox=bbox)
                  for i in retry_indices],
                return_exceptions=True,
            )
            for i, geo in zip(retry_indices, retry_results):
                if isinstance(geo, dict) and geo.get("status") == "ok":
                    stops[i]["_geo"] = geo

    # Step 4: classify confidence + fetch candidates for LOW in parallel
    candidate_tasks = {}
    for i, s in enumerate(stops):
        s["confidence"] = _msi_classify_confidence(s, s["_geo"])
        if s["confidence"] == "low":
            candidate_tasks[i] = _msi_get_candidates_for_stop(client, s, bbox=bbox)
    if candidate_tasks:
        candidates_by_idx = await asyncio.gather(
            *candidate_tasks.values(), return_exceptions=True
        )
        for idx, cands in zip(candidate_tasks.keys(), candidates_by_idx):
            stops[idx]["candidates"] = cands if isinstance(cands, list) else []

    # Step 5: shape output, drop internal _geo, expose flat fields the client expects
    out = []
    for s in stops:
        geo = s.pop("_geo", {}) or {}

        # A stop is "insufficient" if it doesn't carry a *deliverable*
        # street. Two failure modes seen in the field:
        #   1) street is literally empty → obviously empty.
        #   2) street has text but it's a city/town name with no number
        #      and no street-type prefix (e.g. Gemini puts "San José del
        #      Valle" in street because the label only has CP+town). The
        #      driver still can't deliver — there's no calle.
        # We accept a street only if it has a digit (likely number) OR a
        # recognised street-type word (Avda, C/, Plaza, Camino, …). Any
        # other case is treated as empty and the row goes red.
        street_text = (s.get("street") or "").strip()
        has_digit = bool(re.search(r"\d", street_text))
        has_street_type = bool(_MSI_STREET_TYPE_RE.search(street_text))
        is_empty = not street_text or (not has_digit and not has_street_type)

        if is_empty:
            flat_address = ""
        else:
            flat_address_parts = [
                s.get("street") or "",
                (s.get("number") or "").strip(),
            ]
            flat_address = " ".join(p for p in flat_address_parts if p).strip()
            if s.get("postal_code"):
                flat_address = f"{flat_address}, {s['postal_code']}".strip(", ")
            if s.get("city"):
                flat_address = f"{flat_address} {s['city']}".strip()

        s["coords"] = (
            {"lat": geo["lat"], "lng": geo["lng"]} if geo.get("status") == "ok" else None
        )
        s["formatted_address"] = "" if is_empty else (geo.get("formatted_address") or flat_address)
        s["place_id"] = geo.get("place_id", "")
        s["delivery_instructions"] = (s.get("floor_etc") or "").strip()
        s["geocoding_status"] = "empty_extraction" if is_empty else geo.get("status", "error")
        s["is_empty_extraction"] = is_empty
        if is_empty:
            # Force confidence to low so the UI surfaces the row in red
            # and keeps the driver from blindly clicking through.
            s["confidence"] = "low"
        s.setdefault("candidates", [])
        out.append(s)

    return out


@app.post(
    "/ocr/screenshots-batch",
    tags=["ocr", "msi"],
    summary="Importar paradas desde 1-10 pantallazos (Pro+ feature)",
)
async def ocr_screenshots_batch(req: MSIBatchRequest, user=Depends(get_current_user)):
    """Multi-Screenshot Importer: extract stops from a batch of carrier-app
    screenshots using Gemini 2.5 Pro, then normalize and geocode them.

    Pipeline:
      1. Verify Pro+/yearly/trial eligibility
      2. Per-tier daily quota (5 trial, 50 Pro+)
      3. Gemini multi-image extraction with structured output
      4. Normalize each stop (split floor_etc, infer province from CP, scrub)
      5. Geocode round 1 with country:ES + CP anchors (parallel)
      6. Compute centroid bbox; re-geocode failures (round 2) with bounds bias
      7. Classify confidence HIGH/MEDIUM/LOW
      8. For LOW, fetch up to 3 autocomplete candidates so user can pick

    Gate: Pro+ paid OR Pro yearly OR active Pro trial. Pro paid monthly is NOT
    eligible (this is the Pro+ differentiator).

    Rate limit: 5 batches/day on trial, 50 batches/day on Pro+/yearly.
    """
    import time

    auth_user_id = user["id"]
    eligibility = _verify_msi_access(auth_user_id)  # raises 403 if not eligible
    tier = eligibility["tier"]

    # Daily IMAGE quota — counts photos, not requests, so the client-side
    # chunking from 10 imgs into 4 requests doesn't accidentally shrink the
    # effective limit. Bucket is shared with /ocr/label so a user can't
    # bypass MSI quota by spamming single-image label scans.
    _, driver_id = _resolve_user_tier(auth_user_id)
    quota_key = driver_id or auth_user_id
    check_ocr_image_quota(quota_key, tier, len(req.images))

    t_start = time.perf_counter()
    try:
        gemini_result = await asyncio.to_thread(
            _msi_extract_stops_with_gemini, req.images, req.carrier_hint, req.route_context
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"MSI extraction failed: {type(e).__name__}: {e}")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=502, detail="Extraction failed")

    extraction_ms = int((time.perf_counter() - t_start) * 1000)
    raw_stops = gemini_result.get("stops") or []

    # Guarantee one row per source image. If Gemini didn't return any stop
    # for image `i`, inject an empty placeholder so the driver can still see
    # that image in the review sheet and type the address by hand. Those
    # corrections are the highest-value training signal — the model
    # otherwise never sees its own worst cases.
    seen_image_idxs: set[int] = set()
    for s in raw_stops:
        if not isinstance(s, dict):
            continue
        idx = s.get("source_image_idx")
        if isinstance(idx, int):
            seen_image_idxs.add(idx)
    for idx in range(len(req.images)):
        if idx in seen_image_idxs:
            continue
        raw_stops.append({
            "source_image_idx": idx,
            "street": "",
            "number": "",
            "floor_etc": "",
            "postal_code": "",
            "city": "",
            "province": "",
            "name": "",
            "raw_text": "",
            "confidence_per_field": {},
            "context_inferred_fields": [],
        })

    # Day 2 pipeline: normalize + geocode + confidence + candidates
    t_pipe = time.perf_counter()
    enriched_stops = await _msi_normalize_and_geocode(raw_stops, req.route_context)
    pipeline_ms = int((time.perf_counter() - t_pipe) * 1000)

    # Stats for observability
    by_conf = {"high": 0, "medium": 0, "low": 0}
    for s in enriched_stops:
        by_conf[s.get("confidence", "low")] = by_conf.get(s.get("confidence", "low"), 0) + 1

    # OCR learning loop (Day 2). If the driver opted in, upload EVERY
    # screenshot once and create one ocr_corrections row per stop in the
    # response (including empty-extraction placeholders). Each row points
    # to the storage path of its source image via `source_image_idx`, so
    # when the driver later fills in a missing address that signal lands
    # next to the exact image the model failed on.
    correction_ids: List[Optional[str]] = [None] * len(enriched_stops)
    if req.consent_to_training and enriched_stops:
        driver_id = _resolve_driver_id_from_user(auth_user_id)
        if driver_id:
            # Upload every image once, in parallel. Path indexed by image idx.
            async def _upload_idx(idx: int, img: MSIScreenshotImage) -> tuple[int, Optional[str]]:
                try:
                    raw = base64.b64decode(img.image_base64)
                except Exception as e:
                    logger.warning(f"msi image[{idx}] decode failed: {e}")
                    sentry_sdk.capture_exception(e)
                    return idx, None
                path = await asyncio.to_thread(
                    _upload_ocr_image_sync, driver_id, raw, img.media_type, "msi",
                )
                return idx, path

            upload_results = await asyncio.gather(
                *[_upload_idx(i, img) for i, img in enumerate(req.images)],
                return_exceptions=True,
            )
            paths_by_idx: dict[int, Optional[str]] = {}
            for r in upload_results:
                if isinstance(r, tuple):
                    paths_by_idx[r[0]] = r[1]

            carrier_detected = gemini_result.get("carrier_detected")
            country = (req.route_context.country if req.route_context else None) or "ES"
            for i, stop in enumerate(enriched_stops):
                src_idx = stop.get("source_image_idx")
                storage_path = paths_by_idx.get(src_idx) if isinstance(src_idx, int) else None
                cid = await asyncio.to_thread(
                    _create_ocr_correction_row,
                    driver_id=driver_id,
                    source="msi",
                    image_storage_path=storage_path,
                    model_name=_MSI_MODEL,
                    model_extracted_address=stop.get("address") or stop.get("street"),
                    model_extracted_parts={k: stop.get(k) for k in (
                        "name", "street", "city", "postalCode", "province", "floor_etc",
                    ) if stop.get(k) is not None},
                    model_confidence=float(stop.get("extraction_confidence")) if stop.get("extraction_confidence") is not None else None,
                    carrier_hint=req.carrier_hint or carrier_detected,
                    country_iso=country,
                    model_latency_ms=extraction_ms if i == 0 else None,
                    consent=True,
                )
                correction_ids[i] = cid
                # Mirror the id on the response stop so the app can keep the
                # association without zipping arrays.
                if cid:
                    stop["correction_id"] = cid

    if SENTRY_DSN:
        sentry_sdk.add_breadcrumb(
            category="msi",
            message="screenshots-batch processed",
            level="info",
            data={
                "n_images": len(req.images),
                "n_stops": len(enriched_stops),
                "carrier_detected": gemini_result.get("carrier_detected"),
                "carrier_hint": req.carrier_hint,
                "tier": tier,
                "extraction_ms": extraction_ms,
                "pipeline_ms": pipeline_ms,
                "n_high": by_conf["high"],
                "n_medium": by_conf["medium"],
                "n_low": by_conf["low"],
            },
        )

    return {
        "success": True,
        "carrier_detected": gemini_result.get("carrier_detected", "generic"),
        "language": gemini_result.get("language", "es"),
        "stops_count": len(enriched_stops),
        "stops": enriched_stops,
        "correction_ids": correction_ids,
        "global_inference_notes": gemini_result.get("global_inference_notes", ""),
        "confidence_summary": by_conf,
        "extraction_ms": extraction_ms,
        "pipeline_ms": pipeline_ms,
        "processing_ms": extraction_ms + pipeline_ms,
        "model": _MSI_MODEL,
        "tier": tier,
    }


# === OCR LEARNING LOOP (Day 2) ===
#
# When the driver opts in to "ayúdame a mejorar Xpedit", each call to
# /ocr/label or /ocr/screenshots-batch uploads the source image to the
# `ocr-training` bucket and inserts one row per extracted stop in
# `ocr_corrections`. The app then PATCHes each row with the driver's
# accepted/edited address. Admin reviews diffs in /admin/ocr-corrections
# and promotes the best pairs to is_golden_example=TRUE.
#
# No consent  → no upload, no row, no learning. Strict opt-in.
# Vertex AI processes the image either way (extraction needs it), but
# the data is not retained past the request unless consent is given.

_OCR_BUCKET = "ocr-training"
_OCR_EXT_BY_MIME = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/gif": "gif",
}


def _resolve_driver_id_from_user(auth_user_id: str) -> Optional[str]:
    """Map an auth.users.id to the corresponding drivers.id. Returns None
    if the user has no driver row. Used by OCR endpoints that need to write
    `ocr_corrections.driver_id` (FK to drivers, not auth users)."""
    try:
        res = (
            supabase.table("drivers")
            .select("id")
            .eq("user_id", auth_user_id)
            .limit(1)
            .execute()
        )
        if res.data:
            return res.data[0]["id"]
    except Exception as e:
        logger.error(f"driver lookup failed for user {auth_user_id}: {e}")
        sentry_sdk.capture_exception(e)
    return None


def _upload_ocr_image_sync(
    driver_id: str, image_bytes: bytes, media_type: str, source: str
) -> Optional[str]:
    """Upload an OCR source image to the `ocr-training` bucket. Returns the
    storage path on success, None on failure. Synchronous because the
    supabase Storage SDK is blocking — call via asyncio.to_thread().

    Path layout: {driver_id}/{source}/{uuid4}.{ext}
      - driver_id at top so RLS policies can scope reads cheaply
      - source tags the originating endpoint (msi / label_scan)
      - uuid4 avoids collisions across concurrent requests
    """
    ext = _OCR_EXT_BY_MIME.get(media_type, "bin")
    path = f"{driver_id}/{source}/{uuid_mod.uuid4().hex}.{ext}"
    try:
        supabase.storage.from_(_OCR_BUCKET).upload(
            path,
            image_bytes,
            {"content-type": media_type, "upsert": "false"},
        )
        return path
    except Exception as e:
        logger.warning(f"OCR image upload failed (path={path}): {e}")
        sentry_sdk.capture_exception(e)
        return None


def _create_ocr_correction_row(
    *,
    driver_id: str,
    source: str,
    image_storage_path: Optional[str],
    model_name: str,
    model_extracted_address: Optional[str],
    model_extracted_parts: Optional[dict],
    model_confidence: Optional[float],
    carrier_hint: Optional[str],
    country_iso: Optional[str],
    model_latency_ms: Optional[int],
    consent: bool,
    app_version: Optional[str] = None,
) -> Optional[str]:
    """Insert a row in `ocr_corrections` describing the model's output for
    a single extracted stop. Returns the row id, or None if the insert
    failed. Best-effort: a failure here MUST NOT break the OCR response —
    the user still gets their addresses, we just lose one training sample.
    """
    row = {
        "driver_id": driver_id,
        "source": source,
        "image_storage_path": image_storage_path,
        "model_name": model_name,
        "prompt_version": "v1",
        "model_extracted_address": model_extracted_address,
        "model_extracted_parts": model_extracted_parts,
        "model_confidence": model_confidence,
        "model_latency_ms": model_latency_ms,
        "carrier_hint": carrier_hint,
        "country_iso": country_iso,
        "user_action": "pending",
        "user_consented_training": consent,
        "consent_version": "v1" if consent else None,
        "redaction_status": "pending" if image_storage_path else "not_required",
        "app_version": app_version,
    }
    try:
        res = supabase.table("ocr_corrections").insert(row).execute()
        if res.data:
            return res.data[0]["id"]
    except Exception as e:
        logger.warning(f"OCR correction row insert failed: {e}")
        sentry_sdk.capture_exception(e)
    return None


class OCRCorrectionUpdate(BaseModel):
    """Body for PATCH /ocr/corrections/{id}. The driver tells us what the
    final, accepted answer was so we can compare it against what the model
    produced and grow the training set."""
    user_final_address: str = Field(..., max_length=500)
    user_final_lat: Optional[float] = Field(default=None, ge=-90, le=90)
    user_final_lng: Optional[float] = Field(default=None, ge=-180, le=180)
    user_action: Literal["accepted", "edited", "rejected"]
    correction_seconds: Optional[int] = Field(default=None, ge=0, le=86400)
    corrected_fields: Optional[List[str]] = Field(default=None, max_length=20)


@app.patch(
    "/ocr/corrections/{correction_id}",
    tags=["ocr"],
    summary="Driver confirma/edita una extracción OCR (learning loop)",
)
async def update_ocr_correction(
    correction_id: str,
    body: OCRCorrectionUpdate,
    user=Depends(get_current_user),
):
    """Driver-side endpoint to close the OCR learning loop. The app calls
    this when the driver taps "Aceptar" or finishes editing an extracted
    address. We compute `was_corrected` server-side by comparing the user
    answer with the model output, regardless of what the client sent.

    Auth: any logged-in driver. RLS already constrains drivers to their
    own corrections, but we double-check here so a leaked id can't be
    written by a different driver via service_role bypass.
    """
    check_rate_limit(f"ocr-corr-patch:{user['id']}", max_requests=120, window_seconds=60)

    driver_id = _resolve_driver_id_from_user(user["id"])
    if not driver_id:
        raise HTTPException(status_code=403, detail="Driver profile not found")

    try:
        existing_res = (
            supabase.table("ocr_corrections")
            .select("id,driver_id,model_extracted_address,user_consented_training")
            .eq("id", correction_id)
            .limit(1)
            .execute()
        )
    except Exception as e:
        logger.error(f"OCR correction lookup failed: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Lookup failed")

    if not existing_res.data:
        raise HTTPException(status_code=404, detail="Correction not found")
    existing = existing_res.data[0]
    if existing["driver_id"] != driver_id:
        # Ownership mismatch — never reveal whether the id exists.
        raise HTTPException(status_code=404, detail="Correction not found")
    if not existing.get("user_consented_training"):
        # Row was created before consent flag flipped on, or row belongs to
        # a non-consented batch. Refuse the update so we never silently
        # retain a corrected pair without a recorded consent.
        raise HTTPException(status_code=403, detail="Training consent not recorded for this correction")

    model_addr = (existing.get("model_extracted_address") or "").strip()
    user_addr = body.user_final_address.strip()
    was_corrected = body.user_action == "edited" or (
        body.user_action == "accepted" and model_addr.lower() != user_addr.lower()
    )

    update = {
        "user_final_address": user_addr,
        "user_final_lat": body.user_final_lat,
        "user_final_lng": body.user_final_lng,
        "user_action": body.user_action,
        "user_action_at": datetime.now(timezone.utc).isoformat(),
        "correction_seconds": body.correction_seconds,
        "corrected_fields": body.corrected_fields or [],
        "was_corrected": was_corrected,
    }
    try:
        supabase.table("ocr_corrections").update(update).eq("id", correction_id).execute()
    except Exception as e:
        logger.error(f"OCR correction update failed: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Update failed")

    return {
        "success": True,
        "correction_id": correction_id,
        "was_corrected": was_corrected,
    }


# === STRIPE CHECKOUT ===


class StripeCheckoutRequest(BaseModel):
    plan: str  # "pro" or "pro_plus"


@app.post("/stripe/create-checkout", tags=["stripe"], summary="Crear sesión de checkout")
async def create_stripe_checkout(request: StripeCheckoutRequest, user=Depends(get_current_user)):
    """Crea una sesión de Stripe Checkout para suscripción a plan Pro o Pro+."""
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe not configured")

    plan_info = STRIPE_PLANS.get(request.plan)
    if not plan_info:
        raise HTTPException(status_code=400, detail="Plan invalido. Use 'pro' o 'pro_plus'")

    if not plan_info["price_id"]:
        logger.error(f"Stripe Price ID not configured for plan: {request.plan}")
        raise HTTPException(status_code=503, detail="Plan no configurado en Stripe")

    try:
        # Get user email for pre-filling checkout
        user_email = None
        try:
            user_result = supabase.table("users").select("email").eq("id", user["id"]).single().execute()
            if user_result.data:
                user_email = user_result.data.get("email")
        except Exception:
            pass  # Non-critical, proceed without email

        checkout_params = {
            "mode": "subscription",
            "line_items": [{"price": plan_info["price_id"], "quantity": 1}],
            "client_reference_id": user["id"],
            "metadata": {"plan": request.plan, "user_id": user["id"]},
            "subscription_data": {"trial_period_days": 10, "metadata": {"plan": request.plan}},
            "success_url": "https://xpedit.es/dashboard?payment=success",
            "cancel_url": "https://xpedit.es/#pricing",
        }
        if user_email:
            checkout_params["customer_email"] = user_email

        session = stripe.checkout.Session.create(**checkout_params)
        return {"success": True, "url": session.url}

    except stripe.StripeError as e:
        logger.error(f"Stripe error: {e}")
        raise HTTPException(status_code=500, detail="Error en el servicio de pago")


@app.post("/stripe/webhook", tags=["webhooks"], summary="Webhook de Stripe")
async def stripe_webhook(request: Request):
    """Procesa eventos de Stripe (checkout completado, suscripción cancelada, renovación). Verificación por firma."""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    if not STRIPE_WEBHOOK_SECRET:
        logger.error("STRIPE_WEBHOOK_SECRET not configured - rejecting")
        raise HTTPException(status_code=500, detail="Webhook not configured")
    if not sig_header:
        logger.warning("Missing stripe-signature header - rejecting")
        raise HTTPException(status_code=400, detail="Missing signature")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except stripe.SignatureVerificationError:
        logger.warning("Stripe webhook invalid signature")
        raise HTTPException(status_code=400, detail="Invalid signature")
    except Exception as e:
        logger.error(f"Stripe webhook parse error: {e}")
        raise HTTPException(status_code=400, detail="Invalid payload")

    event_type = event.type
    event_id = event.get("id") if isinstance(event, dict) else getattr(event, "id", None)
    if _is_webhook_processed(event_id, "stripe"):
        logger.info(f"Stripe webhook already processed event: {event_id}")
        return {"received": True, "status": "already_processed"}
    _mark_webhook_processed(event_id, "stripe")

    logger.info(f"Stripe webhook received event: {event_type}")

    global _last_stripe_webhook_ok
    try:
        data_obj = event.data.object

        if event_type == "checkout.session.completed":
            user_id = getattr(data_obj, "client_reference_id", None)
            metadata = getattr(data_obj, "metadata", {})
            plan = metadata.get("plan", "pro") if isinstance(metadata, dict) else getattr(metadata, "plan", "pro")
            customer_id = getattr(data_obj, "customer", None)

            logger.info(f"Stripe checkout.session.completed: user_id={user_id}, plan={plan}, customer={customer_id}")

            if user_id:
                expires_at = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
                # Update drivers table (if user has a linked driver).
                # subscription_source='stripe' so getSubscriptionStatus can
                # distinguish a paid subscription from a promo/trial — the
                # TP-2 fix on the client side relies on this being set.
                supabase.table("drivers").update({
                    "promo_plan": plan,
                    "promo_plan_expires_at": expires_at,
                    "subscription_source": "stripe",
                }).eq("user_id", user_id).execute()
                # Update users table (plan + stripe customer id)
                supabase.table("users").update({
                    "stripe_customer_id": customer_id,
                    "promo_plan": plan,
                    "promo_plan_expires_at": expires_at,
                }).eq("id", user_id).execute()
                logger.info(f"Stripe plan {plan} activated for user {user_id}")

        elif event_type == "customer.subscription.updated":
            # Plan change mid-cycle (upgrade / downgrade / cancellation at
            # period end). Without this handler, the new plan was never
            # reflected in drivers.promo_plan — a user who upgraded via
            # the Stripe Customer Portal would still see their old plan.
            customer_id = getattr(data_obj, "customer", None)
            status = getattr(data_obj, "status", None)
            current_period_end = getattr(data_obj, "current_period_end", None)
            items = getattr(data_obj, "items", None)
            new_plan = None
            try:
                # items.data[0].price.id → map to our plan names
                price_id = items.data[0].price.id if items and items.data else None
                if price_id and STRIPE_PLANS:
                    for plan_name, plan_cfg in STRIPE_PLANS.items():
                        if plan_cfg.get("price_id") == price_id:
                            new_plan = plan_name
                            break
            except Exception:
                pass
            if customer_id and status in ("active", "trialing") and new_plan:
                expires_at = None
                if current_period_end:
                    expires_at = datetime.fromtimestamp(current_period_end, tz=timezone.utc).isoformat()
                user_result = supabase.table("users").select("id").eq("stripe_customer_id", customer_id).limit(1).execute()
                if user_result.data:
                    user_id = user_result.data[0]["id"]
                    supabase.table("drivers").update({
                        "promo_plan": new_plan,
                        "promo_plan_expires_at": expires_at,
                        "subscription_source": "stripe",
                    }).eq("user_id", user_id).execute()
                    supabase.table("users").update({
                        "promo_plan": new_plan,
                        "promo_plan_expires_at": expires_at,
                    }).eq("id", user_id).execute()
                    logger.info(f"Stripe subscription updated for user {user_id} → plan={new_plan}")

        elif event_type == "customer.subscription.deleted":
            customer_id = getattr(data_obj, "customer", None)
            if customer_id:
                user_result = supabase.table("users").select("id").eq("stripe_customer_id", customer_id).limit(1).execute()
                if user_result.data:
                    user_id = user_result.data[0]["id"]
                    supabase.table("drivers").update({
                        "promo_plan": None,
                        "promo_plan_expires_at": None,
                        "subscription_source": None,
                    }).eq("user_id", user_id).execute()
                    supabase.table("users").update({
                        "promo_plan": None,
                        "promo_plan_expires_at": None,
                    }).eq("id", user_id).execute()
                    logger.info(f"Stripe subscription deleted for user {user_id}")

        elif event_type in ("invoice.payment_succeeded", "invoice.paid"):
            customer_id = getattr(data_obj, "customer", None)
            billing_reason = getattr(data_obj, "billing_reason", None)
            if customer_id and billing_reason == "subscription_cycle":
                user_result = supabase.table("users").select("id").eq("stripe_customer_id", customer_id).limit(1).execute()
                if user_result.data:
                    user_id = user_result.data[0]["id"]
                    expires_at = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
                    supabase.table("drivers").update({
                        "promo_plan_expires_at": expires_at,
                    }).eq("user_id", user_id).execute()
                    supabase.table("users").update({
                        "promo_plan_expires_at": expires_at,
                    }).eq("id", user_id).execute()
                    logger.info(f"Stripe renewal for user {user_id}")

        elif event_type == "invoice.payment_failed":
            customer_id = getattr(data_obj, "customer", None)
            attempt_count = getattr(data_obj, "attempt_count", None)
            logger.warning(f"Stripe payment failed for customer {customer_id}, attempt {attempt_count}")
            sentry_sdk.capture_message(
                f"Stripe payment failed: customer={customer_id}, attempt={attempt_count}",
                level="warning",
            )
            if attempt_count and int(attempt_count) >= 2:
                try:
                    send_alert_email(
                        ALERT_EMAIL,
                        f"ALERTA: Pago Stripe fallido (intento {attempt_count})",
                        f"Customer: {customer_id}\nIntento: {attempt_count}\n"
                        f"Timestamp: {datetime.now(timezone.utc).isoformat()}Z\n\n"
                        "Revisar en Stripe Dashboard: https://dashboard.stripe.com/payments",
                    )
                except Exception:
                    pass

        else:
            logger.info(f"Stripe webhook unhandled event type: {event_type} (ignored)")

        _last_stripe_webhook_ok = datetime.now(timezone.utc)

    except Exception as e:
        global _last_stripe_webhook_error
        _last_stripe_webhook_error = datetime.now(timezone.utc)
        logger.error(f"Stripe webhook error processing {event_type}: {type(e).__name__}: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Internal webhook error")

    return {"received": True}


# === REVENUECAT WEBHOOK (In-app subscriptions) ===

_last_revenuecat_webhook_ok: Optional[datetime] = None
_last_revenuecat_webhook_error: Optional[datetime] = None


@app.post("/revenuecat/webhook", tags=["webhooks"], summary="Webhook de RevenueCat")
async def revenuecat_webhook(request: Request):
    """Procesa eventos de RevenueCat (compra, renovación, cancelación, expiración).
    app_user_id = driver_id. Entitlements: 'pro', 'pro_plus'."""
    # Auth: RevenueCat sends Authorization header with the shared secret
    auth_header = request.headers.get("authorization", "")
    if not REVENUECAT_WEBHOOK_SECRET:
        logger.error("REVENUECAT_WEBHOOK_SECRET not configured")
        raise HTTPException(status_code=503, detail="Webhook not configured")
    if auth_header != f"Bearer {REVENUECAT_WEBHOOK_SECRET}":
        logger.warning("RevenueCat webhook: invalid authorization")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event = body.get("event", {})
    event_type = event.get("type", "")
    app_user_id = event.get("app_user_id", "")  # This is the driver_id
    product_id = event.get("product_id", "")
    entitlement_ids = event.get("entitlement_ids") or []
    expiration_at = event.get("expiration_at_ms")

    # Idempotency: persistent check first (survives Railway restarts),
    # memory cache inside the helper for hot paths.
    event_id = event.get("id", "")
    if _is_webhook_processed(event_id, "revenuecat"):
        return {"received": True, "status": "already_processed"}
    _mark_webhook_processed(event_id, "revenuecat")

    logger.info(f"RevenueCat webhook: type={event_type}, driver_id={app_user_id}, product={product_id}, entitlements={entitlement_ids}")

    global _last_revenuecat_webhook_ok
    try:
        if not app_user_id:
            logger.warning("RevenueCat webhook: missing app_user_id")
            return {"received": True, "status": "no_user"}

        # Determine plan from entitlements
        plan = None
        if "pro_plus" in entitlement_ids:
            plan = "pro_plus"
        elif "pro" in entitlement_ids:
            plan = "pro"

        # Determine period from product_id (xpedit_pro_yearly / xpedit_pro_monthly).
        # NULL when ambiguous so we don't overwrite a previous correct value.
        period = None
        if "yearly" in product_id or "annual" in product_id:
            period = "yearly"
        elif "monthly" in product_id:
            period = "monthly"

        # Events that grant access
        if event_type in ("INITIAL_PURCHASE", "RENEWAL", "UNCANCELLATION", "NON_RENEWING_PURCHASE"):
            if plan:
                expires_at = None
                if expiration_at:
                    from datetime import datetime as dt
                    expires_at = dt.fromtimestamp(expiration_at / 1000, tz=timezone.utc).isoformat()
                else:
                    fallback_days = 365 if period == "yearly" else 30
                    expires_at = (datetime.now(timezone.utc) + timedelta(days=fallback_days)).isoformat()

                update_payload = {
                    "promo_plan": plan,
                    "promo_plan_expires_at": expires_at,
                    "subscription_source": "revenuecat",
                }
                if period:
                    update_payload["subscription_period"] = period

                result = supabase.table("drivers").update(update_payload).eq("id", app_user_id).execute()

                # Also update users table via driver's user_id
                driver = supabase.table("drivers").select("user_id").eq("id", app_user_id).single().execute()
                if driver.data and driver.data.get("user_id"):
                    supabase.table("users").update({
                        "promo_plan": plan,
                        "promo_plan_expires_at": expires_at,
                    }).eq("id", driver.data["user_id"]).execute()

                logger.info(f"RevenueCat: {plan} activated for driver {app_user_id}, expires {expires_at}")

        # Events that revoke access
        elif event_type in ("EXPIRATION", "BILLING_ISSUE"):
            supabase.table("drivers").update({
                "promo_plan": None,
                "promo_plan_expires_at": None,
                "subscription_source": None,
                "subscription_period": None,
            }).eq("id", app_user_id).execute()

            driver = supabase.table("drivers").select("user_id").eq("id", app_user_id).single().execute()
            if driver.data and driver.data.get("user_id"):
                supabase.table("users").update({
                    "promo_plan": None,
                    "promo_plan_expires_at": None,
                }).eq("id", driver.data["user_id"]).execute()

            logger.info(f"RevenueCat: plan revoked for driver {app_user_id} ({event_type})")

        # CANCELLATION = will not renew, but still active until expiration
        elif event_type == "CANCELLATION":
            logger.info(f"RevenueCat: driver {app_user_id} cancelled (still active until expiration)")

        else:
            logger.info(f"RevenueCat webhook unhandled: {event_type}")

        _last_revenuecat_webhook_ok = datetime.now(timezone.utc)

    except Exception as e:
        global _last_revenuecat_webhook_error
        _last_revenuecat_webhook_error = datetime.now(timezone.utc)
        logger.error(f"RevenueCat webhook error: {type(e).__name__}: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Internal webhook error")

    return {"received": True}


# === RESEND WEBHOOK (Email tracking) ===

@app.post("/webhooks/resend", tags=["webhooks"], summary="Webhook de Resend")
async def resend_webhook(request: Request):
    """Procesa eventos de Resend para tracking de emails (delivered, opened, clicked, bounced). Verificación por Svix."""
    payload_bytes = await request.body()

    # Verify signature if secret is configured
    if RESEND_WEBHOOK_SECRET:
        try:
            from svix.webhooks import Webhook, WebhookVerificationError
            wh = Webhook(RESEND_WEBHOOK_SECRET)
            wh.verify(payload_bytes, dict(request.headers))
        except WebhookVerificationError:
            logger.warning("Resend webhook invalid signature - rejecting")
            raise HTTPException(status_code=400, detail="Invalid signature")
        except Exception as e:
            logger.error(f"Resend webhook verification error: {type(e).__name__}: {e}")
            raise HTTPException(status_code=400, detail="Verification failed")
    else:
        logger.error("No RESEND_WEBHOOK_SECRET configured - rejecting webhook")
        raise HTTPException(status_code=500, detail="Webhook secret not configured")

    try:
        payload = json.loads(payload_bytes)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_type = payload.get("type")
    data = payload.get("data", {})
    email_id = data.get("email_id")
    recipients = data.get("to") or []
    recipient = recipients[0] if isinstance(recipients, list) and recipients else None

    if not event_type or not email_id:
        return {"received": True, "skipped": True}

    logger.info(f"Resend webhook event: {event_type}, email_id: {email_id}")

    now = datetime.now(timezone.utc).isoformat()

    # 1. Always log the raw event so we can analyze open/click rates from SQL,
    #    even for campaigns whose source row is not in email_log.
    try:
        supabase.table("resend_email_events").insert({
            "email_id": email_id,
            "event_type": event_type,
            "recipient": recipient,
            "raw": data,
        }).execute()
    except Exception as e:
        logger.warning(f"resend_email_events insert failed: {type(e).__name__}: {e}")

    # 2. Update email_log (transactional emails table) if a row for this id exists.
    update_data = {}
    if event_type == "email.delivered":
        update_data = {"status": "delivered", "delivered_at": now}
    elif event_type == "email.opened":
        update_data = {"status": "opened", "opened_at": now}
    elif event_type == "email.clicked":
        update_data = {"status": "clicked", "clicked_at": now}
    elif event_type == "email.bounced":
        update_data = {"status": "bounced", "bounced_at": now}
    elif event_type == "email.complained":
        update_data = {"status": "complained"}

    if update_data:
        try:
            result = supabase.table("email_log").update(update_data).eq("message_id", email_id).execute()
            updated = len(result.data) if result.data else 0
            logger.info(f"Resend webhook email_log updated {updated} rows for {email_id} -> {event_type}")
        except Exception as e:
            logger.error(f"Resend webhook email_log error: {type(e).__name__}: {e}")
            sentry_sdk.capture_exception(e)

    # 3. Update reactivation_log when this email belongs to a campaign send.
    #    Stamps opened_at the first time we see an open and flips status to
    #    'opened' so the dashboard query is a simple GROUP BY status.
    if event_type in ("email.opened", "email.clicked"):
        try:
            supabase.table("reactivation_log").update({
                "status": "opened",
                "opened_at": now,
            }).eq("resend_id", email_id).is_("opened_at", "null").execute()
        except Exception as e:
            logger.warning(f"resend webhook reactivation_log update failed: {type(e).__name__}: {e}")

    return {"received": True}


# === SUPABASE AUTH WEBHOOK (Welcome email on signup) ===

@app.post("/webhooks/supabase-auth", tags=["webhooks"], summary="Webhook de Supabase Auth")
async def supabase_auth_webhook(request: Request):
    """Envía welcome email automático cuando un usuario se registra en Supabase."""
    import asyncio

    # Verify webhook secret
    webhook_secret = request.headers.get("x-supabase-webhook-secret", "")
    if not SUPABASE_WEBHOOK_SECRET:
        logger.error("SUPABASE_WEBHOOK_SECRET not configured - rejecting")
        raise HTTPException(status_code=500, detail="Webhook secret not configured")
    if webhook_secret != SUPABASE_WEBHOOK_SECRET:
        logger.warning("Supabase auth webhook invalid secret - rejecting")
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_type = payload.get("type", "")
    record = payload.get("record", {})
    email = record.get("email")

    if not email:
        logger.info(f"Supabase auth webhook: no email in payload, type={event_type}")
        return {"received": True, "skipped": True}

    if event_type != "INSERT":
        logger.info(f"Supabase auth webhook: ignoring event type {event_type}")
        return {"received": True, "skipped": True}

    logger.info(f"Supabase auth webhook: new user signup - {mask_email(email)}")

    # Wait for DB trigger to create driver row
    await asyncio.sleep(3)

    # Look up user name from drivers table
    name = ""
    try:
        driver = supabase.table("drivers").select("name").eq("email", email).execute()
        if driver.data:
            name = driver.data[0].get("name", "")
    except Exception as e:
        logger.warning(f"Supabase auth webhook: could not fetch driver name for {mask_email(email)}: {e}")

    if not name:
        name = email.split("@")[0].replace(".", " ").replace("_", " ").title()

    # Send welcome email
    result = send_welcome_email(email, name)
    if result["success"]:
        logger.info(f"Welcome email sent to {mask_email(email)}")
        # Log in email_log
        try:
            supabase.table("email_log").insert({
                "recipient_email": email,
                "subject": "Bienvenido a Xpedit",
                "body": "welcome email (auto)",
                "message_id": result.get("id"),
                "status": "sent",
                "sent_by": "webhook:supabase-auth",
            }).execute()
        except Exception as e:
            logger.warning(f"Could not log welcome email: {e}")
    else:
        logger.error(f"Failed to send welcome email to {mask_email(email)}: {result.get('error')}")

    return {"received": True, "email_sent": result["success"]}


@app.post("/stripe/portal", tags=["stripe"], summary="Portal de cliente Stripe")
async def create_stripe_portal(user=Depends(get_current_user)):
    """Crea una sesión del portal de cliente de Stripe para gestionar la suscripción."""
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe not configured")

    try:
        # Get stripe_customer_id from users table
        user_result = supabase.table("users").select("stripe_customer_id").eq("id", user["id"]).single().execute()
        customer_id = user_result.data.get("stripe_customer_id") if user_result.data else None

        if not customer_id:
            raise HTTPException(status_code=404, detail="No tienes una suscripcion activa")

        session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url="https://xpedit.es/dashboard",
        )
        return {"success": True, "url": session.url}

    except stripe.StripeError as e:
        logger.error(f"Stripe portal error: {e}")
        raise HTTPException(status_code=500, detail="Error en el servicio de pago")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# === GOOGLE PLACES PROXY ===
# GOOGLE_API_KEY: server-side key (no referrer restriction). Separate from website key.
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
# Track Places API health — if Google is down, log it and alert once
_places_api_healthy = True
_places_api_last_alert: Optional[datetime] = None
_places_api_last_check: Optional[datetime] = None


_STREET_PREFIX_RE = re.compile(r"^(.+?)\s*,?\s+[#]?[\d][\w\-/]*\s*$")
_TRAILING_NUMBER_RE = re.compile(r"[\d][\w\-/]*$")

# Mapa de abreviaturas → forma canónica para normalización (22 may 2026):
# Drivers escriben "c/Mayor 5" o "Av Andalucía" — esto crea cache miss vs
# entries con "calle mayor 5" cacheadas. Expandimos antes del key lookup.
# Soporta tanto "c/" pegado ("c/Mayor") como "c " con espacio ("c Mayor").
_ABBREVIATIONS = {
    r"\bc\.?/+\s*": "calle ",       # c/, c/X, c./X → calle
    r"\bc\.\s+": "calle ",          # c. X → calle
    r"\bcl\.?\s+": "calle ",
    r"\bav\.?\s+": "avenida ",
    r"\bavda\.?\s+": "avenida ",
    r"\bavd\.?\s+": "avenida ",
    r"\bpza\.?\s+": "plaza ",
    r"\bplz\.?\s+": "plaza ",
    r"\bps\.?\s+": "paseo ",
    r"\bctra\.?\s+": "carretera ",
    r"\bcra\.?\s+": "carrera ",     # LATAM (Colombia)
    r"\bavd\.?a\.?\s+": "avenida ",
}
_ABBREV_PATTERNS = [(re.compile(pat, re.IGNORECASE), rep) for pat, rep in _ABBREVIATIONS.items()]
_PUNCT_RE = re.compile(r"[^\w\s\-]")  # quitar puntuación excepto guión

# Bajado de WARNING → INFO 22 may: estos 3 warnings spammean cada cold-start
# en local (SENTRY_DSN ausente, VROOM/PyVRP no instalados local) y NO aportan.
# En prod sí están configurados, por eso nunca aparecen. Mantenemos info-level
# para tener trazabilidad en logs sin contaminar terminal del dev.

# Para filter country en cache HIT — Google predictions.terms[].value contiene
# el nombre del país (e.g. "España", "Argentina"). Mapping inverso por ISO.
_COUNTRY_NAME_BY_ISO = {
    "ES": "ESPAÑA",
    "AR": "ARGENTINA",
    "CL": "CHILE",
    "CO": "COLOMBIA",
    "MX": "MÉXICO",
    "PE": "PERÚ",
    "EC": "ECUADOR",
    "UY": "URUGUAY",
    "BO": "BOLIVIA",
    "PY": "PARAGUAY",
}


def _extract_street_prefix(query: str) -> Optional[str]:
    """Quita el último número (portal) para lookup secundario de cache.

    Ej: 'calle bolsa 32' → 'calle bolsa'
        'calle de la cepa, 16' → 'calle de la cepa'
        'carrera 39c, 84a-07' → 'carrera 39c' (mantiene primer número que es parte calle)
        'pago zahora' → None (no hay número final)
        'calle 13 23' (LATAM) → None (prefix ya contiene número, evita falsos positivos)

    Devuelve None si:
    - no hay número final
    - el prefix sería <5 chars
    - el prefix ya contiene número (LATAM "calle 13 23" donde 13 es parte del nombre
      de calle, no portal). Sin este guard, el regex \\b23\\b matchearía también
      "Calle 23 con Carrera 13" y devolvería predictions WRONG → riesgo entrega
      en sitio equivocado. (Guard añadido 22 may 2026 tras audit.)"""
    q = (query or "").strip()
    if not q:
        return None
    m = _STREET_PREFIX_RE.match(q)
    if not m:
        return None
    prefix = m.group(1).strip().rstrip(",").strip()
    if len(prefix) < 5 or prefix.lower() == q.lower():
        return None
    # Guard LATAM: si el prefix YA contiene un número, NO usar prefix lookup
    # (evita "calle 13 23" → buscar "calle 13" → filtrar por "23" → match con
    # "Calle 23 con Carrera 13" que es OTRA dirección).
    if re.search(r"\d", prefix):
        return None
    return prefix


def _filter_predictions_containing_number(predictions: list, number: str) -> list:
    """Filtra predictions cuyo description contenga el `number` como token completo.

    Ej: predictions = ['Calle Bolsa, 32, Madrid', 'Calle Bolsa, 45, Madrid']
        number='32' → devuelve solo la primera.
        number='99' → devuelve [] (ningún match)."""
    if not number or not predictions:
        return []
    try:
        rx = re.compile(rf"\b{re.escape(number)}\b", re.IGNORECASE)
    except re.error:
        return []
    return [p for p in predictions if isinstance(p, dict) and rx.search(p.get("description", ""))]


def _normalize_query_aggressive(query: str) -> str:
    """Normalización agresiva para maximizar cache hit rate (22 may 2026).

    Antes solo: lower + collapse spaces. Resultado: 'Calle María', 'calle maria',
    'C/ Maria', 'calle maria,' generaban 4 entries distintas. Audit del 22 may
    estimó 30-40% de las 1.700 entries son duplicadas por esto = ~€270/mes
    en calls Google evitables.

    Pasos:
    1. Lower-case
    2. Quitar acentos (NFD + strip combining marks): María → maria
    3. Expandir abreviaturas: c/ → calle, av → avenida, ctra → carretera, etc.
    4. Quitar puntuación (mantener guión y espacios)
    5. Collapse whitespace múltiple
    6. Max 200 chars
    """
    import unicodedata
    if not query:
        return ""
    q = query.lower().strip()
    # NFD decomposes "á" → "a" + "´", encode/ignore strips the combining marks.
    q = unicodedata.normalize("NFD", q)
    q = q.encode("ascii", "ignore").decode("ascii")
    # Expandir abreviaturas ANTES de quitar puntuación (algunas llevan punto: "c.").
    for pat, rep in _ABBREV_PATTERNS:
        q = pat.sub(rep, q)
    # Quitar puntuación (preserve - y space)
    q = _PUNCT_RE.sub(" ", q)
    # Collapse whitespace + trim
    q = " ".join(q.split())
    return q[:200]


def _ac_cache_key(query: str, lat: Optional[float], lng: Optional[float]) -> tuple[str, str]:
    """Normalize query + bias for cache lookup.

    Bias grid: 0.25° (~27km lat × ~21km lng).
    Antes (22 may 2026): round(lat, 1) = 0.1° (~11km) era demasiado granular —
    drivers en ciudades vecinas (Sanlúcar / Chipiona / Jerez / El Puerto, todas
    a <30km) NO compartían cache y cada uno gastaba autocompletes Google
    para las mismas calles. Hit rate empírico <10%. Subimos granularidad a
    0.25° (~27km) → ciudades vecinas comparten cache. Hit rate esperado 40-60%.
    Tradeoff: Google recibirá el mismo bias para zona más grande, sus
    predictions seguirán siendo locales (Google también pondera por GPS real
    del cliente cuando está disponible).

    Query normalization (22 may 2026): se usa normalización agresiva (acentos,
    abreviaturas, puntuación) para maximizar hit rate. Ver _normalize_query_aggressive."""
    norm = _normalize_query_aggressive(query)
    if lat is None or lng is None:
        bias = ""
    else:
        # Round a múltiplos de 0.25 (~27km lat, ~21km lng en España)
        bias = f"{round(lat * 4) / 4:.2f},{round(lng * 4) / 4:.2f}"
    return norm, bias


# ─────────────────────────────────────────────────────────────────────────────
# Places autocomplete cache (re-enabled 21 may 2026 — incident 5 may root cause:
# sync supabase calls inside async handler stalled event loop. Fix: asyncio.to_thread
# for all DB ops + fire-and-forget writes. Feature flag `places_cache_mode` in
# app_config gives instant kill-switch: 'off' | 'shadow' | 'on'.
# ─────────────────────────────────────────────────────────────────────────────
_places_cache_mode_value: str = "off"
_places_cache_mode_fetched_at: float = 0.0
_PLACES_CACHE_MODE_TTL_SEC = 60  # re-read flag from app_config every 60s

# Métricas in-memory por source (22 may 2026): contador desde último cold-start.
# Para medir hit rate REAL distinguiendo exact / prefix / fuzzy / negative / google.
# Reset al deploy (acepto). Devuelto en /admin/cache/places-stats.
_places_source_counters: dict[str, int] = {
    "hit": 0,                 # exact cache hit
    "prefix_hit": 0,          # composite street prefix hit
    "stops_fuzzy_hit": 0,     # pg_trgm fuzzy match en stops
    "negative_hit": 0,        # cached ZERO_RESULTS
    "miss_write": 0,          # Google → cached
    "negative_write": 0,      # Google ZERO_RESULTS → cached negative
    "shadow_would_hit": 0,    # shadow mode: cache habría hit
    "shadow_miss": 0,         # shadow mode: cache miss
    "skipped": 0,             # cache_mode=off o query <3 chars
}
_places_counters_started_at: float = time.time()


def _bump_source_counter(source: str) -> None:
    """Increment in-memory counter para source. Used in /admin/cache/places-stats."""
    if source in _places_source_counters:
        _places_source_counters[source] += 1


# L1 in-memory cache (22 may 2026 P1): antes de Supabase lookup, mira LRU local.
# Primera capa de defensa más rápida (~5ms vs ~500ms Supabase round-trip).
# TTL corto (5 min) para no servir datos muy stale. maxsize 1000 entries con
# eviction LRU (insertion order Python 3.7+ + OrderedDict.move_to_end).
from collections import OrderedDict as _OrderedDict
_PLACES_L1_MAX = 1000
_PLACES_L1_TTL_SEC = 300
_places_l1_cache: "_OrderedDict[tuple[str, str], tuple[float, dict]]" = _OrderedDict()


def _l1_get(norm: str, bias: str) -> Optional[dict]:
    """L1 lookup. Returns cached value if fresh, None if miss/stale."""
    key = (norm, bias)
    entry = _places_l1_cache.get(key)
    if entry is None:
        return None
    ts, value = entry
    if time.time() - ts > _PLACES_L1_TTL_SEC:
        _places_l1_cache.pop(key, None)
        return None
    # LRU touch: move to end (most recent)
    _places_l1_cache.move_to_end(key)
    return value


def _l1_put(norm: str, bias: str, value: dict) -> None:
    """L1 store with LRU eviction when at max capacity."""
    key = (norm, bias)
    _places_l1_cache[key] = (time.time(), value)
    _places_l1_cache.move_to_end(key)
    while len(_places_l1_cache) > _PLACES_L1_MAX:
        _places_l1_cache.popitem(last=False)  # evict oldest


async def _get_places_cache_mode() -> str:
    """Returns current flag value. In-memory cached 60s to avoid hammering app_config.
    Defaults to 'off' on any error (safe-by-default)."""
    global _places_cache_mode_value, _places_cache_mode_fetched_at
    now = time.time()
    if now - _places_cache_mode_fetched_at < _PLACES_CACHE_MODE_TTL_SEC:
        return _places_cache_mode_value
    try:
        def _fetch():
            r = supabase.table("app_config").select("value").eq("key", "places_cache_mode").limit(1).execute()
            return ((r.data or [{}])[0].get("value") or "off").strip().lower()
        mode = await asyncio.to_thread(_fetch)
        if mode not in ("off", "shadow", "on"):
            mode = "off"
        _places_cache_mode_value = mode
        _places_cache_mode_fetched_at = now
        return mode
    except Exception as e:
        logger.warning(f"_get_places_cache_mode failed (defaulting to 'off'): {e}")
        return "off"


def _places_cache_lookup_sync(norm: str, bias: str) -> Optional[dict]:
    """Sync Supabase lookup — MUST be called via asyncio.to_thread.
    Returns dict {predictions, expires_at, hits} or None on miss/error."""
    try:
        r = (
            supabase.table("places_autocomplete_cache")
            .select("predictions,expires_at,hits")
            .eq("query_normalized", norm)
            .eq("bias_geohash5", bias)
            .gt("expires_at", datetime.now(timezone.utc).isoformat())
            .limit(1)
            .execute()
        )
        return (r.data or [None])[0]
    except Exception as e:
        logger.warning(f"places cache lookup failed: {e}")
        return None


def _stops_fuzzy_lookup_sync(
    query: str,
    lat: Optional[float],
    lng: Optional[float],
    max_distance_km: float = 50.0,
    similarity_threshold: float = 0.4,
    limit_count: int = 5,
) -> list:
    """Búsqueda fuzzy en `stops` table vía RPC `find_similar_stop_addresses`.

    Devuelve lista de predictions formato Google-compatible (con `description`,
    `place_id`, `lat`, `lng`, `_source`). Usado por places_autocomplete como
    último lookup local antes de pegar a Google. CERO coste Google si hit.

    Ventaja única Xpedit: drivers REPITEN direcciones (rutas reparto recurrentes).
    Con 13.9k stops válidas + 10.8k direcciones distintas en BD, el hit rate
    fuzzy esperado para queries de clientes recurrentes es alto.

    Returns: list de dicts compatibles con Google Places Autocomplete response.
    """
    if not query or len(query) < 3:
        return []
    try:
        resp = supabase.rpc("find_similar_stop_addresses", {
            "q": query,
            "lat_in": lat,
            "lng_in": lng,
            "max_distance_km": max_distance_km,
            "similarity_threshold": similarity_threshold,
            "limit_count": limit_count,
        }).execute()
        rows = resp.data or []
    except Exception as e:
        logger.warning(f"stops fuzzy lookup failed: {e}")
        return []
    # Transformar a formato Google Places Autocomplete prediction:
    # {description, place_id, structured_formatting: {main_text, secondary_text}, ...}
    predictions = []
    for r in rows:
        address = (r.get("address") or "").strip()
        if not address:
            continue
        # description = primera línea (la "main") + segunda si existe
        lines = address.split("\n")
        main = lines[0].strip()
        secondary = lines[1].strip() if len(lines) > 1 else ""
        description = address.replace("\n", ", ")
        predictions.append({
            "description": description,
            "place_id": r.get("place_id"),
            "structured_formatting": {
                "main_text": main,
                "secondary_text": secondary,
            },
            # Custom fields para que cliente pueda usar coords directos sin Place Details
            "_xpedit_lat": r.get("lat"),
            "_xpedit_lng": r.get("lng"),
            "_xpedit_distance_km": r.get("distance_km"),
            "_xpedit_similarity": r.get("similarity"),
        })
    return predictions


def _places_cache_write_sync(norm: str, bias: str, predictions: list, ttl_days: int = 30) -> bool:
    """Sync Supabase upsert — MUST be called via asyncio.to_thread (fire-and-forget).

    ttl_days: 30 default. Para negative cache (predictions=[]), pasar 1.
    """
    try:
        supabase.table("places_autocomplete_cache").upsert({
            "query_normalized": norm,
            "bias_geohash5": bias,
            "predictions": predictions,
            "hits": 1,
            "last_used_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": (datetime.now(timezone.utc) + timedelta(days=ttl_days)).isoformat(),
        }, on_conflict="query_normalized,bias_geohash5").execute()
        return True
    except Exception as e:
        logger.warning(f"places cache write failed (non-fatal): {e}")
        return False


def _places_cache_bump_sync(norm: str, bias: str, current_hits: int) -> None:
    """Sync bump of hits + last_used_at on cache hit (fire-and-forget).

    TTL escalonado (22 may 2026 P1): direcciones populares (>=10 hits)
    extienden expires_at +90d, >=30 hits extienden +180d. Razón: la
    geografía no cambia, una calle que se ha pedido 50 veces seguro
    sigue existiendo el próximo mes. Reduce re-llamadas Google al
    expirar el TTL base 30d para hits populares (ahorro pasivo).
    """
    new_hits = current_hits + 1
    update_payload: dict = {
        "hits": new_hits,
        "last_used_at": datetime.now(timezone.utc).isoformat(),
    }
    # Extend TTL based on popularity (only on milestone hits to avoid Update spam)
    if new_hits in (10, 30, 100):
        days = 180 if new_hits >= 30 else 90
        update_payload["expires_at"] = (
            datetime.now(timezone.utc) + timedelta(days=days)
        ).isoformat()
    try:
        supabase.table("places_autocomplete_cache").update(update_payload).eq(
            "query_normalized", norm
        ).eq("bias_geohash5", bias).execute()
    except Exception:
        pass  # bump failure is silent — does not break user request


def _country_iso_from_coords(lat: Optional[float], lng: Optional[float]) -> Optional[str]:
    """Devuelve ISO-2 del país probable según lat/lng. None si está en zona ambigua
    (frontera, mar, fuera de mercados conocidos).

    Bounding boxes intencionalmente CONSERVADORAS — solo clasifica el "core" del
    país, dejando frontera ambigua (especialmente CL/AR alrededor de la cordillera
    de los Andes lng ≈ -68 a -70) como None para evitar falsos positivos.
    Caso de uso: detectar mismatch entre GPS y `country` flag del driver — si el
    helper devuelve None lo dejamos en "no sé" (mantén filter por seguridad).
    """
    if lat is None or lng is None:
        return None
    # ES peninsular + Baleares
    if 36.0 <= lat <= 43.8 and -9.5 <= lng <= 4.5:
        return "ES"
    # ES Canarias
    if 27.5 <= lat <= 29.5 and -18.2 <= lng <= -13.4:
        return "ES"
    # MX
    if 14.5 <= lat <= 32.7 and -117.0 <= lng <= -86.7:
        return "MX"
    # CO (excluye costa pacífica que se solapa con EC)
    if 1.5 <= lat <= 12.5 and -79.0 <= lng <= -67.0:
        return "CO"
    # EC
    if -4.8 <= lat <= 1.4 and -81.0 <= lng <= -75.2:
        return "EC"
    # PE (excluye norte Amazónico que toca CO/BR)
    if -18.3 <= lat <= -0.5 and -81.0 <= lng <= -68.8:
        return "PE"
    # CL puro (lng < -70, lejos de la cordillera). Frontera -70/-68 = None
    if -55.5 <= lat <= -17.5 and -76.0 <= lng <= -70.0:
        return "CL"
    # UY — check ANTES que AR porque Río de la Plata cae dentro del bbox AR.
    # lng >= -58.0 excluye Buenos Aires (-58.38) que está al oeste del río.
    if -34.97 <= lat <= -30.08 and -58.0 <= lng <= -53.07:
        return "UY"
    # AR puro (lng > -65, lejos de la cordillera).
    if -55.0 <= lat <= -21.7 and -65.0 <= lng <= -53.5:
        return "AR"
    # BO
    if -22.9 <= lat <= -9.7 and -69.6 <= lng <= -57.5:
        return "BO"
    # PY
    if -27.6 <= lat <= -19.3 and -62.6 <= lng <= -54.3:
        return "PY"
    return None


@app.get("/places/autocomplete", tags=["places"], summary="Autocompletado de direcciones")
async def places_autocomplete(
    input: str,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    country: Optional[str] = None,
    sessiontoken: Optional[str] = None,
    user=Depends(get_current_user),
):
    """Proxy de Google Places Autocomplete. Solo Google, sin Nominatim.

    `sessiontoken` (opcional) agrupa keystrokes + Place Details en una única
    sesión facturable. Con session token, Google factura SOLO el Details final
    y los autocompletes son gratis.

    `country` (ISO-3166-1 alpha-2, p.ej. 'AR'): restringe resultados al país.
    Sin esta restricción Google asume España por defecto y un driver argentino
    buscando "Calle Ancha" recibe direcciones de Madrid antes que las suyas.
    """
    global _places_api_healthy, _places_api_last_alert, _places_api_last_check

    # Cache (21 may 2026 re-enabled): respects 5 may incident root cause by
    # running all Supabase ops via asyncio.to_thread (no event loop blocking)
    # and fire-and-forget for writes (no added latency on response). Killable
    # instantly via app_config.places_cache_mode = 'off' (no redeploy).
    cache_mode = await _get_places_cache_mode()
    norm_query, bias_key = _ac_cache_key(input, lat, lng)
    cache_eligible = cache_mode in ("on", "shadow") and norm_query and len(norm_query) >= 3
    if not cache_eligible:
        _bump_source_counter("skipped")
    cache_row = None
    if cache_eligible:
        # L1 in-memory lookup primero (~5ms vs ~500ms Supabase)
        cache_row = _l1_get(norm_query, bias_key)
        if cache_row is None:
            try:
                cache_row = await asyncio.to_thread(_places_cache_lookup_sync, norm_query, bias_key)
            except Exception as e:
                logger.warning(f"places cache lookup raised in to_thread: {e}")
                cache_row = None
            # Solo cachea en L1 si vino de Supabase con datos (no None)
            if cache_row is not None:
                _l1_put(norm_query, bias_key, cache_row)

    if cache_mode == "on" and cache_row and cache_row.get("predictions") is not None:
        # HIT — return cache, skip Google entirely. Fire-and-forget bump.
        cached_predictions = cache_row["predictions"]

        # Negative cache (22 may 2026): si predictions == [], significa que la
        # query no devolvió resultados (Google ZERO_RESULTS). Devolvemos vacío
        # sin llamar Google (evita driver con typo recurrente quemando $$).
        if not cached_predictions:
            _bump_source_counter("negative_hit")
            logger.info(f"places_cache_event=negative_hit query={norm_query[:30]} bias={bias_key}")
            return {"status": "ZERO_RESULTS", "predictions": [], "source": "cache_negative"}

        # Country filter en HIT (22 may 2026 fix B3 audit): driver Sanlúcar pidió
        # 'avenida andalucía', cache HIT con entry de otro driver Sevilla → devolvía
        # Sevilla. Filtramos por country flag si llega + verificamos GPS coherente.
        cc = (country or "").strip().upper()
        if cc and len(cc) == 2 and cc.isalpha():
            gps_iso = _country_iso_from_coords(lat, lng)
            # Si GPS contradice el flag, NO aplicamos filter (safety net mismo que línea 7580)
            if not gps_iso or gps_iso == cc:
                filtered = [
                    p for p in cached_predictions
                    if isinstance(p, dict) and (
                        not p.get("terms") or
                        any(
                            (t.get("value", "").upper() in {cc, _COUNTRY_NAME_BY_ISO.get(cc, "")})
                            for t in p.get("terms", [])
                        )
                    )
                ]
                # Si el filter elimina TODO, mejor devolver el original cached que ir a Google.
                # En la práctica las predictions Google con country flag siempre llevan el país.
                if filtered:
                    cached_predictions = filtered

        _bump_source_counter("hit")
        logger.info(f"places_cache_event=hit query={norm_query[:30]} bias={bias_key} country={cc or '-'}")
        try:
            asyncio.create_task(asyncio.to_thread(
                _places_cache_bump_sync, norm_query, bias_key, cache_row.get("hits") or 0,
            ))
        except Exception:
            pass
        return {"status": "OK", "predictions": cached_predictions, "source": "cache"}

    # COMPOSITE LOOKUP (22 may 2026): si la query exacta MISS y contiene número
    # al final (ej 'calle bolsa 32'), busca el prefix de calle ('calle bolsa').
    # Si el prefix ESTÁ cacheado, filtra sus predictions para encontrar el número
    # exacto. Hit virtual sin llamar Google. Sube hit rate ~50-70% para zonas
    # con calles recurrentes en distintos portales (cliente entrega en 5 portales
    # de la misma calle = 1 entrada cache cubre los 5).
    if cache_mode == "on" and cache_eligible and not cache_row:
        street_prefix = _extract_street_prefix(input)
        if street_prefix:
            try:
                norm_prefix, prefix_bias = _ac_cache_key(street_prefix, lat, lng)
                prefix_row = await asyncio.to_thread(_places_cache_lookup_sync, norm_prefix, prefix_bias)
            except Exception as e:
                logger.warning(f"places prefix lookup failed: {e}")
                prefix_row = None
            if prefix_row and prefix_row.get("predictions"):
                # Extraer número final original para filtrar predictions
                num_match = _TRAILING_NUMBER_RE.search(input.strip())
                if num_match:
                    number = num_match.group(0)
                    filtered = _filter_predictions_containing_number(prefix_row["predictions"], number)
                    if filtered:
                        _bump_source_counter("prefix_hit")
                        logger.info(
                            f"places_cache_event=prefix_hit query={norm_query[:30]} "
                            f"prefix={street_prefix[:30]} number={number} matches={len(filtered)}"
                        )
                        # Bump prefix entry (sirvió para evitar Google)
                        try:
                            asyncio.create_task(asyncio.to_thread(
                                _places_cache_bump_sync, norm_prefix, prefix_bias,
                                prefix_row.get("hits") or 0,
                            ))
                        except Exception:
                            pass
                        return {"status": "OK", "predictions": filtered, "source": "cache_prefix"}

    # STOPS FUZZY LOOKUP (22 may 2026, P1 audit): cuando exact + prefix cache MISS,
    # buscamos en la tabla `stops` direcciones similares usando pg_trgm + haversine.
    # Ventaja única Xpedit: drivers REPITEN direcciones de clientes recurrentes.
    # Con 10.8k direcciones distintas en BD, hit rate fuzzy esperado significativo
    # para queries recurrentes. Solo si query tiene >=4 chars (evita fuzzy con
    # input muy corto que matcheraría TODO).
    if cache_mode == "on" and cache_eligible and len(norm_query) >= 4:
        try:
            fuzzy_predictions = await asyncio.to_thread(
                _stops_fuzzy_lookup_sync, input, lat, lng,
            )
        except Exception as e:
            logger.warning(f"stops fuzzy lookup raised in to_thread: {e}")
            fuzzy_predictions = []
        if fuzzy_predictions:
            _bump_source_counter("stops_fuzzy_hit")
            logger.info(
                f"places_cache_event=stops_fuzzy_hit query={norm_query[:30]} "
                f"matches={len(fuzzy_predictions)} top_sim={fuzzy_predictions[0].get('_xpedit_similarity', 0):.2f}"
            )
            return {"status": "OK", "predictions": fuzzy_predictions, "source": "stops_fuzzy"}

    if not GOOGLE_API_KEY:
        logger.error("GOOGLE_API_KEY missing — cannot serve /places/autocomplete")
        return {"status": "ZERO_RESULTS", "predictions": [], "error_message": "API key not configured"}

    params = {
        "input": input,
        "language": "es",
        "key": GOOGLE_API_KEY,
    }
    if lat and lng:
        params["location"] = f"{lat},{lng}"
        params["radius"] = "30000"
    cc = (country or "").strip().lower()
    if len(cc) == 2 and cc.isalpha():
        # Safety net: si GPS del driver claramente NO está en `country`
        # (Christian 20 may: BD country=AR pero GPS en La Serena CL → 0 results
        # cada búsqueda, app inservible), saltamos el hard filter. Google sigue
        # priorizando por proximidad gracias a location+radius. Mantiene el
        # filter cuando GPS está en zona ambigua/frontera o no hay GPS — solo lo
        # quita cuando es OBVIO que el flag está mal.
        gps_iso = _country_iso_from_coords(lat, lng)
        if gps_iso and gps_iso.lower() != cc:
            # INFO en vez de WARNING (20 may 21:03): es comportamiento esperado del
            # safety net, no un fallo. Sentry agrupaba estas líneas como regresiones.
            logger.info(
                f"places/autocomplete: country flag '{cc}' != GPS country '{gps_iso}' "
                f"(driver_id={user.get('id')}). Dropping country filter — keeping GPS bias."
            )
            # Sin components ni region — solo location bias. El flag estaba mal,
            # arreglamos la búsqueda ahora y la auditoría country mismatch lo
            # corrige offline.
        else:
            params["components"] = f"country:{cc}"
            params["region"] = cc
    if sessiontoken:
        params["sessiontoken"] = sessiontoken

    # 2 attempts with generous timeouts on the shared HTTPX client (avoids
    # per-request TLS handshake). The earlier shared-client attempt at 10:24
    # failed due to too-tight pool config (max=50, pool_timeout=2s) — this
    # version uses 100/50/keepalive=30s/pool=10s which is more forgiving.
    client = google_maps_client()
    last_error = None
    for attempt in range(2):
        try:
            timeout = 20.0 if attempt == 0 else 25.0
            resp = await client.get(
                "https://maps.googleapis.com/maps/api/place/autocomplete/json",
                params=params,
                timeout=timeout,
            )
            data = resp.json()

            status = data.get("status")
            if status in ("OK", "ZERO_RESULTS"):
                if not _places_api_healthy:
                    logger.info("Google Places API recovered")
                    _places_api_healthy = True
                # Cache write (fire-and-forget). Para OK con predictions, cachea
                # las predictions normales (TTL 30d). Para ZERO_RESULTS, cachea
                # negativo (predictions=[]) con TTL 1d para evitar typos recurrentes.
                # Tanto shadow como on escriben — la diferencia es que shadow NO lee.
                if cache_eligible and status == "OK" and data.get("predictions"):
                    if cache_mode == "shadow" and cache_row:
                        _bump_source_counter("shadow_would_hit")
                        logger.info(f"places_cache_event=shadow_would_hit query={norm_query[:30]} bias={bias_key}")
                    elif cache_mode == "shadow":
                        _bump_source_counter("shadow_miss")
                        logger.info(f"places_cache_event=shadow_miss query={norm_query[:30]} bias={bias_key}")
                    else:
                        _bump_source_counter("miss_write")
                        logger.info(f"places_cache_event=miss_write query={norm_query[:30]} bias={bias_key}")
                    try:
                        asyncio.create_task(asyncio.to_thread(
                            _places_cache_write_sync, norm_query, bias_key, data["predictions"],
                        ))
                    except Exception:
                        pass
                elif cache_eligible and status == "ZERO_RESULTS" and cache_mode != "shadow":
                    # Negative cache TTL 24h (vs 30d normal). Direcciones típicamente
                    # NO desaparecen pero typos pueden corregirse, queremos re-validar antes.
                    _bump_source_counter("negative_write")
                    logger.info(f"places_cache_event=negative_write query={norm_query[:30]} bias={bias_key}")
                    try:
                        asyncio.create_task(asyncio.to_thread(
                            _places_cache_write_sync, norm_query, bias_key, [], 1,
                        ))
                    except Exception:
                        pass
                return data
            if status == "OVER_QUERY_LIMIT":
                logger.warning(f"Google Places rate limited (attempt {attempt + 1})")
                if attempt == 0:
                    await asyncio.sleep(0.5)
                    continue
                last_error = "OVER_QUERY_LIMIT"
                break
            error_msg = data.get("error_message", status or "unknown")
            logger.warning(f"Google Places failed: status={status}, http={resp.status_code}, error={error_msg} (query: {input[:30]})")
            last_error = f"{status}: {error_msg}"
            break
        except Exception as e:
            logger.warning(f"Google Places request error (attempt {attempt + 1}): {e}")
            last_error = str(e)
            if attempt == 0:
                continue

    # Failure path: alert (cooldown 1h) and return empty so the client
    # shows "Sin resultados" instead of stale/imprecise data.
    if _places_api_healthy:
        _places_api_healthy = False
        now = datetime.now(timezone.utc)
        if not _places_api_last_alert or (now - _places_api_last_alert).total_seconds() > 3600:
            _places_api_last_alert = now
            try:
                send_alert_email(
                    ALERT_EMAIL,
                    "ALERTA: Google Places API no responde",
                    f"Google Places falló tras 2 intentos. Última causa: {last_error}\n"
                    f"Key configurada: {'Sí' if GOOGLE_API_KEY else 'NO (vacía!)'}\n"
                    f"Timestamp: {now.isoformat()}Z\n"
                    f"Acción: verificar GOOGLE_API_KEY en Railway, cuotas y restricciones en Google Cloud Console.",
                )
            except Exception:
                pass
            if SENTRY_DSN:
                sentry_sdk.capture_message(f"Google Places failed: {last_error}", level="warning")

    return {"status": "ZERO_RESULTS", "predictions": [], "error_message": last_error}


@app.get("/places/details", tags=["places"], summary="Detalles de lugar")
async def places_details(
    place_id: str,
    sessiontoken: Optional[str] = None,
    user=Depends(get_current_user),
):
    """Proxy de Google Places Details. Devuelve geometría, componentes de dirección y dirección formateada.

    `sessiontoken` (opcional) cierra la sesión facturable abierta en
    /places/autocomplete: pasa el MISMO UUID que el cliente generó al abrir
    el input. Si la sesión es válida, Google factura solo este Details
    (los autocompletes salen gratis)."""
    # Field mask 21 may 2026: quitado `opening_hours` (Miguel + auditoría agentes 21 may 00:24).
    # `opening_hours` dispara el SKU "Place Details Advanced" ($17/1000) en vez de
    # "Basic" ($0 con session token) — 3,4× más caro. Auditoría confirmó que el
    # campo se asignaba en stops.opening_hours pero NUNCA se mostraba al driver
    # (dead data en frontend + admin). Ahorro estimado $20-100/mes ahora, $300-500
    # a 100 paying. Registrado en [[cambios_log_exhaustivo]] 2026-05-21.
    params = {
        "place_id": place_id,
        "fields": "geometry,address_components,formatted_address,name,types",
        "language": "es",
        "key": GOOGLE_API_KEY,
    }
    if sessiontoken:
        params["sessiontoken"] = sessiontoken
    resp = await google_maps_client().get(
        "https://maps.googleapis.com/maps/api/place/details/json",
        params=params,
        timeout=10.0,
    )
    return resp.json()


@app.get("/places/snap", tags=["places"], summary="Alinear coordenadas a la red de carreteras de Google")
async def places_snap(lat: float, lng: float, user=Depends(get_current_user)):
    """Reverse geocode via Google to get road-aligned coordinates.
    Used when stops come from Nominatim (which can be 30-40m off from Google's road network)."""
    params = {
        "latlng": f"{lat},{lng}",
        "language": "es",
        "key": GOOGLE_API_KEY,
    }
    resp = await google_maps_client().get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        params=params,
        timeout=10.0,
    )
    data = resp.json()
    if data.get("status") == "OK" and data.get("results"):
        result = data["results"][0]
        gloc = result.get("geometry", {}).get("location", {})
        return {
            "status": "OK",
            "lat": gloc.get("lat", lat),
            "lng": gloc.get("lng", lng),
            "formatted_address": result.get("formatted_address", ""),
        }
    return {"status": "FALLBACK", "lat": lat, "lng": lng, "formatted_address": ""}


def _parse_latlng_pair(coord: str) -> Optional[dict]:
    """Convierte 'lat,lng' a dict {latitude, longitude} para Routes API v2.
    Devuelve None si el string no es un par válido (place_id, dirección, etc.)."""
    if "," not in coord:
        return None
    try:
        lat_str, lng_str = coord.split(",", 1)
        return {"latitude": float(lat_str.strip()), "longitude": float(lng_str.strip())}
    except (ValueError, TypeError):
        return None


def _routes_v2_waypoint(coord: str) -> dict:
    """Construye un waypoint Routes API v2 a partir de coords o dirección."""
    parsed = _parse_latlng_pair(coord)
    if parsed:
        return {"location": {"latLng": parsed}}
    return {"address": coord}


def _parse_routes_v2_duration(value) -> int:
    """Routes v2 devuelve duración como `'123s'`. Convierte a int segundos."""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(str(value).rstrip("s"))
    except (ValueError, TypeError):
        return 0


def _routes_v2_to_directions_shape(routes_v2_data: dict) -> dict:
    """Mapea la respuesta de Routes API v2 al shape histórico de Directions API
    para que el cliente RN funcione sin cambios, EN AMBOS flujos:

      A) Polyline del mapa: `useRoutes.fetchDirectionsChunk` — lee
         `leg.duration.value`, `leg.end_location`, `step.polyline.points`.
      B) Navegación turn-by-turn: `services/directions.ts::getNavigationRoute`
         (invocado por `useNavigationFlow`) — lee adicionalmente
         `step.html_instructions`, `step.maneuver`, `step.start_location`,
         `step.distance.value`, `step.duration.value` y `route.bounds`.

    El bug del 12 may 2026 (white screen "Calculando ruta..." en navegación)
    fue causado por NO mapear los campos del flujo B: el mapper se diseñó
    pensando solo en A. Esta versión completa los 6 campos faltantes
    (instructions, maneuver, start_location, step.distance, step.duration,
    bounds → viewport en v2).
    """
    if not routes_v2_data or "routes" not in routes_v2_data or not routes_v2_data["routes"]:
        return {"status": "ZERO_RESULTS", "routes": []}

    out_routes = []
    for r in routes_v2_data["routes"]:
        out_legs = []
        for leg in r.get("legs", []):
            dur_value = _parse_routes_v2_duration(leg.get("duration"))
            dist_value = int(leg.get("distanceMeters", 0) or 0)
            end_loc = (leg.get("endLocation", {}) or {}).get("latLng", {}) or {}
            start_loc_leg = (leg.get("startLocation", {}) or {}).get("latLng", {}) or {}
            out_steps = []
            for step in leg.get("steps", []):
                enc = (step.get("polyline", {}) or {}).get("encodedPolyline")
                if not enc:
                    # Sin polyline el cliente no puede pintar el segmento; saltar.
                    continue
                step_dur = _parse_routes_v2_duration(step.get("staticDuration"))
                step_dist = int(step.get("distanceMeters", 0) or 0)
                step_start = (step.get("startLocation", {}) or {}).get("latLng", {}) or {}
                step_end = (step.get("endLocation", {}) or {}).get("latLng", {}) or {}
                nav_instruction = step.get("navigationInstruction", {}) or {}
                instructions_text = nav_instruction.get("instructions", "") or ""
                maneuver = nav_instruction.get("maneuver", "") or ""
                out_steps.append({
                    "polyline": {"points": enc},
                    # html_instructions: Routes v2 ya devuelve texto plano; el
                    # cliente espera string que pasa por `cleanInstruction` (strip
                    # de tags HTML). Plano funciona porque strip no-op sobre él.
                    "html_instructions": instructions_text,
                    # maneuver: enum v2 (`TURN_LEFT`, `MERGE_LEFT`...). Cliente
                    # lo usa como string libre, no compara contra valores legacy.
                    "maneuver": maneuver,
                    "distance": {
                        "value": step_dist,
                        "text": f"{step_dist} m" if step_dist < 1000 else f"{step_dist / 1000:.1f} km",
                    },
                    "duration": {
                        "value": step_dur,
                        "text": f"{step_dur // 60} min" if step_dur >= 60 else f"{step_dur} s",
                    },
                    "start_location": {
                        "lat": step_start.get("latitude", 0),
                        "lng": step_start.get("longitude", 0),
                    },
                    "end_location": {
                        "lat": step_end.get("latitude", 0),
                        "lng": step_end.get("longitude", 0),
                    },
                })
            out_legs.append({
                "duration": {"value": dur_value, "text": f"{dur_value // 60} min"},
                "distance": {"value": dist_value, "text": f"{dist_value / 1000:.1f} km"},
                "start_location": {
                    "lat": start_loc_leg.get("latitude", 0),
                    "lng": start_loc_leg.get("longitude", 0),
                },
                "end_location": {
                    "lat": end_loc.get("latitude", 0),
                    "lng": end_loc.get("longitude", 0),
                },
                "steps": out_steps,
            })
        overview = (r.get("polyline", {}) or {}).get("encodedPolyline", "")
        # bounds: el cliente usa `route.bounds.northeast/southwest` para encajar
        # la cámara. Routes v2 lo llama `viewport` con `low/high` (cada uno
        # `{latitude, longitude}`). low=SW, high=NE.
        viewport = r.get("viewport", {}) or {}
        vp_low = viewport.get("low", {}) or {}
        vp_high = viewport.get("high", {}) or {}
        bounds = {
            "northeast": {
                "lat": vp_high.get("latitude", 0),
                "lng": vp_high.get("longitude", 0),
            },
            "southwest": {
                "lat": vp_low.get("latitude", 0),
                "lng": vp_low.get("longitude", 0),
            },
        }
        out_routes.append({
            "legs": out_legs,
            "overview_polyline": {"points": overview},
            "bounds": bounds,
        })
    return {"status": "OK", "routes": out_routes}


# Routes API v2 — reemplazo de la antigua Directions API.
# fieldMask: TODO lo que el cliente RN consume (ambos flujos A y B descritos
# en `_routes_v2_to_directions_shape`). Esto incluye instrucciones turn-by-turn
# que faltaban en la versión 12 may (causa del white screen "Calculando ruta…").
# Sigue dentro del tier Basic — los campos extra están en el Advanced tier
# SOLO cuando se piden con `routes.travelAdvisory` o `legs.travelAdvisory`,
# que aquí NO pedimos. `navigationInstruction`, `staticDuration` y `viewport`
# son Basic.
_ROUTES_V2_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"
_ROUTES_V2_FIELD_MASK = (
    "routes.duration,"
    "routes.distanceMeters,"
    "routes.polyline.encodedPolyline,"
    "routes.viewport,"
    "routes.legs.duration,"
    "routes.legs.distanceMeters,"
    "routes.legs.startLocation,"
    "routes.legs.endLocation,"
    "routes.legs.steps.polyline.encodedPolyline,"
    "routes.legs.steps.startLocation,"
    "routes.legs.steps.endLocation,"
    "routes.legs.steps.distanceMeters,"
    "routes.legs.steps.staticDuration,"
    "routes.legs.steps.navigationInstruction"
)


@app.get("/places/directions", tags=["places"], summary="Obtener direcciones de ruta")
async def places_directions(
    origin: str,
    destination: str,
    waypoints: Optional[str] = None,
    avoid: Optional[str] = None,
    heading: Optional[float] = None,
    user=Depends(get_current_user)
):
    """Calcula la ruta entre origin/destination con waypoints opcionales.

    11 may 2026: migrado de Directions API legacy a Routes API v2 para reducir
    el coste por llamada ~40 % (fieldMask + tier Basic). El shape de respuesta
    se mantiene compatible con el cliente RN (`useRoutes.fetchDirectionsChunk`)
    via `_routes_v2_to_directions_shape`.

    Si `heading` (0-360) viene, inserta un waypoint ~50 m delante del coche
    en esa dirección. Así el rerouting no propone giros bruscos hacia atrás.
    """
    intermediates: list[dict] = []
    if heading is not None and "," in origin:
        try:
            lat_str, lng_str = origin.split(",", 1)
            lat, lng = float(lat_str), float(lng_str)
            h = math.radians(heading % 360)
            dlat = (50 * math.cos(h)) / 111320.0
            dlng = (50 * math.sin(h)) / (111320.0 * max(math.cos(math.radians(lat)), 1e-6))
            intermediates.append({
                "location": {"latLng": {"latitude": lat + dlat, "longitude": lng + dlng}},
                "via": True,
            })
        except Exception as e:
            logger.warning(f"heading waypoint skipped: {e}")
    if waypoints:
        # El cliente RN pasa waypoints SIN prefijo `via:` cuando son paradas
        # reales (1 leg por parada para extraer duration/end_location/steps).
        # Solo el legacy `via:` prefix indica waypoint guía. Routes API v2 usa
        # `via: True` para "guía no-stop" y omite la propiedad cuando es stop
        # real. Marcar `via: True` por defecto fusiona todos los waypoints en
        # 1 leg y rompe el cliente (durations/snapped vacíos). Bug detectado
        # en smoke staging 11 may 19:48.
        for wp in waypoints.split("|"):
            is_via = wp.startswith("via:")
            if is_via:
                wp = wp[4:]
            if wp:
                w = _routes_v2_waypoint(wp)
                if is_via:
                    w["via"] = True
                intermediates.append(w)

    body: dict = {
        "origin": _routes_v2_waypoint(origin),
        "destination": _routes_v2_waypoint(destination),
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_AWARE",
        "languageCode": "es",
        "polylineEncoding": "ENCODED_POLYLINE",
        "computeAlternativeRoutes": False,
    }
    if intermediates:
        body["intermediates"] = intermediates
    if avoid:
        avoid_set = {a.strip() for a in avoid.split("|")}
        modifiers = {}
        if "tolls" in avoid_set:
            modifiers["avoidTolls"] = True
        if "highways" in avoid_set:
            modifiers["avoidHighways"] = True
        if "ferries" in avoid_set:
            modifiers["avoidFerries"] = True
        if modifiers:
            body["routeModifiers"] = modifiers

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": _ROUTES_V2_FIELD_MASK,
    }
    # Mismo patrón de timeout/retry que la versión legacy: 5 may incident
    # mostró que bajo pico de OTA reload Google puede tardar >12 s.
    last_error = "no attempt"
    client = google_maps_client()
    for attempt in range(2):
        try:
            timeout = 20.0 if attempt == 0 else 25.0
            resp = await client.post(
                _ROUTES_V2_URL,
                json=body,
                headers=headers,
                timeout=timeout,
            )
            if resp.status_code >= 400:
                logger.warning(f"routes_v2 HTTP {resp.status_code}: {resp.text[:300]}")
                if resp.status_code < 500:
                    # 4xx: error de request del cliente, no reintentar.
                    return {"status": "REQUEST_DENIED", "routes": [], "error_message": resp.text[:500]}
                last_error = f"HTTP {resp.status_code}"
                if attempt == 0:
                    continue
                raise HTTPException(status_code=502, detail=f"Routes API 5xx after retry: {last_error}")
            return _routes_v2_to_directions_shape(resp.json())
        except HTTPException:
            raise
        except Exception as e:
            last_error = str(e)
            logger.warning(f"routes_v2 request error (attempt {attempt + 1}): {e}")
            if attempt == 0:
                continue
    raise HTTPException(status_code=504, detail=f"Google Routes API timeout after retry: {last_error}")


  # Street View proxy removed - app opens Google Maps directly (free, no API cost)


# === STREET CLOSURES (scraped from official municipal sources) ===

from street_closures import ALL_SCRAPERS as _CLOSURE_SCRAPERS
from street_closures import upsert_closures as _upsert_closures


@app.post("/admin/scrape-closures/{city}", tags=["admin", "closures"], summary="Disparar scraper de cortes manualmente")
async def admin_scrape_closures(city: str, user=Depends(require_admin)):
    """Run a closures scraper on demand (admin only). City is a slug
    matching keys in `street_closures.ALL_SCRAPERS`. Returns counts."""
    scraper = _CLOSURE_SCRAPERS.get(city)
    if not scraper:
        raise HTTPException(status_code=404, detail=f"Unknown city slug '{city}'. Known: {list(_CLOSURE_SCRAPERS.keys())}")
    if not GOOGLE_API_KEY:
        raise HTTPException(status_code=500, detail="GOOGLE_API_KEY not configured")
    try:
        # Pass supabase para que el scraper use cache lookup (Miguel 21 may 2026)
        records = await scraper(google_api_key=GOOGLE_API_KEY, supabase=supabase)
    except Exception as e:
        logger.exception(f"Scraper '{city}' failed")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail=f"Scraper failed: {e}")
    counts = _upsert_closures(supabase, records)
    return {"city": city, "scraped": len(records), **counts}


def _verify_closures_access(auth_user_id: str) -> None:
    """Shared gate for /closures/* endpoints. Raises 403 if not Pro+ / not opted in.

    Critical: `auth_user_id` is the JWT sub claim (auth.users.id), NOT drivers.id.
    drivers has a foreign-key user_id column pointing back to auth.users.id, so
    we must match on user_id. The original code used .eq("id", ...) and silently
    returned 403 to every legitimate user since the gate first shipped — that's
    why direccion@taespack.com saw zero closures all day on 2 may 2026 despite
    closures_alerts_enabled=true."""
    try:
        d = supabase.table("drivers").select(
            "promo_plan, subscription_period, closures_alerts_enabled"
        ).eq("user_id", auth_user_id).single().execute()
        driver_row = d.data or {}
        is_pro_plus = driver_row.get("promo_plan") == "pro_plus" or driver_row.get("subscription_period") == "yearly"
        has_early_access = bool(driver_row.get("closures_alerts_enabled"))
        if not (is_pro_plus or has_early_access):
            raise HTTPException(
                status_code=403,
                detail="Closures alerts is a Pro+ feature. Upgrade to Pro+ or request early access.",
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Could not verify closures access: {e}")
        raise HTTPException(status_code=403, detail="Could not verify access")


@app.get("/closures/active", tags=["closures"], summary="Todos los cortes de calle activos (sin filtro de distancia)")
async def closures_active(user=Depends(get_current_user)):
    """Returns ALL active/upcoming closures (active = ends_at >= now-1h AND starts_at <= now+7d).
    No geo filter — the app caches the full list and renders whatever falls in the user's
    current map viewport. With ~1-50 closures total this stays well under 50KB.

    Pro+ feature, same gate as /closures/near."""
    _verify_closures_access(user["id"])
    try:
        now = datetime.now(timezone.utc)
        cutoff_past = (now - timedelta(hours=1)).isoformat()
        cutoff_future = (now + timedelta(days=7)).isoformat()
        result = (
            supabase.table("street_closures")
            .select(
                "id, city, street_name, segment_from, segment_to, "
                "closure_type, reason, starts_at, ends_at, all_day, "
                "time_window_start, time_window_end, "
                "lat, lng, lat_from, lng_from, lat_to, lng_to, street_polyline"
            )
            .gte("ends_at", cutoff_past)
            .lte("starts_at", cutoff_future)
            .order("starts_at", desc=False)
            .execute()
        )
        return {"closures": result.data or []}
    except Exception as e:
        logger.warning(f"closures_active failed: {e}")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Failed to query closures")


@app.get("/closures/near", tags=["closures"], summary="Cortes de calle activos cerca de un punto")
async def closures_near(
    lat: float,
    lng: float,
    radius_m: int = 1000,
    user=Depends(get_current_user),
):
    """Returns active street closures within `radius_m` meters of (lat, lng).
    Active = `ends_at >= now - 1h AND starts_at <= now + 7d`.
    Sorted by distance ascending.

    Pro+ feature: gated to drivers with `subscription_period='yearly'`/`pro_plus`
    OR with `closures_alerts_enabled=true` (admin-granted early access).
    Returns 403 otherwise."""
    if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
        raise HTTPException(status_code=400, detail="Invalid coordinates")
    if radius_m <= 0 or radius_m > 50000:
        raise HTTPException(status_code=400, detail="radius_m must be between 1 and 50000")
    _verify_closures_access(user["id"])
    try:
        result = supabase.rpc(
            "closures_near",
            {"p_lat": lat, "p_lng": lng, "p_radius_m": radius_m},
        ).execute()
        return {"closures": result.data or []}
    except Exception as e:
        logger.warning(f"closures_near RPC failed: {e}")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Failed to query closures")


# ============================================================================
# ADMIN COSTS / API USAGE DASHBOARD (Miguel 21 may 2026 — task #169)
# Lee Google Cloud Monitoring REST API para devolver request counts por servicio
# en tiempo real. Cron diario guarda snapshot en daily_api_metrics para histórico
# 30d+. Dashboard /admin/costs auto-refresca cada 60s.
# Pricing 2026 (USD per 1.000 calls) — actualizar si cambia:
# ============================================================================
_API_PRICING_USD_PER_1K = {
    "places": 5.0,            # Place Autocomplete + Details mix promedio
    "geocoding": 5.0,         # Geocoding API
    "routes": 5.0,            # Routes API v2 Essentials
    "directions_legacy": 5.0, # Directions API legacy (Sanlúcar pre-cache)
    "vertex_gemini": 10.0,    # ~$0.01 por imagen OCR (mix input+output Pro 2.5)
}

# Google Maps Platform regala $200 USD/mes de crédito que se descuenta de la
# factura. Aplica al pool conjunto de todos los servicios Maps (places, geocoding,
# routes, directions). Vertex AI NO tiene free tier — cobra desde la 1ª llamada.
_MAPS_FREE_TIER_USD_MONTHLY = 200.0
_MAPS_SERVICES = {"places", "geocoding", "routes", "directions_legacy"}
_VERTEX_SERVICES = {"vertex_gemini"}

# Mapping servicio interno → service name del Google Cloud Monitoring
_GCP_SERVICE_MAP = {
    "places": "places-backend.googleapis.com",
    "geocoding": "geocoding-backend.googleapis.com",
    "routes": "routes.googleapis.com",
    "directions_legacy": "directions-backend.googleapis.com",
}


def _ensure_gcp_creds_loaded() -> bool:
    """Garantiza que GOOGLE_APPLICATION_CREDENTIALS está disponible para
    google.auth.default(). En Railway las creds vienen via env var JSON; las
    materializamos a /tmp si aún no se hizo. Idempotente. Returns True si OK.
    Hot fix (Miguel 21 may 2026 12:09): /admin/costs llamaba google.auth.default()
    antes de que Vertex AI lo inicializara → DefaultCredentialsError spam Sentry."""
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    ):
        return True
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not creds_json:
        return False
    creds_path = "/tmp/gcp_vertex_sa.json"
    try:
        with open(creds_path, "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        return True
    except Exception:
        return False


# Flag para no spammear Sentry con DefaultCredentialsError tras la primera vez.
_gcp_creds_unavailable_logged = False
# Flag para no spammear con Cloud Monitoring 403 (sin rol monitoring.viewer).
_gcp_monitoring_403_logged = False


async def _fetch_gcp_request_count(service_name: str, start_iso: str, end_iso: str) -> int:
    """Llama Cloud Monitoring REST API para sumar request_count entre dos timestamps.
    Returns 0 if no data or error (no romper dashboard si Cloud Monitoring falla).
    NO usa sentry_sdk.capture_exception para errores de creds (sería spam): solo log
    una vez al startup si falta GOOGLE_APPLICATION_CREDENTIALS_JSON en Railway."""
    global _gcp_creds_unavailable_logged
    if not _ensure_gcp_creds_loaded():
        if not _gcp_creds_unavailable_logged:
            logger.warning(
                "GCP creds not available for Cloud Monitoring API "
                "(GOOGLE_APPLICATION_CREDENTIALS_JSON not set in Railway env). "
                "/admin/costs/live returns zeros until configured."
            )
            _gcp_creds_unavailable_logged = True
        return 0
    try:
        import google.auth
        from google.auth.transport.requests import Request as GoogleAuthRequest

        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/monitoring.read"])
        creds.refresh(GoogleAuthRequest())
        token = creds.token

        # Filter por service. Aggregation: SUM por ALIGN_SUM en bucket único.
        period_seconds = max(60, int((datetime.fromisoformat(end_iso.replace("Z", "+00:00")) -
                                       datetime.fromisoformat(start_iso.replace("Z", "+00:00"))).total_seconds()))
        url = (
            f"https://monitoring.googleapis.com/v3/projects/{GCP_PROJECT_ID}/timeSeries"
            f"?filter=metric.type%3D%22serviceruntime.googleapis.com%2Fapi%2Frequest_count%22"
            f"%20AND%20resource.labels.service%3D%22{service_name}%22"
            f"&interval.startTime={start_iso}"
            f"&interval.endTime={end_iso}"
            f"&aggregation.alignmentPeriod={period_seconds}s"
            f"&aggregation.perSeriesAligner=ALIGN_SUM"
            f"&aggregation.crossSeriesReducer=REDUCE_SUM"
        )

        client = google_maps_client()
        resp = await client.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if resp.status_code != 200:
            # 403 = service account sin rol monitoring.viewer. Loggear UNA vez.
            global _gcp_monitoring_403_logged
            if resp.status_code == 403 and not _gcp_monitoring_403_logged:
                logger.warning(
                    f"Cloud Monitoring 403 for {service_name}: service account "
                    f"needs roles/monitoring.viewer on project {GCP_PROJECT_ID}. "
                    f"/admin/costs/live returns zeros until granted. "
                    f"Visit https://console.cloud.google.com/iam-admin/iam?project={GCP_PROJECT_ID}"
                )
                _gcp_monitoring_403_logged = True
            elif resp.status_code != 403:
                logger.warning(f"Cloud Monitoring {service_name} returned {resp.status_code}")
            return 0
        data = resp.json()
        total = 0
        for ts in data.get("timeSeries", []):
            for point in ts.get("points", []):
                val = point.get("value", {}).get("int64Value")
                if val is not None:
                    total += int(val)
        return total
    except Exception as e:
        logger.warning(f"_fetch_gcp_request_count {service_name} error: {e}")
        # NO captureException: ya se logueó y _gcp_creds_unavailable_logged evita spam
        return 0


async def _fetch_vertex_invocation_count(start_iso: str, end_iso: str) -> int:
    """Vertex AI Gemini invocation count (model_invocation_count metric)."""
    if not _ensure_gcp_creds_loaded():
        return 0
    try:
        import google.auth
        from google.auth.transport.requests import Request as GoogleAuthRequest

        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/monitoring.read"])
        creds.refresh(GoogleAuthRequest())
        token = creds.token
        period_seconds = max(60, int((datetime.fromisoformat(end_iso.replace("Z", "+00:00")) -
                                       datetime.fromisoformat(start_iso.replace("Z", "+00:00"))).total_seconds()))
        url = (
            f"https://monitoring.googleapis.com/v3/projects/{GCP_PROJECT_ID}/timeSeries"
            f"?filter=metric.type%3D%22aiplatform.googleapis.com%2Fpublisher%2Fonline_serving%2Fmodel_invocation_count%22"
            f"&interval.startTime={start_iso}"
            f"&interval.endTime={end_iso}"
            f"&aggregation.alignmentPeriod={period_seconds}s"
            f"&aggregation.perSeriesAligner=ALIGN_SUM"
            f"&aggregation.crossSeriesReducer=REDUCE_SUM"
        )
        client = google_maps_client()
        resp = await client.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if resp.status_code != 200:
            return 0
        data = resp.json()
        total = 0
        for ts in data.get("timeSeries", []):
            for point in ts.get("points", []):
                val = point.get("value", {}).get("int64Value")
                if val is not None:
                    total += int(val)
        return total
    except Exception as e:
        logger.warning(f"_fetch_vertex_invocation_count error: {e}")
        return 0


async def _gather_live_costs(start_iso: str, end_iso: str) -> dict:
    """Llama todos los servicios en paralelo y calcula costes estimados.
    Devuelve dict service → {count, cost_usd}."""
    services_results = await asyncio.gather(
        *[_fetch_gcp_request_count(svc, start_iso, end_iso) for svc in _GCP_SERVICE_MAP.values()],
        _fetch_vertex_invocation_count(start_iso, end_iso),
        return_exceptions=True,
    )
    out = {}
    for (local_name, _gcp_name), count in zip(_GCP_SERVICE_MAP.items(), services_results[:-1]):
        if isinstance(count, Exception):
            count = 0
        cost = (count / 1000.0) * _API_PRICING_USD_PER_1K[local_name]
        out[local_name] = {"count": int(count), "cost_usd": round(cost, 4)}
    vertex_count = services_results[-1] if not isinstance(services_results[-1], Exception) else 0
    out["vertex_gemini"] = {
        "count": int(vertex_count),
        "cost_usd": round((int(vertex_count) / 1000.0) * _API_PRICING_USD_PER_1K["vertex_gemini"], 4),
    }
    # BUG FIX (Miguel 21 may 12:09): calcular totales ANTES de añadir las claves
    # _total_* al dict — si añades _total_cost_usd primero y luego iteras out.values()
    # para _total_calls, _total_cost_usd es float y `s["count"]` falla con TypeError.
    # Aislar la suma a solo los dicts de servicios reales (claves que NO empiezan por _).
    service_dicts = {k: v for k, v in out.items() if not k.startswith("_") and isinstance(v, dict)}
    gross_total = round(sum(s["cost_usd"] for s in service_dicts.values()), 4)
    gross_maps = round(sum(v["cost_usd"] for k, v in service_dicts.items() if k in _MAPS_SERVICES), 4)
    gross_vertex = round(sum(v["cost_usd"] for k, v in service_dicts.items() if k in _VERTEX_SERVICES), 4)

    # Descuento Google Maps free tier prorrateado al intervalo consultado.
    # Cálculo (Miguel 21 may 14:05): muestra el coste NETO que realmente se paga
    # para que las cifras no asusten. El "regalo" se reporta aparte como KPI.
    try:
        period_hours = (
            datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
            - datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        ).total_seconds() / 3600.0
    except Exception:
        period_hours = 24.0
    # Días en el mes actual (más preciso que asumir 30) para prorratear $200/mes.
    now_utc = datetime.now(timezone.utc)
    days_in_month = (
        date(now_utc.year + (1 if now_utc.month == 12 else 0),
             1 if now_utc.month == 12 else now_utc.month + 1, 1)
        - date(now_utc.year, now_utc.month, 1)
    ).days
    free_tier_per_hour = _MAPS_FREE_TIER_USD_MONTHLY / (days_in_month * 24.0)
    free_credit_available = round(free_tier_per_hour * period_hours, 4)
    free_credit_applied = round(min(gross_maps, free_credit_available), 4)
    net_maps = round(max(0.0, gross_maps - free_credit_available), 4)
    net_total = round(net_maps + gross_vertex, 4)

    out["_total_cost_usd"] = net_total  # ← lo que muestra "Coste estimado" (NETO)
    out["_total_cost_gross_usd"] = gross_total
    out["_total_cost_maps_gross_usd"] = gross_maps
    out["_total_cost_vertex_usd"] = gross_vertex
    out["_free_tier_available_usd"] = free_credit_available
    out["_free_tier_applied_usd"] = free_credit_applied
    out["_free_tier_monthly_usd"] = _MAPS_FREE_TIER_USD_MONTHLY
    out["_total_calls"] = sum(s["count"] for s in service_dicts.values())
    return out


@app.get("/admin/costs/live", tags=["admin", "costs"], summary="Costes API en tiempo real (Cloud Monitoring)")
async def admin_costs_live(
    hours: int = 24,
    user=Depends(require_admin),
):
    """Llama Google Cloud Monitoring REST API para sumar request counts por servicio
    en las últimas N horas (default 24h). Latencia 3-8s. Cacheable a nivel cliente.
    Devuelve estimación de coste USD basada en pricing 2026."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=max(1, min(hours, 720)))  # cap 30d
    start_iso = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    by_service = await _gather_live_costs(start_iso, end_iso)

    return {
        "ok": True,
        "interval": {"start": start_iso, "end": end_iso, "hours": hours},
        "by_service": by_service,
        "pricing_usd_per_1k": _API_PRICING_USD_PER_1K,
        "fetched_at": now.isoformat(),
    }


@app.get("/admin/costs/history", tags=["admin", "costs"], summary="Histórico snapshots diarios (daily_api_metrics)")
async def admin_costs_history(
    days: int = 30,
    user=Depends(require_admin),
):
    """Devuelve histórico de daily_api_metrics agrupado por fecha + servicio."""
    try:
        days = max(1, min(days, 90))
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
        rows = (
            supabase.table("daily_api_metrics")
            .select("date,service,request_count,est_cost_usd")
            .gte("date", cutoff)
            .order("date")
            .execute()
        )
        return {"ok": True, "days": days, "rows": rows.data or []}
    except Exception as e:
        logger.exception("admin_costs_history failed")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail=f"history failed: {e}")


@app.get("/admin/diag/smoke", tags=["admin", "diag"], summary="Smoke test on-demand: Supabase + Google + JWT + httpx config")
async def admin_diag_smoke(user=Depends(require_admin)):
    """Healthcheck profundo on-demand para validar que el backend tiene todas
    sus dependencias OK.

    Añadido 22 may 2026 tras incidente 1h con bug httpx HTTP/1.1 que rompió
    TODO Supabase silenciosamente (50+ events, ventana 14:42→15:55 CEST).
    El startup hook _startup_smoke_test corre solo al boot — este endpoint
    deja al admin verificarlo MANUAL después de cualquier deploy/cambio.

    Returns 200 con detalles si todo OK, 500 si alguna check falla."""
    checks: dict = {}

    # 1. Supabase real query
    try:
        result = await asyncio.to_thread(
            lambda: supabase.table("app_config").select("key").limit(1).execute()
        )
        checks["supabase_query"] = {"ok": isinstance(result.data, list), "rows": len(result.data or [])}
    except Exception as e:
        checks["supabase_query"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # 2. postgrest session base_url
    try:
        session = supabase.postgrest.session
        base_url = str(session.base_url)
        checks["postgrest_base_url"] = {
            "ok": base_url.startswith(("http://", "https://")),
            "value": base_url,
        }
    except Exception as e:
        checks["postgrest_base_url"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # 3. Env vars críticas
    checks["env"] = {
        "GOOGLE_API_KEY": bool(GOOGLE_API_KEY),
        "SUPABASE_JWT_SECRET": bool(SUPABASE_JWT_SECRET),
        "SUPABASE_URL": bool(os.getenv("SUPABASE_URL")),
        "SUPABASE_SERVICE_KEY": bool(SUPABASE_SERVICE_KEY),
    }
    checks["env"]["ok"] = all(v for k, v in checks["env"].items() if k != "ok")

    # 4. Google Places ping (HEAD a la API endpoint, no real query)
    try:
        client = google_maps_client()
        resp = await client.get(
            "https://maps.googleapis.com/maps/api/place/autocomplete/json",
            params={"input": "test", "key": GOOGLE_API_KEY, "language": "es"},
            timeout=5.0,
        )
        gdata = resp.json() if resp.status_code == 200 else {}
        # Google devuelve status OK o REQUEST_DENIED (key invalida)
        checks["google_places"] = {
            "ok": gdata.get("status") in ("OK", "ZERO_RESULTS"),
            "google_status": gdata.get("status"),
        }
    except Exception as e:
        checks["google_places"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # 5. Startup smoke result (referencia histórica)
    checks["startup_smoke"] = {
        "passed": _startup_smoke_ok,
        "failures_at_boot": _startup_smoke_failures,
    }

    # Veredicto global
    all_ok = (
        checks["supabase_query"].get("ok", False)
        and checks["postgrest_base_url"].get("ok", False)
        and checks["env"].get("ok", False)
        and checks["google_places"].get("ok", False)
    )
    from fastapi.responses import JSONResponse as _DiagJSONResponse
    status_code = 200 if all_ok else 500
    return _DiagJSONResponse(
        status_code=status_code,
        content={"ok": all_ok, "checks": checks, "timestamp": datetime.now(timezone.utc).isoformat()},
    )


@app.get("/admin/cache/places-stats", tags=["admin", "costs"], summary="Stats cache Places Autocomplete")
async def admin_cache_places_stats(user=Depends(require_admin)):
    """Devuelve estado del cache `places_autocomplete_cache`: tamaño, hits, hit
    rate estimado y ahorro USD. Útil para decidir cuándo activar cache mode
    de off → shadow → on."""
    try:
        # Read flag actual del cache
        flag_row = (
            supabase.table("app_config")
            .select("value")
            .eq("key", "places_cache_mode")
            .limit(1)
            .execute()
        )
        cache_mode = (flag_row.data[0]["value"] if flag_row.data else "off") or "off"

        # Stats agregadas (pricing autocomplete legacy: $2.83 por 1000 = $0.00283 por call)
        price_per_call = 0.00283
        now_iso = datetime.now(timezone.utc)
        seven_days_ago_iso = (now_iso - timedelta(days=7)).isoformat()

        # PAGINACIÓN: PostgREST Supabase Cloud cap a 1000 rows por request
        # (feedback_supabase_pagination_cap). Cache tiene >1000 entries tras
        # backfill (22 may, 1.6k+ queries) → iteramos páginas hasta agotar.
        rows = []
        page = 0
        page_size = 1000
        while True:
            chunk = (
                supabase.table("places_autocomplete_cache")
                .select("hits, created_at, last_used_at, expires_at")
                .range(page * page_size, (page + 1) * page_size - 1)
                .execute()
            )
            if not chunk.data:
                break
            rows.extend(chunk.data)
            if len(chunk.data) < page_size:
                break
            page += 1
            if page > 20:  # safety cap 20k entries (no debería pasar nunca)
                break

        total_entries = len(rows)
        total_hits = sum(int(r.get("hits") or 0) for r in rows)
        active_entries = sum(1 for r in rows if r.get("expires_at") and datetime.fromisoformat(r["expires_at"].replace("Z", "+00:00")) > now_iso)
        seven_days_ago = now_iso - timedelta(days=7)
        entries_7d = sum(1 for r in rows if r.get("created_at") and datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")) >= seven_days_ago)
        hits_7d = sum(int(r.get("hits") or 0) for r in rows if r.get("last_used_at") and datetime.fromisoformat(r["last_used_at"].replace("Z", "+00:00")) >= seven_days_ago)

        # Top 10 queries por hits (segunda query, ordenada)
        top_q = (
            supabase.table("places_autocomplete_cache")
            .select("query_normalized, hits, bias_geohash5, last_used_at")
            .order("hits", desc=True)
            .limit(10)
            .execute()
        )

        # Métricas in-memory por source (desde último cold-start del backend)
        counters_uptime_min = round((time.time() - _places_counters_started_at) / 60, 1)
        total_lookups = sum(_places_source_counters.values()) or 1  # avoid div by 0
        free_hits = (
            _places_source_counters["hit"]
            + _places_source_counters["prefix_hit"]
            + _places_source_counters["stops_fuzzy_hit"]
            + _places_source_counters["negative_hit"]
        )
        hit_rate_pct = round(free_hits / total_lookups * 100, 1)
        # Real-time savings (since last cold-start)
        rt_savings_usd = round(free_hits * price_per_call, 4)

        return {
            "ok": True,
            "cache_mode": cache_mode,  # off | shadow | on
            "total_entries": total_entries,
            "active_entries": active_entries,
            "total_hits": total_hits,
            "entries_added_7d": entries_7d,
            "hits_7d": hits_7d,
            "savings_total_usd": round(total_hits * price_per_call, 4),
            "savings_7d_usd": round(hits_7d * price_per_call, 4),
            "price_per_call_usd": price_per_call,
            "top_queries": top_q.data or [],
            # Métricas in-memory (desde último deploy):
            "realtime": {
                "uptime_minutes": counters_uptime_min,
                "total_lookups": total_lookups - (1 if total_lookups == 1 and free_hits == 0 else 0),
                "by_source": dict(_places_source_counters),
                "hit_rate_pct": hit_rate_pct,
                "savings_usd_since_deploy": rt_savings_usd,
                "l1_in_memory_entries": len(_places_l1_cache),
                "l1_max": _PLACES_L1_MAX,
                "l1_ttl_seconds": _PLACES_L1_TTL_SEC,
            },
        }
    except Exception as e:
        logger.exception("admin_cache_places_stats failed")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail=f"cache stats failed: {e}")


@app.post("/admin/cache/backfill-missing", tags=["admin", "costs"], summary="Backfill cache con queries faltantes")
async def admin_cache_backfill_missing(
    min_appearances: int = 2,
    days_lookback: int = 90,
    limit: int = 1000,
    user=Depends(require_admin),
):
    """Backfill places_autocomplete_cache con queries en stops que aún NO están cacheadas.

    Diseñado para ejecutar puntualmente (después de purgar cache o tras descubrir
    direcciones nuevas no cubiertas). Bypass del rate limit /places/* del user
    (llama Google directamente con backend GOOGLE_API_KEY).

    Inserta con hits=0 para no inflar el contador de hits reales de drivers
    (contrast: _places_cache_write_sync usa hits=1 porque sirvió una llamada).

    Params:
        min_appearances: solo cachear queries que aparezcan ≥N veces en stops (default 2)
        days_lookback: ventana stops a considerar (default 90 días)
        limit: máximo de queries a procesar en una llamada (default 1000, hard cap 2000)

    Returns:
        {processed, ok, fail, duration_seconds, sample_failures[:5]}
    """
    if not GOOGLE_API_KEY:
        raise HTTPException(status_code=500, detail="GOOGLE_API_KEY not configured")

    limit = min(limit, 2000)  # safety cap
    import time as _time
    start_ts = _time.monotonic()

    # 1. Identificar queries faltantes via SQL agregada
    sql = """
    WITH should_be_cached AS (
      SELECT
        LOWER(TRIM(REGEXP_REPLACE(SPLIT_PART(address, E'\\n', 1), '\\s+', ' ', 'g'))) AS q,
        AVG(lat)::float AS lat,
        AVG(lng)::float AS lng,
        COUNT(*) AS n
      FROM stops
      WHERE created_at >= NOW() - (%s::text || ' days')::interval
        AND deleted_at IS NULL
        AND lat IS NOT NULL AND lng IS NOT NULL
        AND address IS NOT NULL AND LENGTH(address) > 8
      GROUP BY 1 HAVING COUNT(*) >= %s
    )
    SELECT q, lat, lng, n
    FROM should_be_cached s
    WHERE NOT EXISTS (SELECT 1 FROM places_autocomplete_cache c WHERE c.query_normalized = s.q)
      AND LENGTH(TRIM(q)) >= 5
      AND NOT (s.lat BETWEEN 34.0 AND 35.0 AND s.lng BETWEEN -16.0 AND -15.0)
    ORDER BY n DESC
    LIMIT %s;
    """
    try:
        missing_resp = await asyncio.to_thread(
            lambda: supabase.rpc(
                "exec_sql_select",
                {"q": sql, "params": [days_lookback, min_appearances, limit]},
            ).execute()
        )
        missing = missing_resp.data or []
    except Exception:
        # Fallback: la RPC exec_sql_select puede no existir. Usar query directa.
        # Limitación: SQL via supabase-py requiere RPC o construir via PostgREST.
        # Hacemos la agregación en Python paginando stops.
        logger.info("admin_cache_backfill: falling back to in-app aggregation")
        missing = await _backfill_compute_missing_in_app(days_lookback, min_appearances, limit)

    if not missing:
        return {"processed": 0, "ok": 0, "fail": 0, "duration_seconds": 0, "message": "no missing queries found"}

    # 2. Para cada faltante, llamar Google Places Autocomplete directo
    client = google_maps_client()
    ok = fail = 0
    sample_failures: list[str] = []

    async def _backfill_one(q: str, lat: float, lng: float) -> bool:
        params = {
            "input": q,
            "key": GOOGLE_API_KEY,
            "language": "es",
            "location": f"{lat},{lng}",
            "radius": "20000",  # bias 20km
        }
        try:
            resp = await client.get(
                "https://maps.googleapis.com/maps/api/place/autocomplete/json",
                params=params,
                timeout=15.0,
            )
            data = resp.json()
            if data.get("status") not in ("OK", "ZERO_RESULTS"):
                return False
            if data.get("status") == "OK" and data.get("predictions"):
                norm, bias = _ac_cache_key(q, lat, lng)
                # INSERT con hits=0 (NO incrementar artificial — solo escribir bytes).
                # last_used_at tiene constraint NOT NULL en BD → usamos NOW() pero
                # el flag REAL anti-inflación es hits=0: el bump real de drivers
                # incrementa hits → cualquier query con hits>0 = uso real driver,
                # hits=0 = solo backfill (cuenta correcta sin contar las inserciones).
                try:
                    now_iso = datetime.now(timezone.utc).isoformat()
                    await asyncio.to_thread(
                        lambda: supabase.table("places_autocomplete_cache").upsert({
                            "query_normalized": norm,
                            "bias_geohash5": bias,
                            "predictions": data["predictions"],
                            "hits": 0,
                            "last_used_at": now_iso,
                            "expires_at": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
                        }, on_conflict="query_normalized,bias_geohash5").execute()
                    )
                    return True
                except Exception as e:
                    logger.warning(f"backfill cache write failed: {e}")
                    return False
            return True  # ZERO_RESULTS también "OK" (no es fallo, no hay nada que cachear)
        except Exception as e:
            logger.warning(f"backfill Google call failed for {q[:30]}: {e}")
            return False

    # Procesar en chunks de 20 concurrentes (margen QPS Google = 50)
    chunk_size = 20
    for i in range(0, len(missing), chunk_size):
        chunk = missing[i:i + chunk_size]
        results = await asyncio.gather(
            *[_backfill_one(m["q"], m["lat"], m["lng"]) for m in chunk],
            return_exceptions=True,
        )
        for m, r in zip(chunk, results):
            if r is True:
                ok += 1
            else:
                fail += 1
                if len(sample_failures) < 5:
                    sample_failures.append(m["q"][:60])

    duration = _time.monotonic() - start_ts
    logger.info(f"admin_cache_backfill_missing done: ok={ok} fail={fail} in {duration:.1f}s")
    return {
        "processed": len(missing),
        "ok": ok,
        "fail": fail,
        "duration_seconds": round(duration, 1),
        "sample_failures": sample_failures,
    }


async def _backfill_compute_missing_in_app(days_lookback: int, min_appearances: int, limit: int) -> list[dict]:
    """Fallback Python si exec_sql_select RPC no existe.

    Lista en cache existentes (set de query_normalized) + lista de stops paginadas
    + group by Python. Más lento pero independiente de RPCs custom."""
    # 1. Set de queries ya cacheadas
    cached_q: set[str] = set()
    offset = 0
    while True:
        chunk = await asyncio.to_thread(
            lambda: supabase.table("places_autocomplete_cache")
            .select("query_normalized")
            .range(offset, offset + 999)
            .execute()
        )
        if not chunk.data:
            break
        for r in chunk.data:
            cached_q.add(r["query_normalized"])
        if len(chunk.data) < 1000:
            break
        offset += 1000

    # 2. Stops paginadas + group by en Python
    from collections import defaultdict
    agg: dict[str, list] = defaultdict(lambda: [0.0, 0.0, 0])  # q → [lat_sum, lng_sum, n]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_lookback)).isoformat()
    offset = 0
    while True:
        chunk = await asyncio.to_thread(
            lambda: supabase.table("stops")
            .select("address,lat,lng")
            .gte("created_at", cutoff)
            .is_("deleted_at", "null")
            .not_.is_("lat", "null")
            .not_.is_("lng", "null")
            .not_.is_("address", "null")
            .range(offset, offset + 999)
            .execute()
        )
        if not chunk.data:
            break
        for row in chunk.data:
            addr = (row.get("address") or "").strip()
            lat = row.get("lat"); lng = row.get("lng")
            if not addr or lat is None or lng is None or len(addr) < 9:
                continue
            q = " ".join(addr.split("\n")[0].lower().strip().split())[:200]
            if not q or len(q) < 5:
                continue
            # Excluir demo Madrid sintético
            if 34.0 < lat < 35.0 and -16.0 < lng < -15.0:
                continue
            agg[q][0] += float(lat)
            agg[q][1] += float(lng)
            agg[q][2] += 1
        if len(chunk.data) < 1000:
            break
        offset += 1000

    # 3. Filtrar faltantes + ordenar por n DESC + limit
    missing = []
    for q, (lat_sum, lng_sum, n) in agg.items():
        if n < min_appearances:
            continue
        if q in cached_q:
            continue
        missing.append({"q": q, "lat": lat_sum / n, "lng": lng_sum / n, "n": n})
    missing.sort(key=lambda x: -x["n"])
    return missing[:limit]


async def run_daily_costs_snapshot():
    """Cron diario 09:00 UTC: snapshot del día anterior. Persistente histórico."""
    try:
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        start_iso = f"{yesterday.isoformat()}T00:00:00Z"
        end_iso = f"{yesterday.isoformat()}T23:59:59Z"
        by_service = await _gather_live_costs(start_iso, end_iso)
        rows_to_upsert = []
        for svc, vals in by_service.items():
            if svc.startswith("_"):
                continue
            rows_to_upsert.append({
                "date": yesterday.isoformat(),
                "service": svc,
                "request_count": vals["count"],
                "est_cost_usd": vals["cost_usd"],
                "snapshot_at": datetime.now(timezone.utc).isoformat(),
            })
        if rows_to_upsert:
            supabase.table("daily_api_metrics").upsert(
                rows_to_upsert, on_conflict="date,service"
            ).execute()
        logger.info(f"Daily costs snapshot saved for {yesterday}: {len(rows_to_upsert)} services")
    except Exception as e:
        logger.exception("run_daily_costs_snapshot failed")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)


async def run_all_closure_scrapers():
    """Scheduled job: run every closure scraper and upsert. Logged + Sentry-captured on failure.

    Pasamos `supabase` al scraper para que pueda hacer cache lookup por source_url
    (Miguel 21 may 2026): cierres ya geocodificados antes NO se vuelven a geocodificar
    si la `localizacion` no cambió. Ahorro ~$900/mes solo Sanlúcar.
    """
    if not GOOGLE_API_KEY:
        logger.info("Skipping closures scrape: GOOGLE_API_KEY missing")
        return
    for city, scraper in _CLOSURE_SCRAPERS.items():
        try:
            records = await scraper(google_api_key=GOOGLE_API_KEY, supabase=supabase)
            counts = _upsert_closures(supabase, records)
            logger.info(f"Closures scrape [{city}]: {len(records)} scraped, {counts}")
        except Exception as e:
            logger.exception(f"Closure scraper '{city}' failed")
            if SENTRY_DSN:
                sentry_sdk.capture_exception(e)


# === ACCOUNT DELETION ===

@app.delete("/auth/delete-account", tags=["auth"], summary="Eliminar cuenta")
async def delete_account(user=Depends(get_current_user)):
    """Elimina la cuenta del usuario y todos sus datos asociados (rutas, paradas, ubicaciones, conductor, etc.). Requerido por Apple y GDPR."""
    user_id = user["id"]
    deletion_errors = []
    try:
        # First, find the driver_id for this user
        driver_result = supabase.table("drivers").select("id").eq("user_id", user_id).execute()
        driver_row = safe_first(driver_result)
        driver_id = driver_row["id"] if driver_row else None

        if driver_id:
            # Get all route IDs for this driver (needed for stops and delivery_proofs)
            routes_result = supabase.table("routes").select("id").eq("driver_id", driver_id).execute()
            route_ids = [r["id"] for r in (routes_result.data or [])]

            if route_ids:
                # Collect all stop IDs — batch query instead of N+1
                try:
                    stops_result = supabase.table("stops").select("id").in_("route_id", route_ids).execute()
                    all_stop_ids = [s["id"] for s in (stops_result.data or [])]
                except Exception as e:
                    all_stop_ids = []
                    deletion_errors.append(f"stops select: {e}")

                # Batch delete delivery_proofs for all stops at once
                if all_stop_ids:
                    try:
                        supabase.table("delivery_proofs").delete().in_("stop_id", all_stop_ids).execute()
                    except Exception as e:
                        deletion_errors.append(f"delivery_proofs: {e}")

                # Batch delete tracking_links and stops for all routes at once
                try:
                    supabase.table("tracking_links").delete().in_("route_id", route_ids).execute()
                except Exception as e:
                    deletion_errors.append(f"tracking_links: {e}")
                try:
                    supabase.table("stops").delete().in_("route_id", route_ids).execute()
                except Exception as e:
                    deletion_errors.append(f"stops delete: {e}")

                # Delete all routes
                try:
                    supabase.table("routes").delete().eq("driver_id", driver_id).execute()
                except Exception as e:
                    deletion_errors.append(f"routes: {e}")

            # Delete recurring_places by user_id (created_by stores auth.uid())
            try:
                supabase.table("recurring_places").delete().eq("created_by", user_id).execute()
            except Exception as e:
                deletion_errors.append(f"recurring_places: {e}")

            # Delete other driver-specific data. The list MUST cover every
            # table with a FK to public.drivers having ON DELETE NO ACTION,
            # otherwise the final `DELETE FROM drivers` blows up on FK
            # violation (Postgres 23503) and the user is left in limbo:
            # auth.users alive, drivers row alive, app told them "deleted".
            #
            # Incidents 2026-05-09 (zamorakareilys) and 2026-05-10
            # (arroceriadevicent) hit exactly this — both via missing
            # trial_claims cleanup. The complete list, derived from
            # information_schema for FK delete_rule='NO ACTION' on
            # public.drivers as referenced table:
            #   trial_claims        (driver_id)
            #   app_events          (driver_id)
            #   customer_notifications (driver_id)
            #   stop_check_ins      (driver_id)
            #   delivery_proofs     (driver_id)   -- already partially handled by stop_id batch above
            #   tracking_links      (driver_id)   -- already partially handled by route_id batch above
            #   location_history    (driver_id)
            #   daily_usage         (driver_id)
            #   referrals           (referrer_driver_id, referred_driver_id)
            tables_driver = [
                ("trial_claims", "driver_id"),
                ("app_events", "driver_id"),
                ("customer_notifications", "driver_id"),
                ("stop_check_ins", "driver_id"),
                ("delivery_proofs", "driver_id"),
                ("tracking_links", "driver_id"),
                ("location_history", "driver_id"),
                ("daily_usage", "driver_id"),
                ("referrals", "referrer_driver_id"),
                ("referrals", "referred_driver_id"),
            ]
            for table, column in tables_driver:
                try:
                    supabase.table(table).delete().eq(column, driver_id).execute()
                except Exception as e:
                    deletion_errors.append(f"{table}.{column}: {e}")

            # Delete the driver record
            try:
                supabase.table("drivers").delete().eq("id", driver_id).execute()
            except Exception as e:
                deletion_errors.append(f"drivers: {e}")
                # If the driver row didn't go away, the auth.users row will
                # almost certainly fail too (the gotrue admin endpoint runs
                # CASCADE checks from auth.users). Sentry-capture for visibility.
                if SENTRY_DSN:
                    sentry_sdk.capture_message(
                        f"delete_account: drivers row for {driver_id} could not be deleted. "
                        f"FK violation likely — check tables_driver list. Error: {e}",
                        level="error",
                    )

        # Delete user-level data
        tables_user = [
            ("code_redemptions", "user_id"),
            ("company_driver_links", "user_id"),
            ("company_invites", "user_id"),
        ]
        for table, column in tables_user:
            try:
                supabase.table(table).delete().eq(column, user_id).execute()
            except Exception as e:
                deletion_errors.append(f"{table}: {e}")

        # Delete user profile
        try:
            supabase.table("users").delete().eq("id", user_id).execute()
        except Exception as e:
            deletion_errors.append(f"users: {e}")

        # Delete auth user via Supabase Admin API — CRITICAL step
        try:
            supabase.auth.admin.delete_user(user_id)
        except Exception as e:
            deletion_errors.append(f"auth.admin.delete_user: {e}")
            logger.error(f"CRITICAL: Failed to delete auth user {user_id}: {e}")
            sentry_sdk.capture_exception(e)

        # Critical post-condition check: did auth.users actually disappear?
        # If the auth.admin.delete_user step recorded an error, the auth row
        # is still alive and the user is in limbo: app tells them "OK"
        # while GDPR says we still hold their data, opening us to AEPD
        # penalties. We track this via the deletion_errors list — every
        # exception from auth.admin.delete_user is appended with the prefix
        # 'auth.admin.delete_user:' (see above). If that prefix is in the
        # list, treat the auth user as still alive and refuse to return 200.
        auth_user_still_exists = any(
            e.startswith("auth.admin.delete_user:") for e in deletion_errors
        )

        # Log any partial failures for GDPR audit trail
        if deletion_errors:
            logger.warning(f"Account deletion partial errors for {user_id}: {deletion_errors}")
            sentry_sdk.capture_message(
                f"Account deletion had {len(deletion_errors)} errors for user {user_id}: "
                f"{'; '.join(deletion_errors[:5])}",
                level="warning" if not auth_user_still_exists else "error",
            )

        # Decide response status.
        #   - auth.users still alive → 502: account NOT deleted, the client
        #     should show the user a real error (not "ok") and let them retry
        #     or contact support. This is the GDPR-compliant outcome: never
        #     lie about deletion when the row is still there.
        #   - errors recorded but auth.users gone → 207 Multi-Status: data
        #     bulk gone, some auxiliary rows orphaned. User-facing this is
        #     still a successful delete (their identity is gone), but the
        #     audit log captures the cleanup gaps.
        #   - clean run → 200.
        if auth_user_still_exists:
            raise HTTPException(
                status_code=502,
                detail={
                    "error": "deletion_incomplete",
                    "message": "No hemos podido completar la eliminación. Inténtalo de nuevo o contacta con soporte.",
                    "errors_count": len(deletion_errors),
                },
            )
        if deletion_errors:
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=207,
                content={
                    "status": "deleted",
                    "message": "Cuenta eliminada con incidencias menores en los datos auxiliares.",
                    "errors_count": len(deletion_errors),
                },
            )
        return {"status": "deleted", "message": "Cuenta eliminada correctamente"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete account error: {e}")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error al eliminar la cuenta")


# === SOCIAL MEDIA MANAGEMENT ===

import uuid as uuid_mod

import tweepy
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Twitter/X credentials from env
TWITTER_CONSUMER_KEY = os.getenv("TWITTER_CONSUMER_KEY", "")
TWITTER_CONSUMER_SECRET = os.getenv("TWITTER_CONSUMER_SECRET", "")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN", "")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET", "")

def get_twitter_client():
    """Create tweepy client for X/Twitter API"""
    return tweepy.Client(
        consumer_key=TWITTER_CONSUMER_KEY,
        consumer_secret=TWITTER_CONSUMER_SECRET,
        access_token=TWITTER_ACCESS_TOKEN,
        access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    )

def get_twitter_api_v1():
    """Create tweepy v1 API for media uploads"""
    auth = tweepy.OAuth1UserHandler(
        TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET,
        TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET,
    )
    return tweepy.API(auth)


class SocialPostCreate(BaseModel):
    content: str
    platforms: List[str]  # ['twitter', 'linkedin']
    scheduled_at: Optional[str] = None
    image_urls: Optional[List[str]] = None

class SocialPostUpdate(BaseModel):
    content: Optional[str] = None
    platforms: Optional[List[str]] = None
    scheduled_at: Optional[str] = None
    image_urls: Optional[List[str]] = None
    status: Optional[str] = None


async def publish_to_twitter(content: str, image_urls: list = None) -> dict:
    """Publish a post to X/Twitter. Returns dict with post_id and url."""
    client = get_twitter_client()
    media_ids = []

    if image_urls:
        api_v1 = get_twitter_api_v1()
        for img_url in image_urls[:4]:  # X allows max 4 images
            try:
                async with httpx.AsyncClient(timeout=15) as http:
                    resp = await http.get(img_url)
                    if resp.status_code == 200:
                        import tempfile
                        ext = img_url.split(".")[-1].split("?")[0][:4]
                        with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
                            tmp.write(resp.content)
                            tmp_path = tmp.name
                        media = api_v1.media_upload(filename=tmp_path)
                        media_ids.append(media.media_id)
                        os.unlink(tmp_path)
            except Exception as e:
                logger.error(f"Social media upload error: {e}")

    kwargs = {"text": content}
    if media_ids:
        kwargs["media_ids"] = media_ids

    response = client.create_tweet(**kwargs)
    tweet_id = response.data["id"]
    return {
        "post_id": tweet_id,
        "url": f"https://x.com/Xpedit_es/status/{tweet_id}",
    }


async def publish_post(post_id: str):
    """Publish a social media post to all selected platforms."""
    try:
        result = supabase.table("social_posts").select("*").eq("id", post_id).single().execute()
        post = result.data
        if not post or post["status"] in ("published", "publishing"):
            return

        # Mark as publishing
        supabase.table("social_posts").update({"status": "publishing"}).eq("id", post_id).execute()

        twitter_post_id = None
        twitter_url = None
        linkedin_post_id = None
        linkedin_url = None
        errors = []

        if "twitter" in post["platforms"]:
            try:
                result_tw = await publish_to_twitter(post["content"], post.get("image_urls"))
                twitter_post_id = result_tw["post_id"]
                twitter_url = result_tw["url"]
            except Exception as e:
                errors.append(f"Twitter: {str(e)}")

        if "linkedin" in post["platforms"]:
            linkedin_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "")
            linkedin_member_sub = os.getenv("LINKEDIN_MEMBER_SUB", "")
            if linkedin_token and linkedin_member_sub:
                try:
                    async with httpx.AsyncClient(timeout=15) as http:
                        headers = {"Authorization": f"Bearer {linkedin_token}", "X-Restli-Protocol-Version": "2.0.0", "Content-Type": "application/json"}
                        payload = {
                            "author": f"urn:li:person:{linkedin_member_sub}",
                            "lifecycleState": "PUBLISHED",
                            "specificContent": {
                                "com.linkedin.ugc.ShareContent": {
                                    "shareCommentary": {"text": post["content"]},
                                    "shareMediaCategory": "NONE",
                                }
                            },
                            "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"},
                        }
                        resp = await http.post("https://api.linkedin.com/v2/ugcPosts", headers=headers, json=payload)
                        if resp.status_code in (200, 201):
                            li_id = resp.json().get("id", "")
                            linkedin_post_id = li_id
                            linkedin_url = f"https://www.linkedin.com/feed/update/{li_id}/"
                        else:
                            errors.append(f"LinkedIn: {resp.status_code} {resp.text[:200]}")
                except Exception as e:
                    errors.append(f"LinkedIn: {str(e)}")
            else:
                errors.append("LinkedIn: API no configurada")

        # Update post with results
        update = {"updated_at": datetime.now(timezone.utc).isoformat()}
        if twitter_post_id:
            update["twitter_post_id"] = twitter_post_id
            update["twitter_url"] = twitter_url
        if linkedin_post_id:
            update["linkedin_post_id"] = linkedin_post_id
            update["linkedin_url"] = linkedin_url

        if errors and not twitter_post_id and not linkedin_post_id:
            update["status"] = "failed"
            update["error_message"] = "; ".join(errors)
            update["retry_count"] = (post.get("retry_count") or 0) + 1
        else:
            update["status"] = "published"
            update["published_at"] = datetime.now(timezone.utc).isoformat()
            if errors:
                update["error_message"] = "; ".join(errors)

        supabase.table("social_posts").update(update).eq("id", post_id).execute()
        logger.info(f"Social post {post_id} published: twitter={twitter_post_id}, linkedin={linkedin_post_id}")

    except Exception as e:
        logger.error(f"Social error publishing post {post_id}: {e}")
        try:
            supabase.table("social_posts").update({
                "status": "failed",
                "error_message": str(e),
                "retry_count": (post.get("retry_count", 0) if "post" in dir() else 0) + 1,
            }).eq("id", post_id).execute()
        except Exception:
            pass


async def check_scheduled_posts():
    """Check and publish scheduled posts that are due."""
    try:
        now = datetime.now(timezone.utc).isoformat()
        result = supabase.table("social_posts")\
            .select("id")\
            .eq("status", "scheduled")\
            .lte("scheduled_at", now)\
            .lt("retry_count", 3)\
            .execute()

        for post in (result.data or []):
            logger.info(f"Social publishing scheduled post: {post['id']}")
            await publish_post(post["id"])
    except Exception as e:
        logger.error(f"Social scheduler error: {e}")


# Initialize scheduler
social_scheduler = AsyncIOScheduler()

# RUN_SCHEDULER controls whether this process owns the cron jobs (digest,
# trial expiry, reactivation followup, snapshot, etc.). When the backend runs
# in two-process mode (web with --workers 2 + dedicated worker), only the
# worker should set RUN_SCHEDULER=true. Default true keeps the legacy single-
# process behavior so this change is backwards-compatible until the worker
# service is provisioned in Railway.
SHOULD_RUN_SCHEDULER = os.getenv("RUN_SCHEDULER", "true").lower() != "false"

@app.on_event("startup")
async def start_social_scheduler():
    if not SHOULD_RUN_SCHEDULER:
        logger.info("Scheduler skipped on this process (RUN_SCHEDULER=false)")
        return
    if TWITTER_CONSUMER_KEY:
        social_scheduler.add_job(check_scheduled_posts, "interval", seconds=60, id="social_checker", replace_existing=True)
        logger.info("Social scheduler: checking every 60s")
    else:
        logger.info("Social: Twitter credentials not configured, social posts not scheduled")
    # Closures scrapers: refresh every 30 minutes
    social_scheduler.add_job(
        run_all_closure_scrapers, "interval", minutes=30,
        id="closures_scraper", replace_existing=True,
        next_run_time=datetime.now(timezone.utc) + timedelta(minutes=2),  # first run 2 min after boot
    )
    logger.info("Closures scrapers: every 30 min")
    # Daily costs snapshot (Miguel 21 may 2026 — task #169): snapshot del día
    # anterior cada día a las 09:17 UTC (minuto :17 para evitar ruido cron).
    social_scheduler.add_job(
        run_daily_costs_snapshot, "cron", hour=9, minute=17,
        id="daily_costs_snapshot", replace_existing=True,
    )
    logger.info("Daily costs snapshot: cron 09:17 UTC")
    # Bootstrap único 22 may 2026: ejecutar snapshot 60s tras startup para
    # backfill inmediato (tabla daily_api_metrics estaba vacía por bug TypeError
    # ya corregido). on_conflict del upsert hace idempotente esta ejecución
    # extra. Tras primer snapshot, el cron diario continúa normal.
    from datetime import timedelta as _td
    social_scheduler.add_job(
        run_daily_costs_snapshot, "date",
        run_date=datetime.now(timezone.utc) + _td(seconds=60),
        id="daily_costs_snapshot_bootstrap", replace_existing=True,
    )
    logger.info("Daily costs snapshot: bootstrap run in 60s")
    # Siempre arrancar el scheduler (tambien para backups y retention)
    if not social_scheduler.running:
        social_scheduler.start()
        logger.info("Scheduler started")


# --- Social API Endpoints ---

@app.get("/social/posts", tags=["social"], summary="Listar publicaciones")
async def list_social_posts(status: Optional[str] = None, user=Depends(require_admin)):
    """Lista todas las publicaciones de redes sociales, opcionalmente filtradas por estado. Solo admin."""
    query = supabase.table("social_posts").select("*").order("created_at", desc=True)
    if status:
        query = query.eq("status", status)
    result = query.limit(200).execute()
    return result.data or []


@app.post("/social/posts", tags=["social"], summary="Crear publicación")
async def create_social_post(post: SocialPostCreate, user=Depends(require_admin)):
    """Crea una nueva publicación (borrador o programada) para redes sociales. Solo admin."""
    data = {
        "content": post.content,
        "platforms": post.platforms,
        "image_urls": post.image_urls or [],
        "created_by": user.get("id"),
    }
    if post.scheduled_at:
        data["scheduled_at"] = post.scheduled_at
        data["status"] = "scheduled"
    else:
        data["status"] = "draft"

    result = supabase.table("social_posts").insert(data).execute()
    return safe_first(result) or {}


@app.put("/social/posts/{post_id}", tags=["social"], summary="Actualizar publicación")
async def update_social_post(post_id: str, update: SocialPostUpdate, user=Depends(require_admin)):
    """Actualiza una publicación en borrador o programada. No se pueden editar posts publicados. Solo admin."""
    existing = supabase.table("social_posts").select("status").eq("id", post_id).single().execute()
    if not existing.data:
        raise HTTPException(status_code=404, detail="Post no encontrado")
    if existing.data["status"] in ("published", "publishing"):
        raise HTTPException(status_code=400, detail="No se puede editar un post ya publicado")

    data = {"updated_at": datetime.now(timezone.utc).isoformat()}
    if update.content is not None:
        data["content"] = update.content
    if update.platforms is not None:
        data["platforms"] = update.platforms
    if update.image_urls is not None:
        data["image_urls"] = update.image_urls
    if update.scheduled_at is not None:
        data["scheduled_at"] = update.scheduled_at
        data["status"] = "scheduled"
    if update.status is not None:
        data["status"] = update.status

    result = supabase.table("social_posts").update(data).eq("id", post_id).execute()
    return safe_first(result) or {}


@app.delete("/social/posts/{post_id}", tags=["social"], summary="Eliminar publicación")
async def delete_social_post(post_id: str, user=Depends(require_admin)):
    """Elimina una publicación en borrador o programada. Limpia imágenes del storage. Solo admin."""
    existing = supabase.table("social_posts").select("status, image_urls").eq("id", post_id).single().execute()
    if not existing.data:
        raise HTTPException(status_code=404, detail="Post no encontrado")
    if existing.data["status"] in ("published", "publishing"):
        raise HTTPException(status_code=400, detail="No se puede eliminar un post ya publicado")

    # Clean up images from storage
    for img_url in (existing.data.get("image_urls") or []):
        try:
            path = img_url.split("/social-media/")[1] if "/social-media/" in img_url else None
            if path:
                supabase.storage.from_("social-media").remove([path])
        except Exception:
            pass

    supabase.table("social_posts").delete().eq("id", post_id).execute()
    return {"status": "deleted"}


@app.post("/social/posts/{post_id}/publish", tags=["social"], summary="Publicar ahora")
async def publish_social_post_now(post_id: str, user=Depends(require_admin)):
    """Publica un post inmediatamente en las plataformas seleccionadas. Solo admin."""
    existing = supabase.table("social_posts").select("status").eq("id", post_id).single().execute()
    if not existing.data:
        raise HTTPException(status_code=404, detail="Post no encontrado")
    if existing.data["status"] == "published":
        raise HTTPException(status_code=400, detail="Post ya publicado")

    await publish_post(post_id)

    updated = supabase.table("social_posts").select("*").eq("id", post_id).single().execute()
    return updated.data


@app.get("/social/accounts", tags=["social"], summary="Cuentas conectadas")
async def list_social_accounts(user=Depends(require_admin)):
    """Lista las cuentas de redes sociales conectadas. Solo admin."""
    result = supabase.table("social_accounts").select("*").execute()
    return result.data or []


@app.post("/social/upload-image", tags=["social"], summary="Subir imagen")
async def upload_social_image(file: UploadFile = File(...), user=Depends(require_admin)):
    """Sube una imagen a Supabase Storage para publicaciones de redes sociales. Máximo 10MB. Solo admin."""
    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Solo se permiten imágenes")

    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Imagen demasiado grande (máx 10MB)")

    ext = file.filename.split(".")[-1] if file.filename else "jpg"
    filename = f"{uuid_mod.uuid4().hex[:12]}.{ext}"
    path = f"posts/{filename}"

    supabase.storage.from_("social-media").upload(path, content, {"content-type": file.content_type})

    public_url = f"{os.getenv('SUPABASE_URL')}/storage/v1/object/public/social-media/{path}"
    return {"url": public_url, "path": path, "filename": filename}


@app.delete("/social/images/{filename}", tags=["social"], summary="Eliminar imagen")
async def delete_social_image(filename: str, user=Depends(require_admin)):
    """Elimina una imagen del storage de redes sociales. Solo admin."""
    path = f"posts/{filename}"
    try:
        supabase.storage.from_("social-media").remove([path])
    except Exception:
        pass
    return {"status": "deleted"}


# === GEMINI AI FOR SOCIAL MEDIA ===

GOOGLE_AI_API_KEY = os.getenv("GOOGLE_AI_API_KEY")
gemini_client = None

def get_gemini_client():
    global gemini_client
    if gemini_client is None and GOOGLE_AI_API_KEY:
        from google import genai
        gemini_client = genai.Client(api_key=GOOGLE_AI_API_KEY)
    return gemini_client


# === GEMINI VERTEX AI FOR OCR/MSI (personal data → EU region + DPA) ===
# Used by /ocr/label and /ocr/screenshots-batch (MSI). The text social path
# stays on AI Studio (no personal data, only marketing prompts).
#
# Auth: Application Default Credentials. In Railway set
#   GOOGLE_APPLICATION_CREDENTIALS_JSON  → JSON blob of service account key
# (loaded into a tempfile at startup) or
#   GOOGLE_APPLICATION_CREDENTIALS       → path to mounted key file.
# Service account needs role "Vertex AI User" on project
# trim-odyssey-465314-r2.
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "trim-odyssey-465314-r2")
GCP_VERTEX_LOCATION = os.getenv("GCP_VERTEX_LOCATION", "europe-west4")
gemini_vertex_client = None

def get_gemini_vertex_client():
    """Return a google-genai Client wired to Vertex AI (region
    europe-west4). All processing of label/screenshot images for OCR/MSI
    must go through this client so that personal data stays inside the EU
    and is covered by the Google Cloud DPA, not the AI Studio terms."""
    global gemini_vertex_client
    if gemini_vertex_client is None:
        from google import genai
        # If JSON creds shipped via env, write them to /tmp once so the SDK
        # can pick them up via Application Default Credentials.
        creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        if creds_json and not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            creds_path = "/tmp/gcp_vertex_sa.json"
            try:
                with open(creds_path, "w") as f:
                    f.write(creds_json)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
            except Exception as e:
                logger.error(f"Failed to materialize GCP creds: {e}")
        try:
            gemini_vertex_client = genai.Client(
                vertexai=True,
                project=GCP_PROJECT_ID,
                location=GCP_VERTEX_LOCATION,
            )
        except Exception as e:
            logger.error(f"Vertex AI client init failed: {e}")
            sentry_sdk.capture_exception(e)
            return None
    return gemini_vertex_client

XPEDIT_CONTEXT = """Eres el community manager experto de Xpedit, la app española de optimización de rutas para repartidores.

=== SOBRE XPEDIT ===
Xpedit es una app móvil (Android) que optimiza rutas de reparto usando inteligencia artificial.
Desarrollada en España por TAES PACK S.L. Pensada para repartidores autónomos y pequeñas empresas de mensajería.

=== FUNCIONALIDADES REALES (solo menciona estas) ===
- Optimización de rutas con IA: calcula el orden óptimo de paradas para ahorrar tiempo y combustible
- Entrada por voz: añade direcciones hablando, sin teclear mientras conduces
- Notificaciones automáticas por email: notifica a cada cliente cuando estás de camino con ETA y enlace de seguimiento
- Seguimiento en tiempo real: los clientes ven al repartidor en un mapa en vivo
- Funciona offline: la ruta optimizada se mantiene sin conexión a internet
- Prueba de entrega con foto: haz foto en cada parada como comprobante
- Lugares recurrentes: guarda tus direcciones habituales para añadirlas rápido
- Navegación integrada: abre cada parada en Google Maps, Waze o Apple Maps con un toque
- Historial de rutas: consulta rutas anteriores completadas
- Panel de empresa: las empresas pueden asignar rutas a sus repartidores desde la web
- Sin límite de paradas: optimiza rutas con todas las paradas que necesites

=== PRECIOS ===
- Plan Gratis: funcional, disponible en Google Play
- Plan Pro: 4,99€/mes - todas las funcionalidades premium
- Plan Pro+: 9,99€/mes - para equipos y empresas
- Sin compromiso, cancela cuando quieras

=== COMPETENCIA (datos reales) ===
- OptimoRoute: desde $35/conductor/mes (~32€) = 7x más caro que Xpedit Pro
- Circuit: desde $20 a $200/mes = 4x a 40x más caro
- Routific: desde $49 a $93/mes = 10x a 19x más caro
- Todos los competidores están en inglés y pensados para grandes empresas
- Xpedit es la ÚNICA alternativa asequible en español para autónomos

=== PÚBLICO OBJETIVO ===
- Repartidores autónomos de paquetería (Amazon Flex, MRW, SEUR, GLS, etc.)
- Empresas pequeñas de mensajería y paquetería
- Repartidores de comida y ecommerce
- Técnicos de mantenimiento con rutas diarias
- Comerciales con visitas planificadas
- Cualquier profesional con múltiples paradas diarias
- Mercado: España y Latinoamérica

=== PRESENCIA ONLINE ===
- Web: xpedit.es
- X/Twitter: @Xpedit_es
- Descarga: Google Play Store (buscar "Xpedit")
- Email: info@xpedit.es

=== DATOS DEL SECTOR (para posts informativos) ===
- La última milla supone el 53% del coste total de envío
- En España se entregan más de 2 millones de paquetes al día
- El ecommerce en España creció un 25% en 2025
- Un repartidor medio recorre 150-200km diarios
- La optimización de rutas puede ahorrar un 20-30% en combustible
- El 85% de los repartidores autónomos no usa software de optimización
- Más de 100.000 repartidores autónomos operan en España

=== REGLAS DE CONTENIDO ===
- Escribe SIEMPRE en español de España
- Usa emojis con moderación (1-3 por post, nunca excesivo)
- Para X/Twitter: el texto completo CON hashtags debe tener MÁXIMO 230 caracteres (dejar margen para URLs t.co = 23 chars). NUNCA excedas 230 caracteres en total.
- Para LinkedIn: 500-1500 caracteres, tono más profesional, detallado y con párrafos
- Incluye siempre un CTA sutil y natural (descargar, probar gratis, visitar web)
- NO inventes funcionalidades que no existan
- NO uses hashtags genéricos vacíos, solo relevantes del sector logístico
- Los hashtags van INCLUIDOS dentro del texto (al final), NO por separado
- Varía el estilo: a veces pregunta, a veces dato, a veces historia, a veces consejo
- Haz que el contenido sea COMPARTIBLE y genere engagement
- IMPORTANTE para image_prompt: NUNCA incluyas texto, letras, números, porcentajes ni palabras en la imagen.
  Los modelos de IA generan texto con errores ortográficos. Describe solo elementos visuales (personas, objetos, escenas, colores).
  Ejemplo MALO: "infographic showing 53% with text DEL COSTE TOTAL"
  Ejemplo BUENO: "delivery driver smiling next to organized packages in a modern van, warm sunlight, urban setting"
"""


class GenerateTextRequest(BaseModel):
    topic: str  # feature, tip, comparativa, oferta, dato, detras, custom
    custom_topic: Optional[str] = None
    platforms: List[str] = ["twitter", "linkedin"]
    tone: str = "profesional"  # profesional, casual, inspirador, informativo


class GenerateImageRequest(BaseModel):
    prompt: str
    aspect_ratio: str = "1:1"  # 1:1, 16:9, 9:16
    style: str = "flat"  # flat, realistic, infographic, minimal


class GenerateCalendarRequest(BaseModel):
    days: int = 7
    posts_per_day: int = 1
    platforms: List[str] = ["twitter", "linkedin"]
    themes: List[str] = ["feature", "tip", "comparativa", "oferta"]


TOPIC_PROMPTS = {
    "feature": "Destaca una funcionalidad de Xpedit (optimización, voz, notificaciones email, offline, foto entrega, lugares recurrentes)",
    "tip": "Comparte un consejo práctico de reparto/logística que ayude a repartidores",
    "comparativa": "Compara Xpedit con la competencia (OptimoRoute $35, Circuit $20-200, Routific $49-93) destacando la ventaja de precio",
    "oferta": "Destaca que Xpedit es gratis en Google Play y el Pro es solo 4.99€/mes vs competencia de $35-93/mes",
    "dato": "Comparte un dato curioso o estadística sobre logística, reparto o última milla en España/Latam",
    "detras": "Muestra el lado humano: desarrollo de la app, equipo, mejoras recientes, feedback de usuarios",
    "custom": "",
}

TONE_INSTRUCTIONS = {
    "profesional": "Tono serio y profesional, enfocado en valor y datos.",
    "casual": "Tono cercano y relajado, como hablando con un colega repartidor.",
    "inspirador": "Tono motivacional, empoderando al repartidor autónomo.",
    "informativo": "Tono educativo, compartiendo conocimiento útil del sector.",
}

STYLE_PROMPTS = {
    "flat": "flat vector illustration, modern design, clean lines, blue and white color palette, delivery/logistics theme. NO text, NO letters, NO numbers, NO words in the image.",
    "realistic": "photorealistic, professional photography style, warm natural lighting. NO text, NO letters, NO numbers, NO words in the image.",
    "infographic": "clean visual design with icons and simple shapes, blue and white palette. NO text, NO letters, NO numbers, NO words in the image.",
    "minimal": "minimalist design, lots of white space, simple geometric shapes, blue accent color, elegant. NO text, NO letters, NO numbers, NO words in the image.",
}


@app.post("/social/generate-text", tags=["social"], summary="Generar texto con IA")
async def generate_social_text(req: GenerateTextRequest, user=Depends(require_admin)):
    """Genera texto para publicaciones de redes sociales usando Gemini AI. Solo admin."""
    client = get_gemini_client()
    if not client:
        raise HTTPException(status_code=500, detail="Gemini AI no configurado (falta GOOGLE_AI_API_KEY)")

    topic_desc = TOPIC_PROMPTS.get(req.topic, "")
    if req.topic == "custom" and req.custom_topic:
        topic_desc = req.custom_topic

    tone_desc = TONE_INSTRUCTIONS.get(req.tone, TONE_INSTRUCTIONS["profesional"])

    platforms_str = " y ".join(p for p in req.platforms)

    prompt = f"""{XPEDIT_CONTEXT}

Genera un post para las plataformas: {platforms_str}.
Tema: {topic_desc}
Tono: {tone_desc}

IMPORTANTE SOBRE X/TWITTER:
- El campo twitter_text DEBE incluir los hashtags al final
- El texto COMPLETO (mensaje + hashtags) NO puede superar 230 caracteres
- Cuenta cada carácter. Si te pasas, acorta el mensaje. NUNCA excedas 230.
- Las URLs cuentan como 23 caracteres (t.co)

Responde SOLO con un JSON válido (sin markdown, sin ```), con esta estructura exacta:
{{
  "twitter_text": "texto corto para X con hashtags incluidos (MÁXIMO 230 chars TOTAL)",
  "linkedin_text": "texto largo para LinkedIn (500-1500 chars, profesional, con párrafos y hashtags al final)",
  "hashtags": ["hashtag1", "hashtag2", "hashtag3"],
  "image_prompt": "descripción detallada en inglés para generar una imagen profesional, atractiva y moderna relacionada con el post. Describe composición, colores, estilo y elementos visuales."
}}"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-pro",
            contents=prompt,
        )
        text = response.text.strip()
        # Clean markdown code blocks if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()

        result = json.loads(text)
        return result
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail=f"Error parseando respuesta de Gemini: {text[:200]}")
    except Exception as e:
        logger.error(f"Error generating text: {e}")
        raise HTTPException(status_code=500, detail="Error generando texto. Inténtalo de nuevo.")


@app.post("/social/generate-image", tags=["social"], summary="Generar imagen con IA")
async def generate_social_image(req: GenerateImageRequest, user=Depends(require_admin)):
    """Genera una imagen con Gemini Imagen y la guarda en Supabase Storage. Solo admin."""
    client = get_gemini_client()
    if not client:
        raise HTTPException(status_code=500, detail="Gemini AI no configurado")

    from google.genai import types

    style_suffix = STYLE_PROMPTS.get(req.style, STYLE_PROMPTS["flat"])
    full_prompt = f"{req.prompt}. Style: {style_suffix}"

    try:
        response = client.models.generate_images(
            model="imagen-4.0-ultra-generate-001",
            prompt=full_prompt,
            config=types.GenerateImagesConfig(
                number_of_images=1,
                aspect_ratio=req.aspect_ratio,
            ),
        )

        if not response.generated_images:
            raise HTTPException(status_code=500, detail="No se generó ninguna imagen")

        image_bytes = response.generated_images[0].image.image_bytes
        filename = f"ai_{uuid_mod.uuid4().hex[:12]}.png"
        path = f"posts/{filename}"

        supabase.storage.from_("social-media").upload(
            path, image_bytes, {"content-type": "image/png"}
        )

        public_url = f"{os.getenv('SUPABASE_URL')}/storage/v1/object/public/social-media/{path}"
        return {"url": public_url, "prompt_used": full_prompt, "filename": filename}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating image: {e}")
        raise HTTPException(status_code=500, detail="Error generando imagen. Inténtalo de nuevo.")


@app.post("/social/generate-calendar", tags=["social"], summary="Generar calendario editorial")
async def generate_social_calendar(req: GenerateCalendarRequest, user=Depends(require_admin)):
    """Genera un calendario editorial de contenido para redes sociales usando Gemini AI. Solo admin."""
    client = get_gemini_client()
    if not client:
        raise HTTPException(status_code=500, detail="Gemini AI no configurado")

    total_posts = req.days * req.posts_per_day
    themes_str = ", ".join(req.themes)
    start_date = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")

    prompt = f"""{XPEDIT_CONTEXT}

Genera un calendario editorial de {total_posts} posts para redes sociales.
Período: {req.days} días empezando desde {start_date}, {req.posts_per_day} post(s) por día.
Plataformas: {", ".join(req.platforms)}
Mezcla estos temas: {themes_str}
Horarios sugeridos: 9:00-11:00 (mañana) o 17:00-19:00 (tarde), variando.

IMPORTANTE:
- Varía los temas, NO repitas el mismo tema dos días seguidos.
- Cada twitter_text DEBE incluir hashtags y NO superar 230 caracteres en TOTAL.
- Cada linkedin_text debe tener 500-1500 caracteres con hashtags al final.
- Haz que cada post sea único, creativo y genere engagement.
- Varía el formato: preguntas, datos, consejos, historias, comparativas.

Responde SOLO con un JSON válido (sin markdown, sin ```), con esta estructura exacta:
{{
  "posts": [
    {{
      "twitter_text": "texto para X con hashtags (MÁXIMO 230 chars TOTAL)",
      "linkedin_text": "texto para LinkedIn (500-1500 chars, profesional, con hashtags al final)",
      "suggested_date": "YYYY-MM-DD",
      "suggested_time": "HH:MM",
      "image_prompt": "descripción detallada en inglés para imagen profesional y atractiva",
      "theme": "nombre del tema usado",
      "hashtags": ["tag1", "tag2"]
    }}
  ]
}}"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-pro",
            contents=prompt,
        )
        text = response.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()

        result = json.loads(text)
        return result
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail=f"Error parseando calendario: {text[:200]}")
    except Exception as e:
        logger.error(f"Error generating calendar: {e}")
        raise HTTPException(status_code=500, detail="Error generando calendario. Inténtalo de nuevo.")


# === HEALTH CHECK & MONITORING ===


@app.get("/debug/sentry-test", tags=["health"], summary="Force a Sentry event (no auth)")
async def debug_sentry_test(level: str = "error"):
    """Diagnostic endpoint to verify Sentry ingestion.

    Sentry shows 0 events for python-fastapi in 30 days despite SENTRY_DSN
    being configured in Railway and explicit integrations registered. This
    endpoint forces a real capture and returns metadata so we can correlate
    via curl.
    """
    import time
    info = {
        "sentry_dsn_present": bool(SENTRY_DSN),
        "sentry_environment": os.getenv("SENTRY_ENVIRONMENT", "production"),
        "release": "xpedit-backend@1.1.4",
        "timestamp": time.time(),
        "client_active": sentry_sdk.Hub.current.client is not None,
    }
    if level == "exception":
        try:
            raise RuntimeError("debug/sentry-test: forced exception")
        except Exception as e:
            event_id = sentry_sdk.capture_exception(e)
            info["captured"] = "exception"
            info["event_id"] = str(event_id) if event_id else None
    else:
        event_id = sentry_sdk.capture_message(
            f"debug/sentry-test forced {level}",
            level=level if level in ("error", "warning", "info", "debug") else "error",
        )
        info["captured"] = level
        info["event_id"] = str(event_id) if event_id else None
    # Force flush so the SDK actually sends before the response returns.
    try:
        sentry_sdk.Hub.current.flush(timeout=5.0)
        info["flushed"] = True
    except Exception as e:
        info["flushed"] = False
        info["flush_error"] = str(e)
    return info


@app.get("/health/loop", tags=["health"], summary="Event-loop lag probe (cheap)")
async def health_loop():
    """Cheap probe to detect when the asyncio event loop is starved.

    Schedules a no-op coroutine and measures how long it actually takes to
    run. Under a healthy worker the lag is sub-millisecond. If sync code
    is blocking the loop (5 may 2026 incident: supabase-py sync inside
    async handlers), this endpoint shows lag in the hundreds of ms or
    seconds — much earlier than `/health` which also does DB queries.

    Curl one-liner for a tight monitor loop:
        watch -n 2 'curl -s -w "\\nlag=%{time_total}s\\n" .../health/loop'
    """
    import time
    t0 = time.perf_counter()
    await asyncio.sleep(0)  # yield once; healthy loop returns immediately
    yielded_ms = (time.perf_counter() - t0) * 1000.0
    return {
        "status": "ok",
        "yielded_ms": round(yielded_ms, 3),
        "warning": yielded_ms > 100,  # > 100ms means loop was busy when we yielded
    }


@app.get("/health", tags=["health"], summary="Health check", response_model=HealthCheckResponse)
async def health_check():
    """Verifica el estado de la base de datos, Sentry, scheduler y uptime del servidor. Devuelve 503 si hay problemas."""
    checks = {}
    healthy = True

    # Check Supabase DB
    try:
        result = supabase.table("drivers").select("id", count="exact").limit(1).execute()
        checks["database"] = {"status": "ok", "drivers_count": result.count}
    except Exception as e:
        checks["database"] = {"status": "error", "detail": str(e)[:100]}
        healthy = False

    # Check Sentry
    checks["sentry"] = {"status": "ok" if SENTRY_DSN else "not_configured"}

    # Check scheduler
    try:
        scheduler_running = social_scheduler.running if social_scheduler else False
        checks["scheduler"] = {"status": "ok" if scheduler_running else "stopped"}
    except Exception:
        checks["scheduler"] = {"status": "unknown"}

    # Uptime
    checks["uptime_seconds"] = int((datetime.now(timezone.utc) - _server_start_time).total_seconds())
    checks["version"] = "1.1.4"
    checks["environment"] = os.getenv("SENTRY_ENVIRONMENT", "production")

    # Stripe webhook status
    checks["stripe"] = {
        "last_webhook_ok": _last_stripe_webhook_ok.isoformat() if _last_stripe_webhook_ok else None,
        "last_webhook_error": _last_stripe_webhook_error.isoformat() if _last_stripe_webhook_error else None,
    }

    # RevenueCat webhook status
    checks["revenuecat"] = {
        "last_webhook_ok": _last_revenuecat_webhook_ok.isoformat() if _last_revenuecat_webhook_ok else None,
        "last_webhook_error": _last_revenuecat_webhook_error.isoformat() if _last_revenuecat_webhook_error else None,
    }

    # Solver availability
    from optimizer import HAS_PYVRP, HAS_VROOM
    checks["solvers"] = {"vroom": HAS_VROOM, "pyvrp": HAS_PYVRP, "ortools": True}

    # Min app version — set to >0 to force users on old builds to update from store
    # e.g. {"android": 27, "ios": 38} to require builds 27+ and 38+
    # iOS=45 (23 Apr 2026): bN45 contains the AIRGoogleMap nil-guard patch
    # (PR #5873). Older builds (bN39 etc.) crash with REACT-NATIVE-17 on
    # any re-render that touches MapView children — force the App Store update.
    # android=0 (15 may 2026, postmortem): el 13 may pusimos android=32 para
    # forzar update por el bug RLS soft-delete, pero NO mandamos el email de
    # aviso ni verificamos que la nueva build estuviera disponible en stores
    # para todos los países. Resultado: 250+ drivers Android (1.1.5 build 28 y
    # 1.1.6 build 29) churned 12-15 may, incluyendo 3 paying activos
    # (silvento555, transporteselninio, nachoalbigerdoval). Bajamos a 0 para
    # desbloquear de inmediato. El bug RLS soft-delete que motivó el force-update
    # NO es bloqueante (drivers pueden crear/completar/navegar, solo no borrar
    # paradas); reactivar este gate exige antes: email/push de aviso + vC32
    # confirmada en todas las stores + verificación de cohort impactada.
    checks["min_app_version"] = {"android": 0, "ios": 45}

    status_code = 200 if healthy else 503
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=status_code,
        content={"status": "healthy" if healthy else "degraded", "checks": checks}
    )


# === AUTOMATIC BACKUPS ===

async def backup_critical_tables():
    """Backup de tablas criticas a Supabase Storage (diario)."""
    try:
        # Sentry cron check-in
        if SENTRY_DSN:
            sentry_check_in(
                monitor_slug="daily-backup",
                status="in_progress",
            )

        tables = ["drivers", "users", "routes", "stops", "referrals", "promo_codes"]
        backup_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        backup_data = {}

        for table in tables:
            try:
                result = supabase.table(table).select("*").execute()
                backup_data[table] = {
                    "count": len(result.data),
                    "data": result.data,
                    "backed_up_at": datetime.now(timezone.utc).isoformat()
                }
            except Exception as e:
                logger.error(f"Backup error for table {table}: {e}")
                backup_data[table] = {"error": str(e)}

        # Guardar en Supabase Storage
        backup_json = json.dumps(backup_data, default=str, ensure_ascii=False)
        backup_uid = uuid_mod.uuid4().hex[:12]
        backup_path = f"backups/{backup_date}/backup_{backup_date}_{backup_uid}.json"

        try:
            supabase.storage.from_("backups").upload(
                backup_path,
                backup_json.encode("utf-8"),
                {"content-type": "application/json"}
            )
            logger.info(f"Backup completed: {backup_path} ({len(backup_json)} bytes, {len(tables)} tables)")
        except Exception as e:
            logger.error(f"Backup upload error: {e}")
            raise

        # Sentry cron OK
        if SENTRY_DSN:
            sentry_check_in(
                monitor_slug="daily-backup",
                status="ok",
            )

    except Exception as e:
        logger.error(f"Backup failed: {e}")
        if SENTRY_DSN:
            sentry_check_in(
                monitor_slug="daily-backup",
                status="error",
            )
            sentry_sdk.capture_exception(e)


async def run_retention_cleanup():
    """Ejecutar limpieza de datos antiguos (semanal)."""
    try:
        if SENTRY_DSN:
            sentry_check_in(
                monitor_slug="weekly-retention-cleanup",
                status="in_progress",
            )

        # Llamar a las funciones SQL de retencion creadas en la migracion
        import httpx as httpx_client
        headers = {
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
            "Content-Type": "application/json",
        }
        base_url = os.getenv("SUPABASE_URL")

        async with httpx_client.AsyncClient() as client:
            # Clean location_history > 90 days
            r1 = await client.post(
                f"{base_url}/rest/v1/rpc/clean_old_location_history",
                headers=headers,
                json={}
            )
            # Clean email_log > 180 days
            r2 = await client.post(
                f"{base_url}/rest/v1/rpc/clean_old_email_logs",
                headers=headers,
                json={}
            )
            logger.info(f"Retention cleanup: location_history={r1.status_code}, email_log={r2.status_code}")

        if SENTRY_DSN:
            sentry_check_in(
                monitor_slug="weekly-retention-cleanup",
                status="ok",
            )
    except Exception as e:
        logger.error(f"Retention cleanup failed: {e}")
        if SENTRY_DSN:
            sentry_check_in(
                monitor_slug="weekly-retention-cleanup",
                status="error",
            )
            sentry_sdk.capture_exception(e)


async def send_weekly_reengagement_push():
    """Weekly push to drivers registered in last 30 days with 0 routes. Runs Monday 10:00 UTC."""
    import asyncio

    if SENTRY_DSN:
        sentry_check_in(monitor_slug="weekly-reengagement-push", status="in_progress")
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        # Drivers registered in last 30 days with push tokens
        drivers_result = (
            supabase.table("drivers")
            .select("id, name, push_token")
            .not_.is_("push_token", "null")
            .gte("created_at", cutoff)
            .execute()
        )
        if not drivers_result.data:
            logger.info("Weekly re-engagement: no recent drivers with push tokens")
            return

        # Filter out those who already created routes (only check the candidate drivers)
        candidate_ids = [d["id"] for d in drivers_result.data]
        routes_result = supabase.table("routes").select("driver_id").in_("driver_id", candidate_ids).execute()
        drivers_with_routes = {r["driver_id"] for r in (routes_result.data or []) if r.get("driver_id")}
        inactive = [d for d in drivers_result.data if d["id"] not in drivers_with_routes]

        if not inactive:
            logger.info("Weekly re-engagement: all recent drivers already have routes")
            return

        results = await asyncio.gather(*[
            send_push_to_token(
                d["push_token"],
                "Tu primera ruta te espera",
                "Crea una ruta en 2 minutos y ahorra hasta un 30% en km. Abre Xpedit.",
            )
            for d in inactive
        ])
        sent = sum(1 for r in results if r)
        logger.info(f"Weekly re-engagement: {sent}/{len(inactive)} pushes sent")
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="weekly-reengagement-push", status="ok")
    except Exception as e:
        logger.error(f"Weekly re-engagement push error: {e}")
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="weekly-reengagement-push", status="error")
            sentry_sdk.capture_exception(e)


def _compute_trial_kpis(driver_id: str, signup_at: datetime) -> dict:
    """Sum routes optimized + stops completed + km traveled for a driver since
    `signup_at`. Used by the D-2 value-recap email so the user sees concrete
    numbers instead of an abstract upgrade ask.

    Bounded to the trial window (signup_at..now) — we don't want to leak
    pre-signup activity (which shouldn't exist anyway, but defensive).
    """
    routes_optimized, stops_completed, total_km = 0, 0, 0.0
    try:
        # Routes with optimized_hash IS NOT NULL = the user actually pressed
        # "Optimizar". A created-but-never-optimized route doesn't count.
        r = (
            supabase.table("routes")
            .select("id, total_distance_km, optimized_hash")
            .eq("driver_id", driver_id)
            .is_("deleted_at", "null")
            .gte("created_at", signup_at.isoformat())
            .execute()
        )
        if r.data:
            for row in r.data:
                if row.get("optimized_hash"):
                    routes_optimized += 1
                if row.get("total_distance_km"):
                    try:
                        total_km += float(row["total_distance_km"])
                    except (TypeError, ValueError):
                        pass

        s = (
            supabase.table("stops")
            .select("id", count="exact")
            .eq("driver_id", driver_id)
            .eq("status", "completed")
            .is_("deleted_at", "null")
            .gte("created_at", signup_at.isoformat())
            .execute()
        )
        stops_completed = s.count or 0
    except Exception as e:
        logger.warning(f"Trial KPI compute failed for {driver_id}: {e}")
    return {
        "routes_optimized": routes_optimized,
        "stops_completed": stops_completed,
        "total_km": round(total_km, 1),
    }


async def check_expiring_trials():
    """Daily conversion touchpoints: D-3 reminder + D-2 value recap + D-1 urgency + D-1 push.

    Runs once a day. Picks users whose trial expires in [3,4) days (D-3 cohort),
    [2,3) days (D-2 cohort) or [1,2) days (D-1 cohort) and sends the corresponding
    template. email_log dedup ensures each touch is sent at most once.

    D-1 also fires a push notification (best-effort, fire-and-forget) so users who
    don't open email still see the final urgency.
    """
    EXCLUDED_IDS = [
        "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b",  # admin
        "e481de53-bb8c-4b76-8b56-04a7d00f9c6f",  # test
        "d773b1aa-b077-4b44-a66b-1cb79cf1059b",  # Demo Xpedit
        "b903e5ad-6f82-4cdc-beb4-1a36cec113f4",  # Apple Reviewer
    ]
    if SENTRY_DSN:
        sentry_check_in(monitor_slug="check-expiring-trials", status="in_progress")
    try:
        now = datetime.now(timezone.utc)

        # Single query covering all three windows (24h..96h). We split into
        # D-3 / D-2 / D-1 in Python so we only hit Supabase once.
        window_start = (now + timedelta(days=1)).isoformat()
        window_end = (now + timedelta(days=4)).isoformat()

        result = (
            supabase.table("drivers")
            .select("id, email, name, promo_plan, promo_plan_expires_at, subscription_source, push_token, created_at")
            .in_("promo_plan", ["pro", "pro_plus"])
            .eq("is_ambassador", False)
            .is_("subscription_source", "null")  # already-paying users skip
            .not_.is_("email", "null")
            .not_.is_("promo_plan_expires_at", "null")
            .gte("promo_plan_expires_at", window_start)
            .lt("promo_plan_expires_at", window_end)
            .execute()
        )

        if not result.data:
            logger.info("Trial expiry check: no trials in D-3, D-2 or D-1 windows")
            if SENTRY_DSN:
                sentry_check_in(monitor_slug="check-expiring-trials", status="ok")
            return

        sent_d3, sent_d2, sent_d1, pushed_d1, skipped, failed = 0, 0, 0, 0, 0, 0
        for driver in result.data:
            if driver["id"] in EXCLUDED_IDS or not driver.get("email"):
                skipped += 1
                continue
            expires_at = datetime.fromisoformat(driver["promo_plan_expires_at"].replace("Z", "+00:00"))
            hours_left = (expires_at - now).total_seconds() / 3600

            # Bucket selection: most-urgent wins if windows overlap (shouldn't happen).
            if 24 <= hours_left < 48:
                template_subject = TRIAL_EXPIRING_D1_SUBJECT
                template_kind = "d1"
            elif 48 <= hours_left < 72:
                template_subject = TRIAL_VALUE_RECAP_SUBJECT
                template_kind = "d2"
            elif 72 <= hours_left < 96:
                template_subject = TRIAL_EXPIRING_D3_SUBJECT
                template_kind = "d3"
            else:
                # Outside our three cohorts (e.g. exactly 24h or 48h boundary) — skip silently.
                skipped += 1
                continue

            # email_log dedup: never send the same touch twice to the same address.
            existing = (
                supabase.table("email_log")
                .select("id")
                .eq("recipient_email", driver["email"])
                .eq("subject", template_subject)
                .limit(1)
                .execute()
            )
            if existing.data:
                skipped += 1
                continue

            if template_kind == "d3":
                days_left = max(0, int(hours_left // 24))
                email_result = send_trial_expiring_email(
                    driver["email"], driver.get("name", ""), driver["promo_plan"], days_left
                )
            elif template_kind == "d2":
                # D-2 needs concrete KPIs — query routes + stops since signup.
                signup_str = driver.get("created_at") or driver["promo_plan_expires_at"]
                try:
                    signup_at = datetime.fromisoformat(signup_str.replace("Z", "+00:00"))
                except Exception:
                    signup_at = now - timedelta(days=7)
                kpis = _compute_trial_kpis(driver["id"], signup_at)
                email_result = send_trial_value_recap_email(
                    driver["email"],
                    driver.get("name", ""),
                    kpis["routes_optimized"],
                    kpis["stops_completed"],
                    kpis["total_km"],
                )
            else:  # d1
                email_result = send_trial_last_day_email(driver["email"], driver.get("name", ""))

            if email_result.get("success"):
                if template_kind == "d3":
                    sent_d3 += 1
                elif template_kind == "d2":
                    sent_d2 += 1
                else:
                    sent_d1 += 1
                try:
                    supabase.table("email_log").insert({
                        "recipient_email": driver["email"],
                        "recipient_name": driver.get("name"),
                        "subject": template_subject,
                        "body": f"trial expiry reminder ({template_kind}, auto)",
                        "message_id": email_result.get("id"),
                    }).execute()
                except Exception:
                    # Log failure isn't fatal — the email already went out. Worst case the user
                    # gets one duplicate next run; that's acceptable.
                    pass

                # D-1: also fire a push so users who skip email still see urgency.
                # Best-effort, no email_log dedup since email_log already gates the email.
                if template_kind == "d1" and driver.get("push_token"):
                    try:
                        push_ok = await send_push_to_token(
                            driver["push_token"],
                            "Mañana acaba tu prueba Pro",
                            "Mantén Pro por 4,99€/mes y sigue sin límites de paradas.",
                            data={"type": "trial_d1", "deeplink": "xpedit://upgrade"},
                        )
                        if push_ok:
                            pushed_d1 += 1
                    except Exception as push_err:
                        logger.warning(f"Trial D-1 push failed for {driver['id']}: {push_err}")
            else:
                failed += 1
                logger.warning(f"Trial expiry email failed for {driver['id']}: {email_result.get('error')}")

        logger.info(
            f"Trial expiry: D-3={sent_d3} D-2={sent_d2} D-1={sent_d1} push_D-1={pushed_d1} "
            f"skipped={skipped} failed={failed} of {len(result.data)} candidates"
        )
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="check-expiring-trials", status="ok")
    except Exception as e:
        logger.error(f"Trial expiry check failed: {e}")
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="check-expiring-trials", status="error")
            sentry_sdk.capture_exception(e)


async def invite_active_free_to_trial():
    """Weekly job: surface 7-day Pro trial to free users who're already engaged.

    Audience criteria (ALL must hold):
      - promo_plan IS NULL (not in trial, not paying via promo)
      - subscription_source IS NULL (not paying via stripe/revenuecat)
      - email IS NOT NULL
      - is_ambassador = false
      - signed up >= 14 days ago (skips users still in their natural trial window)
      - >= 5 routes with optimized_hash IS NOT NULL in the last 30 days
      - never received this email before (email_log dedup by subject prefix)

    Sends email + push (best-effort). The CTA deep-links to xpedit://trial which
    the app routes to claimTrial() — device_id + IP abuse guards already in place
    inside that endpoint, so re-attempts by users who already burned their trial
    fall through gracefully.

    Runs Mondays 11:00 UTC (= 12:00 CET / 13:00 CEST). Weekly cadence keeps the
    audience fresh without burning attention.
    """
    EXCLUDED_IDS = [
        "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b",  # admin
        "e481de53-bb8c-4b76-8b56-04a7d00f9c6f",  # test
        "d773b1aa-b077-4b44-a66b-1cb79cf1059b",  # Demo Xpedit
        "b903e5ad-6f82-4cdc-beb4-1a36cec113f4",  # Apple Reviewer
    ]
    if SENTRY_DSN:
        sentry_check_in(monitor_slug="invite-active-free-to-trial", status="in_progress")
    try:
        now = datetime.now(timezone.utc)
        cutoff_signup = (now - timedelta(days=14)).isoformat()
        cutoff_routes = (now - timedelta(days=30)).isoformat()

        # Step 1: pull free + email-able + not-too-fresh drivers via paginated
        # helper (Supabase Cloud caps server-side at 1000 rows).
        free_drivers = []
        page_size = 1000
        offset = 0
        while True:
            page = (
                supabase.table("drivers")
                .select("id, email, name, push_token")
                .is_("promo_plan", "null")
                .is_("subscription_source", "null")
                .eq("is_ambassador", False)
                .not_.is_("email", "null")
                .lt("created_at", cutoff_signup)
                .range(offset, offset + page_size - 1)
                .execute()
            )
            if not page.data:
                break
            free_drivers.extend(page.data)
            if len(page.data) < page_size:
                break
            offset += page_size

        if not free_drivers:
            logger.info("Active-free invite: no eligible drivers")
            if SENTRY_DSN:
                sentry_check_in(monitor_slug="invite-active-free-to-trial", status="ok")
            return

        sent, pushed, skipped, failed = 0, 0, 0, 0
        for driver in free_drivers:
            if driver["id"] in EXCLUDED_IDS:
                skipped += 1
                continue

            # Step 2: count optimized routes in last 30d for this driver. Cheap
            # per-driver count vs single huge query — keeps RAM bounded for
            # 800+ drivers and lets us shortcut at >=5.
            try:
                rcount = (
                    supabase.table("routes")
                    .select("id", count="exact")
                    .eq("driver_id", driver["id"])
                    .not_.is_("optimized_hash", "null")
                    .is_("deleted_at", "null")
                    .gte("created_at", cutoff_routes)
                    .limit(1)
                    .execute()
                )
                routes_30d = rcount.count or 0
            except Exception as q_err:
                logger.warning(f"Active-free invite: route count failed for {driver['id']}: {q_err}")
                skipped += 1
                continue

            if routes_30d < 5:
                skipped += 1
                continue

            # Step 3: dedup against prior sends. We use a LIKE on the formatted
            # subject so any past invite (regardless of route count number)
            # blocks future invites for this user.
            existing = (
                supabase.table("email_log")
                .select("id")
                .eq("recipient_email", driver["email"])
                .like("subject", "Has optimizado%rutas. Prueba Pro%")
                .limit(1)
                .execute()
            )
            if existing.data:
                skipped += 1
                continue

            email_result = send_active_free_pro_invite_email(
                driver["email"], driver.get("name", ""), routes_30d
            )
            if not email_result.get("success"):
                failed += 1
                logger.warning(f"Active-free invite email failed for {driver['id']}: {email_result.get('error')}")
                continue

            sent += 1
            try:
                supabase.table("email_log").insert({
                    "recipient_email": driver["email"],
                    "recipient_name": driver.get("name"),
                    "subject": ACTIVE_FREE_PRO_INVITE_SUBJECT.format(n=routes_30d),
                    "body": f"active-free invite (auto, {routes_30d} routes/30d)",
                    "message_id": email_result.get("id"),
                }).execute()
            except Exception:
                pass  # Log failure isn't fatal; email already sent.

            # Best-effort push alongside the email — silent if no token.
            if driver.get("push_token"):
                try:
                    push_ok = await send_push_to_token(
                        driver["push_token"],
                        f"Has optimizado {routes_30d} rutas con Xpedit",
                        "Prueba Pro 7 días gratis. Sin tarjeta.",
                        data={"type": "active_free_invite", "deeplink": "xpedit://trial"},
                    )
                    if push_ok:
                        pushed += 1
                except Exception as push_err:
                    logger.warning(f"Active-free invite push failed for {driver['id']}: {push_err}")

        logger.info(
            f"Active-free invite: sent={sent} pushed={pushed} skipped={skipped} failed={failed} "
            f"of {len(free_drivers)} eligible candidates"
        )
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="invite-active-free-to-trial", status="ok")
    except Exception as e:
        logger.error(f"Active-free invite failed: {e}")
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="invite-active-free-to-trial", status="error")
            sentry_sdk.capture_exception(e)


async def degrade_expired_trials():
    """Daily check for expired trials. Downgrades to Free and sends notification. Runs 09:05 UTC."""
    EXCLUDED_IDS = [
        "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b",  # admin
        "e481de53-bb8c-4b76-8b56-04a7d00f9c6f",  # test
        "d773b1aa-b077-4b44-a66b-1cb79cf1059b",  # Demo Xpedit (demo@xpedit.es)
        "b903e5ad-6f82-4cdc-beb4-1a36cec113f4",  # Apple Reviewer (appledemo@xpedit.es)
    ]
    if SENTRY_DSN:
        sentry_check_in(monitor_slug="degrade-expired-trials", status="in_progress")
    try:
        now = datetime.now(timezone.utc).isoformat()

        result = (
            supabase.table("drivers")
            .select("id, email, name, promo_plan, promo_plan_expires_at")
            .in_("promo_plan", ["pro", "pro_plus"])
            .eq("is_ambassador", False)
            .not_.is_("promo_plan_expires_at", "null")
            .lt("promo_plan_expires_at", now)
            .execute()
        )

        logger.info(f"Trial degrade: found {len(result.data) if result.data else 0} expired trials to process")
        if not result.data:
            if SENTRY_DSN:
                sentry_check_in(monitor_slug="degrade-expired-trials", status="ok")
            return

        degraded, emailed = 0, 0
        for driver in result.data:
            if driver["id"] in EXCLUDED_IDS:
                continue
            old_plan = driver["promo_plan"]
            # Downgrade to free (NULL = free in the system).
            # NOTE: promo_plan_expires_at is intentionally PRESERVED.
            # The send_trial_feedback_emails cron filters by `expires_at IS NOT
            # NULL` to find users 7-8 days post-expiry. Clearing this field
            # silently broke that cron for months — 0 feedback emails sent,
            # zero qualitative data on why drivers don't convert.
            # Side-effects audited: /promo/status, MRR queries, AuthContext,
            # admin/stats — none rely on this field being null after expiry.
            supabase.table("drivers").update({
                "promo_plan": None,
            }).eq("id", driver["id"]).execute()
            degraded += 1

            # Send notification email
            if driver.get("email"):
                email_result = send_trial_expired_email(
                    driver["email"], driver.get("name", ""), old_plan
                )
                if email_result.get("success"):
                    emailed += 1

        logger.info(f"Trial degrade: {degraded} users downgraded to Free, {emailed} emails sent")
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="degrade-expired-trials", status="ok")
    except Exception as e:
        logger.error(f"Trial degrade failed: {e}")
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="degrade-expired-trials", status="error")
            sentry_sdk.capture_exception(e)


async def send_trial_feedback_emails():
    """Daily job: send feedback email 8 days after trial expiry to users who didn't subscribe.
    Only sends once per user (checks email_log for prior send)."""
    EXCLUDED_IDS = [
        "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b",  # admin
        "e481de53-bb8c-4b76-8b56-04a7d00f9c6f",  # test
        "d773b1aa-b077-4b44-a66b-1cb79cf1059b",  # Demo Xpedit
        "b903e5ad-6f82-4cdc-beb4-1a36cec113f4",  # Apple Reviewer
    ]
    FEEDBACK_SUBJECT = "Tu prueba de Xpedit ha terminado - nos encantaria saber por que"
    try:
        now = datetime.now(timezone.utc)
        # Window: trial expired between 7 and 8 days ago
        window_start = (now - timedelta(days=8)).isoformat()
        window_end = (now - timedelta(days=7)).isoformat()

        # Find users whose trial expired 7-8 days ago AND have no active subscription
        result = (
            supabase.table("drivers")
            .select("id, email, name, promo_plan, promo_plan_expires_at, subscription_source")
            .is_("subscription_source", "null")
            .is_("promo_plan", "null")
            .not_.is_("email", "null")
            .not_.is_("promo_plan_expires_at", "null")
            .gte("promo_plan_expires_at", window_start)
            .lte("promo_plan_expires_at", window_end)
            .execute()
        )

        if not result.data:
            logger.info("Trial feedback: no users in 7-8 day post-expiry window")
            return

        sent, skipped = 0, 0
        for driver in result.data:
            if driver["id"] in EXCLUDED_IDS:
                skipped += 1
                continue

            # Check if we already sent this feedback email (deduplicate via email_log)
            existing = (
                supabase.table("email_log")
                .select("id")
                .eq("recipient_email", driver["email"])
                .eq("subject", FEEDBACK_SUBJECT)
                .limit(1)
                .execute()
            )
            if existing.data:
                skipped += 1
                continue

            # Generate one HMAC token per reason so each click in the email is
            # bound to its (driver_id, reason) and validated server-side.
            tokens = {
                r: _trial_feedback_token(driver["id"], r)
                for r in ("price", "feature", "time", "competitor")
            }
            email_result = send_trial_feedback_email(
                driver["email"], driver.get("name", ""), driver["id"], tokens=tokens
            )
            if email_result.get("success"):
                sent += 1
                # Log in email_log so we don't send again
                try:
                    supabase.table("email_log").insert({
                        "recipient_email": driver["email"],
                        "recipient_name": driver.get("name"),
                        "subject": FEEDBACK_SUBJECT,
                        "body": "trial feedback email (auto)",
                        "message_id": email_result.get("id"),
                    }).execute()
                except Exception:
                    pass
            else:
                logger.warning(f"Trial feedback email failed for {driver['id']}: {email_result.get('error')}")

        logger.info(f"Trial feedback: {sent} emails sent, {skipped} skipped")
    except Exception as e:
        logger.error(f"Trial feedback job failed: {e}")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)


# Inline pydantic — local to this endpoint, no need to bubble up to top of file.
class _FeedbackBackfillRequest(BaseModel):
    """Run trial feedback emails over a custom window (one-shot recovery).
    Default daily cron is 7-8 days; this lets us sweep recent orphans (e.g. 14d)
    one time after fixing the degrade bug that left expires_at NULL for months.
    """
    window_days: int = Field(default=14, ge=7, le=30)
    require_stops: bool = True  # only email drivers who actually USED the trial
    dry_run: bool = True  # default to dry_run for safety


@app.post("/admin/cron/trial-feedback-backfill", tags=["admin", "cron"], summary="One-shot feedback email backfill (window-aware)")
async def admin_trial_feedback_backfill(body: _FeedbackBackfillRequest, user=Depends(require_admin)):
    """Sweep drivers whose trial expired in the last `window_days` (default 14)
    and send the feedback email to those who haven't received it yet.

    Default `dry_run=true` — caller must explicitly pass `dry_run=false` to send.
    `require_stops=true` filters to drivers with at least 1 stop in their history
    (signal of actual product use, not "installed and forgot").
    """
    EXCLUDED_IDS = [
        "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b",
        "e481de53-bb8c-4b76-8b56-04a7d00f9c6f",
        "d773b1aa-b077-4b44-a66b-1cb79cf1059b",
        "b903e5ad-6f82-4cdc-beb4-1a36cec113f4",
    ]
    FEEDBACK_SUBJECT = "Tu prueba de Xpedit ha terminado - nos encantaria saber por que"

    try:
        now = datetime.now(timezone.utc)
        window_start = (now - timedelta(days=body.window_days)).isoformat()
        window_end = now.isoformat()

        result = (
            supabase.table("drivers")
            .select("id, email, name")
            .is_("subscription_source", "null")
            .is_("promo_plan", "null")
            .not_.is_("email", "null")
            .not_.is_("promo_plan_expires_at", "null")
            .gte("promo_plan_expires_at", window_start)
            .lte("promo_plan_expires_at", window_end)
            .execute()
        )
        candidates = [d for d in (result.data or []) if d["id"] not in EXCLUDED_IDS]

        # Filter: only drivers with at least 1 stop. Excludes "installed but
        # never used" cohort whose feedback would just be silence.
        if body.require_stops and candidates:
            ids = [d["id"] for d in candidates]
            routes_with_stops = (
                supabase.table("routes")
                .select("driver_id, stops!inner(id)")
                .in_("driver_id", ids)
                .limit(1000)
                .execute()
            )
            active_drivers = {r["driver_id"] for r in (routes_with_stops.data or []) if r.get("stops")}
            candidates = [d for d in candidates if d["id"] in active_drivers]

        # Drop ones already emailed
        eligible = []
        for d in candidates:
            existing = (
                supabase.table("email_log").select("id")
                .eq("recipient_email", d["email"])
                .eq("subject", FEEDBACK_SUBJECT)
                .limit(1).execute()
            )
            if not existing.data:
                eligible.append(d)

        if body.dry_run:
            return {
                "dry_run": True,
                "window_days": body.window_days,
                "require_stops": body.require_stops,
                "candidates_in_window": len(result.data or []),
                "after_filters": len(eligible),
                "sample": [{"name": d.get("name"), "email": d["email"]} for d in eligible[:10]],
            }

        # Real send. 1s sleep between emails — avoids burst spike to Resend.
        sent, failed = 0, 0
        import asyncio
        for d in eligible:
            tokens = {r: _trial_feedback_token(d["id"], r) for r in ("price", "feature", "time", "competitor")}
            res = send_trial_feedback_email(d["email"], d.get("name", ""), d["id"], tokens=tokens)
            if res.get("success"):
                sent += 1
                try:
                    supabase.table("email_log").insert({
                        "recipient_email": d["email"],
                        "recipient_name": d.get("name"),
                        "subject": FEEDBACK_SUBJECT,
                        "body": "trial feedback email (one-shot backfill)",
                        "message_id": res.get("id"),
                    }).execute()
                except Exception:
                    pass
            else:
                failed += 1
                logger.warning(f"Backfill feedback email failed for {d['id']}: {res.get('error')}")
            await asyncio.sleep(1.0)

        return {"dry_run": False, "sent": sent, "failed": failed, "total": len(eligible)}
    except Exception as e:
        logger.error(f"Trial feedback backfill failed: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Backfill failed")


class _TestFeedbackEmailRequest(BaseModel):
    """Send the trial feedback email to a single address for visual QA.
    Bypasses dedup, EXCLUDED_IDS, and the post-trial filter — by design.
    Admin-only. Use the admin's own driver_id so the HMAC tokens validate.
    """
    to_email: str = Field(..., pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    name: str = "Admin"
    driver_id: str  # used to sign the per-reason HMAC tokens


@app.post("/admin/email/send-test-feedback", tags=["admin", "email"], summary="Send trial feedback email to a test address")
async def admin_send_test_feedback(body: _TestFeedbackEmailRequest, user=Depends(require_admin)):
    """Test-send the feedback email so the admin can review the rendered HTML
    and verify the 4 click buttons resolve correctly. Logs to email_log with a
    distinct subject suffix so it doesn't dedupe future real sends."""
    tokens = {r: _trial_feedback_token(body.driver_id, r) for r in ("price", "feature", "time", "competitor")}
    res = send_trial_feedback_email(body.to_email, body.name, body.driver_id, tokens=tokens)
    if res.get("success"):
        try:
            supabase.table("email_log").insert({
                "recipient_email": body.to_email,
                "recipient_name": body.name,
                "subject": "[TEST] Trial feedback email preview",
                "body": "test send via /admin/email/send-test-feedback",
                "message_id": res.get("id"),
            }).execute()
        except Exception:
            pass
    return res


# ============================================================
# Daily Health Digest — detects silent regressions automatically
# ============================================================

HEALTH_DIGEST_RECIPIENTS = [
    e.strip() for e in os.getenv("HEALTH_DIGEST_TO", "direccion@taespack.com").split(",") if e.strip()
]


def _status_from_value(value: int, baseline: float, min_expected: float = 1.0) -> str:
    """Classify a metric based on value vs baseline.

    - "bad": value is 0 AND baseline was meaningfully above min_expected (silent regression).
    - "warn": value is well below baseline (< 50%) but not zero.
    - "ok": within normal range.
    """
    if baseline < min_expected:
        # Not enough baseline to judge — treat as OK
        return "ok"
    if value == 0:
        return "bad"
    if value < baseline * 0.5:
        return "warn"
    return "ok"


def compute_daily_health_digest() -> dict:
    """Collect the key daily metrics. Returns a dict ready for the email template.

    Each metric: {label, value, baseline, status, note}
    status: "ok" | "warn" | "bad"
    """
    madrid = ZoneInfo("Europe/Madrid")
    now_local = datetime.now(madrid)
    date_str = now_local.strftime("%d %b %Y")

    now_utc = datetime.now(timezone.utc)
    last_24h = (now_utc - timedelta(hours=24)).isoformat()
    last_7d = (now_utc - timedelta(days=7)).isoformat()

    # Track whether any count() call failed so we can flag the digest as
    # "partially unreliable" instead of silently sending 0s. See incident
    # 2026-05-09: the digest mailed all-zeros while DB had real data.
    digest_errors: list[str] = []

    def count(table: str, filters: list, timestamp_col: str, since: str) -> Optional[int]:
        """Returns the row count or None on failure.

        We DO NOT swallow errors as 0 because that's indistinguishable from
        a real zero (e.g. a quiet day) — the email would tell the user the
        whole platform is dead when in fact only the metrics fetch failed.
        Returning None lets the caller render "?" / "ERROR" instead, and
        we capture the failure to Sentry so we know the digest is unreliable.
        """
        q = supabase.table(table).select("id", count="exact")
        for f in filters:
            col, op, val = f
            q = getattr(q, op)(col, val) if val is not None else getattr(q, op)(col)
        q = q.gte(timestamp_col, since)
        try:
            return q.execute().count or 0
        except Exception as e:
            err_msg = f"Health digest count failed [{table}]: {type(e).__name__}: {e}"
            logger.warning(err_msg)
            digest_errors.append(f"{table}/{timestamp_col}")
            if SENTRY_DSN:
                sentry_sdk.capture_message(err_msg, level="warning")
            return None

    def baseline_avg(count_7d: Optional[int], count_24h: Optional[int]) -> Optional[float]:
        # If either side failed, baseline is meaningless.
        if count_7d is None or count_24h is None:
            return None
        # 6 previous days average (exclude today)
        return max(0.0, (count_7d - count_24h) / 6.0)

    def _metric_value_or_error(value: Optional[int]) -> tuple:
        """For metrics where None means lookup failure, returns (display_value,
        status). When value is None, status='error' so the email shows it
        explicitly as broken instead of green/zero."""
        if value is None:
            return ("?", "error")
        return (value, None)

    metrics: list[dict] = []

    def _build_metric(label: str, value: Optional[int], baseline: Optional[float], min_expected: int) -> dict:
        """Wraps the common pattern: if either value or baseline is None,
        the metric is rendered as 'error' so the email never shows a fake 0."""
        if value is None:
            return {"label": label, "value": "?", "baseline": "—", "status": "error",
                    "note": "Lookup falló — ver Sentry para diagnóstico."}
        bl = round(baseline, 1) if baseline is not None else None
        return {"label": label, "value": value, "baseline": bl,
                "status": _status_from_value(value, baseline or 0.0, min_expected=min_expected)}

    # 1. Signups (new drivers) 24h vs 6d baseline
    signups_24h = count("drivers", [], "created_at", last_24h)
    signups_7d = count("drivers", [], "created_at", last_7d)
    metrics.append(_build_metric("Nuevos registros (24h)", signups_24h,
                                 baseline_avg(signups_7d, signups_24h), min_expected=2))

    # 2. Routes created 24h
    routes_24h = count("routes", [], "created_at", last_24h)
    routes_7d = count("routes", [], "created_at", last_7d)
    metrics.append(_build_metric("Rutas creadas (24h)", routes_24h,
                                 baseline_avg(routes_7d, routes_24h), min_expected=3))

    # 3. Stops created 24h
    stops_24h = count("stops", [], "created_at", last_24h)
    stops_7d = count("stops", [], "created_at", last_7d)
    metrics.append(_build_metric("Paradas creadas (24h)", stops_24h,
                                 baseline_avg(stops_7d, stops_24h), min_expected=20))

    # 3b. Stop processing rate (completed+failed / total created 24h).
    # Guards against the April 2026 silent sync bug where 93% of stops stayed
    # "pending" in DB. A healthy day is >= 50%. Below 30% is likely broken sync.
    stops_processed_24h: Optional[int] = None
    try:
        processed_res = (
            supabase.table("stops")
            .select("id", count="exact")
            .in_("status", ["completed", "failed"])
            .gte("created_at", last_24h)
            .execute()
        )
        stops_processed_24h = processed_res.count or 0
    except Exception as e:
        logger.warning(f"Health digest stops processed count failed: {e}")
        digest_errors.append("stops/processed")
        if SENTRY_DSN:
            sentry_sdk.capture_message(
                f"Health digest stops processed count failed: {e}", level="warning",
            )
    if stops_24h is not None and stops_processed_24h is not None and stops_24h > 0:
        processing_rate = 100.0 * stops_processed_24h / stops_24h
        if processing_rate < 30:
            rate_status = "bad"
            rate_note = "ALERTA: <30% procesadas — posible bug de sync (abr 2026 revisitado)."
        elif processing_rate < 50:
            rate_status = "warn"
            rate_note = "Tasa de procesamiento por debajo del objetivo (50%)."
        else:
            rate_status = "ok"
            rate_note = ""
        metrics.append({
            "label": "Paradas procesadas (24h)",
            "value": f"{stops_processed_24h}/{stops_24h} ({processing_rate:.0f}%)",
            "baseline": "objetivo >=50%",
            "status": rate_status,
            "note": rate_note,
        })

    # 4. Active drivers 24h (drivers who created at least one route)
    active_24h: Optional[int] = None
    try:
        active_rows = (
            supabase.table("routes").select("driver_id").gte("created_at", last_24h).execute()
        )
        active_24h = len({r["driver_id"] for r in (active_rows.data or []) if r.get("driver_id")})
    except Exception as e:
        logger.warning(f"Health digest active_24h failed: {e}")
        digest_errors.append("routes/active_drivers")
        if SENTRY_DSN:
            sentry_sdk.capture_message(f"Health digest active_24h failed: {e}", level="warning")
    if active_24h is None:
        metrics.append({
            "label": "Drivers activos (24h)",
            "value": "?", "baseline": "—", "status": "error",
            "note": "Lookup falló — ver Sentry para diagnóstico.",
        })
    else:
        metrics.append({
            "label": "Drivers activos (24h)",
            "value": active_24h,
            "baseline": None,
            "status": "ok" if active_24h > 0 else "warn",
        })

    # 5-6. Google Sign-In — Android vs iOS (7d) via public.google_signin_stats RPC
    # (auth schema is not exposed to PostgREST, so we use a SECURITY DEFINER function).
    android_google_count = 0
    ios_google_count = 0
    try:
        stats_res = supabase.rpc("google_signin_stats", {"since_ts": last_7d}).execute()
        if stats_res.data:
            row = stats_res.data[0] if isinstance(stats_res.data, list) else stats_res.data
            android_google_count = row.get("android_count", 0) or 0
            ios_google_count = row.get("ios_count", 0) or 0
    except Exception as e:
        logger.warning(f"Health digest google_signin_stats RPC failed: {e}")

    android_status = "bad" if android_google_count == 0 else "ok"
    android_note = (
        "0 logins Google Android en 7 dias. Revisar Google Cloud OAuth + webClientId."
        if android_google_count == 0 else ""
    )
    metrics.append({
        "label": "Google Sign-In Android (7d)",
        "value": android_google_count,
        "baseline": None,
        "status": android_status,
        "note": android_note,
    })
    metrics.append({
        "label": "Google Sign-In iOS (7d)",
        "value": ios_google_count,
        "baseline": None,
        "status": "ok" if ios_google_count > 0 else "warn",
    })

    # 7. Trial claims 24h
    trial_claims_24h = count("trial_claims", [], "claimed_at", last_24h)
    trial_claims_7d = count("trial_claims", [], "claimed_at", last_7d)
    metrics.append(_build_metric("Trials nuevos (24h)", trial_claims_24h,
                                 baseline_avg(trial_claims_7d, trial_claims_24h), min_expected=1))

    # 8. Paid users — use daily_metrics_snapshot for 7d delta (drivers table has no updated_at)
    paid_today: Optional[int] = None
    paid_7d_ago: Optional[int] = None
    try:
        today_snap = (
            supabase.table("daily_metrics_snapshot")
            .select("paid_users")
            .order("snapshot_date", desc=True)
            .limit(1)
            .execute()
        )
        if today_snap.data:
            paid_today = today_snap.data[0].get("paid_users", 0) or 0

        cutoff_date = (now_utc - timedelta(days=7)).date().isoformat()
        old_snap = (
            supabase.table("daily_metrics_snapshot")
            .select("paid_users")
            .lte("snapshot_date", cutoff_date)
            .order("snapshot_date", desc=True)
            .limit(1)
            .execute()
        )
        if old_snap.data:
            paid_7d_ago = old_snap.data[0].get("paid_users", 0) or 0
    except Exception as e:
        logger.warning(f"Health digest paid users snapshot failed: {e}")
        digest_errors.append("daily_metrics_snapshot")
        if SENTRY_DSN:
            sentry_sdk.capture_message(
                f"Health digest paid users snapshot failed: {e}", level="warning",
            )

    if paid_today is None or paid_7d_ago is None:
        metrics.append({
            "label": "Paid users totales",
            "value": "?", "baseline": "—", "status": "error",
            "note": "Snapshot diario no disponible — ver Sentry.",
        })
    else:
        conversions_delta = paid_today - paid_7d_ago
        if conversions_delta > 0:
            conv_status = "ok"
            conv_note = ""
        elif conversions_delta == 0:
            conv_status = "warn"
            conv_note = "Sin nuevas conversiones esta semana."
        else:
            conv_status = "bad"
            conv_note = f"Han cancelado {abs(conversions_delta)} suscripcion(es) esta semana."

        metrics.append({
            "label": "Paid users totales",
            "value": f"{paid_today} (delta 7d: {conversions_delta:+d})",
            "baseline": None,
            "status": conv_status,
            "note": conv_note,
        })

    # 9. Backend self-health
    try:
        uptime_hours = int((now_utc - _server_start_time).total_seconds() // 3600)
        metrics.append({
            "label": "Backend uptime",
            "value": f"{uptime_hours}h",
            "baseline": None,
            "status": "ok",
        })
    except Exception:
        pass

    # Surface digest-level health: if any sub-query failed, mail subject prefix
    # becomes "DIGEST-ERROR" so it's visually distinct from real ALERT/WARN/OK.
    if digest_errors:
        if SENTRY_DSN:
            sentry_sdk.capture_message(
                f"Daily Health digest had {len(digest_errors)} failed lookups: "
                f"{', '.join(digest_errors)}. Some metrics shown as '?' in email.",
                level="warning",
            )

    return {
        "date": date_str,
        "metrics": metrics,
        "digest_errors": digest_errors,
    }


async def send_daily_health_digest_job():
    """APScheduler job wrapper — runs daily. Sends to HEALTH_DIGEST_RECIPIENTS."""
    try:
        digest = compute_daily_health_digest()
        for recipient in HEALTH_DIGEST_RECIPIENTS:
            result = send_daily_health_digest_email(recipient, digest)
            if not result.get("success"):
                logger.warning(f"Daily health digest email failed for {recipient}: {result.get('error')}")
            else:
                logger.info(f"Daily health digest sent to {recipient}")

        # Escalate to Sentry if any metric is in "bad" state
        bad_metrics = [m for m in digest.get("metrics", []) if m.get("status") == "bad"]
        if bad_metrics and SENTRY_DSN:
            labels = ", ".join(m["label"] for m in bad_metrics)
            sentry_sdk.capture_message(
                f"Daily health digest: {len(bad_metrics)} critical metric(s) failing — {labels}",
                level="error",
            )
    except Exception as e:
        logger.error(f"Daily health digest job failed: {e}")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)


async def reactivation_push_followup_job():
    """Hourly: drivers who got a reactivation push >=5h ago and have not opened the app since
    receive a follow-up email. If they did open the app, mark the push as 'opened' instead.

    Idempotent via reactivation_log uniqueness on (driver_id, channel, campaign).
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
        push_logs = (
            supabase.table("reactivation_log")
            .select("id, driver_id, campaign, sent_at, session_at_send")
            .eq("channel", "push").eq("status", "sent")
            .lt("sent_at", cutoff)
            .execute()
        ).data or []
        if not push_logs:
            return

        driver_ids = list({p["driver_id"] for p in push_logs})
        existing_emails = (
            supabase.table("reactivation_log")
            .select("driver_id, campaign")
            .eq("channel", "email").in_("driver_id", driver_ids)
            .execute()
        ).data or []
        already_emailed = {(e["driver_id"], e["campaign"]) for e in existing_emails}
        candidates = [p for p in push_logs if (p["driver_id"], p["campaign"]) not in already_emailed]
        if not candidates:
            return

        drivers = (
            supabase.table("drivers")
            .select("id, email, name, session_started_at")
            .in_("id", list({c["driver_id"] for c in candidates}))
            .execute()
        ).data or []
        driver_map = {d["id"]: d for d in drivers}

        sent_email = 0
        marked_opened = 0
        failed = 0
        for c in candidates:
            d = driver_map.get(c["driver_id"])
            if not d or not d.get("email"):
                continue
            current_session = d.get("session_started_at")
            log_session = c.get("session_at_send")
            if current_session and current_session != log_session:
                # User opened the app after the push — mark and skip email.
                supabase.table("reactivation_log").update({
                    "status": "opened",
                    "opened_at": datetime.now(timezone.utc).isoformat(),
                }).eq("id", c["id"]).execute()
                marked_opened += 1
                continue

            res = send_reactivation_persistence_email(d["email"], d.get("name") or "")
            row = {
                "driver_id": c["driver_id"],
                "channel": "email",
                "campaign": c["campaign"],
                "status": "sent" if res.get("success") else "failed",
                "session_at_send": current_session,
                "resend_id": res.get("id"),
                "error": None if res.get("success") else res.get("error"),
            }
            try:
                supabase.table("reactivation_log").insert(row).execute()
            except Exception as ins_err:
                logger.warning(f"reactivation_log followup insert failed: {ins_err}")

            if res.get("success"):
                sent_email += 1
            else:
                failed += 1

        logger.info(
            f"reactivation_push_followup: candidates={len(candidates)} email_sent={sent_email} opened={marked_opened} failed={failed}"
        )
    except Exception as e:
        logger.error(f"reactivation_push_followup error: {type(e).__name__}: {e}")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)


@app.get("/admin/health-digest", tags=["admin", "health"], summary="Disparar digest de salud manualmente")
async def admin_health_digest(user=Depends(require_admin)):
    """Calcula y envía el digest diario bajo demanda (útil para probar)."""
    digest = compute_daily_health_digest()
    sent_to = []
    for recipient in HEALTH_DIGEST_RECIPIENTS:
        result = send_daily_health_digest_email(recipient, digest)
        if result.get("success"):
            sent_to.append(recipient)
    return {"success": True, "digest": digest, "sent_to": sent_to}


@app.get("/admin/trials-expiring", tags=["admin"], summary="Trials expirando proximos N dias")
async def admin_trials_expiring(days: int = 7, user=Depends(require_admin)):
    """Devuelve drivers cuyo trial Pro expira en los proximos N dias (1-30).

    Incluye email, telefono, stops y rutas ultimos 7 dias para priorizar
    a quien contactar. Ordenados por fecha de expiracion ascendente.
    Excluye drivers ambassador y los ya suscritos via RevenueCat/Stripe.
    """
    days = max(1, min(30, days))
    now = datetime.now(timezone.utc)
    window_start = now.isoformat()
    window_end = (now + timedelta(days=days)).isoformat()
    last_7d = (now - timedelta(days=7)).isoformat()

    try:
        drivers_res = (
            supabase.table("drivers")
            .select("id, email, name, phone, promo_plan, promo_plan_expires_at, subscription_source, created_at")
            .in_("promo_plan", ["pro", "pro_plus"])
            .is_("subscription_source", "null")
            .not_.is_("email", "null")
            .not_.is_("promo_plan_expires_at", "null")
            .gte("promo_plan_expires_at", window_start)
            .lte("promo_plan_expires_at", window_end)
            .order("promo_plan_expires_at", desc=False)
            .execute()
        )
        drivers = drivers_res.data or []

        if not drivers:
            return {"success": True, "count": 0, "drivers": []}

        driver_ids = [d["id"] for d in drivers]

        # Aggregate routes + stops per driver last 7 days (one query each, then bucket in Python)
        routes_res = (
            supabase.table("routes")
            .select("id, driver_id")
            .in_("driver_id", driver_ids)
            .gte("created_at", last_7d)
            .execute()
        )
        routes_by_driver: dict = {}
        route_ids_by_driver: dict = {}
        for r in (routes_res.data or []):
            did = r.get("driver_id")
            if not did:
                continue
            routes_by_driver[did] = routes_by_driver.get(did, 0) + 1
            route_ids_by_driver.setdefault(did, []).append(r["id"])

        # Count stops per driver by joining via route_id
        all_route_ids = [rid for ids in route_ids_by_driver.values() for rid in ids]
        stops_by_driver: dict = {}
        if all_route_ids:
            stops_res = (
                supabase.table("stops")
                .select("id, route_id")
                .in_("route_id", all_route_ids)
                .gte("created_at", last_7d)
                .execute()
            )
            route_to_driver = {rid: did for did, ids in route_ids_by_driver.items() for rid in ids}
            for s in (stops_res.data or []):
                did = route_to_driver.get(s.get("route_id"))
                if did:
                    stops_by_driver[did] = stops_by_driver.get(did, 0) + 1

        enriched = []
        for d in drivers:
            try:
                expires_at = datetime.fromisoformat(d["promo_plan_expires_at"].replace("Z", "+00:00"))
                hours_left = (expires_at - now).total_seconds() / 3600.0
                days_left = hours_left / 24.0
            except Exception:
                days_left = None
            enriched.append({
                "driver_id": d["id"],
                "email": d.get("email"),
                "name": d.get("name"),
                "phone": d.get("phone"),
                "promo_plan": d.get("promo_plan"),
                "promo_plan_expires_at": d.get("promo_plan_expires_at"),
                "days_left": round(days_left, 2) if days_left is not None else None,
                "created_at": d.get("created_at"),
                "stops_7d": stops_by_driver.get(d["id"], 0),
                "routes_7d": routes_by_driver.get(d["id"], 0),
            })

        return {"success": True, "count": len(enriched), "days": days, "drivers": enriched}
    except Exception as e:
        logger.error(f"/admin/trials-expiring failed: {e}")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error fetching expiring trials")


# Track server start time for uptime
_server_start_time = datetime.now(timezone.utc)


@app.on_event("startup")
async def start_monitoring_jobs():
    """Iniciar jobs de backup y limpieza."""
    if not SHOULD_RUN_SCHEDULER:
        logger.info("Monitoring jobs skipped on this process (RUN_SCHEDULER=false)")
        return
    # Re-engagement push semanal (lunes 10:00 UTC)
    social_scheduler.add_job(
        send_weekly_reengagement_push,
        "cron",
        day_of_week="mon",
        hour=10,
        minute=0,
        id="weekly_reengagement_push",
        replace_existing=True,
    )
    # Backup diario a las 3:00 AM UTC
    social_scheduler.add_job(
        backup_critical_tables,
        "cron",
        hour=3,
        minute=0,
        id="daily_backup",
        replace_existing=True,
    )
    # Limpieza semanal (domingos 4:00 AM UTC)
    social_scheduler.add_job(
        run_retention_cleanup,
        "cron",
        day_of_week="sun",
        hour=4,
        minute=0,
        id="weekly_retention",
        replace_existing=True,
    )
    # Health check cada 5 minutos (reporta a Sentry Crons)
    social_scheduler.add_job(
        periodic_health_check,
        "interval",
        minutes=5,
        id="health_check",
        replace_existing=True,
    )
    # Website health monitor cada 15 minutos
    social_scheduler.add_job(
        monitor_website_health,
        "interval",
        minutes=15,
        id="website_health",
        replace_existing=True,
    )
    # Trial expiry warning emails (daily 09:00 UTC)
    social_scheduler.add_job(
        check_expiring_trials,
        "cron",
        hour=9,
        minute=0,
        id="trial_expiry_check",
        replace_existing=True,
    )
    # Degrade expired trials to Free (every hour at :05)
    social_scheduler.add_job(
        degrade_expired_trials,
        "cron",
        minute=5,
        id="trial_degrade",
        replace_existing=True,
    )
    # Trial feedback emails (daily 10:00 UTC — 1h after expiry emails)
    social_scheduler.add_job(
        send_trial_feedback_emails,
        "cron",
        hour=10,
        minute=30,
        id="trial_feedback_emails",
        replace_existing=True,
    )
    # Daily health digest (08:00 Europe/Madrid — detect silent regressions).
    # coalesce=True + max_instances=1 + misfire_grace_time=300: si tras un
    # deploy el job se registra varias veces o se solapan disparos (incidente
    # 22 may 2026: llegaron 2 emails 06:00:05 y 06:00:07 con datos opuestos
    # porque el primero leyó snapshots aún no calculados), APScheduler
    # descarta los disparos extra automáticamente.
    social_scheduler.add_job(
        send_daily_health_digest_job,
        "cron",
        hour=8,
        minute=0,
        timezone=ZoneInfo("Europe/Madrid"),
        id="daily_health_digest",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=300,
    )
    # Reactivation push 5h follow-up (hourly at :15 → email if user did not open the app after push)
    social_scheduler.add_job(
        reactivation_push_followup_job,
        "cron",
        minute=15,
        id="reactivation_push_followup",
        replace_existing=True,
    )
    # Weekly invite for active free users (Mondays 11:00 UTC = 12:00 CET / 13:00 CEST)
    social_scheduler.add_job(
        invite_active_free_to_trial,
        "cron",
        day_of_week="mon",
        hour=11,
        minute=0,
        id="invite_active_free_to_trial",
        replace_existing=True,
    )
    logger.info("Monitoring jobs scheduled: health (5min), website (15min), daily backup (3:00 UTC), weekly retention (Sun 4:00 UTC), re-engagement push (Mon 10:00 UTC), trial expiry (9:00 UTC), trial degrade (9:05 UTC), trial feedback (10:30 UTC), daily health digest (08:00 CET), reactivation followup (hourly :15), active-free invite (Mon 11:00 UTC)")


async def periodic_health_check():
    """Health check periodico que reporta a Sentry Crons. Incluye verificación de Google Places."""
    global _places_api_healthy, _places_api_last_alert, _places_api_last_check
    try:
        result = supabase.table("drivers").select("id", count="exact").limit(1).execute()
        db_ok = result.count is not None
        scheduler_ok = social_scheduler.running if social_scheduler else False

        # Check Google Places API — only once per hour to save API quota (was 288 calls/day)
        places_ok = _places_api_healthy  # Use cached value by default
        if GOOGLE_API_KEY and (not _places_api_last_check or (datetime.now(timezone.utc) - _places_api_last_check).total_seconds() > 3600):
            places_ok = False
            for _hc_attempt in range(2):
                try:
                    resp = await google_maps_client().get(
                        "https://maps.googleapis.com/maps/api/place/autocomplete/json",
                        params={"input": "test", "key": GOOGLE_API_KEY, "language": "es"},
                        timeout=8.0,
                    )
                    places_data = resp.json()
                    places_ok = places_data.get("status") in ("OK", "ZERO_RESULTS")
                except Exception:
                    places_ok = False
                if places_ok:
                    break
            _places_api_last_check = datetime.now(timezone.utc)

            if not places_ok and _places_api_healthy:
                _places_api_healthy = False
                now = datetime.now(timezone.utc)
                if not _places_api_last_alert or (now - _places_api_last_alert).total_seconds() > 3600:
                    _places_api_last_alert = now
                    await alert_admin(
                        "ALERTA: Google Places API caída",
                        f"Health check detectó que Google Places no funciona.\nTimestamp: {now.isoformat()}Z",
                    )
            elif places_ok and not _places_api_healthy:
                _places_api_healthy = True
                logger.info("Google Places API recovered (detected by health check)")

        # Check Resend email activity (alert if 0 emails in 24h and there are active routes)
        try:
            yesterday = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            email_count = supabase.table("email_log").select("id", count="exact").gte("created_at", yesterday).limit(1).execute()
            emails_24h = email_count.count or 0
            if emails_24h == 0:
                active_routes = supabase.table("routes").select("id", count="exact").eq("status", "in_progress").limit(1).execute()
                if (active_routes.count or 0) > 0:
                    logger.warning("Resend alert: 0 emails in 24h with active routes")
                    sentry_sdk.capture_message("Resend may be down: 0 emails sent in 24h with active routes", level="warning")
        except Exception:
            pass  # Don't fail health check for email monitoring

        all_ok = db_ok and scheduler_ok and (places_ok or not GOOGLE_API_KEY)
        if all_ok:
            if SENTRY_DSN:
                sentry_check_in(monitor_slug="backend-health-check", status="ok")
        else:
            logger.warning(f"Health check degraded: db={db_ok}, scheduler={scheduler_ok}, places={places_ok}")
            if SENTRY_DSN:
                sentry_check_in(monitor_slug="backend-health-check", status="error")
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="backend-health-check", status="error")
            sentry_sdk.capture_exception(e)


# Website health monitor - cooldown timestamp
_last_website_alert: Optional[datetime] = None
WEBSITE_HEALTH_URL = "https://www.xpedit.es/api/health"
ALERT_COOLDOWN_HOURS = 2
ALERT_EMAIL = "direccion@taespack.com"
ADMIN_DRIVER_ID = "8c0aa30a-6de1-43e8-8a6c-71c1c8a6670b"


async def alert_admin(title: str, body: str):
    """Send critical alert via email + push to admin. Fire and forget."""
    try:
        send_alert_email(ALERT_EMAIL, title, body)
    except Exception:
        pass
    try:
        admin = supabase.table("drivers").select("push_token").eq("id", ADMIN_DRIVER_ID).single().execute()
        if admin.data and admin.data.get("push_token"):
            await send_push_to_token(admin.data["push_token"], title, body[:200])
    except Exception:
        pass


async def monitor_website_health():
    """Pinga /api/health de la web cada 15 min. Si falla, envia email alerta."""
    global _last_website_alert
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(WEBSITE_HEALTH_URL)

        if resp.status_code == 200:
            if SENTRY_DSN:
                sentry_check_in(monitor_slug="website-health-monitor", status="ok")
            return

        # Degraded or error
        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text[:500]}

        logger.warning(f"Website health degraded: status={resp.status_code} body={body}")

        if SENTRY_DSN:
            sentry_check_in(monitor_slug="website-health-monitor", status="error")

        # Check cooldown before sending alert email
        now = datetime.now(timezone.utc)
        if _last_website_alert and (now - _last_website_alert).total_seconds() < ALERT_COOLDOWN_HOURS * 3600:
            logger.info("Website alert skipped (cooldown active)")
            return

        details = f"Status: {resp.status_code}\nURL: {WEBSITE_HEALTH_URL}\nTimestamp: {now.isoformat()}Z\n\n"
        if isinstance(body, dict) and "checks" in body:
            for check, result in body["checks"].items():
                details += f"{check}: {result}\n"
        else:
            details += f"Response: {json.dumps(body, indent=2)}"

        await alert_admin("Web xpedit.es degradada", details)
        _last_website_alert = now
        logger.info("Website alert sent to admin")

    except Exception as e:
        logger.error(f"Website health monitor failed: {e}")
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="website-health-monitor", status="error")
            sentry_sdk.capture_exception(e)

        # Also alert on connection failures (site completely down)
        now = datetime.now(timezone.utc)
        if not _last_website_alert or (now - _last_website_alert).total_seconds() >= ALERT_COOLDOWN_HOURS * 3600:
            await alert_admin("Web xpedit.es NO responde", f"URL: {WEBSITE_HEALTH_URL}\nError: {e}\nTimestamp: {now.isoformat()}Z")
            _last_website_alert = now


# === VOICE ASSISTANT (Gemini Flash) ===

VOICE_ASSISTANT_PROMPT = """Eres COPILOTO, el asistente de voz de Xpedit, una app de reparto de ultima milla.
Eres el copiloto del repartidor. Hablas con naturalidad, eres directo y util.
El repartidor te habla mientras conduce. Tu respuesta sera leida en voz alta.

Devuelve UN JSON con esta estructura exacta:
{{
  "action": "<action>",
  "target_stop": "current",
  "payload": {{}},
  "confirmation": "texto para leer en voz alta (max 20 palabras)"
}}

ACCIONES DISPONIBLES:
--- Navegacion ---
- "complete": marcar parada como entregada. payload: {{"recipient": "nombre"}} si lo menciona
- "fail": marcar parada como fallida. payload: {{"reason": "motivo"}} si lo menciona
- "next": ir a la siguiente parada pendiente
- "skip": saltar parada actual, moverla al final
- "reorder": mover parada. payload: {{"stop_number": N, "new_position": M}}
- "call": llamar al cliente
- "mute": silenciar voz
- "unmute": activar voz
- "pause": pausar navegacion
- "resume": reanudar navegacion
- "navigate": ir a parada especifica. payload: {{"stop_number": N}} o {{"address": "..."}}

--- Ruta ---
- "optimize_route": optimizar/reorganizar la ruta
- "invert_route": invertir el orden de la ruta
- "clear_route": borrar todas las paradas
- "share_route": abrir pantalla de compartir ruta
- "save_route": guardar ruta. payload: {{"name": "nombre"}} si lo dice
- "start_route": empezar navegacion a la primera parada
- "finish_route": terminar/finalizar la ruta

--- Paradas ---
- "add_stop": añadir parada. payload: {{"address": "direccion completa"}}
- "delete_last_stop": borrar la ultima parada
- "undo_last": deshacer la ultima accion (completar/fallar)

--- Consultas ---
- "query_remaining": cuantas paradas quedan
- "query_status": resumen (completadas, pendientes, fallidas)
- "query_distance": distancia total de la ruta
- "query_time": a que hora terminara
- "query_clock": que hora es ahora
- "eta": cuanto falta para terminar

--- Modales ---
- "open_modal": abrir pantalla. payload: {{"modal": "settings|routes|share|recurring|depot"}}
- "close_modal": cerrar pantalla actual

--- Settings ---
- "toggle_dark_mode": cambiar modo oscuro
- "toggle_customer_alerts": activar/desactivar avisos a clientes

--- Otros ---
- "note": añadir nota. payload: {{"text": "contenido"}}
- "reminder": recordatorio. payload: {{"text": "contenido", "time": "HH:MM"}}
- "take_photo": abrir camara para foto
- "sign_delivery": abrir firma digital
- "info": pregunta general. Responde en confirmation
- "unknown": no entendiste. Sugiere que repita

CONTEXTO DEL REPARTO ACTUAL:
{context}

IDIOMA: Responde en {language}.

REGLAS:
- Confirmacion CORTA, natural. Como un copiloto humano hablaria.
- Para consultas (query_*) calcula con los datos del contexto y pon resultado en confirmation
- "a las 5" / "sobre las cinco" → "17:00"
- "apunta" / "anota" → "note"
- "recuerdame" → "reminder"
- "salta esta" / "dejala para el final" → "skip"
- "optimiza" / "organiza la ruta" → "optimize_route"
- "cuantas me quedan" / "como voy" → "query_status"
- "que hora es" → "query_clock"
- "abre ajustes" → open_modal con modal "settings"
- "borra todo" / "limpia la ruta" → "clear_route"
- "invierte la ruta" → "invert_route"
- Devuelve SOLO el JSON, sin markdown ni texto extra"""


class VoiceCommandRequest(BaseModel):
    transcript: str
    current_stop: Optional[dict] = None
    stops_summary: Optional[str] = None
    remaining_minutes: Optional[float] = None
    total_stops: Optional[int] = None
    completed_stops: Optional[int] = None
    failed_stops: Optional[int] = None
    total_distance_km: Optional[float] = None
    driver_name: Optional[str] = None
    screen: Optional[str] = None
    route_name: Optional[str] = None
    is_optimized: Optional[bool] = None
    language: Optional[str] = "es"


@app.post("/voice/command")
async def parse_voice_command(req: VoiceCommandRequest, user=Depends(get_current_user)):
    client = get_gemini_client()
    if not client:
        raise HTTPException(status_code=503, detail="Servicio de IA no disponible")

    # Build context string
    context_parts = []
    if req.driver_name:
        context_parts.append(f"Conductor: {req.driver_name}")
    if req.current_stop:
        stop = req.current_stop
        context_parts.append(f"Parada actual: {stop.get('address', 'desconocida')}")
        if stop.get('phone'):
            context_parts.append(f"Telefono cliente: {stop['phone']}")
        if stop.get('notes'):
            context_parts.append(f"Notas existentes: {stop['notes']}")
        if stop.get('packageId'):
            context_parts.append(f"ID paquete: {stop['packageId']}")
        if stop.get('position') is not None:
            context_parts.append(f"Posicion en ruta: {stop['position'] + 1}")
    if req.total_stops is not None:
        pending = (req.total_stops or 0) - (req.completed_stops or 0) - (req.failed_stops or 0)
        context_parts.append(f"Total: {req.total_stops} paradas ({req.completed_stops or 0} hechas, {req.failed_stops or 0} fallidas, {pending} pendientes)")
    elif req.stops_summary:
        context_parts.append(f"Resumen paradas: {req.stops_summary}")
    if req.remaining_minutes is not None:
        context_parts.append(f"Tiempo restante estimado: {req.remaining_minutes:.0f} minutos")
    if req.total_distance_km is not None:
        context_parts.append(f"Distancia total: {req.total_distance_km:.1f} km")
    if req.screen:
        context_parts.append(f"Pantalla actual: {req.screen}")
    if req.route_name:
        context_parts.append(f"Nombre ruta: {req.route_name}")
    if req.is_optimized is not None:
        context_parts.append(f"Ruta optimizada: {'si' if req.is_optimized else 'no'}")
    context_parts.append(f"Hora actual: {datetime.now(timezone.utc).strftime('%H:%M')}")

    language = req.language or "es"
    lang_label = "español" if language == "es" else "English"
    context = "\n".join(context_parts) if context_parts else "Sin contexto disponible"
    prompt = VOICE_ASSISTANT_PROMPT.replace("{context}", context).replace("{language}", lang_label)

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=[
                {"role": "user", "parts": [{"text": f"Comando del repartidor: \"{req.transcript}\""}]},
            ],
            config={
                "system_instruction": prompt,
                "temperature": 0.1,
                "max_output_tokens": 256,
            },
        )

        raw = response.text.strip()
        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)

        result = json.loads(raw)

        # Validate required fields
        if "action" not in result:
            result["action"] = "unknown"
        if "confirmation" not in result:
            result["confirmation"] = "No entendi el comando"

        return result

    except json.JSONDecodeError:
        return {
            "action": "unknown",
            "target_stop": "current",
            "payload": {},
            "confirmation": "No pude procesar el comando",
        }
    except Exception as e:
        logger.error(f"Voice command error: {e}")
        raise HTTPException(status_code=500, detail="Error procesando comando de voz")


# === FLEET MANAGEMENT ===


@app.get("/fleet/dashboard", tags=["fleet"], summary="Fleet dashboard KPIs")
async def fleet_dashboard_stats(user=Depends(require_admin_or_dispatcher)):
    """Real-time fleet KPIs for dispatcher/admin dashboard."""
    try:
        now = datetime.now(timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        five_min_ago = (now - timedelta(minutes=5)).isoformat()

        company_id = user.get("company_id")

        # Build base queries
        routes_q = supabase.table("routes").select("id,status,started_at,completed_at,driver_id")
        stops_q = supabase.table("stops").select("id,status,completed_at,time_window_start,time_window_end")
        drivers_q = supabase.table("drivers").select("id,active")

        if company_id:
            routes_q = routes_q.eq("company_id", company_id)
            drivers_q = drivers_q.eq("company_id", company_id)

        # Today's routes
        routes_today = routes_q.gte("created_at", today_start).execute()
        routes_data = [r for r in (routes_today.data or []) if r.get("driver_id") not in ADMIN_EXCLUDE_IDS]

        # All drivers
        drivers_result = drivers_q.eq("active", True).execute()
        driver_ids = [d["id"] for d in (drivers_result.data or []) if d["id"] not in ADMIN_EXCLUDE_IDS]
        total_drivers = len(driver_ids)

        # Active drivers (location update in last 5 min) — batch query instead of N+1
        active_count = 0
        if driver_ids:
            locs = supabase.table("location_history").select("driver_id").in_("driver_id", driver_ids).gte("recorded_at", five_min_ago).execute()
            active_driver_ids = set(loc["driver_id"] for loc in (locs.data or []))
            active_count = len(active_driver_ids)

        # Today's stops (from today's routes) — batch query instead of N+1
        route_ids = [r["id"] for r in routes_data]
        completed_stops = 0
        failed_stops = 0
        on_time_count = 0
        total_with_window = 0
        delivery_times = []

        if route_ids:
            all_stops = stops_q.in_("route_id", route_ids).execute()
            for s in (all_stops.data or []):
                if s["status"] == "completed":
                    completed_stops += 1
                    if s.get("time_window_end") and s.get("completed_at"):
                        total_with_window += 1
                        completed_time = s["completed_at"][:5] if isinstance(s["completed_at"], str) else ""
                        if completed_time <= s["time_window_end"]:
                            on_time_count += 1
                elif s["status"] == "failed":
                    failed_stops += 1

        # Route timing
        for r in routes_data:
            if r.get("started_at") and r.get("completed_at") and r["status"] == "completed":
                try:
                    started = datetime.fromisoformat(r["started_at"].replace("Z", "+00:00"))
                    completed = datetime.fromisoformat(r["completed_at"].replace("Z", "+00:00"))
                    delivery_times.append((completed - started).total_seconds() / 60)
                except Exception:
                    pass

        routes_in_progress = sum(1 for r in routes_data if r["status"] == "in_progress")
        routes_pending = sum(1 for r in routes_data if r["status"] == "pending")
        total_deliveries = completed_stops + failed_stops
        success_rate = round((completed_stops / total_deliveries * 100) if total_deliveries > 0 else 100, 1)
        avg_time = round(sum(delivery_times) / len(delivery_times), 1) if delivery_times else 0
        on_time_pct = round((on_time_count / total_with_window * 100) if total_with_window > 0 else 100, 1)

        return {
            "deliveries_today": completed_stops,
            "deliveries_failed_today": failed_stops,
            "active_drivers": active_count,
            "total_drivers": total_drivers,
            "routes_in_progress": routes_in_progress,
            "routes_pending": routes_pending,
            "avg_delivery_time_min": avg_time,
            "success_rate_pct": success_rate,
            "on_time_pct": on_time_pct,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fleet dashboard error: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error obteniendo estadísticas de flota")


@app.get("/fleet/drivers/locations", tags=["fleet"], summary="Batch driver locations")
async def fleet_driver_locations(user=Depends(require_admin_or_dispatcher)):
    """Get latest locations for all company drivers at once."""
    try:
        now = datetime.now(timezone.utc)
        two_min_ago = (now - timedelta(minutes=2)).isoformat()
        company_id = user.get("company_id")

        drivers_q = supabase.table("drivers").select("id,full_name,phone,active")
        if company_id:
            drivers_q = drivers_q.eq("company_id", company_id)
        drivers_result = drivers_q.eq("active", True).execute()

        # Filter out admin drivers
        valid_drivers = [d for d in (drivers_result.data or []) if d["id"] not in ADMIN_EXCLUDE_IDS]
        valid_driver_ids = [d["id"] for d in valid_drivers]

        # Fetch latest location per driver — one query per driver but with limit(1)
        # Using individual queries because batch would return 49K+ rows from location_history
        # and Supabase caps responses at 1000 rows by default
        latest_by_driver = {}
        for did in valid_driver_ids:
            try:
                loc = supabase.table("location_history").select(
                    "driver_id,lat,lng,speed,heading,accuracy,recorded_at"
                ).eq("driver_id", did).order("recorded_at", desc=True).limit(1).execute()
                if loc.data:
                    latest_by_driver[did] = loc.data[0]
            except Exception:
                pass

        locations = []
        for driver in valid_drivers:
            loc_data = latest_by_driver.get(driver["id"])
            if loc_data:
                is_online = loc_data.get("recorded_at", "") >= two_min_ago
                locations.append({
                    "driver_id": driver["id"],
                    "driver_name": driver.get("full_name") or driver.get("name", ""),
                    "phone": driver.get("phone"),
                    "lat": loc_data["lat"],
                    "lng": loc_data["lng"],
                    "speed": loc_data.get("speed"),
                    "heading": loc_data.get("heading"),
                    "accuracy": loc_data.get("accuracy"),
                    "recorded_at": loc_data["recorded_at"],
                    "is_online": is_online,
                })

        return {"drivers": locations}
    except Exception as e:
        logger.error(f"Fleet locations error: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error obteniendo ubicaciones")


@app.get("/fleet/drivers/{driver_id}/performance", tags=["fleet"], summary="Driver performance stats")
async def fleet_driver_performance(driver_id: str, period: str = "today", user=Depends(require_admin_or_dispatcher)):
    """Driver performance metrics for a given period."""
    try:
        start_dt, end_dt = _period_to_date_range(period)
        start_str = start_dt.isoformat()

        routes = supabase.table("routes").select(
            "id,status,total_distance_km,started_at,completed_at"
        ).eq("driver_id", driver_id).gte("created_at", start_str).execute()

        completed_deliveries = 0
        failed_deliveries = 0
        total_distance = 0.0
        delivery_times = []
        on_time = 0
        total_with_window = 0

        for route in (routes.data or []):
            total_distance += route.get("total_distance_km") or 0
            if route.get("started_at") and route.get("completed_at") and route["status"] == "completed":
                try:
                    s = datetime.fromisoformat(route["started_at"].replace("Z", "+00:00"))
                    c = datetime.fromisoformat(route["completed_at"].replace("Z", "+00:00"))
                    delivery_times.append((c - s).total_seconds() / 60)
                except Exception:
                    pass

            stops = supabase.table("stops").select(
                "status,completed_at,time_window_end"
            ).eq("route_id", route["id"]).execute()
            for stop in (stops.data or []):
                if stop["status"] == "completed":
                    completed_deliveries += 1
                    if stop.get("time_window_end") and stop.get("completed_at"):
                        total_with_window += 1
                        ct = stop["completed_at"][:5] if isinstance(stop["completed_at"], str) else ""
                        if ct <= stop["time_window_end"]:
                            on_time += 1
                elif stop["status"] == "failed":
                    failed_deliveries += 1

        total = completed_deliveries + failed_deliveries
        return {
            "driver_id": driver_id,
            "period": period,
            "deliveries_completed": completed_deliveries,
            "deliveries_failed": failed_deliveries,
            "success_rate": round((completed_deliveries / total * 100) if total > 0 else 100, 1),
            "avg_delivery_time_min": round(sum(delivery_times) / len(delivery_times), 1) if delivery_times else 0,
            "total_distance_km": round(total_distance, 1),
            "on_time_rate": round((on_time / total_with_window * 100) if total_with_window > 0 else 100, 1),
            "routes_completed": sum(1 for r in (routes.data or []) if r["status"] == "completed"),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Driver performance error: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error obteniendo rendimiento del conductor")


@app.get("/fleet/activity", tags=["fleet"], summary="Fleet activity feed")
async def fleet_activity_feed(limit: int = Query(default=50, le=200), user=Depends(require_admin_or_dispatcher)):
    """Recent fleet events: route starts, completions, failures."""
    try:
        company_id = user.get("company_id")
        now = datetime.now(timezone.utc)
        since = (now - timedelta(hours=24)).isoformat()

        # Recent routes with status changes
        routes_q = supabase.table("routes").select(
            "id,name,status,driver_id,started_at,completed_at,created_at"
        ).gte("created_at", since).order("created_at", desc=True).limit(limit)
        if company_id:
            routes_q = routes_q.eq("company_id", company_id)
        routes = routes_q.execute()

        # Get driver names
        driver_ids = list({r["driver_id"] for r in (routes.data or []) if r.get("driver_id")})
        driver_names = {}
        if driver_ids:
            for did in driver_ids:
                d = supabase.table("drivers").select("full_name").eq("id", did).limit(1).execute()
                if d.data:
                    driver_names[did] = d.data[0].get("full_name", "")

        events = []
        for r in (routes.data or []):
            if r.get("driver_id") in ADMIN_EXCLUDE_IDS:
                continue
            dname = driver_names.get(r.get("driver_id", ""), "Sin asignar")

            if r["status"] == "completed" and r.get("completed_at"):
                events.append({
                    "type": "route_completed",
                    "message": f"{dname} completó la ruta '{r.get('name', '')}'",
                    "timestamp": r["completed_at"],
                    "driver_name": dname,
                    "details": {"route_id": r["id"]},
                })
            elif r["status"] == "in_progress" and r.get("started_at"):
                events.append({
                    "type": "route_started",
                    "message": f"{dname} inició la ruta '{r.get('name', '')}'",
                    "timestamp": r["started_at"],
                    "driver_name": dname,
                    "details": {"route_id": r["id"]},
                })
            elif r["status"] == "pending":
                events.append({
                    "type": "route_created",
                    "message": f"Nueva ruta '{r.get('name', '')}' creada",
                    "timestamp": r["created_at"],
                    "driver_name": dname,
                    "details": {"route_id": r["id"]},
                })

        # Failed stops
        for r in (routes.data or []):
            if r.get("driver_id") in ADMIN_EXCLUDE_IDS:
                continue
            failed = supabase.table("stops").select(
                "id,address,completed_at"
            ).eq("route_id", r["id"]).eq("status", "failed").execute()
            dname = driver_names.get(r.get("driver_id", ""), "")
            for s in (failed.data or []):
                events.append({
                    "type": "delivery_failed",
                    "message": f"Entrega fallida en {s.get('address', '')[:40]}",
                    "timestamp": s.get("completed_at") or r["created_at"],
                    "driver_name": dname,
                    "details": {"stop_id": s["id"], "route_id": r["id"]},
                })

        events.sort(key=lambda e: e["timestamp"], reverse=True)
        return {"events": events[:limit]}
    except Exception as e:
        logger.error(f"Fleet activity error: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error obteniendo actividad de flota")


# -- Fleet Zones CRUD --

@app.get("/fleet/zones", tags=["fleet"], summary="List fleet zones")
async def list_fleet_zones(user=Depends(require_admin_or_dispatcher)):
    """List all fleet zones for the company."""
    try:
        company_id = user.get("company_id")
        is_admin = user.get("role") == "admin"
        if not company_id and not is_admin:
            raise HTTPException(status_code=403, detail="No tiene empresa asignada")
        q = supabase.table("fleet_zones").select("*")
        if company_id:
            q = q.eq("company_id", company_id)
        result = q.order("priority", desc=True).execute()
        return {"zones": result.data or []}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"List zones error: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error listando zonas")


@app.post("/fleet/zones", tags=["fleet"], summary="Create fleet zone", status_code=201)
async def create_fleet_zone(zone: FleetZoneCreate, user=Depends(require_admin_or_dispatcher)):
    """Create a new fleet zone."""
    try:
        company_id = user.get("company_id")
        if not company_id:
            raise HTTPException(status_code=400, detail="Se requiere company_id")

        data = {
            "company_id": company_id,
            "name": zone.name,
            "polygon": [{"lat": p.lat, "lng": p.lng} for p in zone.polygon],
            "color": zone.color,
            "priority": zone.priority,
        }
        result = supabase.table("fleet_zones").insert(data).execute()
        return result.data[0] if result.data else {}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create zone error: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error creando zona")


@app.put("/fleet/zones/{zone_id}", tags=["fleet"], summary="Update fleet zone")
async def update_fleet_zone(zone_id: str, zone: FleetZoneUpdate, user=Depends(require_admin_or_dispatcher)):
    """Update a fleet zone."""
    try:
        company_id = user.get("company_id")
        is_admin = user.get("role") == "admin"
        if not company_id and not is_admin:
            raise HTTPException(status_code=403, detail="No tiene empresa asignada")

        # Verify zone belongs to user's company (admins can update any zone)
        if company_id:
            existing = supabase.table("fleet_zones").select("id, company_id").eq("id", zone_id).single().execute()
            if not existing.data:
                raise HTTPException(status_code=404, detail="Zona no encontrada")
            if existing.data.get("company_id") != company_id:
                raise HTTPException(status_code=403, detail="No tiene acceso a esta zona")

        update_data = {}
        if zone.name is not None:
            update_data["name"] = zone.name
        if zone.polygon is not None:
            update_data["polygon"] = [{"lat": p.lat, "lng": p.lng} for p in zone.polygon]
        if zone.color is not None:
            update_data["color"] = zone.color
        if zone.priority is not None:
            update_data["priority"] = zone.priority
        if zone.active is not None:
            update_data["active"] = zone.active
        if not update_data:
            raise HTTPException(status_code=400, detail="No hay campos para actualizar")

        update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
        result = supabase.table("fleet_zones").update(update_data).eq("id", zone_id).execute()
        return result.data[0] if result.data else {}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update zone error: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error actualizando zona")


@app.delete("/fleet/zones/{zone_id}", tags=["fleet"], summary="Delete fleet zone")
async def delete_fleet_zone(zone_id: str, user=Depends(require_admin_or_dispatcher)):
    """Delete a fleet zone."""
    try:
        company_id = user.get("company_id")
        is_admin = user.get("role") == "admin"
        if not company_id and not is_admin:
            raise HTTPException(status_code=403, detail="No tiene empresa asignada")

        # Verify zone belongs to user's company (admins can delete any zone)
        if company_id:
            existing = supabase.table("fleet_zones").select("id, company_id").eq("id", zone_id).single().execute()
            if not existing.data:
                raise HTTPException(status_code=404, detail="Zona no encontrada")
            if existing.data.get("company_id") != company_id:
                raise HTTPException(status_code=403, detail="No tiene acceso a esta zona")

        supabase.table("fleet_zones").delete().eq("id", zone_id).execute()
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete zone error: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error eliminando zona")


# -- Fleet Chat --

@app.post("/fleet/messages", tags=["fleet"], summary="Send chat message")
async def send_fleet_message(msg: FleetMessageCreate, user=Depends(require_admin_or_dispatcher)):
    """Send a message from dispatcher to driver."""
    try:
        data = {
            "company_id": user.get("company_id"),
            "sender_id": user["id"],
            "sender_role": user.get("role", "dispatcher"),
            "recipient_id": msg.driver_id,
            "message": msg.message,
        }
        result = supabase.table("chat_messages").insert(data).execute()

        # Send push notification to driver
        driver = supabase.table("drivers").select("push_token,full_name").eq("id", msg.driver_id).limit(1).execute()
        if driver.data and driver.data[0].get("push_token"):
            try:
                sender_name = user.get("full_name") or user.get("email", "Despacho")
                async with httpx.AsyncClient(timeout=5) as client:
                    await client.post(
                        "https://exp.host/--/api/v2/push/send",
                        json={
                            "to": driver.data[0]["push_token"],
                            "title": f"Mensaje de {sender_name}",
                            "body": msg.message[:100],
                            "data": {"type": "chat", "sender_id": user["id"]},
                            "sound": "default",
                        },
                    )
            except Exception:
                pass  # Non-critical

        return result.data[0] if result.data else {}
    except Exception as e:
        logger.error(f"Send message error: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error enviando mensaje")


@app.get("/fleet/messages/{driver_id}", tags=["fleet"], summary="Get chat history")
async def get_fleet_messages(driver_id: str, limit: int = Query(default=50, le=200), user=Depends(require_admin_or_dispatcher)):
    """Get chat history between dispatcher and driver."""
    try:
        dispatcher_id = user["id"]
        # Get messages where sender/recipient is either the dispatcher or driver
        result = supabase.table("chat_messages").select("*").or_(
            f"and(sender_id.eq.{dispatcher_id},recipient_id.eq.{driver_id}),and(sender_id.eq.{driver_id},recipient_id.eq.{dispatcher_id})"
        ).order("created_at", desc=True).limit(limit).execute()

        # Mark unread messages as read
        supabase.table("chat_messages").update({"read": True}).eq(
            "recipient_id", dispatcher_id
        ).eq("sender_id", driver_id).eq("read", False).execute()

        return {"messages": list(reversed(result.data or []))}
    except Exception as e:
        logger.error(f"Get messages error: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error obteniendo mensajes")


# -- Fleet Auth (login endpoint for fleet dashboard) --

@app.post("/fleet/login", tags=["fleet"], summary="Fleet dashboard login")
async def fleet_login(request: Request):
    """Login for fleet dashboard - validates dispatcher/admin role."""
    try:
        body = await request.json()
        email = body.get("email", "").strip().lower()
        password = body.get("password", "")

        if not email or not password:
            raise HTTPException(status_code=400, detail="Email y contraseña requeridos")

        # Authenticate via Supabase
        SUPABASE_URL = os.getenv("SUPABASE_URL", "")
        SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
                json={"email": email, "password": password},
                headers={"apikey": SUPABASE_KEY, "Content-Type": "application/json"},
            )

        if resp.status_code != 200:
            raise HTTPException(status_code=401, detail="Credenciales inválidas")

        auth_data = resp.json()
        access_token = auth_data.get("access_token", "")
        user_id = auth_data.get("user", {}).get("id", "")

        # Check user role
        user_result = supabase.table("users").select("id,email,full_name,role,company_id").eq("id", user_id).limit(1).execute()
        if not user_result.data:
            raise HTTPException(status_code=403, detail="Usuario no encontrado")

        user_data = user_result.data[0]
        if user_data.get("role") not in ("admin", "dispatcher"):
            raise HTTPException(status_code=403, detail="Acceso restringido a dispatchers y administradores")

        return {
            "token": access_token,
            "user": {
                "id": user_data["id"],
                "email": user_data.get("email", email),
                "name": user_data.get("full_name", ""),
                "role": user_data.get("role", ""),
                "company_id": user_data.get("company_id"),
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fleet login error: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error en el inicio de sesión")


@app.post("/feedback/trial", tags=["feedback"], summary="Registrar feedback post-trial")
async def submit_trial_feedback(request: TrialFeedbackRequest):
    """Public endpoint (no auth) — receives feedback from email links.
    Inserts into trial_feedback table. If reason=time, extends trial by 3 days.

    Authorization: HMAC token bound to (driver_id, reason). Tokens are
    generated by the cron that sends the feedback email and embedded in the
    link, so legitimate clicks always carry one. Without this, anyone with
    a driver_id UUID could extend any user's Pro trial — a free-tier exploit
    that scales as soon as we have paying users.
    """
    try:
        # Verify HMAC token. Reject if missing or doesn't match. We log a
        # breadcrumb (not Sentry) because invalid tokens are expected from
        # bots scraping the URL pattern.
        expected_token = _trial_feedback_token(request.driver_id, request.reason)
        if not FEEDBACK_TOKEN_SECRET:
            logger.error("FEEDBACK_TOKEN_SECRET not configured — rejecting feedback")
            raise HTTPException(status_code=500, detail="Feedback service unavailable")
        if not request.token or not _hmac.compare_digest(request.token, expected_token):
            logger.info(f"trial feedback rejected: invalid/missing token for driver={request.driver_id}")
            raise HTTPException(status_code=403, detail="Invalid token")

        # Verify driver exists
        driver_result = supabase.table("drivers").select("id, email, name, promo_plan, promo_plan_expires_at").eq("id", request.driver_id).single().execute()
        if not driver_result.data:
            raise HTTPException(status_code=404, detail="Driver not found")

        # Insert feedback
        feedback_data = {
            "driver_id": request.driver_id,
            "reason": request.reason,
            "detail": request.detail,
        }
        insert_result = supabase.table("trial_feedback").insert(feedback_data).execute()
        if not insert_result.data:
            raise HTTPException(status_code=500, detail="Failed to save feedback")

        response = {"success": True, "reason": request.reason}

        # If reason is "time", extend trial by 3 extra days
        if request.reason == "time":
            now = datetime.now(timezone.utc)
            new_expiry = (now + timedelta(days=3)).isoformat()
            supabase.table("drivers").update({
                "promo_plan": "pro",
                "promo_plan_expires_at": new_expiry,
            }).eq("id", request.driver_id).execute()
            response["trial_extended"] = True
            response["new_expiry"] = new_expiry
            logger.info(f"Trial extended 3 days for driver {request.driver_id} (feedback: no time)")

        logger.info(f"Trial feedback received: driver={request.driver_id}, reason={request.reason}")
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Trial feedback error: {e}")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail="Error saving feedback")


# === MAIN ===
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
