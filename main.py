"""
Xpedit API - Backend de optimización de rutas
"""

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
from datetime import datetime, timedelta, timezone
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
SENTRY_DSN = os.getenv("SENTRY_DSN")
if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=0.1,
        profiles_sample_rate=0.1,
        environment=os.getenv("SENTRY_ENVIRONMENT", "production"),
        release="xpedit-backend@1.1.4",
        send_default_pii=False,
    )
    logger.info("Sentry initialized for error monitoring")

from emails import (
    TRIAL_EXPIRING_D1_SUBJECT,
    TRIAL_EXPIRING_D3_SUBJECT,
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

# Inicializar Supabase (service role key para bypass RLS en servidor)
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY", ""))
supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    SUPABASE_SERVICE_KEY
)

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

        # Get user profile from DB
        result = supabase.table("users").select("id, email, role, company_id").eq("id", user_id).single().execute()
        if not result.data:
            raise HTTPException(status_code=401, detail="Usuario no encontrado")

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
            check_rate_limit(f"ocr:{client_ip}", max_requests=5, window_seconds=60)
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


async def get_road_distance_matrix(locations: list) -> dict | None:
    """
    Obtiene matrices de distancias y duraciones reales por carretera usando OSRM.
    Reintenta agresivamente. Para >100 paradas, chunkea con sources/destinations.
    La matriz de duraciones es ASIMÉTRICA (A→B ≠ B→A) por calles de un sentido.
    Retorna {"distances": [...], "durations": [...]} o None si OSRM es inalcanzable.
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
            logger.error(f"OSRM FAILED for {n} locations after all retries")
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
            logger.error(f"OSRM chunk {chunk_start}-{chunk_end} FAILED for {n} locations")
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
    """Convierte una dirección de texto en coordenadas (lat/lng) usando Nominatim."""
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            response = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": request.address, "format": "json", "limit": 1},
                headers={"User-Agent": "Xpedit/1.0"},
                timeout=10.0
            )
            data = response.json()
            if not data:
                return {"success": False, "error": "Dirección no encontrada"}
            return {
                "success": True,
                "lat": float(data[0]["lat"]),
                "lng": float(data[0]["lon"]),
                "display_name": data[0]["display_name"]
            }
        except Exception as e:
            logger.error(f"Geocode error: {e}")
            raise HTTPException(status_code=500, detail="Error interno del servidor")


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
    result = supabase.table("routes").select("*, stops(*)").eq("id", route_id).single().execute()
    return result.data


@app.patch("/routes/{route_id}/start", tags=["routes"], summary="Iniciar ruta")
async def start_route(route_id: str, user=Depends(get_current_user)):
    """Marca una ruta como 'in_progress' y registra la hora de inicio."""
    await verify_route_access(route_id, user)
    result = supabase.table("routes").update({
        "status": "in_progress",
        "started_at": datetime.now(timezone.utc).isoformat()
    }).eq("id", route_id).execute()
    route = safe_first(result)
    if not route:
        raise HTTPException(status_code=404, detail="Ruta no encontrada")
    return {"success": True, "route": route}


@app.patch("/routes/{route_id}/complete", tags=["routes"], summary="Completar ruta")
async def complete_route(route_id: str, user=Depends(get_current_user)):
    """Marca una ruta como 'completed' y registra la hora de finalización."""
    await verify_route_access(route_id, user)
    result = supabase.table("routes").update({
        "status": "completed",
        "completed_at": datetime.now(timezone.utc).isoformat()
    }).eq("id", route_id).execute()
    route = safe_first(result)
    if not route:
        raise HTTPException(status_code=404, detail="Ruta no encontrada")
    return {"success": True, "route": route}


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
    result = supabase.table("stops").update({
        "status": "completed",
        "completed_at": datetime.now(timezone.utc).isoformat()
    }).eq("id", stop_id).execute()
    stop = safe_first(result)
    if not stop:
        raise HTTPException(status_code=404, detail="Parada no encontrada")
    return {"success": True, "stop": stop}


@app.patch("/stops/{stop_id}/fail", tags=["stops"], summary="Marcar parada fallida")
async def fail_stop(stop_id: str, user=Depends(get_current_user)):
    """Marca una parada como 'failed' y registra la hora."""
    await verify_stop_access(stop_id, user)
    result = supabase.table("stops").update({
        "status": "failed",
        "completed_at": datetime.now(timezone.utc).isoformat()
    }).eq("id", stop_id).execute()
    stop = safe_first(result)
    if not stop:
        raise HTTPException(status_code=404, detail="Parada no encontrada")
    return {"success": True, "stop": stop}


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
    """Registra la ubicación GPS actual del conductor. Fuerza el driver_id del usuario autenticado."""
    # Force driver_id to be the authenticated user's driver
    user_driver_id = await get_user_driver_id(user)
    if not user_driver_id:
        raise HTTPException(status_code=400, detail="No se encontró perfil de conductor")
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

    result = supabase.table("location_history").insert(data).execute()
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

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


class OCRLabelRequest(BaseModel):
    image_base64: str = Field(..., max_length=10_000_000)  # ~7.5MB max image
    media_type: Literal["image/jpeg", "image/png", "image/gif", "image/webp"] = "image/jpeg"


@app.post("/ocr/label", tags=["ocr"], summary="OCR de etiqueta de envío")
async def ocr_label(request: OCRLabelRequest, user=Depends(get_current_user)):
    """Extrae datos de una etiqueta de envío (nombre, dirección, ciudad, CP, provincia) usando IA. La API key se mantiene en el servidor."""
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="OCR service not configured")

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 500,
                    "messages": [{
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": request.media_type,
                                    "data": request.image_base64,
                                },
                            },
                            {
                                "type": "text",
                                "text": """Esta es una foto de una etiqueta de envío de paquetería (iMile, Shein, etc.).
IMPORTANTE: La imagen puede estar ROTADA 90°, 180° o 270°. Analiza la orientación del texto primero.

Busca la sección "TO" o destinatario que contiene:
- Nombre del destinatario (persona)
- Dirección: calle y número
- Ciudad (ej: Arcos De La Frontera)
- Código postal (5 dígitos, ej: 11630)
- Provincia (ej: Cádiz)

Responde ÚNICAMENTE con este JSON (sin explicaciones):
{
  "name": "Nombre completo del destinatario",
  "street": "Calle y número exacto",
  "city": "Ciudad/Localidad",
  "postalCode": "Código postal de 5 dígitos",
  "province": "Provincia"
}

CRÍTICO: Lee TODA la etiqueta cuidadosamente aunque esté rotada.""",
                            },
                        ],
                    }],
                },
            )

        if response.status_code != 200:
            raise HTTPException(status_code=502, detail=f"OCR API error: {response.status_code}")

        data = response.json()
        content = data.get("content", [{}])[0].get("text", "")
        return {"success": True, "content": content}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


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

@app.get("/places/autocomplete", tags=["places"], summary="Autocompletado de direcciones")
async def places_autocomplete(
    input: str,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    sessiontoken: Optional[str] = None,
    user=Depends(get_current_user),
):
    """Proxy de Google Places Autocomplete con sesgo de ubicación. Fallback a Nominatim si falla.

    `sessiontoken` (opcional) agrupa keystrokes + Place Details en una única
    sesión facturable. Con session token, Google factura SOLO el Details final
    y los autocompletes son gratis. Sin token, cada autocomplete cuesta $2.83/1k.
    El cliente debe generar un UUID al abrir el input y pasarlo en cada
    autocomplete + en el details posterior."""
    global _places_api_healthy, _places_api_last_alert, _places_api_last_check
    import re

    google_ok = False
    if GOOGLE_API_KEY:
        params = {
            "input": input,
            "language": "es",
            "key": GOOGLE_API_KEY,
        }
        if lat and lng:
            params["location"] = f"{lat},{lng}"
            params["radius"] = "30000"
        if sessiontoken:
            params["sessiontoken"] = sessiontoken

        # Retry once on timeout/error before falling back to Nominatim
        for attempt in range(2):
            try:
                timeout = 8.0 if attempt == 0 else 10.0
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.get("https://maps.googleapis.com/maps/api/place/autocomplete/json", params=params)
                    data = resp.json()

                status = data.get("status")
                if status == "OK":
                    google_ok = True
                    if not _places_api_healthy:
                        logger.info("Google Places API recovered")
                        _places_api_healthy = True
                    return data
                elif status == "ZERO_RESULTS":
                    # API is working fine, just no matches — NOT a failure
                    google_ok = True
                    if not _places_api_healthy:
                        logger.info("Google Places API recovered")
                        _places_api_healthy = True
                    break  # Fall through to Nominatim for better results
                elif status == "OVER_QUERY_LIMIT":
                    logger.warning(f"Google Places rate limited (attempt {attempt + 1})")
                    if attempt == 0:
                        import asyncio
                        await asyncio.sleep(1)
                        continue
                    break
                else:
                    error_msg = data.get("error_message", status or "unknown")
                    logger.warning(f"Google Places failed: status={status}, http={resp.status_code}, error={error_msg} (query: {input[:30]})")
                    break
            except Exception as e:
                logger.warning(f"Google Places request error (attempt {attempt + 1}): {e}")
                if attempt == 0:
                    continue

    # Alert on first failure (then cooldown 1h)
    if not google_ok and _places_api_healthy:
        _places_api_healthy = False
        now = datetime.now(timezone.utc)
        if not _places_api_last_alert or (now - _places_api_last_alert).total_seconds() > 3600:
            _places_api_last_alert = now
            try:
                send_alert_email(
                    ALERT_EMAIL,
                    "ALERTA: Google Places API caída - usando Nominatim",
                    f"Google Places no responde. Autocomplete usa Nominatim como fallback.\n"
                    f"Key configurada: {'Sí' if GOOGLE_API_KEY else 'NO (vacía!)'}\n"
                    f"Timestamp: {now.isoformat()}Z\n"
                    f"Acción: verificar GOOGLE_API_KEY en Railway y restricciones en Google Cloud Console.",
                )
            except Exception:
                pass
            if SENTRY_DSN:
                sentry_sdk.capture_message("Google Places API down, using Nominatim fallback", level="warning")

    # Fallback: Nominatim (OpenStreetMap) - free, no API key
    nom_query = input
    nom_base_params = {
        "format": "json",
        "addressdetails": "1",
        "limit": "5",
        "accept-language": "es",
    }
    if lat and lng:
        nom_base_params["viewbox"] = f"{lng-0.5},{lat+0.5},{lng+0.5},{lat-0.5}"
        nom_base_params["bounded"] = "0"

    street_prefixes = r"^(calle|avenida|avda|av|plaza|paseo|camino|carretera|ctra|ronda|travesia|urbanizacion|urb|poligono|pol)\s+"
    stripped = re.sub(street_prefixes, "", nom_query, flags=re.IGNORECASE).strip()
    queries = [nom_query] if stripped == nom_query else [nom_query, stripped]

    nom_data = []
    async with httpx.AsyncClient(timeout=10) as client:
        for q in queries:
            nom_resp = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={**nom_base_params, "q": q},
                headers={"User-Agent": "Xpedit/1.1"}
            )
            nom_data = nom_resp.json()
            if nom_data:
                break

    if nom_data:
        results = [{"place_id": None, "display_name": r["display_name"], "lat": r["lat"], "lon": r["lon"], "source": "nominatim"} for r in nom_data]
        return {"status": "OK", "predictions": results, "source": "nominatim"}
    return {"status": "ZERO_RESULTS", "predictions": []}


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
    params = {
        "place_id": place_id,
        "fields": "geometry,address_components,formatted_address,name,opening_hours,types",
        "language": "es",
        "key": GOOGLE_API_KEY,
    }
    if sessiontoken:
        params["sessiontoken"] = sessiontoken
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get("https://maps.googleapis.com/maps/api/place/details/json", params=params)
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
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get("https://maps.googleapis.com/maps/api/geocode/json", params=params)
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


@app.get("/places/directions", tags=["places"], summary="Obtener direcciones de ruta")
async def places_directions(
    origin: str,
    destination: str,
    waypoints: Optional[str] = None,
    avoid: Optional[str] = None,
    heading: Optional[float] = None,
    user=Depends(get_current_user)
):
    """Proxy de Google Directions API. Devuelve polylines y pasos de navegación.

    Si `heading` (0-360) viene, inserta un micro-waypoint ~50 m delante del coche
    en esa dirección. Así el rerouting no propone giros bruscos hacia atrás.
    """
    params = {
        "origin": origin,
        "destination": destination,
        "key": GOOGLE_API_KEY,
        "language": "es",
    }
    extra_wp = ""
    if heading is not None and "," in origin:
        try:
            lat_str, lng_str = origin.split(",", 1)
            lat, lng = float(lat_str), float(lng_str)
            h = math.radians(heading % 360)
            # ~50 m ahead in the driver's facing direction (good enough for routing)
            dlat = (50 * math.cos(h)) / 111320.0
            dlng = (50 * math.sin(h)) / (111320.0 * max(math.cos(math.radians(lat)), 1e-6))
            extra_wp = f"via:{lat + dlat:.6f},{lng + dlng:.6f}"
        except Exception as e:
            logger.warning(f"heading waypoint skipped: {e}")
    combined_waypoints = "|".join(p for p in [extra_wp, waypoints] if p)
    if combined_waypoints:
        params["waypoints"] = combined_waypoints
    if avoid:
        params["avoid"] = avoid
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get("https://maps.googleapis.com/maps/api/directions/json", params=params)
    return resp.json()


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
        records = await scraper(google_api_key=GOOGLE_API_KEY)
    except Exception as e:
        logger.exception(f"Scraper '{city}' failed")
        if SENTRY_DSN:
            sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail=f"Scraper failed: {e}")
    counts = _upsert_closures(supabase, records)
    return {"city": city, "scraped": len(records), **counts}


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
    # Gate: only Pro+ users or admin-granted early access can see closures
    try:
        d = supabase.table("drivers").select(
            "promo_plan, subscription_period, closures_alerts_enabled"
        ).eq("id", user["id"]).single().execute()
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
        # Fail closed: if we can't verify the user, deny.
        raise HTTPException(status_code=403, detail="Could not verify access")
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


async def run_all_closure_scrapers():
    """Scheduled job: run every closure scraper and upsert. Logged + Sentry-captured on failure."""
    if not GOOGLE_API_KEY:
        logger.info("Skipping closures scrape: GOOGLE_API_KEY missing")
        return
    for city, scraper in _CLOSURE_SCRAPERS.items():
        try:
            records = await scraper(google_api_key=GOOGLE_API_KEY)
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

            # Delete other driver-specific data
            tables_driver = [
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

        # Log any partial failures for GDPR audit trail
        if deletion_errors:
            logger.warning(f"Account deletion partial errors for {user_id}: {deletion_errors}")
            sentry_sdk.capture_message(
                f"Account deletion had {len(deletion_errors)} errors for user {user_id}",
                level="warning",
            )

        return {"status": "deleted", "message": "Cuenta eliminada correctamente"}
    except Exception as e:
        logger.error(f"Delete account error: {e}")
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


async def check_expiring_trials():
    """Daily conversion touchpoints: D-3 reminder + D-1 final urgency.

    Runs once a day. Picks ONLY users whose trial expires in [3, 4) days (D-3 cohort)
    or in [1, 2) days (D-1 cohort) and sends the corresponding template. email_log
    dedup ensures each user receives each touch at most once even if the cron retries
    or the window slightly shifts day-to-day.
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

        # Single query covering both windows. We split into D-3 / D-1 in Python
        # so we only hit Supabase once.
        window_start = (now + timedelta(days=1)).isoformat()
        window_end = (now + timedelta(days=4)).isoformat()

        result = (
            supabase.table("drivers")
            .select("id, email, name, promo_plan, promo_plan_expires_at, subscription_source")
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
            logger.info("Trial expiry check: no trials in D-3 or D-1 windows")
            if SENTRY_DSN:
                sentry_check_in(monitor_slug="check-expiring-trials", status="ok")
            return

        sent_d3, sent_d1, skipped, failed = 0, 0, 0, 0
        for driver in result.data:
            if driver["id"] in EXCLUDED_IDS or not driver.get("email"):
                skipped += 1
                continue
            expires_at = datetime.fromisoformat(driver["promo_plan_expires_at"].replace("Z", "+00:00"))
            hours_left = (expires_at - now).total_seconds() / 3600

            # Bucket selection: D-1 takes precedence if both could match (shouldn't happen).
            if 24 <= hours_left < 48:
                template_subject = TRIAL_EXPIRING_D1_SUBJECT
                template_kind = "d1"
            elif 72 <= hours_left < 96:
                template_subject = TRIAL_EXPIRING_D3_SUBJECT
                template_kind = "d3"
            else:
                # Outside our two cohorts (e.g. 50h or 25h gap day) — skip silently.
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
            else:  # d1
                email_result = send_trial_last_day_email(driver["email"], driver.get("name", ""))

            if email_result.get("success"):
                if template_kind == "d3":
                    sent_d3 += 1
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
            else:
                failed += 1
                logger.warning(f"Trial expiry email failed for {driver['id']}: {email_result.get('error')}")

        logger.info(f"Trial expiry: D-3={sent_d3} D-1={sent_d1} skipped={skipped} failed={failed} of {len(result.data)} candidates")
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="check-expiring-trials", status="ok")
    except Exception as e:
        logger.error(f"Trial expiry check failed: {e}")
        if SENTRY_DSN:
            sentry_check_in(monitor_slug="check-expiring-trials", status="error")
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

    def count(table: str, filters: list, timestamp_col: str, since: str) -> int:
        q = supabase.table(table).select("id", count="exact")
        for f in filters:
            col, op, val = f
            q = getattr(q, op)(col, val) if val is not None else getattr(q, op)(col)
        q = q.gte(timestamp_col, since)
        try:
            return q.execute().count or 0
        except Exception as e:
            logger.warning(f"Health digest count failed [{table}]: {e}")
            return 0

    def baseline_avg(count_7d: int, count_24h: int) -> float:
        # 6 previous days average (exclude today)
        return max(0.0, (count_7d - count_24h) / 6.0)

    metrics: list[dict] = []

    # 1. Signups (new drivers) 24h vs 6d baseline
    signups_24h = count("drivers", [], "created_at", last_24h)
    signups_7d = count("drivers", [], "created_at", last_7d)
    base_signups = baseline_avg(signups_7d, signups_24h)
    metrics.append({
        "label": "Nuevos registros (24h)",
        "value": signups_24h,
        "baseline": round(base_signups, 1),
        "status": _status_from_value(signups_24h, base_signups, min_expected=2),
    })

    # 2. Routes created 24h
    routes_24h = count("routes", [], "created_at", last_24h)
    routes_7d = count("routes", [], "created_at", last_7d)
    base_routes = baseline_avg(routes_7d, routes_24h)
    metrics.append({
        "label": "Rutas creadas (24h)",
        "value": routes_24h,
        "baseline": round(base_routes, 1),
        "status": _status_from_value(routes_24h, base_routes, min_expected=3),
    })

    # 3. Stops created 24h
    stops_24h = count("stops", [], "created_at", last_24h)
    stops_7d = count("stops", [], "created_at", last_7d)
    base_stops = baseline_avg(stops_7d, stops_24h)
    metrics.append({
        "label": "Paradas creadas (24h)",
        "value": stops_24h,
        "baseline": round(base_stops, 1),
        "status": _status_from_value(stops_24h, base_stops, min_expected=20),
    })

    # 3b. Stop processing rate (completed+failed / total created 24h).
    # Guards against the April 2026 silent sync bug where 93% of stops stayed
    # "pending" in DB. A healthy day is >= 50%. Below 30% is likely broken sync.
    stops_processed_24h = 0
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
    if stops_24h > 0:
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
    try:
        active_rows = (
            supabase.table("routes").select("driver_id").gte("created_at", last_24h).execute()
        )
        active_24h = len({r["driver_id"] for r in (active_rows.data or []) if r.get("driver_id")})
    except Exception:
        active_24h = 0
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
    base_trials = baseline_avg(trial_claims_7d, trial_claims_24h)
    metrics.append({
        "label": "Trials nuevos (24h)",
        "value": trial_claims_24h,
        "baseline": round(base_trials, 1),
        "status": _status_from_value(trial_claims_24h, base_trials, min_expected=1),
    })

    # 8. Paid users — use daily_metrics_snapshot for 7d delta (drivers table has no updated_at)
    paid_today = 0
    paid_7d_ago = 0
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

    return {
        "date": date_str,
        "metrics": metrics,
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
    # Daily health digest (08:00 Europe/Madrid — detect silent regressions)
    social_scheduler.add_job(
        send_daily_health_digest_job,
        "cron",
        hour=8,
        minute=0,
        timezone=ZoneInfo("Europe/Madrid"),
        id="daily_health_digest",
        replace_existing=True,
    )
    # Reactivation push 5h follow-up (hourly at :15 → email if user did not open the app after push)
    social_scheduler.add_job(
        reactivation_push_followup_job,
        "cron",
        minute=15,
        id="reactivation_push_followup",
        replace_existing=True,
    )
    logger.info("Monitoring jobs scheduled: health (5min), website (15min), daily backup (3:00 UTC), weekly retention (Sun 4:00 UTC), re-engagement push (Mon 10:00 UTC), trial expiry (9:00 UTC), trial degrade (9:05 UTC), trial feedback (10:30 UTC), daily health digest (08:00 CET), reactivation followup (hourly :15)")


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
                    async with httpx.AsyncClient(timeout=8.0) as client:
                        resp = await client.get(
                            "https://maps.googleapis.com/maps/api/place/autocomplete/json",
                            params={"input": "test", "key": GOOGLE_API_KEY, "language": "es"},
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
