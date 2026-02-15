"""
RutaMax API - Backend de optimización de rutas
"""

import os
import random
import time
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
import json
import hashlib
import httpx
import jwt as pyjwt
from jwt import PyJWKClient
from dotenv import load_dotenv
from supabase import create_client, Client

from optimizer import (
    optimize_route,
    calculate_eta,
    calculate_route_etas,
    cluster_stops_by_zone,
    assign_drivers_to_zones,
    optimize_multi_vehicle
)
from emails import (
    send_welcome_email,
    send_delivery_started_email,
    send_delivery_completed_email,
    send_delivery_failed_email,
    send_daily_summary_email
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

# Stripe
import stripe
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = STRIPE_SECRET_KEY

STRIPE_PLANS = {
    "pro": {"name": "Xpedit Pro", "amount": 499, "interval": "month"},
    "pro_plus": {"name": "Xpedit Pro+", "amount": 999, "interval": "month"},
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
        print(f"[AUTH] JWT alg={alg}, token_len={len(token)}")

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

        # Get user profile from DB
        result = supabase.table("users").select("id, email, role, company_id").eq("id", user_id).single().execute()
        if not result.data:
            raise HTTPException(status_code=401, detail="Usuario no encontrado")

        return result.data
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado - cierra sesion y vuelve a entrar")
    except pyjwt.InvalidTokenError as e:
        print(f"[AUTH] InvalidTokenError: {e}")
        raise HTTPException(status_code=401, detail="Token invalido - cierra sesion y vuelve a entrar")
    except HTTPException:
        raise
    except Exception as e:
        print(f"[AUTH] Unexpected error: {type(e).__name__}: {e}")
        raise HTTPException(status_code=401, detail="Error de autenticacion")


async def require_admin(user=Depends(get_current_user)):
    """Require admin role"""
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Acceso restringido a administradores")
    return user


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
        if driver_result.data and driver_result.data[0].get("company_id") == user["company_id"]:
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
        if driver_result.data and driver_result.data[0].get("company_id") == user["company_id"]:
            return True
    raise HTTPException(status_code=403, detail="No tienes acceso a este conductor")


async def verify_company_management(user: dict, company_id: str = None):
    """Verify the user can manage this company (admin or dispatcher of the company)."""
    if user["role"] == "admin":
        return True
    if user["role"] == "dispatcher" and user.get("company_id"):
        if company_id is None or user["company_id"] == company_id:
            return True
    raise HTTPException(status_code=403, detail="No tienes permisos para gestionar esta empresa")


app = FastAPI(
    title="RutaMax API",
    description="API de optimización de rutas para entregas de última milla",
    version="0.2.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://xpedit.es", "https://www.xpedit.es", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting (in-memory, single instance)
from collections import defaultdict
_rate_limits: dict = defaultdict(list)

def check_rate_limit(key: str, max_requests: int = 30, window_seconds: int = 60):
    """Simple in-memory rate limiter. Raises 429 if exceeded."""
    now = time.time()
    _rate_limits[key] = [t for t in _rate_limits[key] if t > now - window_seconds]
    if len(_rate_limits[key]) >= max_requests:
        raise HTTPException(status_code=429, detail="Demasiadas solicitudes. Inténtalo en unos minutos.")
    _rate_limits[key].append(now)

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Apply rate limiting to sensitive endpoints"""
    path = request.url.path
    client_ip = request.client.host if request.client else "unknown"
    try:
        if path.startswith("/admin"):
            check_rate_limit(f"admin:{client_ip}", max_requests=60, window_seconds=60)
        elif path.startswith("/auth") or path == "/promo/redeem":
            check_rate_limit(f"auth:{client_ip}", max_requests=20, window_seconds=60)
        elif path == "/optimize":
            check_rate_limit(f"optimize:{client_ip}", max_requests=10, window_seconds=60)
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
    time_window_start: Optional[str] = None
    time_window_end: Optional[str] = None


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


# === ENDPOINTS BÁSICOS ===

@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "Xpedit API",
        "version": "0.5.0",
        "stripe_ok": bool(STRIPE_SECRET_KEY),
        "jwks_ok": _jwks_client is not None,
    }


APK_DOWNLOAD_URL = "https://github.com/direccion-bit2/xpedit-releases/releases/download/v1.1.4/xpedit-latest.apk"


@app.get("/download/apk")
async def download_apk(request: Request):
    """Track APK download (unique device fingerprint) and redirect to GitHub"""
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
        print(f"[DOWNLOAD] Error tracking: {e}")

    return RedirectResponse(url=APK_DOWNLOAD_URL, status_code=302)


@app.post("/optimize")
async def optimize(request: OptimizeRequest, user=Depends(get_current_user)):
    if len(request.locations) > 100:
        raise HTTPException(status_code=400, detail="Máximo 100 paradas")

    locations_data = [loc.model_dump() for loc in request.locations]
    result = optimize_route(locations=locations_data, depot_index=request.start_index or 0)
    return result


@app.post("/geocode")
async def geocode(request: GeocodeRequest, user=Depends(get_current_user)):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": request.address, "format": "json", "limit": 1},
                headers={"User-Agent": "RutaMax/0.2"},
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
            print(f"[GEOCODE] Error: {e}")
            raise HTTPException(status_code=500, detail="Error interno del servidor")


# === ENDPOINTS AVANZADOS DE OPTIMIZACIÓN ===

@app.post("/optimize-multi")
async def optimize_multi(request: MultiVehicleOptimizeRequest, user=Depends(get_current_user)):
    """Optimiza rutas para múltiples vehículos (CVRP)"""
    if len(request.locations) > 200:
        raise HTTPException(status_code=400, detail="Máximo 200 paradas para multi-vehicle")

    locations_data = [loc.model_dump() for loc in request.locations]

    max_distance = None
    if request.max_distance_per_vehicle_km:
        max_distance = int(request.max_distance_per_vehicle_km * 1000)

    result = optimize_multi_vehicle(
        locations=locations_data,
        num_vehicles=request.num_vehicles,
        depot_index=request.depot_index or 0,
        max_distance_per_vehicle=max_distance
    )
    return result


@app.post("/cluster-zones")
async def cluster_zones(request: ClusterRequest, user=Depends(get_current_user)):
    """Agrupa paradas en zonas geográficas"""
    if len(request.stops) > 500:
        raise HTTPException(status_code=400, detail="Máximo 500 paradas para clustering")

    stops_data = [stop.model_dump() for stop in request.stops]

    result = cluster_stops_by_zone(
        stops=stops_data,
        n_zones=request.n_zones,
        max_stops_per_zone=request.max_stops_per_zone or 15
    )
    return result


@app.post("/eta")
async def get_eta(request: ETARequest, user=Depends(get_current_user)):
    """Calcula ETA entre dos puntos"""
    result = calculate_eta(
        current_location=(request.current_lat, request.current_lng),
        destination=(request.destination_lat, request.destination_lng),
        avg_speed_kmh=request.avg_speed_kmh or 30.0,
        stop_time_minutes=request.stop_time_minutes or 5.0
    )
    return {"success": True, **result}


@app.post("/route-etas")
async def get_route_etas(request: RouteETARequest, user=Depends(get_current_user)):
    """Calcula ETAs para todas las paradas de una ruta"""
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


@app.post("/assign-drivers")
async def assign_drivers(request: AssignDriversRequest, user=Depends(get_current_user)):
    """Asigna conductores a zonas de forma inteligente"""
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


@app.get("/stats/daily")
async def get_daily_stats(company_id: Optional[str] = None, user=Depends(get_current_user)):
    """Obtiene estadísticas del día - filtradas por permisos del usuario"""
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        # Obtener rutas filtradas por permisos
        query = supabase.table("routes").select("*, stops(*)")
        # Filter by today's date
        query = query.gte("created_at", f"{today}T00:00:00")
        if user["role"] == "admin":
            if company_id:
                query = query.eq("company_id", company_id)
        elif user["role"] == "dispatcher" and user.get("company_id"):
            company_drivers = supabase.table("drivers").select("id").eq("company_id", user["company_id"]).execute()
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
        print(f"[STATS] Error: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# === ENDPOINTS SUPABASE ===

# -- Conductores --

@app.get("/drivers")
async def get_drivers(user=Depends(get_current_user)):
    """Lista conductores - admin ve todos, dispatcher ve su empresa, driver ve solo él"""
    query = supabase.table("drivers").select("*").eq("active", True)
    if user["role"] == "admin":
        pass  # Admin sees all
    elif user["role"] == "dispatcher" and user.get("company_id"):
        query = query.eq("company_id", user["company_id"])
    else:
        query = query.eq("user_id", user["id"])
    result = query.execute()
    return {"drivers": result.data}


@app.get("/drivers/{driver_id}")
async def get_driver(driver_id: str, user=Depends(get_current_user)):
    """Obtiene un conductor por ID - verificando acceso"""
    await verify_driver_access(driver_id, user)
    result = supabase.table("drivers").select("*").eq("id", driver_id).single().execute()
    return result.data


# -- Rutas --

@app.get("/routes")
async def get_routes(driver_id: Optional[str] = None, date: Optional[str] = None, user=Depends(get_current_user)):
    """Lista rutas - filtradas por propiedad del usuario"""
    query = supabase.table("routes").select("*, stops(*)")

    if user["role"] == "admin":
        if driver_id:
            query = query.eq("driver_id", driver_id)
    elif user["role"] == "dispatcher" and user.get("company_id"):
        # Dispatcher: only routes from drivers in their company
        company_drivers = supabase.table("drivers").select("id").eq("company_id", user["company_id"]).execute()
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


@app.post("/routes")
async def create_route(route: RouteCreate, user=Depends(get_current_user)):
    """Crea una nueva ruta con sus paradas"""
    # Verify user can create route for this driver
    if user["role"] != "admin":
        user_driver_id = await get_user_driver_id(user)
        if route.driver_id != user_driver_id:
            raise HTTPException(status_code=403, detail="No puedes crear rutas para otro conductor")
    # Crear la ruta
    route_data = {
        "driver_id": route.driver_id,
        "name": route.name or f"Ruta {datetime.now().strftime('%d/%m %H:%M')}",
        "total_distance_km": route.total_distance_km,
        "total_stops": len(route.stops),
        "status": "pending"
    }

    route_result = supabase.table("routes").insert(route_data).execute()
    route_id = route_result.data[0]["id"]

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
            "time_window_start": stop.time_window_start,
            "time_window_end": stop.time_window_end,
        }
        for stop in route.stops
    ]

    supabase.table("stops").insert(stops_data).execute()

    # Devolver ruta completa
    result = supabase.table("routes").select("*, stops(*)").eq("id", route_id).single().execute()
    return result.data


@app.get("/routes/{route_id}")
async def get_route(route_id: str, user=Depends(get_current_user)):
    """Obtiene una ruta con sus paradas - verificando acceso"""
    await verify_route_access(route_id, user)
    result = supabase.table("routes").select("*, stops(*)").eq("id", route_id).single().execute()
    return result.data


@app.patch("/routes/{route_id}/start")
async def start_route(route_id: str, user=Depends(get_current_user)):
    """Marca una ruta como iniciada - verificando acceso"""
    await verify_route_access(route_id, user)
    result = supabase.table("routes").update({
        "status": "in_progress",
        "started_at": datetime.now().isoformat()
    }).eq("id", route_id).execute()
    return {"success": True, "route": result.data[0]}


@app.patch("/routes/{route_id}/complete")
async def complete_route(route_id: str, user=Depends(get_current_user)):
    """Marca una ruta como completada - verificando acceso"""
    await verify_route_access(route_id, user)
    result = supabase.table("routes").update({
        "status": "completed",
        "completed_at": datetime.now().isoformat()
    }).eq("id", route_id).execute()
    return {"success": True, "route": result.data[0]}


@app.delete("/routes/{route_id}")
async def delete_route(route_id: str, user=Depends(get_current_user)):
    """Elimina una ruta y todas sus dependencias - verificando acceso"""
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

@app.patch("/stops/{stop_id}/complete")
async def complete_stop(stop_id: str, user=Depends(get_current_user)):
    """Marca una parada como completada - verificando acceso"""
    await verify_stop_access(stop_id, user)
    result = supabase.table("stops").update({
        "status": "completed",
        "completed_at": datetime.now().isoformat()
    }).eq("id", stop_id).execute()
    return {"success": True, "stop": result.data[0]}


@app.patch("/stops/{stop_id}/fail")
async def fail_stop(stop_id: str, user=Depends(get_current_user)):
    """Marca una parada como fallida - verificando acceso"""
    await verify_stop_access(stop_id, user)
    result = supabase.table("stops").update({
        "status": "failed",
        "completed_at": datetime.now().isoformat()
    }).eq("id", stop_id).execute()
    return {"success": True, "stop": result.data[0]}


# -- GPS Tracking --

@app.post("/location")
async def update_location(location: LocationUpdate, user=Depends(get_current_user)):
    """Registra la ubicación actual del conductor - fuerza driver_id del usuario"""
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
    return {"success": True, "id": result.data[0]["id"]}


@app.get("/location/{driver_id}/latest")
async def get_latest_location(driver_id: str, user=Depends(get_current_user)):
    """Obtiene la última ubicación conocida de un conductor - verificando acceso"""
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


@app.get("/location/{driver_id}/history")
async def get_location_history(driver_id: str, route_id: Optional[str] = None, limit: int = 100, user=Depends(get_current_user)):
    """Obtiene el historial de ubicaciones de un conductor - verificando acceso"""
    await verify_driver_access(driver_id, user)
    query = supabase.table("location_history")\
        .select("*")\
        .eq("driver_id", driver_id)

    if route_id:
        query = query.eq("route_id", route_id)

    result = query.order("recorded_at", desc=True).limit(limit).execute()
    return {"locations": result.data}


# === EMAILS ===

@app.post("/email/welcome")
async def api_send_welcome_email(request: WelcomeEmailRequest, user=Depends(get_current_user)):
    """Envía email de bienvenida a nuevo usuario"""
    result = send_welcome_email(request.to_email, request.user_name)
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "Error enviando email"))
    return result


@app.post("/email/delivery-started")
async def api_send_delivery_started_email(request: DeliveryStartedEmailRequest, user=Depends(get_current_user)):
    """Envía email cuando el pedido está en camino"""
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


@app.post("/email/delivery-completed")
async def api_send_delivery_completed_email(request: DeliveryCompletedEmailRequest, user=Depends(get_current_user)):
    """Envía email de confirmación de entrega"""
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


@app.post("/email/delivery-failed")
async def api_send_delivery_failed_email(request: DeliveryFailedEmailRequest, user=Depends(get_current_user)):
    """Envía email cuando la entrega falla"""
    result = send_delivery_failed_email(
        request.to_email,
        request.client_name,
        request.reason,
        request.next_attempt
    )
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "Error enviando email"))
    return result


@app.post("/email/daily-summary")
async def api_send_daily_summary_email(request: DailySummaryEmailRequest, user=Depends(get_current_user)):
    """Envía resumen diario al dispatcher"""
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

@app.post("/promo/redeem")
async def redeem_promo_code(request: PromoRedeemRequest, user=Depends(get_current_user)):
    """Redeem a promo code for a user"""
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
            if datetime.now(expires_at.tzinfo) > expires_at:
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
        now = datetime.now()
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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.get("/promo/check/{driver_id}")
async def check_promo_benefit(driver_id: str, user=Depends(get_current_user)):
    """Check if a driver has an active promo benefit - only own data or admin"""
    # Verify ownership: look up driver and check user_id matches authenticated user
    driver_check = supabase.table("drivers").select("user_id").eq("id", driver_id).single().execute()
    if not driver_check.data:
        raise HTTPException(status_code=404, detail="Driver no encontrado")
    if user["role"] != "admin" and user["id"] != driver_check.data["user_id"]:
        raise HTTPException(status_code=403, detail="No tienes acceso a estos datos")
    try:
        result = supabase.table("drivers")\
            .select("promo_plan, promo_plan_expires_at")\
            .eq("id", driver_id)\
            .single()\
            .execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="User not found")

        driver = result.data
        promo_plan = driver.get("promo_plan")
        expires_at_str = driver.get("promo_plan_expires_at")

        if not promo_plan:
            return {
                "has_promo": False,
                "plan": None,
                "expires_at": None,
                "days_remaining": 0,
                "permanent": False
            }

        # Permanent plan (no expiration date)
        if not expires_at_str:
            return {
                "has_promo": True,
                "plan": promo_plan,
                "expires_at": None,
                "days_remaining": -1,
                "permanent": True
            }

        expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
        now = datetime.now(expires_at.tzinfo) if expires_at.tzinfo else datetime.now()
        remaining = expires_at - now
        days_remaining = max(0, remaining.days)

        has_promo = days_remaining > 0

        return {
            "has_promo": has_promo,
            "plan": promo_plan if has_promo else None,
            "expires_at": expires_at_str if has_promo else None,
            "days_remaining": days_remaining,
            "permanent": False
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# === ADMIN ENDPOINTS ===

@app.get("/admin/promo-codes")
async def list_promo_codes(user=Depends(require_admin)):
    """List all promo codes with their stats (admin)"""
    try:
        result = supabase.table("promo_codes")\
            .select("*")\
            .order("created_at", desc=True)\
            .execute()

        return {"success": True, "promo_codes": result.data}

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.post("/admin/promo-codes")
async def create_promo_code(request: PromoCodeCreateRequest, user=Depends(require_admin)):
    """Create a new promo code (admin)"""
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

        return {"success": True, "promo_code": result.data[0]}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.patch("/admin/promo-codes/{code_id}")
async def update_promo_code(code_id: str, request: PromoCodeUpdateRequest, user=Depends(require_admin)):
    """Update a promo code (admin)"""
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

        return {"success": True, "promo_code": result.data[0]}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.get("/admin/users")
async def list_admin_users(user=Depends(require_admin)):
    """List all users/drivers with promo status (admin)"""
    try:
        result = supabase.table("drivers")\
            .select("*")\
            .order("created_at", desc=True)\
            .execute()

        return {"success": True, "users": result.data}

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.patch("/admin/users/{user_id}/grant")
async def grant_plan(user_id: str, request: AdminGrantRequest, user=Depends(require_admin)):
    """Grant plan to a user - permanent or temporary (admin)"""
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
            now = datetime.now()
            expires_at = now + timedelta(days=request.days)
            expires_at_iso = expires_at.isoformat()
            update_data = {"promo_plan": request.plan, "promo_plan_expires_at": expires_at_iso}
            message = f"Granted {request.days} days of {request.plan} to user."

        result = supabase.table("drivers").update(update_data).eq("id", user_id).execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="User not found")

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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


class AdminResetPasswordRequest(BaseModel):
    password: Optional[str] = None  # If None, generate random


@app.post("/admin/users/{user_id}/reset-password")
async def admin_reset_password(user_id: str, request: AdminResetPasswordRequest, user=Depends(require_admin)):
    """Reset a user's password (admin only)"""
    try:
        # Generate random password if not provided
        chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789!@#"
        new_password = request.password or "".join(random.choices(chars, k=12))

        if len(new_password) < 6:
            raise HTTPException(status_code=400, detail="Password must be at least 6 characters")

        # Update password via Supabase Admin API
        result = supabase.auth.admin.update_user_by_id(user_id, {"password": new_password})

        if not result:
            raise HTTPException(status_code=404, detail="User not found")

        return {
            "success": True,
            "user_id": user_id,
            "password": new_password,
            "message": "Password reset successfully."
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


class AdminCreateCompanyRequest(BaseModel):
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    payment_model: str = "driver_pays"


@app.post("/admin/companies")
async def admin_create_company(request: AdminCreateCompanyRequest, user=Depends(require_admin)):
    """Create a company from admin panel"""
    try:
        result = supabase.table("companies").insert({
            "name": request.name,
            "email": request.email,
            "phone": request.phone,
            "payment_model": request.payment_model,
            "active": True,
        }).execute()

        if not result.data:
            raise HTTPException(status_code=500, detail="Failed to create company")

        company = result.data[0]

        # Create trial subscription
        supabase.table("company_subscriptions").insert({
            "company_id": company["id"],
            "plan": "free",
            "max_drivers": 15,
            "price_per_month": 0,
            "status": "trialing",
            "trial_ends_at": (datetime.now() + timedelta(days=14)).isoformat(),
            "current_period_start": datetime.now().isoformat(),
            "current_period_end": (datetime.now() + timedelta(days=14)).isoformat(),
        }).execute()

        return {"success": True, "company": company}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# === REFERRAL SYSTEM ===

INVITE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


class ReferralRedeemRequest(BaseModel):
    referral_code: str


@app.get("/referral/code")
async def get_referral_code(user=Depends(get_current_user)):
    """Get or generate the user's referral code"""
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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.post("/referral/redeem")
async def redeem_referral(request: ReferralRedeemRequest, user=Depends(get_current_user)):
    """Redeem a referral code (new user gets 7 days Pro, referrer gets 7 days Pro)"""
    try:
        referred_driver_id = await get_user_driver_id(user)
        if not referred_driver_id:
            raise HTTPException(status_code=404, detail="Driver not found")

        code = request.referral_code.strip().upper()

        # Find referrer
        referrer = supabase.table("drivers").select("id, referral_code").eq("referral_code", code).single().execute()
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
        now = datetime.now()
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

        return {
            "success": True,
            "reward_days": REWARD_DAYS,
            "reward_plan": REWARD_PLAN,
            "message": f"Codigo canjeado. {REWARD_DAYS} dias de {REWARD_PLAN} para ti y para quien te invito."
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@app.get("/referral/stats")
async def get_referral_stats(user=Depends(get_current_user)):
    """Get referral stats for the current user"""
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
        print(f"[ERROR] {type(e).__name__}: {e}")
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
@app.post("/company/register")
async def register_company(request: CompanyRegisterRequest, user=Depends(get_current_user)):
    """Register a new company and set up owner"""
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

        if not company_result.data:
            raise HTTPException(status_code=500, detail="Failed to create company")

        company = company_result.data[0]
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

        # Create subscription with 14-day trial
        now = datetime.now()
        trial_end = now + timedelta(days=14)
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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 15. GET /company/check-access/{driver_id}
# NOTE: Defined before /company/{company_id} to avoid route shadowing
@app.get("/company/check-access/{driver_id}")
async def check_company_access(driver_id: str, user=Depends(get_current_user)):
    """Check if a driver has company-paid access - only own data or admin"""
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

        if not link_result.data:
            return {"has_access": False}

        link = link_result.data[0]
        mode = link.get("mode", "driver_pays")

        if mode in ("company_pays", "company_complete"):
            # Get company name
            company_result = supabase.table("companies")\
                .select("name")\
                .eq("id", link["company_id"])\
                .limit(1)\
                .execute()

            company_name = company_result.data[0]["name"] if company_result.data else None

            return {
                "has_access": True,
                "plan": "pro_plus",
                "company_name": company_name,
            }

        return {"has_access": False}

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 2. GET /company/{company_id}
@app.get("/company/{company_id}")
async def get_company(company_id: str, user=Depends(get_current_user)):
    """Get company details with subscription info"""
    # Authorization: user must belong to this company or be admin
    if user["company_id"] != company_id and user["role"] != "admin":
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

        subscription = sub_result.data[0] if sub_result.data else None

        return {
            "success": True,
            "company": company_result.data,
            "subscription": subscription,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 3. PATCH /company/{company_id}
@app.patch("/company/{company_id}")
async def update_company(company_id: str, request: CompanyUpdateRequest, user=Depends(get_current_user)):
    """Update company details"""
    # Authorization: user must belong to this company or be admin
    if user["company_id"] != company_id and user["role"] != "admin":
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

        update_data["updated_at"] = datetime.now().isoformat()

        result = supabase.table("companies")\
            .update(update_data)\
            .eq("id", company_id)\
            .execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="Company not found")

        return {"success": True, "company": result.data[0]}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 4. GET /company/{company_id}/drivers
@app.get("/company/{company_id}/drivers")
async def get_company_drivers(company_id: str, user=Depends(get_current_user)):
    """List drivers in a company with mode, cost, plan info"""
    # Authorization: user must belong to this company or be admin
    if user["company_id"] != company_id and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="No tienes acceso a esta empresa")

    try:
        # Get all driver links for this company (including inactive)
        links_result = supabase.table("company_driver_links")\
            .select("*")\
            .eq("company_id", company_id)\
            .execute()

        links = links_result.data or []
        drivers_list = []

        for link in links:
            # Get driver info
            driver_result = supabase.table("drivers")\
                .select("*")\
                .eq("user_id", link["user_id"])\
                .limit(1)\
                .execute()

            # Get user info
            user_result = supabase.table("users")\
                .select("id, email, full_name, phone, role")\
                .eq("id", link["user_id"])\
                .limit(1)\
                .execute()

            driver_data = driver_result.data[0] if driver_result.data else {}
            user_data = user_result.data[0] if user_result.data else {}

            drivers_list.append({
                "link_id": link["id"],
                "user_id": link["user_id"],
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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 5. GET /company/{company_id}/stats
@app.get("/company/{company_id}/stats")
async def get_company_stats(company_id: str, user=Depends(get_current_user)):
    """Get fleet stats: total drivers, active today, routes/stops/deliveries today"""
    # Authorization: user must belong to this company or be admin
    if user["company_id"] != company_id and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="No tienes acceso a esta empresa")

    try:
        today = datetime.now().strftime("%Y-%m-%d")

        # Total drivers in company
        links_result = supabase.table("company_driver_links")\
            .select("user_id")\
            .eq("company_id", company_id)\
            .eq("active", True)\
            .execute()
        total_drivers = len(links_result.data or [])
        driver_user_ids = [l["user_id"] for l in (links_result.data or [])]

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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 6. POST /company/invites
@app.post("/company/invites")
async def create_company_invite(request: CompanyInviteRequest, user=Depends(get_current_user)):
    """Generate an invite code for a company - admin/dispatcher only"""
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

        expires_at = (datetime.now() + timedelta(hours=request.expires_hours)).isoformat()

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

        if not result.data:
            raise HTTPException(status_code=500, detail="Failed to create invite")

        return {"success": True, "invite": result.data[0]}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 7. GET /company/{company_id}/invites
@app.get("/company/{company_id}/invites")
async def get_company_invites(company_id: str, user=Depends(get_current_user)):
    """List invite codes for a company"""
    # Authorization: user must belong to this company or be admin
    if user["company_id"] != company_id and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="No tienes acceso a esta empresa")

    try:
        result = supabase.table("company_invites")\
            .select("*")\
            .eq("company_id", company_id)\
            .order("created_at", desc=True)\
            .execute()

        return {"success": True, "invites": result.data or []}

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 8. DELETE /company/invites/{invite_id}
@app.delete("/company/invites/{invite_id}")
async def deactivate_company_invite(invite_id: str, user=Depends(get_current_user)):
    """Deactivate an invite code - verify ownership"""
    try:
        # Verify invite belongs to user's company
        invite_check = supabase.table("company_invites").select("company_id").eq("id", invite_id).limit(1).execute()
        if invite_check.data:
            await verify_company_management(user, invite_check.data[0]["company_id"])
        result = supabase.table("company_invites")\
            .update({"active": False})\
            .eq("id", invite_id)\
            .execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="Invite not found")

        return {"success": True, "invite": result.data[0]}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 9. POST /company/join
@app.post("/company/join")
async def join_company(request: CompanyJoinRequest, user=Depends(get_current_user)):
    """Driver joins a company via invite code"""
    try:
        # Use authenticated user's ID instead of request body
        user_id = user["id"]
        code = request.code.strip().upper()

        # Find the invite
        invite_result = supabase.table("company_invites")\
            .select("*")\
            .eq("code", code)\
            .execute()

        if not invite_result.data:
            raise HTTPException(status_code=404, detail="Invite code not found")

        invite = invite_result.data[0]

        # Validate: active
        if not invite.get("active", False):
            raise HTTPException(status_code=400, detail="This invite code is no longer active")

        # Validate: not expired
        if invite.get("expires_at"):
            expires_at = datetime.fromisoformat(invite["expires_at"].replace("Z", "+00:00"))
            now = datetime.now(expires_at.tzinfo) if expires_at.tzinfo else datetime.now()
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

        driver_id = driver_result.data[0]["id"] if driver_result.data else None
        driver_plan = driver_result.data[0].get("promo_plan") if driver_result.data else None

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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 10. POST /company/leave
@app.post("/company/leave")
async def leave_company(request: CompanyLeaveRequest, user=Depends(get_current_user)):
    """Driver leaves their company"""
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

        if not link_result.data:
            raise HTTPException(status_code=404, detail="User is not linked to any company")

        link = link_result.data[0]
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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 11. POST /company/drivers
@app.post("/company/drivers")
async def create_company_driver(request: CompanyCreateDriverRequest, user=Depends(get_current_user)):
    """Create a driver directly - admin/dispatcher of the company only"""
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

        driver_id = driver_result.data[0]["id"] if driver_result.data else None

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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 12. DELETE /company/drivers/{user_id}
@app.delete("/company/drivers/{user_id}")
async def remove_company_driver(user_id: str, user=Depends(get_current_user)):
    """Remove a driver from the company - admin/dispatcher only"""
    try:
        # Get current driver link to check mode and verify company ownership
        link_result = supabase.table("company_driver_links")\
            .select("*")\
            .eq("user_id", user_id)\
            .eq("active", True)\
            .limit(1)\
            .execute()

        if not link_result.data:
            raise HTTPException(status_code=404, detail="Driver is not linked to any company")

        link = link_result.data[0]
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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 13b. PATCH /company/drivers/{user_id}/active - toggle driver active/inactive
@app.patch("/company/drivers/{user_id}/active")
async def toggle_driver_active(user_id: str, user=Depends(get_current_user)):
    """Toggle a driver's active status - admin/dispatcher of company only"""
    try:
        # Get current driver link
        link_result = supabase.table("company_driver_links")\
            .select("*")\
            .eq("user_id", user_id)\
            .limit(1)\
            .execute()

        if not link_result.data:
            raise HTTPException(status_code=404, detail="Driver link not found")

        link = link_result.data[0]
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
            period_end = sub_result.data[0].get("current_period_end") if sub_result.data else None

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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 13. PATCH /company/drivers/{user_id}/mode
@app.patch("/company/drivers/{user_id}/mode")
async def change_driver_mode(user_id: str, request: CompanyDriverModeRequest, user=Depends(get_current_user)):
    """Change a driver's payment mode - admin/dispatcher of company only"""
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

        if not link_result.data:
            raise HTTPException(status_code=404, detail="Driver is not linked to any company")

        link = link_result.data[0]
        await verify_company_management(user, link["company_id"])
        company_id = link["company_id"]

        # Get subscription for period end date
        sub_result = supabase.table("company_subscriptions")\
            .select("*")\
            .eq("company_id", company_id)\
            .order("created_at", desc=True)\
            .limit(1)\
            .execute()

        subscription = sub_result.data[0] if sub_result.data else None
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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# 14. GET /company/{company_id}/subscription
@app.get("/company/{company_id}/subscription")
async def get_company_subscription(company_id: str, user=Depends(get_current_user)):
    """Get company subscription details"""
    # Authorization: user must belong to this company or be admin
    if user["company_id"] != company_id and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="No tienes acceso a esta empresa")

    try:
        result = supabase.table("company_subscriptions")\
            .select("*")\
            .eq("company_id", company_id)\
            .order("created_at", desc=True)\
            .limit(1)\
            .execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="No subscription found for this company")

        return {"success": True, "subscription": result.data[0]}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# === OCR PROXY ===

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


class OCRLabelRequest(BaseModel):
    image_base64: str = Field(..., max_length=10_000_000)  # ~7.5MB max image
    media_type: str = "image/jpeg"


@app.post("/ocr/label")
async def ocr_label(request: OCRLabelRequest, user=Depends(get_current_user)):
    """Proxy OCR request to Anthropic API - keeps API key server-side"""
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
                    "model": "claude-3-haiku-20240307",
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
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# === STRIPE CHECKOUT ===


class StripeCheckoutRequest(BaseModel):
    plan: str  # "pro" or "pro_plus"


@app.post("/stripe/create-checkout")
async def create_stripe_checkout(request: StripeCheckoutRequest, user=Depends(get_current_user)):
    """Create a Stripe Checkout Session for subscription"""
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe not configured")

    plan_info = STRIPE_PLANS.get(request.plan)
    if not plan_info:
        raise HTTPException(status_code=400, detail="Plan invalido. Use 'pro' o 'pro_plus'")

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            line_items=[{
                "price_data": {
                    "currency": "eur",
                    "product_data": {"name": plan_info["name"]},
                    "unit_amount": plan_info["amount"],
                    "recurring": {"interval": plan_info["interval"]},
                },
                "quantity": 1,
            }],
            client_reference_id=user["id"],
            metadata={"plan": request.plan, "user_id": user["id"]},
            success_url="https://xpedit.es/dashboard?payment=success",
            cancel_url="https://xpedit.es/dashboard?payment=cancel",
        )
        return {"success": True, "url": session.url}

    except stripe.StripeError as e:
        print(f"[STRIPE] Error: {e}")
        raise HTTPException(status_code=500, detail="Error en el servicio de pago")


@app.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    """Handle Stripe webhook events - no JWT auth, uses Stripe signature"""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    if not STRIPE_WEBHOOK_SECRET:
        print("[STRIPE WEBHOOK] STRIPE_WEBHOOK_SECRET not configured - rejecting")
        raise HTTPException(status_code=500, detail="Webhook not configured")
    if not sig_header:
        print("[STRIPE WEBHOOK] Missing stripe-signature header - rejecting")
        raise HTTPException(status_code=400, detail="Missing signature")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except stripe.SignatureVerificationError:
        print("[STRIPE WEBHOOK] Invalid signature")
        raise HTTPException(status_code=400, detail="Invalid signature")
    except Exception as e:
        print(f"[STRIPE WEBHOOK] Parse error: {e}")
        raise HTTPException(status_code=400, detail="Invalid payload")

    event_type = event.type
    print(f"[STRIPE WEBHOOK] Received event: {event_type}")

    try:
        data_obj = event.data.object

        if event_type == "checkout.session.completed":
            user_id = getattr(data_obj, "client_reference_id", None)
            metadata = getattr(data_obj, "metadata", {})
            plan = metadata.get("plan", "pro") if isinstance(metadata, dict) else getattr(metadata, "plan", "pro")
            customer_id = getattr(data_obj, "customer", None)

            print(f"[STRIPE WEBHOOK] checkout.session.completed: user_id={user_id}, plan={plan}, customer={customer_id}")

            if user_id:
                expires_at = (datetime.now() + timedelta(days=30)).isoformat()
                # Update drivers table (if user has a linked driver)
                supabase.table("drivers").update({
                    "promo_plan": plan,
                    "promo_plan_expires_at": expires_at,
                }).eq("user_id", user_id).execute()
                # Update users table (plan + stripe customer id)
                supabase.table("users").update({
                    "stripe_customer_id": customer_id,
                    "promo_plan": plan,
                    "promo_plan_expires_at": expires_at,
                }).eq("id", user_id).execute()
                print(f"[STRIPE WEBHOOK] Plan {plan} activated for user {user_id}")

        elif event_type == "customer.subscription.deleted":
            customer_id = getattr(data_obj, "customer", None)
            if customer_id:
                user_result = supabase.table("users").select("id").eq("stripe_customer_id", customer_id).limit(1).execute()
                if user_result.data:
                    user_id = user_result.data[0]["id"]
                    supabase.table("drivers").update({
                        "promo_plan": None,
                        "promo_plan_expires_at": None,
                    }).eq("user_id", user_id).execute()
                    supabase.table("users").update({
                        "promo_plan": None,
                        "promo_plan_expires_at": None,
                    }).eq("id", user_id).execute()
                    print(f"[STRIPE WEBHOOK] Subscription deleted for user {user_id}")

        elif event_type in ("invoice.payment_succeeded", "invoice.paid"):
            customer_id = getattr(data_obj, "customer", None)
            billing_reason = getattr(data_obj, "billing_reason", None)
            if customer_id and billing_reason == "subscription_cycle":
                user_result = supabase.table("users").select("id").eq("stripe_customer_id", customer_id).limit(1).execute()
                if user_result.data:
                    user_id = user_result.data[0]["id"]
                    expires_at = (datetime.now() + timedelta(days=30)).isoformat()
                    supabase.table("drivers").update({
                        "promo_plan_expires_at": expires_at,
                    }).eq("user_id", user_id).execute()
                    supabase.table("users").update({
                        "promo_plan_expires_at": expires_at,
                    }).eq("id", user_id).execute()
                    print(f"[STRIPE WEBHOOK] Renewal for user {user_id}")

        else:
            print(f"[STRIPE WEBHOOK] Unhandled event type: {event_type} (ignored)")

    except Exception as e:
        print(f"[STRIPE WEBHOOK] Error processing {event_type}: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Internal webhook error")

    return {"received": True}


@app.post("/stripe/portal")
async def create_stripe_portal(user=Depends(get_current_user)):
    """Create a Stripe Customer Portal session to manage subscription"""
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
        print(f"[STRIPE] Error: {e}")
        raise HTTPException(status_code=500, detail="Error en el servicio de pago")
    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# === GOOGLE PLACES PROXY ===
# Proxy to avoid API key restrictions on mobile clients
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

@app.get("/places/autocomplete")
async def places_autocomplete(input: str, lat: Optional[float] = None, lng: Optional[float] = None, user=Depends(get_current_user)):
    """Proxy for Google Places Autocomplete API"""
    params = {
        "input": input,
        "types": "address",
        "components": "country:es",
        "language": "es",
        "key": GOOGLE_API_KEY,
    }
    if lat and lng:
        params["location"] = f"{lat},{lng}"
        params["radius"] = "50000"

    async with httpx.AsyncClient() as client:
        resp = await client.get("https://maps.googleapis.com/maps/api/place/autocomplete/json", params=params)
        data = resp.json()

    if data.get("status") != "OK":
        # Fallback: Nominatim (OpenStreetMap) - free, no API key
        async with httpx.AsyncClient() as client:
            nom_resp = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={
                    "q": f"{input}, España",
                    "format": "json",
                    "addressdetails": "1",
                    "limit": "5",
                    "accept-language": "es",
                },
                headers={"User-Agent": "Xpedit/1.1"}
            )
            nom_data = nom_resp.json()

        if nom_data:
            results = [{"place_id": None, "display_name": r["display_name"], "lat": r["lat"], "lon": r["lon"], "source": "nominatim"} for r in nom_data]
            return {"status": "OK", "predictions": results, "source": "nominatim"}
        return {"status": "ZERO_RESULTS", "predictions": []}

    return data


@app.get("/places/details")
async def places_details(place_id: str, user=Depends(get_current_user)):
    """Proxy for Google Places Details API"""
    params = {
        "place_id": place_id,
        "fields": "geometry,address_components,formatted_address",
        "language": "es",
        "key": GOOGLE_API_KEY,
    }
    async with httpx.AsyncClient() as client:
        resp = await client.get("https://maps.googleapis.com/maps/api/place/details/json", params=params)
    return resp.json()


@app.get("/places/directions")
async def places_directions(
    origin: str,
    destination: str,
    waypoints: Optional[str] = None,
    user=Depends(get_current_user)
):
    """Proxy for Google Directions API"""
    params = {
        "origin": origin,
        "destination": destination,
        "key": GOOGLE_API_KEY,
        "language": "es",
    }
    if waypoints:
        params["waypoints"] = waypoints
    async with httpx.AsyncClient() as client:
        resp = await client.get("https://maps.googleapis.com/maps/api/directions/json", params=params)
    return resp.json()


  # Street View proxy removed - app opens Google Maps directly (free, no API cost)


# === ACCOUNT DELETION ===

@app.delete("/auth/delete-account")
async def delete_account(user=Depends(get_current_user)):
    """Delete user account and all associated data (Apple/GDPR requirement)"""
    user_id = user["id"]
    try:
        # First, find the driver_id for this user
        driver_result = supabase.table("drivers").select("id").eq("user_id", user_id).execute()
        driver_id = driver_result.data[0]["id"] if driver_result.data else None

        if driver_id:
            # Get all route IDs for this driver (needed for stops and delivery_proofs)
            routes_result = supabase.table("routes").select("id").eq("driver_id", driver_id).execute()
            route_ids = [r["id"] for r in (routes_result.data or [])]

            if route_ids:
                # Delete stops for all driver's routes
                for route_id in route_ids:
                    try:
                        # Delete delivery_proofs for stops in this route
                        stops_result = supabase.table("stops").select("id").eq("route_id", route_id).execute()
                        stop_ids = [s["id"] for s in (stops_result.data or [])]
                        if stop_ids:
                            for stop_id in stop_ids:
                                try:
                                    supabase.table("delivery_proofs").delete().eq("stop_id", stop_id).execute()
                                except Exception:
                                    pass
                        # Delete tracking_links for this route
                        try:
                            supabase.table("tracking_links").delete().eq("route_id", route_id).execute()
                        except Exception:
                            pass
                        # Delete stops
                        supabase.table("stops").delete().eq("route_id", route_id).execute()
                    except Exception:
                        pass

                # Delete all routes
                try:
                    supabase.table("routes").delete().eq("driver_id", driver_id).execute()
                except Exception:
                    pass

            # Delete recurring_places by user_id (created_by stores auth.uid())
            try:
                supabase.table("recurring_places").delete().eq("created_by", user_id).execute()
            except Exception:
                pass

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
                except Exception:
                    pass

            # Delete the driver record
            try:
                supabase.table("drivers").delete().eq("id", driver_id).execute()
            except Exception:
                pass

        # Delete user-level data
        tables_user = [
            ("code_redemptions", "user_id"),
            ("company_driver_links", "user_id"),
            ("company_invites", "user_id"),
        ]
        for table, column in tables_user:
            try:
                supabase.table(table).delete().eq(column, user_id).execute()
            except Exception:
                pass

        # Delete user profile
        try:
            supabase.table("users").delete().eq("id", user_id).execute()
        except Exception:
            pass

        # Delete auth user via Supabase Admin API
        try:
            supabase.auth.admin.delete_user(user_id)
        except Exception:
            pass

        return {"status": "deleted", "message": "Cuenta eliminada correctamente"}
    except Exception as e:
        print(f"[DELETE_ACCOUNT] Error: {e}")
        raise HTTPException(status_code=500, detail="Error al eliminar la cuenta")


# === MAIN ===
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
