"""
RutaMax API - Backend de optimización de rutas
"""

import os
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
import httpx
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

# Inicializar Supabase
supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

app = FastAPI(
    title="RutaMax API",
    description="API de optimización de rutas para entregas de última milla",
    version="0.2.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
    return {"status": "ok", "service": "RutaMax API", "version": "0.2.0"}


@app.post("/optimize")
async def optimize(request: OptimizeRequest):
    if len(request.locations) > 100:
        raise HTTPException(status_code=400, detail="Máximo 100 paradas")

    locations_data = [loc.model_dump() for loc in request.locations]
    result = optimize_route(locations=locations_data, depot_index=request.start_index or 0)
    return result


@app.post("/geocode")
async def geocode(request: GeocodeRequest):
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
            raise HTTPException(status_code=500, detail=str(e))


# === ENDPOINTS AVANZADOS DE OPTIMIZACIÓN ===

@app.post("/optimize-multi")
async def optimize_multi(request: MultiVehicleOptimizeRequest):
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
async def cluster_zones(request: ClusterRequest):
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
async def get_eta(request: ETARequest):
    """Calcula ETA entre dos puntos"""
    result = calculate_eta(
        current_location=(request.current_lat, request.current_lng),
        destination=(request.destination_lat, request.destination_lng),
        avg_speed_kmh=request.avg_speed_kmh or 30.0,
        stop_time_minutes=request.stop_time_minutes or 5.0
    )
    return {"success": True, **result}


@app.post("/route-etas")
async def get_route_etas(request: RouteETARequest):
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
async def assign_drivers(request: AssignDriversRequest):
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
async def get_daily_stats(company_id: Optional[str] = None):
    """Obtiene estadísticas del día para el dashboard"""
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        # Obtener rutas de hoy
        query = supabase.table("routes").select("*, stops(*)")
        if company_id:
            query = query.eq("company_id", company_id)

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
        raise HTTPException(status_code=500, detail=str(e))


# === ENDPOINTS SUPABASE ===

# -- Conductores --

@app.get("/drivers")
async def get_drivers():
    """Lista todos los conductores"""
    result = supabase.table("drivers").select("*").eq("active", True).execute()
    return {"drivers": result.data}


@app.get("/drivers/{driver_id}")
async def get_driver(driver_id: str):
    """Obtiene un conductor por ID"""
    result = supabase.table("drivers").select("*").eq("id", driver_id).single().execute()
    return result.data


# -- Rutas --

@app.get("/routes")
async def get_routes(driver_id: Optional[str] = None, date: Optional[str] = None):
    """Lista rutas, opcionalmente filtradas por conductor o fecha"""
    query = supabase.table("routes").select("*, stops(*)")

    if driver_id:
        query = query.eq("driver_id", driver_id)
    if date:
        query = query.eq("date", date)

    query = query.order("created_at", desc=True)
    result = query.execute()
    return {"routes": result.data}


@app.post("/routes")
async def create_route(route: RouteCreate):
    """Crea una nueva ruta con sus paradas"""
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
async def get_route(route_id: str):
    """Obtiene una ruta con sus paradas"""
    result = supabase.table("routes").select("*, stops(*)").eq("id", route_id).single().execute()
    return result.data


@app.patch("/routes/{route_id}/start")
async def start_route(route_id: str):
    """Marca una ruta como iniciada"""
    result = supabase.table("routes").update({
        "status": "in_progress",
        "started_at": datetime.now().isoformat()
    }).eq("id", route_id).execute()
    return {"success": True, "route": result.data[0]}


@app.patch("/routes/{route_id}/complete")
async def complete_route(route_id: str):
    """Marca una ruta como completada"""
    result = supabase.table("routes").update({
        "status": "completed",
        "completed_at": datetime.now().isoformat()
    }).eq("id", route_id).execute()
    return {"success": True, "route": result.data[0]}


@app.delete("/routes/{route_id}")
async def delete_route(route_id: str):
    """Elimina una ruta y sus paradas"""
    supabase.table("routes").delete().eq("id", route_id).execute()
    return {"success": True}


# -- Paradas --

@app.patch("/stops/{stop_id}/complete")
async def complete_stop(stop_id: str):
    """Marca una parada como completada"""
    result = supabase.table("stops").update({
        "status": "completed",
        "completed_at": datetime.now().isoformat()
    }).eq("id", stop_id).execute()
    return {"success": True, "stop": result.data[0]}


@app.patch("/stops/{stop_id}/fail")
async def fail_stop(stop_id: str):
    """Marca una parada como fallida"""
    result = supabase.table("stops").update({
        "status": "failed",
        "completed_at": datetime.now().isoformat()
    }).eq("id", stop_id).execute()
    return {"success": True, "stop": result.data[0]}


# -- GPS Tracking --

@app.post("/location")
async def update_location(location: LocationUpdate):
    """Registra la ubicación actual del conductor"""
    data = {
        "driver_id": location.driver_id,
        "route_id": location.route_id,
        "lat": location.lat,
        "lng": location.lng,
        "speed": location.speed,
        "accuracy": location.accuracy
    }

    result = supabase.table("location_history").insert(data).execute()
    return {"success": True, "id": result.data[0]["id"]}


@app.get("/location/{driver_id}/latest")
async def get_latest_location(driver_id: str):
    """Obtiene la última ubicación conocida de un conductor"""
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
async def get_location_history(driver_id: str, route_id: Optional[str] = None, limit: int = 100):
    """Obtiene el historial de ubicaciones de un conductor"""
    query = supabase.table("location_history")\
        .select("*")\
        .eq("driver_id", driver_id)

    if route_id:
        query = query.eq("route_id", route_id)

    result = query.order("recorded_at", desc=True).limit(limit).execute()
    return {"locations": result.data}


# === EMAILS ===

@app.post("/email/welcome")
async def api_send_welcome_email(request: WelcomeEmailRequest):
    """Envía email de bienvenida a nuevo usuario"""
    result = send_welcome_email(request.to_email, request.user_name)
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "Error enviando email"))
    return result


@app.post("/email/delivery-started")
async def api_send_delivery_started_email(request: DeliveryStartedEmailRequest):
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
async def api_send_delivery_completed_email(request: DeliveryCompletedEmailRequest):
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
async def api_send_delivery_failed_email(request: DeliveryFailedEmailRequest):
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
async def api_send_daily_summary_email(request: DailySummaryEmailRequest):
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


# === MAIN ===
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
