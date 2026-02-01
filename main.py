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

from optimizer import optimize_route

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


class OptimizeRequest(BaseModel):
    locations: List[Location] = Field(..., min_length=1)
    start_index: Optional[int] = Field(default=0)


class GeocodeRequest(BaseModel):
    address: str = Field(..., min_length=3)


class StopCreate(BaseModel):
    address: str
    lat: float
    lng: float
    position: int
    notes: Optional[str] = None
    phone: Optional[str] = None


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
            "phone": stop.phone
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


# === MAIN ===
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
