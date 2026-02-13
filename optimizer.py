"""
RutaMax - Motor de Optimización de Rutas
Usa Google OR-Tools para resolver el Vehicle Routing Problem (VRP)
Incluye: ETA, clustering por zonas, asignación inteligente, multi-vehicle
"""

from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import math
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict


def haversine_distance(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> int:
    """
    Calcula la distancia en metros entre dos coordenadas GPS.
    Fórmula de Haversine para distancia en esfera.
    """
    lat1, lon1 = coord1
    lat2, lon2 = coord2

    R = 6371000  # Radio de la Tierra en metros

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return int(R * c)


def create_distance_matrix(locations: List[Dict[str, float]]) -> List[List[int]]:
    """
    Crea una matriz de distancias entre todas las ubicaciones.
    locations: Lista de dicts con 'lat' y 'lng'
    """
    n = len(locations)
    matrix = [[0] * n for _ in range(n)]

    for i in range(n):
        for j in range(n):
            if i != j:
                coord1 = (locations[i]['lat'], locations[i]['lng'])
                coord2 = (locations[j]['lat'], locations[j]['lng'])
                matrix[i][j] = haversine_distance(coord1, coord2)

    return matrix


def _parse_time_to_minutes(time_str: Optional[str]) -> Optional[int]:
    """Convierte '09:00' a minutos desde medianoche (540)."""
    if not time_str:
        return None
    try:
        parts = time_str.split(':')
        return int(parts[0]) * 60 + int(parts[1])
    except (ValueError, IndexError):
        return None


def optimize_route(
    locations: List[Dict[str, Any]],
    depot_index: int = 0,
    num_vehicles: int = 1,
    avg_speed_kmh: float = 30.0,
    stop_time_minutes: float = 5.0
) -> Dict[str, Any]:
    """
    Optimiza la ruta para visitar todas las ubicaciones.
    Soporta ventanas horarias (time_window_start, time_window_end) en cada parada.

    Args:
        locations: Lista de paradas con 'lat', 'lng', y opcionalmente 'id', 'address',
                   'time_window_start' ("HH:MM"), 'time_window_end' ("HH:MM")
        depot_index: Índice del punto de inicio (default: primera ubicación)
        num_vehicles: Número de vehículos/conductores
        avg_speed_kmh: Velocidad promedio para calcular tiempos
        stop_time_minutes: Tiempo de servicio por parada

    Returns:
        Dict con la ruta optimizada y métricas
    """
    if len(locations) < 2:
        return {
            "success": True,
            "route": locations,
            "total_distance_meters": 0,
            "total_distance_km": 0,
            "message": "Solo hay una parada, no hay nada que optimizar"
        }

    # Validar depot_index
    if depot_index < 0 or depot_index >= len(locations):
        depot_index = 0

    # Crear matriz de distancias
    distance_matrix = create_distance_matrix(locations)

    # Comprobar si hay ventanas horarias
    has_time_windows = any(
        loc.get('time_window_start') or loc.get('time_window_end')
        for loc in locations
    )

    # Crear el modelo de routing
    manager = pywrapcp.RoutingIndexManager(
        len(locations),  # número de nodos
        num_vehicles,    # número de vehículos
        depot_index      # índice del depósito/inicio
    )

    routing = pywrapcp.RoutingModel(manager)

    # Definir callback de distancia
    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return distance_matrix[from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Si hay ventanas horarias, añadir dimensión de tiempo
    if has_time_windows:
        # Crear callback de tiempo (minutos de viaje entre nodos)
        def time_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            distance_m = distance_matrix[from_node][to_node]
            travel_time_min = int((distance_m / 1000 / avg_speed_kmh) * 60)
            return travel_time_min + int(stop_time_minutes)

        time_callback_index = routing.RegisterTransitCallback(time_callback)

        # Dimensión de tiempo: max 24 horas (1440 minutos)
        routing.AddDimension(
            time_callback_index,
            60,    # max espera (slack) en minutos - esperar hasta 60 min
            1440,  # max tiempo total (24h)
            False, # no forzar start a cero
            'Time'
        )

        time_dimension = routing.GetDimensionOrDie('Time')

        # Hora actual en minutos desde medianoche
        now = datetime.now()
        current_time_minutes = now.hour * 60 + now.minute

        # Aplicar ventanas horarias a cada nodo
        for i, loc in enumerate(locations):
            index = manager.NodeToIndex(i)
            tw_start = _parse_time_to_minutes(loc.get('time_window_start'))
            tw_end = _parse_time_to_minutes(loc.get('time_window_end'))

            if i == depot_index:
                # Depot: empezar ahora (con 5 min de margen)
                time_dimension.CumulVar(index).SetRange(
                    current_time_minutes, current_time_minutes + 5
                )
            elif tw_start is not None and tw_end is not None:
                # Parada con ventana horaria
                time_dimension.CumulVar(index).SetRange(tw_start, tw_end)
            else:
                # Sin ventana: cualquier hora del día
                time_dimension.CumulVar(index).SetRange(
                    current_time_minutes, 1440
                )

    # Configurar parámetros de búsqueda
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    # Tiempo adaptativo: 2s para <20 paradas, 5s para <50, 10s para más
    if len(locations) < 20:
        search_parameters.time_limit.seconds = 2
    elif len(locations) < 50:
        search_parameters.time_limit.seconds = 5
    else:
        search_parameters.time_limit.seconds = 10

    # Resolver
    solution = routing.SolveWithParameters(search_parameters)

    if not solution:
        # Si falla con time windows, reintentar sin ellas (copia para no mutar input)
        if has_time_windows:
            locations_copy = [
                {k: v for k, v in loc.items() if k not in ('time_window_start', 'time_window_end')}
                for loc in locations
            ]
            result = optimize_route(locations_copy, depot_index, num_vehicles)
            result['warning'] = 'No se pudo respetar todas las ventanas horarias. Ruta optimizada sin restricciones horarias.'
            return result
        return {
            "success": False,
            "error": "No se encontró solución",
            "route": locations
        }

    # Extraer la ruta optimizada
    optimized_route = []
    total_distance = 0
    index = routing.Start(0)

    while not routing.IsEnd(index):
        node = manager.IndexToNode(index)
        optimized_route.append(locations[node])
        previous_index = index
        index = solution.Value(routing.NextVar(index))
        total_distance += routing.GetArcCostForVehicle(previous_index, index, 0)

    return {
        "success": True,
        "route": optimized_route,
        "total_distance_meters": total_distance,
        "total_distance_km": round(total_distance / 1000, 2),
        "num_stops": len(optimized_route),
        "has_time_windows": has_time_windows,
        "message": f"Ruta optimizada: {len(optimized_route)} paradas, {round(total_distance/1000, 2)} km"
    }


# ============================================================
# ETA Y TIEMPOS ESTIMADOS
# ============================================================

def calculate_eta(
    current_location: Tuple[float, float],
    destination: Tuple[float, float],
    avg_speed_kmh: float = 30.0,
    stop_time_minutes: float = 5.0
) -> Dict[str, Any]:
    """
    Calcula ETA considerando distancia, velocidad promedio y tiempo de parada.

    Args:
        current_location: (lat, lng) ubicación actual
        destination: (lat, lng) destino
        avg_speed_kmh: Velocidad promedio en km/h (default 30 para urbano)
        stop_time_minutes: Tiempo estimado en cada parada

    Returns:
        Dict con distance_km, travel_time_min, stop_time_min, eta
    """
    distance_m = haversine_distance(current_location, destination)
    distance_km = distance_m / 1000

    # Tiempo de viaje en minutos
    travel_time_min = (distance_km / avg_speed_kmh) * 60

    # ETA
    total_minutes = travel_time_min + stop_time_minutes
    eta = datetime.now() + timedelta(minutes=total_minutes)

    return {
        "distance_km": round(distance_km, 2),
        "travel_time_min": round(travel_time_min),
        "stop_time_min": stop_time_minutes,
        "total_time_min": round(total_minutes),
        "eta": eta.isoformat(),
        "eta_formatted": eta.strftime("%H:%M")
    }


def calculate_route_etas(
    route: List[Dict[str, Any]],
    start_location: Optional[Tuple[float, float]] = None,
    avg_speed_kmh: float = 30.0,
    stop_time_minutes: float = 5.0
) -> List[Dict[str, Any]]:
    """
    Calcula ETAs para todas las paradas de una ruta.

    Args:
        route: Lista de paradas con lat, lng
        start_location: Ubicación inicial (si None, usa primera parada)
        avg_speed_kmh: Velocidad promedio
        stop_time_minutes: Tiempo por parada

    Returns:
        Lista de paradas con ETAs añadidos
    """
    if not route:
        return []

    result = []
    current_time = datetime.now()
    current_pos = start_location or (route[0]['lat'], route[0]['lng'])

    for i, stop in enumerate(route):
        stop_pos = (stop['lat'], stop['lng'])

        # Calcular distancia y tiempo desde posición actual
        distance_m = haversine_distance(current_pos, stop_pos)
        distance_km = distance_m / 1000
        travel_time_min = (distance_km / avg_speed_kmh) * 60

        # Actualizar tiempo actual
        current_time += timedelta(minutes=travel_time_min)
        arrival_time = current_time

        # Añadir tiempo de parada
        current_time += timedelta(minutes=stop_time_minutes)

        # Añadir información al stop
        stop_with_eta = {
            **stop,
            "eta": arrival_time.isoformat(),
            "eta_formatted": arrival_time.strftime("%H:%M"),
            "distance_from_prev_km": round(distance_km, 2),
            "travel_time_from_prev_min": round(travel_time_min),
            "sequence": i + 1
        }
        result.append(stop_with_eta)

        # Actualizar posición actual
        current_pos = stop_pos

    return result


# ============================================================
# CLUSTERING POR ZONAS
# ============================================================

def cluster_stops_by_zone(
    stops: List[Dict[str, Any]],
    n_zones: Optional[int] = None,
    max_stops_per_zone: int = 15
) -> Dict[str, Any]:
    """
    Agrupa paradas en zonas geográficas usando K-means simplificado.

    Args:
        stops: Lista de paradas con lat, lng
        n_zones: Número de zonas (si None, se calcula automáticamente)
        max_stops_per_zone: Máximo de paradas por zona

    Returns:
        Dict con zones (lista de zonas con sus paradas)
    """
    if not stops:
        return {"zones": [], "num_zones": 0}

    if len(stops) <= max_stops_per_zone:
        # Una sola zona si hay pocas paradas
        center_lat = sum(s['lat'] for s in stops) / len(stops)
        center_lng = sum(s['lng'] for s in stops) / len(stops)
        return {
            "zones": [{
                "id": 0,
                "center": {"lat": center_lat, "lng": center_lng},
                "stops": stops,
                "num_stops": len(stops)
            }],
            "num_zones": 1
        }

    # Calcular número óptimo de zonas
    if n_zones is None:
        n_zones = max(2, len(stops) // max_stops_per_zone + 1)

    # K-means simplificado (sin sklearn para evitar dependencia)
    # Inicializar centroides con paradas espaciadas
    step = len(stops) // n_zones
    centroids = [
        (stops[i * step]['lat'], stops[i * step]['lng'])
        for i in range(n_zones)
    ]

    # Iterar para mejorar centroides
    for _ in range(10):  # 10 iteraciones
        # Asignar paradas al centroide más cercano
        clusters: Dict[int, List[Dict]] = defaultdict(list)
        for stop in stops:
            stop_pos = (stop['lat'], stop['lng'])
            distances = [
                haversine_distance(stop_pos, centroid)
                for centroid in centroids
            ]
            nearest = distances.index(min(distances))
            clusters[nearest].append(stop)

        # Recalcular centroides
        new_centroids = []
        for i in range(n_zones):
            if clusters[i]:
                avg_lat = sum(s['lat'] for s in clusters[i]) / len(clusters[i])
                avg_lng = sum(s['lng'] for s in clusters[i]) / len(clusters[i])
                new_centroids.append((avg_lat, avg_lng))
            else:
                new_centroids.append(centroids[i])
        centroids = new_centroids

    # Construir resultado final
    zones = []
    for i in range(n_zones):
        if clusters[i]:
            zones.append({
                "id": i,
                "center": {"lat": centroids[i][0], "lng": centroids[i][1]},
                "stops": clusters[i],
                "num_stops": len(clusters[i])
            })

    return {
        "zones": zones,
        "num_zones": len(zones)
    }


# ============================================================
# ASIGNACIÓN INTELIGENTE DE CONDUCTORES
# ============================================================

def calculate_driver_score(
    driver: Dict[str, Any],
    zone_center: Tuple[float, float],
    pending_routes: int,
    weights: Optional[Dict[str, float]] = None
) -> float:
    """
    Calcula un score de idoneidad para asignar un conductor a una zona.

    Args:
        driver: Dict con id, location (lat, lng), etc.
        zone_center: Centro de la zona
        pending_routes: Número de rutas pendientes del conductor
        weights: Pesos para cada factor

    Returns:
        Score (menor es mejor)
    """
    if weights is None:
        weights = {
            "distance": 0.6,  # 60% peso a la distancia
            "workload": 0.4,  # 40% peso a la carga de trabajo
        }

    score = 0.0

    # Factor distancia (km)
    if driver.get('location'):
        driver_pos = (driver['location']['lat'], driver['location']['lng'])
        distance_km = haversine_distance(driver_pos, zone_center) / 1000
        score += distance_km * weights.get("distance", 0.6)
    else:
        score += 50 * weights.get("distance", 0.6)  # Penalización si no hay ubicación

    # Factor carga de trabajo
    score += pending_routes * 5 * weights.get("workload", 0.4)

    return score


def assign_drivers_to_zones(
    zones: List[Dict[str, Any]],
    drivers: List[Dict[str, Any]],
    driver_routes: Dict[str, int]  # driver_id -> pending_routes
) -> Dict[str, Any]:
    """
    Asigna conductores a zonas de forma inteligente.

    Args:
        zones: Lista de zonas del clustering
        drivers: Lista de conductores disponibles
        driver_routes: Dict con rutas pendientes por conductor

    Returns:
        Dict con assignments (zone_id -> driver_id)
    """
    if not zones or not drivers:
        return {"assignments": {}, "unassigned_zones": [z['id'] for z in zones]}

    assignments = {}
    assigned_drivers = set()
    unassigned_zones = []

    for zone in zones:
        zone_center = (zone['center']['lat'], zone['center']['lng'])

        # Calcular scores para conductores no asignados
        scores = []
        for driver in drivers:
            if driver['id'] in assigned_drivers:
                continue

            pending = driver_routes.get(driver['id'], 0)
            score = calculate_driver_score(driver, zone_center, pending)
            scores.append((driver['id'], score))

        if scores:
            # Asignar conductor con mejor score (menor)
            scores.sort(key=lambda x: x[1])
            best_driver_id = scores[0][0]
            assignments[zone['id']] = best_driver_id
            assigned_drivers.add(best_driver_id)
        else:
            unassigned_zones.append(zone['id'])

    return {
        "assignments": assignments,
        "unassigned_zones": unassigned_zones,
        "assigned_drivers": list(assigned_drivers)
    }


# ============================================================
# OPTIMIZACIÓN MULTI-VEHÍCULO
# ============================================================

def optimize_multi_vehicle(
    locations: List[Dict[str, Any]],
    num_vehicles: int,
    depot_index: int = 0,
    max_distance_per_vehicle: Optional[int] = None
) -> Dict[str, Any]:
    """
    Optimiza rutas para múltiples vehículos usando CVRP.

    Args:
        locations: Lista de paradas
        num_vehicles: Número de vehículos
        depot_index: Índice del depósito
        max_distance_per_vehicle: Límite de distancia por vehículo (metros)

    Returns:
        Dict con routes (lista de rutas, una por vehículo)
    """
    if len(locations) < 2:
        return {
            "success": True,
            "routes": [{"vehicle": 0, "route": locations, "distance_km": 0}],
            "total_distance_km": 0
        }

    # Validar depot_index
    if depot_index < 0 or depot_index >= len(locations):
        depot_index = 0

    # Crear matriz de distancias
    distance_matrix = create_distance_matrix(locations)

    # Crear modelo
    manager = pywrapcp.RoutingIndexManager(
        len(locations),
        num_vehicles,
        depot_index
    )
    routing = pywrapcp.RoutingModel(manager)

    # Callback de distancia
    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return distance_matrix[from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Añadir dimensión de distancia si hay límite
    if max_distance_per_vehicle:
        routing.AddDimension(
            transit_callback_index,
            0,  # slack
            max_distance_per_vehicle,
            True,  # start cumul to zero
            'Distance'
        )

    # Parámetros de búsqueda
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_parameters.time_limit.seconds = 10

    # Resolver
    solution = routing.SolveWithParameters(search_parameters)

    if not solution:
        return {
            "success": False,
            "error": "No se encontró solución para multi-vehicle",
            "routes": []
        }

    # Extraer rutas por vehículo
    routes = []
    total_distance = 0

    for vehicle_id in range(num_vehicles):
        route = []
        vehicle_distance = 0
        index = routing.Start(vehicle_id)

        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            if node != depot_index:  # No incluir depot en la ruta
                route.append(locations[node])
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            vehicle_distance += routing.GetArcCostForVehicle(
                previous_index, index, vehicle_id
            )

        if route:  # Solo añadir si tiene paradas
            routes.append({
                "vehicle": vehicle_id,
                "route": route,
                "distance_km": round(vehicle_distance / 1000, 2),
                "num_stops": len(route)
            })
            total_distance += vehicle_distance

    return {
        "success": True,
        "routes": routes,
        "total_distance_km": round(total_distance / 1000, 2),
        "num_vehicles_used": len(routes)
    }


# Test rápido
if __name__ == "__main__":
    # Ejemplo: 5 paradas en Madrid
    test_locations = [
        {"id": 1, "address": "Puerta del Sol", "lat": 40.4168, "lng": -3.7038},
        {"id": 2, "address": "Plaza Mayor", "lat": 40.4155, "lng": -3.7074},
        {"id": 3, "address": "Retiro", "lat": 40.4153, "lng": -3.6845},
        {"id": 4, "address": "Gran Vía", "lat": 40.4203, "lng": -3.7016},
        {"id": 5, "address": "Atocha", "lat": 40.4065, "lng": -3.6895},
    ]

    result = optimize_route(test_locations)
    print("\n=== RESULTADO ===")
    print(f"Éxito: {result['success']}")
    print(f"Distancia total: {result['total_distance_km']} km")
    print("\nOrden óptimo:")
    for i, stop in enumerate(result['route'], 1):
        print(f"  {i}. {stop['address']}")
