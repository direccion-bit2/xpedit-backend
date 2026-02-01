"""
RutaMax - Motor de Optimización de Rutas
Usa Google OR-Tools para resolver el Vehicle Routing Problem (VRP)
"""

from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import math
from typing import List, Tuple, Dict, Any


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


def optimize_route(
    locations: List[Dict[str, Any]],
    depot_index: int = 0,
    num_vehicles: int = 1
) -> Dict[str, Any]:
    """
    Optimiza la ruta para visitar todas las ubicaciones.

    Args:
        locations: Lista de paradas con 'lat', 'lng', y opcionalmente 'id', 'address'
        depot_index: Índice del punto de inicio (default: primera ubicación)
        num_vehicles: Número de vehículos/conductores

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

    # Crear matriz de distancias
    distance_matrix = create_distance_matrix(locations)

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

    # Configurar parámetros de búsqueda
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_parameters.time_limit.seconds = 5  # Máximo 5 segundos de cálculo

    # Resolver
    solution = routing.SolveWithParameters(search_parameters)

    if not solution:
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
        "message": f"Ruta optimizada: {len(optimized_route)} paradas, {round(total_distance/1000, 2)} km"
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
