"""
Extended tests for optimizer.py to increase coverage:
  - haversine_distance (edge cases)
  - create_distance_matrix (various sizes)
  - _parse_time_to_minutes (valid/invalid)
  - optimize_route (OR-Tools: <2 stops, many stops, time windows, no solution)
  - solve_with_vroom (if available)
  - solve_with_pyvrp (if available)
  - hybrid_optimize_route (fallback chain)
  - calculate_eta (basic, custom speed)
  - calculate_route_etas (empty, single, multi)
  - cluster_stops_by_zone (0, few, many stops)
  - calculate_driver_score (with/without location)
  - assign_drivers_to_zones (empty, normal, more zones than drivers)
  - optimize_multi_vehicle (various configs)
"""

from unittest.mock import patch

import pytest

from optimizer import (
    _parse_time_to_minutes,
    assign_drivers_to_zones,
    calculate_driver_score,
    calculate_eta,
    calculate_route_etas,
    cluster_stops_by_zone,
    create_distance_matrix,
    haversine_distance,
    hybrid_optimize_route,
    optimize_multi_vehicle,
    optimize_route,
)

# ===================== HAVERSINE DISTANCE =====================

class TestHaversineDistance:
    """Tests for haversine_distance function."""

    def test_same_point(self):
        """Distance between same point should be 0."""
        result = haversine_distance((40.4168, -3.7038), (40.4168, -3.7038))
        assert result == 0

    def test_known_distance(self):
        """Test with two known locations (Sol to Atocha ~1.2 km)."""
        sol = (40.4168, -3.7038)
        atocha = (40.4065, -3.6895)
        distance = haversine_distance(sol, atocha)
        assert 1000 < distance < 2000  # approx 1.4 km

    def test_long_distance(self):
        """Madrid to Barcelona ~500 km."""
        madrid = (40.4168, -3.7038)
        barcelona = (41.3851, 2.1734)
        distance = haversine_distance(madrid, barcelona)
        assert 450_000 < distance < 550_000

    def test_returns_integer(self):
        result = haversine_distance((0.0, 0.0), (1.0, 1.0))
        assert isinstance(result, int)

    def test_equator_points(self):
        """Two points on equator 1 degree apart ~111 km."""
        d = haversine_distance((0.0, 0.0), (0.0, 1.0))
        assert 110_000 < d < 112_000

    def test_negative_coordinates(self):
        """Works with negative coordinates (Southern/Western hemisphere)."""
        d = haversine_distance((-34.6037, -58.3816), (-33.4489, -70.6693))
        assert d > 0


# ===================== CREATE DISTANCE MATRIX =====================

class TestCreateDistanceMatrix:
    """Tests for create_distance_matrix function."""

    def test_single_location(self):
        locs = [{"lat": 40.4168, "lng": -3.7038}]
        matrix = create_distance_matrix(locs)
        assert matrix == [[0]]

    def test_two_locations(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038},
            {"lat": 40.4065, "lng": -3.6895},
        ]
        matrix = create_distance_matrix(locs)
        assert len(matrix) == 2
        assert len(matrix[0]) == 2
        assert matrix[0][0] == 0
        assert matrix[1][1] == 0
        assert matrix[0][1] > 0
        assert matrix[0][1] == matrix[1][0]  # Symmetric

    def test_three_locations(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038},
            {"lat": 40.4155, "lng": -3.7074},
            {"lat": 40.4153, "lng": -3.6845},
        ]
        matrix = create_distance_matrix(locs)
        assert len(matrix) == 3
        # Diagonal should be zero
        for i in range(3):
            assert matrix[i][i] == 0

    def test_matrix_values_are_integers(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038},
            {"lat": 40.4065, "lng": -3.6895},
        ]
        matrix = create_distance_matrix(locs)
        for row in matrix:
            for val in row:
                assert isinstance(val, int)


# ===================== PARSE TIME TO MINUTES =====================

class TestParseTimeToMinutes:
    """Tests for _parse_time_to_minutes helper."""

    def test_midnight(self):
        assert _parse_time_to_minutes("00:00") == 0

    def test_noon(self):
        assert _parse_time_to_minutes("12:00") == 720

    def test_morning(self):
        assert _parse_time_to_minutes("09:30") == 570

    def test_evening(self):
        assert _parse_time_to_minutes("23:59") == 1439

    def test_none_input(self):
        assert _parse_time_to_minutes(None) is None

    def test_empty_string(self):
        assert _parse_time_to_minutes("") is None

    def test_invalid_format(self):
        assert _parse_time_to_minutes("invalid") is None

    def test_partial_format(self):
        assert _parse_time_to_minutes("9") is None


# ===================== OPTIMIZE ROUTE (OR-Tools) =====================

class TestOptimizeRoute:
    """Tests for optimize_route (OR-Tools solver)."""

    def test_single_stop(self):
        locs = [{"lat": 40.4168, "lng": -3.7038, "id": 1}]
        result = optimize_route(locs)
        assert result["success"] is True
        assert result["total_distance_meters"] == 0
        assert len(result["route"]) == 1

    def test_two_stops(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
        ]
        result = optimize_route(locs)
        assert result["success"] is True
        assert len(result["route"]) == 2
        assert result["total_distance_km"] > 0

    def test_five_stops(self):
        locs = [
            {"id": 1, "address": "Puerta del Sol", "lat": 40.4168, "lng": -3.7038},
            {"id": 2, "address": "Plaza Mayor", "lat": 40.4155, "lng": -3.7074},
            {"id": 3, "address": "Retiro", "lat": 40.4153, "lng": -3.6845},
            {"id": 4, "address": "Gran Vía", "lat": 40.4203, "lng": -3.7016},
            {"id": 5, "address": "Atocha", "lat": 40.4065, "lng": -3.6895},
        ]
        result = optimize_route(locs)
        assert result["success"] is True
        assert result["num_stops"] == 5
        assert result["total_distance_km"] > 0

    def test_invalid_depot_index(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
        ]
        result = optimize_route(locs, depot_index=99)
        assert result["success"] is True
        # depot_index should be reset to 0

    def test_negative_depot_index(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
        ]
        result = optimize_route(locs, depot_index=-1)
        assert result["success"] is True

    def test_with_distance_matrix(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
        ]
        matrix = create_distance_matrix(locs)
        result = optimize_route(locs, distance_matrix=matrix)
        assert result["success"] is True

    def test_with_time_windows(self):
        """Time windows are passed to OR-Tools. Result depends on current time
        (the solver may fall back to no-time-windows if current time is past the window).
        Either way, the route should be optimized successfully."""
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2, "time_window_start": "00:00", "time_window_end": "23:59"},
            {"lat": 40.4153, "lng": -3.6845, "id": 3, "time_window_start": "00:00", "time_window_end": "23:59"},
        ]
        result = optimize_route(locs)
        assert result["success"] is True

    def test_with_duration_matrix(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2, "time_window_start": "09:00", "time_window_end": "23:00"},
        ]
        # Duration matrix in seconds
        dur_matrix = [[0, 300], [300, 0]]
        dist_matrix = create_distance_matrix(locs)
        result = optimize_route(locs, distance_matrix=dist_matrix, duration_matrix=dur_matrix)
        assert result["success"] is True


# ===================== HYBRID OPTIMIZE ROUTE =====================

class TestHybridOptimizeRoute:
    """Tests for hybrid_optimize_route (fallback chain)."""

    def test_single_stop(self):
        locs = [{"lat": 40.4168, "lng": -3.7038, "id": 1}]
        result = hybrid_optimize_route(locs)
        assert result["success"] is True
        assert result["solver"] == "none"

    def test_few_stops(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
            {"lat": 40.4153, "lng": -3.6845, "id": 3},
        ]
        result = hybrid_optimize_route(locs)
        assert result["success"] is True
        assert result["solver"] in ("pyvrp", "vroom", "ortools")

    def test_invalid_depot_index(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
        ]
        result = hybrid_optimize_route(locs, depot_index=100)
        assert result["success"] is True

    def test_with_distance_matrix(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
            {"lat": 40.4155, "lng": -3.7074, "id": 3},
        ]
        matrix = create_distance_matrix(locs)
        result = hybrid_optimize_route(locs, distance_matrix=matrix)
        assert result["success"] is True

    def test_ortools_fallback_when_no_solvers(self):
        """When PyVRP and VROOM are unavailable, falls back to OR-Tools."""
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
        ]
        with patch("optimizer.HAS_PYVRP", False), \
             patch("optimizer.HAS_VROOM", False):
            result = hybrid_optimize_route(locs)
        assert result["success"] is True
        assert result["solver"] == "ortools"

    def test_vroom_fallback_when_pyvrp_unavailable(self):
        """When PyVRP is unavailable but VROOM is, uses VROOM."""
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
            {"lat": 40.4153, "lng": -3.6845, "id": 3},
        ]
        with patch("optimizer.HAS_PYVRP", False):
            from optimizer import HAS_VROOM
            if HAS_VROOM:
                result = hybrid_optimize_route(locs)
                assert result["success"] is True
                assert result["solver"] == "vroom"

    def test_pyvrp_crash_falls_back(self):
        """When PyVRP raises exception, falls back to next solver."""
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
        ]
        with patch("optimizer.HAS_PYVRP", True), \
             patch("optimizer.solve_with_pyvrp", side_effect=RuntimeError("crash")):
            result = hybrid_optimize_route(locs)
        assert result["success"] is True
        assert result["solver"] in ("vroom", "ortools")


# ===================== CALCULATE ETA =====================

class TestCalculateEta:
    """Tests for calculate_eta function."""

    def test_basic_eta(self):
        current = (40.4168, -3.7038)
        dest = (40.4065, -3.6895)
        result = calculate_eta(current, dest)
        assert "distance_km" in result
        assert "travel_time_min" in result
        assert "eta" in result
        assert "eta_formatted" in result
        assert result["distance_km"] > 0
        assert result["total_time_min"] > 0

    def test_same_location(self):
        pos = (40.4168, -3.7038)
        result = calculate_eta(pos, pos)
        assert result["distance_km"] == 0.0
        assert result["travel_time_min"] == 0

    def test_custom_speed(self):
        current = (40.4168, -3.7038)
        dest = (40.4065, -3.6895)
        fast = calculate_eta(current, dest, avg_speed_kmh=60.0)
        slow = calculate_eta(current, dest, avg_speed_kmh=15.0)
        assert fast["travel_time_min"] < slow["travel_time_min"]

    def test_custom_stop_time(self):
        current = (40.4168, -3.7038)
        dest = (40.4065, -3.6895)
        r1 = calculate_eta(current, dest, stop_time_minutes=0)
        r2 = calculate_eta(current, dest, stop_time_minutes=10)
        assert r2["total_time_min"] > r1["total_time_min"]

    def test_eta_format(self):
        current = (40.4168, -3.7038)
        dest = (40.4065, -3.6895)
        result = calculate_eta(current, dest)
        # eta_formatted should be HH:MM
        assert ":" in result["eta_formatted"]
        parts = result["eta_formatted"].split(":")
        assert len(parts) == 2


# ===================== CALCULATE ROUTE ETAS =====================

class TestCalculateRouteEtas:
    """Tests for calculate_route_etas function."""

    def test_empty_route(self):
        result = calculate_route_etas([])
        assert result == []

    def test_single_stop(self):
        route = [{"lat": 40.4168, "lng": -3.7038, "id": 1}]
        result = calculate_route_etas(route)
        assert len(result) == 1
        assert "eta" in result[0]
        assert "sequence" in result[0]
        assert result[0]["sequence"] == 1

    def test_multi_stop(self):
        route = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
            {"lat": 40.4153, "lng": -3.6845, "id": 3},
        ]
        result = calculate_route_etas(route)
        assert len(result) == 3
        # Sequences should be 1, 2, 3
        for i, stop in enumerate(result):
            assert stop["sequence"] == i + 1
        # All should have ETAs
        for stop in result:
            assert "eta" in stop
            assert "eta_formatted" in stop
            assert "distance_from_prev_km" in stop

    def test_with_start_location(self):
        route = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
        ]
        start = (40.42, -3.71)
        result = calculate_route_etas(route, start_location=start)
        assert len(result) == 2
        assert result[0]["distance_from_prev_km"] > 0

    def test_custom_speed(self):
        route = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
        ]
        fast = calculate_route_etas(route, avg_speed_kmh=60)
        slow = calculate_route_etas(route, avg_speed_kmh=15)
        assert fast[1]["travel_time_from_prev_min"] <= slow[1]["travel_time_from_prev_min"]


# ===================== CLUSTER STOPS BY ZONE =====================

class TestClusterStopsByZone:
    """Tests for cluster_stops_by_zone function."""

    def test_empty_stops(self):
        result = cluster_stops_by_zone([])
        assert result["zones"] == []
        assert result["num_zones"] == 0

    def test_few_stops_single_zone(self):
        stops = [
            {"lat": 40.4168, "lng": -3.7038},
            {"lat": 40.4155, "lng": -3.7074},
            {"lat": 40.4153, "lng": -3.6845},
        ]
        result = cluster_stops_by_zone(stops, max_stops_per_zone=15)
        assert result["num_zones"] == 1
        assert len(result["zones"][0]["stops"]) == 3

    def test_forced_zones(self):
        """When n_zones is specified, use that many zones."""
        stops = [
            {"lat": 40.4168, "lng": -3.7038},
            {"lat": 40.4155, "lng": -3.7074},
            {"lat": 40.4153, "lng": -3.6845},
            {"lat": 40.4203, "lng": -3.7016},
        ]
        result = cluster_stops_by_zone(stops, n_zones=2, max_stops_per_zone=2)
        assert result["num_zones"] == 2

    def test_many_stops_auto_zones(self):
        """Many stops should be split into multiple zones."""
        stops = []
        for i in range(25):
            stops.append({
                "lat": 40.4 + (i * 0.01),
                "lng": -3.7 + (i * 0.005),
            })
        result = cluster_stops_by_zone(stops, max_stops_per_zone=10)
        assert result["num_zones"] >= 2
        total_stops = sum(z["num_stops"] for z in result["zones"])
        assert total_stops == 25

    def test_zone_has_center(self):
        stops = [
            {"lat": 40.4168, "lng": -3.7038},
            {"lat": 40.4155, "lng": -3.7074},
        ]
        result = cluster_stops_by_zone(stops, max_stops_per_zone=15)
        zone = result["zones"][0]
        assert "center" in zone
        assert "lat" in zone["center"]
        assert "lng" in zone["center"]

    def test_single_stop(self):
        stops = [{"lat": 40.4168, "lng": -3.7038}]
        result = cluster_stops_by_zone(stops)
        assert result["num_zones"] == 1
        assert result["zones"][0]["num_stops"] == 1


# ===================== CALCULATE DRIVER SCORE =====================

class TestCalculateDriverScore:
    """Tests for calculate_driver_score function."""

    def test_with_location(self):
        driver = {"id": "d1", "location": {"lat": 40.4168, "lng": -3.7038}}
        zone_center = (40.4065, -3.6895)
        score = calculate_driver_score(driver, zone_center, pending_routes=0)
        assert score > 0

    def test_without_location(self):
        driver = {"id": "d1"}
        zone_center = (40.4065, -3.6895)
        score = calculate_driver_score(driver, zone_center, pending_routes=0)
        # Should apply penalty (50 * 0.6 = 30)
        assert score >= 30

    def test_high_workload_increases_score(self):
        driver = {"id": "d1", "location": {"lat": 40.4168, "lng": -3.7038}}
        zone_center = (40.4168, -3.7038)  # same location
        score_0 = calculate_driver_score(driver, zone_center, pending_routes=0)
        score_5 = calculate_driver_score(driver, zone_center, pending_routes=5)
        assert score_5 > score_0

    def test_custom_weights(self):
        driver = {"id": "d1", "location": {"lat": 40.4168, "lng": -3.7038}}
        zone_center = (40.4065, -3.6895)
        custom_weights = {"distance": 1.0, "workload": 0.0}
        score = calculate_driver_score(driver, zone_center, pending_routes=10, weights=custom_weights)
        # With workload weight 0, pending routes shouldn't affect score
        score_no_pending = calculate_driver_score(driver, zone_center, pending_routes=0, weights=custom_weights)
        assert abs(score - score_no_pending) < 0.01


# ===================== ASSIGN DRIVERS TO ZONES =====================

class TestAssignDriversToZones:
    """Tests for assign_drivers_to_zones function."""

    def test_empty_zones(self):
        result = assign_drivers_to_zones([], [{"id": "d1"}], {"d1": 0})
        assert result["assignments"] == {}
        assert result["unassigned_zones"] == []

    def test_empty_drivers(self):
        zones = [{"id": 0, "center": {"lat": 40.4168, "lng": -3.7038}, "stops": []}]
        result = assign_drivers_to_zones(zones, [], {})
        assert result["assignments"] == {}
        assert result["unassigned_zones"] == [0]

    def test_one_zone_one_driver(self):
        zones = [{"id": 0, "center": {"lat": 40.4168, "lng": -3.7038}, "stops": []}]
        drivers = [{"id": "d1", "location": {"lat": 40.4168, "lng": -3.7038}}]
        result = assign_drivers_to_zones(zones, drivers, {"d1": 0})
        assert result["assignments"][0] == "d1"
        assert "d1" in result["assigned_drivers"]

    def test_two_zones_two_drivers(self):
        zones = [
            {"id": 0, "center": {"lat": 40.4168, "lng": -3.7038}, "stops": []},
            {"id": 1, "center": {"lat": 41.3851, "lng": 2.1734}, "stops": []},
        ]
        drivers = [
            {"id": "d1", "location": {"lat": 40.42, "lng": -3.70}},  # Near Madrid
            {"id": "d2", "location": {"lat": 41.38, "lng": 2.17}},  # Near Barcelona
        ]
        result = assign_drivers_to_zones(zones, drivers, {"d1": 0, "d2": 0})
        assert len(result["assignments"]) == 2
        assert result["unassigned_zones"] == []
        # d1 should be assigned to Madrid zone, d2 to Barcelona zone
        assert result["assignments"][0] == "d1"
        assert result["assignments"][1] == "d2"

    def test_more_zones_than_drivers(self):
        zones = [
            {"id": 0, "center": {"lat": 40.4168, "lng": -3.7038}, "stops": []},
            {"id": 1, "center": {"lat": 41.3851, "lng": 2.1734}, "stops": []},
            {"id": 2, "center": {"lat": 37.3891, "lng": -5.9845}, "stops": []},
        ]
        drivers = [{"id": "d1", "location": {"lat": 40.42, "lng": -3.70}}]
        result = assign_drivers_to_zones(zones, drivers, {"d1": 0})
        assert len(result["assignments"]) == 1
        assert len(result["unassigned_zones"]) == 2

    def test_workload_affects_assignment(self):
        zones = [{"id": 0, "center": {"lat": 40.4168, "lng": -3.7038}, "stops": []}]
        drivers = [
            {"id": "d1", "location": {"lat": 40.4168, "lng": -3.7038}},
            {"id": "d2", "location": {"lat": 40.4168, "lng": -3.7038}},
        ]
        # d1 has 10 pending routes, d2 has 0
        result = assign_drivers_to_zones(zones, drivers, {"d1": 10, "d2": 0})
        assert result["assignments"][0] == "d2"


# ===================== OPTIMIZE MULTI VEHICLE =====================

class TestOptimizeMultiVehicle:
    """Tests for optimize_multi_vehicle function."""

    def test_single_stop(self):
        locs = [{"lat": 40.4168, "lng": -3.7038, "id": 1}]
        result = optimize_multi_vehicle(locs, num_vehicles=2)
        assert result["success"] is True
        assert result["total_distance_km"] == 0

    def test_basic_two_vehicles(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},  # depot
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
            {"lat": 40.4153, "lng": -3.6845, "id": 3},
            {"lat": 40.4203, "lng": -3.7016, "id": 4},
            {"lat": 40.4100, "lng": -3.7100, "id": 5},
        ]
        result = optimize_multi_vehicle(locs, num_vehicles=2)
        assert result["success"] is True
        assert result["num_vehicles_used"] >= 1

    def test_invalid_depot_index(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
        ]
        result = optimize_multi_vehicle(locs, num_vehicles=1, depot_index=99)
        assert result["success"] is True

    def test_with_distance_matrix(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
            {"lat": 40.4153, "lng": -3.6845, "id": 3},
        ]
        matrix = create_distance_matrix(locs)
        result = optimize_multi_vehicle(locs, num_vehicles=2, distance_matrix=matrix)
        assert result["success"] is True

    def test_with_max_distance(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
            {"lat": 40.4153, "lng": -3.6845, "id": 3},
        ]
        result = optimize_multi_vehicle(
            locs, num_vehicles=2, max_distance_per_vehicle=50000
        )
        assert result["success"] is True

    def test_result_structure(self):
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
            {"lat": 40.4153, "lng": -3.6845, "id": 3},
        ]
        result = optimize_multi_vehicle(locs, num_vehicles=1)
        assert "routes" in result
        assert "total_distance_km" in result
        assert "num_vehicles_used" in result
        if result["routes"]:
            route = result["routes"][0]
            assert "vehicle" in route
            assert "route" in route
            assert "distance_km" in route
            assert "num_stops" in route


# ===================== SOLVER-SPECIFIC TESTS =====================

class TestSolveWithVroom:
    """Tests for solve_with_vroom (only when available)."""

    def test_single_stop(self):
        from optimizer import HAS_VROOM, solve_with_vroom
        if not HAS_VROOM:
            pytest.skip("VROOM not installed")
        locs = [{"lat": 40.4168, "lng": -3.7038, "id": 1}]
        result = solve_with_vroom(locs)
        assert result["success"] is True
        assert result["solver"] == "vroom"

    def test_basic_route(self):
        from optimizer import HAS_VROOM, solve_with_vroom
        if not HAS_VROOM:
            pytest.skip("VROOM not installed")
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
            {"lat": 40.4153, "lng": -3.6845, "id": 3},
        ]
        result = solve_with_vroom(locs)
        assert result["success"] is True
        assert result["solver"] == "vroom"
        assert result["num_stops"] >= 2

    def test_with_time_windows(self):
        from optimizer import HAS_VROOM, solve_with_vroom
        if not HAS_VROOM:
            pytest.skip("VROOM not installed")
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2, "time_window_start": "09:00", "time_window_end": "17:00"},
        ]
        result = solve_with_vroom(locs)
        assert result["success"] is True

    def test_not_available(self):
        with patch("optimizer.HAS_VROOM", False):
            from optimizer import solve_with_vroom
            locs = [
                {"lat": 40.4168, "lng": -3.7038, "id": 1},
                {"lat": 40.4065, "lng": -3.6895, "id": 2},
            ]
            result = solve_with_vroom(locs)
        assert result["success"] is False
        assert "VROOM not available" in result["error"]


class TestSolveWithPyvrp:
    """Tests for solve_with_pyvrp (only when available)."""

    def test_single_stop(self):
        from optimizer import HAS_PYVRP, solve_with_pyvrp
        if not HAS_PYVRP:
            pytest.skip("PyVRP not installed")
        locs = [{"lat": 40.4168, "lng": -3.7038, "id": 1}]
        result = solve_with_pyvrp(locs)
        assert result["success"] is True
        assert result["solver"] == "pyvrp"

    def test_basic_route(self):
        from optimizer import HAS_PYVRP, solve_with_pyvrp
        if not HAS_PYVRP:
            pytest.skip("PyVRP not installed")
        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": 1},
            {"lat": 40.4065, "lng": -3.6895, "id": 2},
            {"lat": 40.4153, "lng": -3.6845, "id": 3},
        ]
        result = solve_with_pyvrp(locs, time_limit_s=3)
        assert result["success"] is True
        assert result["solver"] == "pyvrp"

    def test_not_available(self):
        with patch("optimizer.HAS_PYVRP", False):
            from optimizer import solve_with_pyvrp
            locs = [
                {"lat": 40.4168, "lng": -3.7038, "id": 1},
                {"lat": 40.4065, "lng": -3.6895, "id": 2},
            ]
            result = solve_with_pyvrp(locs)
        assert result["success"] is False
        assert "PyVRP not available" in result["error"]
