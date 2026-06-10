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


# ===================== PYVRP DURATION UNITS (regression 22 may 2026) =====================


class _FakePyVRPLocation:
    """Marker object used as `from`/`to` in add_edge so we can identify pairs."""

    def __init__(self, idx: int) -> None:
        self.idx = idx


class _FakePyVRPRoute:
    def __init__(self) -> None:
        self._visits: list[int] = []

    def visits(self):
        return self._visits


class _FakePyVRPSolution:
    def __init__(self, routes_visits: list[list[int]]) -> None:
        self._routes = []
        for visits in routes_visits:
            r = _FakePyVRPRoute()
            r._visits = visits
            self._routes.append(r)

    def routes(self):
        return self._routes

    def distance(self):
        return 1234


class _FakePyVRPResult:
    def __init__(self, feasible: bool, routes_visits: list[list[int]]) -> None:
        self._feasible = feasible
        self.best = _FakePyVRPSolution(routes_visits)

    def is_feasible(self):
        return self._feasible


class _FakePyVRPModel:
    """Captures every model call so we can assert that durations are MINUTES."""

    instances: list = []

    def __init__(self) -> None:
        self.depot_kwargs: dict = {}
        self.client_kwargs_list: list[dict] = []
        self.vehicle_kwargs: dict = {}
        # Each entry: {"from": loc, "to": loc, "distance": int, "duration": int}
        self.edges: list[dict] = []
        self._locs: list[_FakePyVRPLocation] = []
        _FakePyVRPModel.instances.append(self)

    def add_depot(self, **kwargs):
        self.depot_kwargs = kwargs
        loc = _FakePyVRPLocation(idx=len(self._locs))
        self._locs.append(loc)
        return loc

    def add_client(self, **kwargs):
        self.client_kwargs_list.append(kwargs)
        loc = _FakePyVRPLocation(idx=len(self._locs))
        self._locs.append(loc)
        return loc

    def add_vehicle_type(self, **kwargs):
        self.vehicle_kwargs = kwargs

    @property
    def locations(self):
        return list(self._locs)

    def add_edge(self, frm, to, distance, duration):
        # PyVRP rejects self-loops with duration > 0 in real life; record so we
        # can assert i==j has duration=0 without invoking the real binding.
        self.edges.append({"from": frm, "to": to, "distance": distance, "duration": duration})

    def solve(self, stop, display=False):
        # Visits ordered: simply 1..N (clients) — enough for the optimizer to
        # build an `optimized_route` and return success. visits() indices map to
        # `model.locations` order: [depot, client0, client1, ...].
        client_indices = [i + 1 for i in range(len(self._locs) - 1)]
        return _FakePyVRPResult(feasible=True, routes_visits=[client_indices])


class _FakeMaxRuntime:
    def __init__(self, *_a, **_kw):
        pass


class TestPyVrpDurationUnits:
    """Regression tests for commit e8bde70 (22 may 2026).

    Bug: optimizer.py:422 pasaba `model.add_edge(... duration=dist)` con `dist`
    en METROS. PyVRP 0.13.x interpretaba esos metros como minutos al cuadrarlos
    con tw_early/tw_late (que vienen en minutos), produciendo siempre infeasible
    en rutas urbanas (~1500m/edge) con ventanas de 2h.

    Fix: convertir a minutos vía OSRM duration_matrix (segundos→minutos) o,
    en su defecto, aproximar 40 km/h (1.5 min/km). Self-loops siempre duration=0.
    """

    def _patch_pyvrp(self, monkeypatch):
        _FakePyVRPModel.instances.clear()
        monkeypatch.setattr("optimizer.HAS_PYVRP", True)
        # raising=False so test runs even when PyVRP isn't installed locally
        # (HAS_PYVRP=False path leaves these names absent from the module).
        monkeypatch.setattr("optimizer.PyVRPModel", _FakePyVRPModel, raising=False)
        monkeypatch.setattr("optimizer.MaxRuntime", _FakeMaxRuntime, raising=False)

    def test_pyvrp_duration_uses_minutes_with_duration_matrix(self, monkeypatch):
        """duration_matrix viene en SEGUNDOS → debe traducirse a MINUTOS (s/60).

        Antes del fix se pasaba `duration=distance_matrix[i][j]` (metros). Aquí
        verificamos que para una entry de 600 segundos (=10 min) llegue exactamente
        10 al `add_edge`, NO 600 ni el valor en metros.
        """
        self._patch_pyvrp(monkeypatch)
        from optimizer import solve_with_pyvrp

        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": "depot"},
            {"lat": 40.4065, "lng": -3.6895, "id": "a"},
            {"lat": 40.4153, "lng": -3.6845, "id": "b"},
        ]
        # 3x3 distance matrix in METERS (real numbers don't matter, just non-zero)
        dist_m = [
            [0, 1500, 2200],
            [1500, 0, 1800],
            [2200, 1800, 0],
        ]
        # duration matrix in SECONDS — must be converted to minutes inside solver
        # 600s=10min, 720s=12min, 480s=8min, etc.
        dur_s = [
            [0, 600, 1320],
            [600, 0, 720],
            [1320, 720, 0],
        ]

        result = solve_with_pyvrp(locs, distance_matrix=dist_m, duration_matrix=dur_s)
        assert result["success"] is True

        model = _FakePyVRPModel.instances[-1]
        # Map each edge by (from_idx, to_idx) and check duration
        edge_by_pair = {(e["from"].idx, e["to"].idx): e for e in model.edges}

        # depot(idx0) -> client_a(idx1): distance 1500m, duration 600s = 10 min
        e01 = edge_by_pair[(0, 1)]
        assert e01["distance"] == 1500, "distance must stay in meters"
        assert e01["duration"] == 10, (
            f"duration must be MINUTES (600s/60=10), got {e01['duration']} — "
            "likely passing seconds or meters again"
        )

        # client_a(idx1) -> client_b(idx2): 1800m, 720s = 12 min
        e12 = edge_by_pair[(1, 2)]
        assert e12["distance"] == 1800
        assert e12["duration"] == 12

        # depot(0) -> client_b(2): 2200m, 1320s = 22 min
        e02 = edge_by_pair[(0, 2)]
        assert e02["distance"] == 2200
        assert e02["duration"] == 22

    def test_pyvrp_duration_fallback_40kmh_when_no_duration_matrix(self, monkeypatch):
        """Sin duration_matrix, debe estimar 40 km/h urbano = 1.5 min/km.

        Fórmula del fix: `dur_min = max(1, int(dist / 1000 / 40 * 60))`.
        Para 1500m: 1500/1000/40*60 = 2.25 → int = 2 min.
        Para 2200m: 2200/1000/40*60 = 3.3 → int = 3 min.
        """
        self._patch_pyvrp(monkeypatch)
        from optimizer import solve_with_pyvrp

        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": "depot"},
            {"lat": 40.4065, "lng": -3.6895, "id": "a"},
            {"lat": 40.4153, "lng": -3.6845, "id": "b"},
        ]
        dist_m = [
            [0, 1500, 2200],
            [1500, 0, 1800],
            [2200, 1800, 0],
        ]

        result = solve_with_pyvrp(locs, distance_matrix=dist_m, duration_matrix=None)
        assert result["success"] is True

        model = _FakePyVRPModel.instances[-1]
        edge_by_pair = {(e["from"].idx, e["to"].idx): e for e in model.edges}

        # 1500m at 40 km/h ≈ 2.25 min → int(2.25) = 2
        assert edge_by_pair[(0, 1)]["duration"] == 2
        # 2200m at 40 km/h ≈ 3.3 min → int(3.3) = 3
        assert edge_by_pair[(0, 2)]["duration"] == 3
        # 1800m at 40 km/h = 2.7 → int = 2
        assert edge_by_pair[(1, 2)]["duration"] == 2

        # Distance must remain in METERS — the fix only touches duration
        assert edge_by_pair[(0, 1)]["distance"] == 1500

    def test_pyvrp_self_loop_has_zero_duration(self, monkeypatch):
        """Self-loop (i==j) debe tener duration=0 — PyVRP rechaza self-loops con
        duration>0 con ValueError. Antes del fix se enviaba el valor 0 metros
        como duración, que coincidía pero por accidente; ahora es explícito.
        """
        self._patch_pyvrp(monkeypatch)
        from optimizer import solve_with_pyvrp

        locs = [
            {"lat": 40.4168, "lng": -3.7038, "id": "depot"},
            {"lat": 40.4065, "lng": -3.6895, "id": "a"},
        ]
        dist_m = [[0, 800], [800, 0]]
        dur_s = [[0, 300], [300, 0]]

        result = solve_with_pyvrp(locs, distance_matrix=dist_m, duration_matrix=dur_s)
        assert result["success"] is True

        model = _FakePyVRPModel.instances[-1]
        # Every (i, i) edge must be duration=0 AND distance=0
        for e in model.edges:
            if e["from"].idx == e["to"].idx:
                assert e["duration"] == 0, (
                    f"self-loop ({e['from'].idx}->{e['to'].idx}) must have duration=0, "
                    f"got {e['duration']}"
                )
                assert e["distance"] == 0


# ===================== O5: SANITIZE TIME WINDOWS (#81) =====================


class TestSanitizeTimeWindows:
    """(#81 O5) Ventanas invertidas (inicio >= fin) → se ignora SOLO esa, el resto conserva la suya."""

    def test_drops_inverted_window_keeps_valid(self):
        from optimizer import _sanitize_time_windows
        locs = [
            {"id": 0, "lat": 40.0, "lng": -3.0},
            {"id": 1, "lat": 40.1, "lng": -3.1, "time_window_start": "17:00", "time_window_end": "09:00"},  # invertida
            {"id": 2, "lat": 40.2, "lng": -3.2, "time_window_start": "09:00", "time_window_end": "17:00"},  # válida
        ]
        out = _sanitize_time_windows(locs)
        assert "time_window_start" not in out[1] and "time_window_end" not in out[1]
        assert out[2]["time_window_start"] == "09:00" and out[2]["time_window_end"] == "17:00"
        # no muta el input original
        assert locs[1]["time_window_start"] == "17:00"

    def test_equal_start_end_is_invalid(self):
        from optimizer import _sanitize_time_windows
        locs = [{"id": 1, "lat": 40.0, "lng": -3.0, "time_window_start": "10:00", "time_window_end": "10:00"}]
        out = _sanitize_time_windows(locs)
        assert "time_window_start" not in out[0]

    def test_optimize_route_with_inverted_window_does_not_silently_fail(self):
        # Antes: una ventana invertida hacía a OR-Tools infactible → reintento sin
        # NINGUNA ventana. Ahora se normaliza esa parada y la ruta sale OK.
        locs = [
            {"id": 0, "lat": 40.4168, "lng": -3.7038},
            {"id": 1, "lat": 40.4065, "lng": -3.6895, "time_window_start": "20:00", "time_window_end": "08:00"},
            {"id": 2, "lat": 40.4153, "lng": -3.6845, "time_window_start": "09:00", "time_window_end": "18:00"},
        ]
        result = optimize_route(locs)
        assert result["success"] is True
        assert len(result["route"]) == 3


# ===================== O7: GREEDY FALLBACK (#81) =====================


class TestGreedyFallback:
    """(#81 O7) Si TODOS los solvers fallan, el conductor recibe SIEMPRE una ruta usable."""

    def test_greedy_returns_valid_route_depot_first(self):
        from optimizer import _greedy_nearest_neighbor
        locs = [{"id": 0}, {"id": 1}, {"id": 2}]
        matrix = [
            [0, 10, 20],
            [10, 0, 5],
            [20, 5, 0],
        ]
        result = _greedy_nearest_neighbor(locs, 0, matrix)
        assert result["success"] is True
        assert result["solver"] == "fallback-greedy"
        assert result["degraded"] is True
        # depósito primero + vecino más cercano: A(0) -> B(1, d=10) -> C(2, d=5)
        assert [s["id"] for s in result["route"]] == [0, 1, 2]
        assert result["total_distance_meters"] == 15
        # permutación completa (no pierde paradas)
        assert sorted(s["id"] for s in result["route"]) == [0, 1, 2]

    def test_hybrid_always_returns_route_when_all_solvers_fail(self):
        # PyVRP/VROOM no disponibles + OR-Tools peta → debe caer al greedy (P4),
        # NUNCA devolver success:false sin ruta (el cliente la descartaría).
        from unittest.mock import patch as _patch
        locs = [
            {"id": 0, "lat": 40.4168, "lng": -3.7038},
            {"id": 1, "lat": 40.4065, "lng": -3.6895},
            {"id": 2, "lat": 40.4153, "lng": -3.6845},
        ]
        with _patch("optimizer.HAS_PYVRP", False), \
             _patch("optimizer.HAS_VROOM", False), \
             _patch("optimizer.optimize_route", side_effect=Exception("boom")):
            result = hybrid_optimize_route(locs)
        assert result["success"] is True
        assert result["solver"] == "fallback-greedy"
        assert len(result["route"]) == 3


class TestParseTimeRangeValidation:
    """(#81 review) '25:99' parseaba como 1599 min y llegaba al solver — ahora None."""

    def test_out_of_range_hours_returns_none(self):
        from optimizer import _parse_time_to_minutes
        assert _parse_time_to_minutes("25:99") is None
        assert _parse_time_to_minutes("24:00") is None
        assert _parse_time_to_minutes("10:60") is None
        assert _parse_time_to_minutes("-1:30") is None

    def test_valid_bounds_still_parse(self):
        from optimizer import _parse_time_to_minutes
        assert _parse_time_to_minutes("00:00") == 0
        assert _parse_time_to_minutes("23:59") == 1439
        assert _parse_time_to_minutes("09:00") == 540
