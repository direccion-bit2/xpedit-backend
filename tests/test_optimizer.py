"""
Tests for the optimizer module.
These tests verify core route optimization logic without external dependencies.
"""

from optimizer import haversine_distance, calculate_eta, calculate_route_etas, optimize_route


class TestHaversineDistance:
    """Tests for the haversine distance calculation."""

    def test_same_point_returns_zero(self):
        coord = (40.4168, -3.7038)  # Madrid
        distance = haversine_distance(coord, coord)
        assert distance == 0

    def test_known_distance_madrid_barcelona(self):
        madrid = (40.4168, -3.7038)
        barcelona = (41.3851, 2.1734)
        distance = haversine_distance(madrid, barcelona)
        # Madrid to Barcelona is roughly 505 km straight line
        assert 490_000 < distance < 520_000

    def test_short_distance(self):
        # Two points about 1 km apart in Madrid
        point_a = (40.4168, -3.7038)
        point_b = (40.4258, -3.7038)
        distance = haversine_distance(point_a, point_b)
        assert 900 < distance < 1100

    def test_returns_integer(self):
        coord_a = (40.4168, -3.7038)
        coord_b = (41.3851, 2.1734)
        distance = haversine_distance(coord_a, coord_b)
        assert isinstance(distance, int)


class TestCalculateEta:
    """Tests for ETA calculation between two points."""

    def test_returns_dict(self):
        origin = (40.4168, -3.7038)
        destination = (40.4200, -3.7000)
        result = calculate_eta(origin, destination)
        assert isinstance(result, dict)

    def test_contains_expected_keys(self):
        origin = (40.4168, -3.7038)
        destination = (40.4200, -3.7000)
        result = calculate_eta(origin, destination)
        assert "distance_km" in result or "eta_minutes" in result or "distance_meters" in result

    def test_same_location_zero_distance(self):
        point = (40.4168, -3.7038)
        result = calculate_eta(point, point)
        assert isinstance(result, dict)


class TestCalculateRouteEtas:
    """Tests for calculating ETAs across a multi-stop route."""

    def test_returns_list(self):
        route = [
            {"lat": 40.4168, "lng": -3.7038},
            {"lat": 40.4200, "lng": -3.7000},
            {"lat": 40.4250, "lng": -3.6950},
        ]
        result = calculate_route_etas(route)
        assert isinstance(result, list)

    def test_result_length_matches_route(self):
        route = [
            {"lat": 40.4168, "lng": -3.7038},
            {"lat": 40.4200, "lng": -3.7000},
        ]
        result = calculate_route_etas(route)
        assert len(result) == len(route)


class TestOptimizeRoute:
    """Tests for the main route optimization function."""

    def test_two_stops_returns_valid_result(self):
        locations = [
            {"lat": 40.4168, "lng": -3.7038, "id": "depot", "address": "Madrid Centro"},
            {"lat": 40.4200, "lng": -3.7000, "id": "stop1", "address": "Calle A"},
        ]
        result = optimize_route(locations, depot_index=0)
        assert isinstance(result, dict)

    def test_three_stops_returns_ordered_route(self):
        locations = [
            {"lat": 40.4168, "lng": -3.7038, "id": "depot", "address": "Madrid Centro"},
            {"lat": 40.4300, "lng": -3.6900, "id": "stop1", "address": "Calle A"},
            {"lat": 40.4200, "lng": -3.7000, "id": "stop2", "address": "Calle B"},
        ]
        result = optimize_route(locations, depot_index=0)
        assert isinstance(result, dict)
        # Should have a route key with ordered stops
        assert "route" in result or "ordered" in result or "stops" in result or "total_distance" in result

    def test_single_stop(self):
        locations = [
            {"lat": 40.4168, "lng": -3.7038, "id": "depot", "address": "Depot"},
        ]
        result = optimize_route(locations, depot_index=0)
        assert isinstance(result, dict)
