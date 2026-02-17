"""
Tests for route optimization and route CRUD endpoints:
  - POST /optimize
  - POST /optimize-multi
  - GET /routes
  - POST /routes
  - GET /routes/{route_id}
  - PATCH /routes/{route_id}/start
  - PATCH /routes/{route_id}/complete
  - DELETE /routes/{route_id}
  - POST /eta
  - POST /geocode
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.conftest import FAKE_DRIVER_ID


class AttrDict(dict):
    """Dict subclass that supports attribute access (for Supabase-style results)."""
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)


# === Optimize Endpoint Tests ===

class TestOptimizeEndpoint:
    """Tests for POST /optimize"""

    @pytest.mark.asyncio
    async def test_optimize_valid_stops(self, client):
        """Optimization with valid stops should return a successful result."""
        locations = [
            {"lat": 40.416775, "lng": -3.703790, "address": "Depot - Madrid"},
            {"lat": 40.453054, "lng": -3.688344, "address": "Stop 1"},
            {"lat": 40.420000, "lng": -3.710000, "address": "Stop 2"},
        ]

        with patch("main.optimize_route") as mock_optimize:
            mock_optimize.return_value = {
                "success": True,
                "optimized_order": [0, 2, 1],
                "total_distance_km": 5.2,
                "locations": locations,
            }
            response = await client.post("/optimize", json={
                "locations": locations,
                "start_index": 0,
            })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "optimized_order" in data
        assert "total_distance_km" in data

    @pytest.mark.asyncio
    async def test_optimize_single_stop(self, client):
        """Optimization with a single stop should still work."""
        locations = [{"lat": 40.416775, "lng": -3.703790}]

        with patch("main.optimize_route") as mock_optimize:
            mock_optimize.return_value = {
                "success": True,
                "optimized_order": [0],
                "total_distance_km": 0,
                "locations": locations,
            }
            response = await client.post("/optimize", json={
                "locations": locations,
            })

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_optimize_empty_stops_rejected(self, client):
        """Optimization with empty stops list should be rejected by validation."""
        response = await client.post("/optimize", json={
            "locations": [],
        })
        assert response.status_code == 422  # Pydantic validation error

    @pytest.mark.asyncio
    async def test_optimize_too_many_stops(self, client):
        """Optimization with more than 100 stops should return 400."""
        locations = [{"lat": 40.0 + i * 0.001, "lng": -3.0} for i in range(101)]
        response = await client.post("/optimize", json={
            "locations": locations,
        })
        assert response.status_code == 400
        assert "100" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_optimize_invalid_coordinates(self, client):
        """Lat/lng outside valid range should be rejected."""
        response = await client.post("/optimize", json={
            "locations": [{"lat": 200.0, "lng": -3.0}],
        })
        assert response.status_code == 422  # Pydantic validation

    @pytest.mark.asyncio
    async def test_optimize_missing_lat(self, client):
        """Location without lat should be rejected."""
        response = await client.post("/optimize", json={
            "locations": [{"lng": -3.0}],
        })
        assert response.status_code == 422


class TestOptimizeMultiEndpoint:
    """Tests for POST /optimize-multi"""

    @pytest.mark.asyncio
    async def test_optimize_multi_valid(self, client):
        """Multi-vehicle optimization with valid input."""
        locations = [
            {"lat": 40.416, "lng": -3.703},
            {"lat": 40.453, "lng": -3.688},
            {"lat": 40.420, "lng": -3.710},
            {"lat": 40.430, "lng": -3.700},
        ]

        with patch("main.optimize_multi_vehicle") as mock_opt:
            mock_opt.return_value = {
                "success": True,
                "vehicle_routes": [[0, 1], [0, 2, 3]],
                "total_distance_km": 10.5,
            }
            response = await client.post("/optimize-multi", json={
                "locations": locations,
                "num_vehicles": 2,
            })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    @pytest.mark.asyncio
    async def test_optimize_multi_too_many_stops(self, client):
        """More than 200 stops should be rejected."""
        locations = [{"lat": 40.0 + i * 0.001, "lng": -3.0} for i in range(201)]
        response = await client.post("/optimize-multi", json={
            "locations": locations,
            "num_vehicles": 2,
        })
        assert response.status_code == 400


# === ETA Endpoint Tests ===

class TestETAEndpoint:
    """Tests for POST /eta"""

    @pytest.mark.asyncio
    async def test_eta_returns_estimate(self, client):
        """ETA calculation should return time and distance."""
        with patch("main.calculate_eta") as mock_eta:
            mock_eta.return_value = {
                "distance_km": 5.0,
                "estimated_minutes": 10.0,
                "arrival_time": "14:30",
            }
            response = await client.post("/eta", json={
                "current_lat": 40.416,
                "current_lng": -3.703,
                "destination_lat": 40.453,
                "destination_lng": -3.688,
            })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "distance_km" in data
        assert "estimated_minutes" in data


# === Geocode Endpoint Tests ===

class TestGeocodeEndpoint:
    """Tests for POST /geocode"""

    @pytest.mark.asyncio
    async def test_geocode_success(self, client):
        """Successful geocoding should return lat/lng."""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"lat": "40.416775", "lon": "-3.703790", "display_name": "Madrid, Spain"}
        ]
        mock_response.status_code = 200

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__aenter__.return_value = mock_http_client
        mock_http_client.__aexit__.return_value = False

        with patch("main.httpx.AsyncClient", return_value=mock_http_client):
            response = await client.post("/geocode", json={
                "address": "Puerta del Sol, Madrid"
            })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "lat" in data
        assert "lng" in data

    @pytest.mark.asyncio
    async def test_geocode_address_too_short(self, client):
        """Address shorter than 3 chars should be rejected by validation."""
        response = await client.post("/geocode", json={
            "address": "ab"
        })
        assert response.status_code == 422


# === Routes CRUD Tests ===

class TestRoutesList:
    """Tests for GET /routes"""

    @pytest.mark.asyncio
    async def test_list_routes_driver(self, client):
        """Regular driver should see their own routes."""
        with patch("main.supabase") as mock_sb:
            # get_user_driver_id lookup
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            routes_data = [
                {"id": "route-1", "driver_id": FAKE_DRIVER_ID, "status": "pending", "stops": []},
            ]
            routes_result = MagicMock()
            routes_result.data = routes_data

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "routes":
                    chain.select.return_value.eq.return_value.order.return_value.execute.return_value = routes_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/routes")

        assert response.status_code == 200
        data = response.json()
        assert "routes" in data

    @pytest.mark.asyncio
    async def test_list_routes_no_driver_profile(self, client):
        """User without driver profile should get empty routes."""
        with patch("main.supabase") as mock_sb:
            no_driver = MagicMock()
            no_driver.data = []

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = no_driver
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.get("/routes")

        assert response.status_code == 200
        data = response.json()
        assert data["routes"] == []


class TestRoutesCreate:
    """Tests for POST /routes"""

    @pytest.mark.asyncio
    async def test_create_route_success(self, client):
        """Creating a route for the authenticated user's driver should work."""
        route_payload = {
            "driver_id": FAKE_DRIVER_ID,
            "name": "Test Route",
            "stops": [
                {"address": "Calle Gran Via 1", "lat": 40.420, "lng": -3.705, "position": 0},
                {"address": "Calle Alcala 50", "lat": 40.418, "lng": -3.690, "position": 1},
            ],
            "total_distance_km": 3.5,
        }

        with patch("main.supabase") as mock_sb:
            # get_user_driver_id
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            # Insert route result -- must support attribute access because
            # main.py reassigns the 'route' variable and then does route.stops
            route_insert_result = MagicMock()
            route_insert_result.data = [AttrDict({
                "id": "new-route-id",
                "driver_id": FAKE_DRIVER_ID,
                "name": "Test Route",
                "status": "pending",
                "total_stops": 2,
                "stops": [
                    AttrDict({"address": "Calle Gran Via 1", "lat": 40.420, "lng": -3.705,
                              "position": 0, "notes": None, "phone": None,
                              "time_window_start": None, "time_window_end": None}),
                    AttrDict({"address": "Calle Alcala 50", "lat": 40.418, "lng": -3.690,
                              "position": 1, "notes": None, "phone": None,
                              "time_window_start": None, "time_window_end": None}),
                ],
            })]

            # Insert stops result
            stops_insert_result = MagicMock()
            stops_insert_result.data = []

            # Final route fetch with stops
            final_route = MagicMock()
            final_route.data = {
                "id": "new-route-id",
                "driver_id": FAKE_DRIVER_ID,
                "name": "Test Route",
                "status": "pending",
                "stops": [
                    {"id": "s1", "address": "Calle Gran Via 1"},
                    {"id": "s2", "address": "Calle Alcala 50"},
                ],
            }

            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] <= 1:
                        # insert call
                        chain.insert.return_value.execute.return_value = route_insert_result
                    else:
                        # final select
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = final_route
                elif name == "stops":
                    chain.insert.return_value.execute.return_value = stops_insert_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.post("/routes", json=route_payload)

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_create_route_empty_stops_rejected(self, client):
        """Route creation without stops should be rejected."""
        response = await client.post("/routes", json={
            "driver_id": FAKE_DRIVER_ID,
            "stops": [],
        })
        # May return 403 (auth check before validation) or 422
        assert response.status_code in (403, 422)


class TestRouteActions:
    """Tests for route start/complete/delete"""

    @pytest.mark.asyncio
    async def test_start_route(self, client):
        """Starting a route should succeed with proper access."""
        with patch("main.supabase") as mock_sb:
            # verify_route_access needs route lookup + driver lookup
            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]

            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            update_result = MagicMock()
            update_result.data = [{"id": "route-1", "status": "in_progress"}]

            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] == 1:
                        # verify_route_access select
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                    else:
                        # update call
                        chain.update.return_value.eq.return_value.execute.return_value = update_result
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.patch("/routes/route-1/start")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    @pytest.mark.asyncio
    async def test_complete_route(self, client):
        """Completing a route should succeed with proper access."""
        with patch("main.supabase") as mock_sb:
            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]

            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            update_result = MagicMock()
            update_result.data = [{"id": "route-1", "status": "completed"}]

            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                    else:
                        chain.update.return_value.eq.return_value.execute.return_value = update_result
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.patch("/routes/route-1/complete")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

    @pytest.mark.asyncio
    async def test_delete_route(self, client):
        """Deleting a route should succeed with proper access."""
        with patch("main.supabase") as mock_sb:
            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]

            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            stops_result = MagicMock()
            stops_result.data = [{"id": "stop-1"}]

            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                    else:
                        chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "stops":
                    chain.select.return_value.eq.return_value.execute.return_value = stops_result
                    chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                else:
                    chain.delete.return_value.eq.return_value.execute.return_value = MagicMock()
                    chain.delete.return_value.in_.return_value.execute.return_value = MagicMock()
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.delete("/routes/route-1")

        assert response.status_code == 200
        assert response.json()["success"] is True
