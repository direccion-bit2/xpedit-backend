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

        with patch("main.hybrid_optimize_route") as mock_optimize:
            mock_optimize.return_value = {
                "success": True,
                "route": locations,
                "total_distance_km": 5.2,
                "total_distance_meters": 5200,
                "num_stops": 3,
                "solver": "vroom",
            }
            response = await client.post("/optimize", json={
                "locations": locations,
                "start_index": 0,
            })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "total_distance_km" in data
        assert "solver" in data

    @pytest.mark.asyncio
    async def test_optimize_single_stop(self, client):
        """Optimization with a single stop should still work."""
        locations = [{"lat": 40.416775, "lng": -3.703790}]

        with patch("main.hybrid_optimize_route") as mock_optimize:
            mock_optimize.return_value = {
                "success": True,
                "route": locations,
                "total_distance_km": 0,
                "total_distance_meters": 0,
                "num_stops": 1,
                "solver": "none",
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
        """Optimization with more than 500 stops should return 400."""
        locations = [{"lat": 40.0 + i * 0.001, "lng": -3.0} for i in range(501)]
        response = await client.post("/optimize", json={
            "locations": locations,
        })
        assert response.status_code == 400
        assert "500" in response.json()["detail"]

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
        """More than 500 stops should be rejected."""
        locations = [{"lat": 40.0 + i * 0.001, "lng": -3.0} for i in range(501)]
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
        """Successful geocoding via Google → returns lat/lng + place_id + location_type."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "OK",
            "results": [{
                "geometry": {
                    "location": {"lat": 40.416775, "lng": -3.703790},
                    "location_type": "ROOFTOP",
                },
                "formatted_address": "Puerta del Sol, 28013 Madrid, Spain",
                "place_id": "ChIJSomePlaceId",
            }],
        }
        mock_response.status_code = 200

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__aenter__.return_value = mock_http_client
        mock_http_client.__aexit__.return_value = False

        with patch("main.httpx.AsyncClient", return_value=mock_http_client), \
             patch("main.GOOGLE_API_KEY", "fake-key"):
            response = await client.post("/geocode", json={
                "address": "Puerta del Sol, Madrid",
                "country": "ES",
            })

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["lat"] == 40.416775
        assert data["lng"] == -3.703790
        assert data["place_id"] == "ChIJSomePlaceId"
        assert data["location_type"] == "ROOFTOP"
        # Verify country biases the request
        call_params = mock_http_client.get.call_args.kwargs["params"]
        assert call_params["components"] == "country:ES"
        assert call_params["region"] == "es"

    @pytest.mark.asyncio
    async def test_geocode_zero_results(self, client):
        """When Google returns ZERO_RESULTS, surface a 200 with success=False."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "ZERO_RESULTS", "results": []}
        mock_response.status_code = 200

        mock_http_client = AsyncMock()
        mock_http_client.get.return_value = mock_response
        mock_http_client.__aenter__.return_value = mock_http_client
        mock_http_client.__aexit__.return_value = False

        with patch("main.httpx.AsyncClient", return_value=mock_http_client), \
             patch("main.GOOGLE_API_KEY", "fake-key"):
            response = await client.post("/geocode", json={"address": "asdfghjkl qwerty"})

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is False
        assert "no encontrada" in body["error"].lower()

    @pytest.mark.asyncio
    async def test_geocode_no_api_key_returns_503(self, client):
        with patch("main.GOOGLE_API_KEY", ""):
            response = await client.post("/geocode", json={"address": "Calle Mayor 1"})
        assert response.status_code == 503

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

            # Insert stops result (non-empty = success)
            stops_insert_result = MagicMock()
            stops_insert_result.data = [
                {"id": "s1", "address": "Calle Gran Via 1"},
                {"id": "s2", "address": "Calle Alcala 50"},
            ]

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
    async def test_create_route_assigns_company_id_for_company_driver(self, client):
        """Si el driver pertenece a una empresa, la ruta creada DEBE llevar company_id
        (cimiento V1 multi-empresa: el dispatcher solo ve/gestiona rutas de su empresa)."""
        route_payload = {
            "driver_id": FAKE_DRIVER_ID,
            "name": "Ruta Empresa",
            "stops": [{"address": "Calle Gran Via 1", "lat": 40.420, "lng": -3.705, "position": 0}],
            "total_distance_km": 1.0,
        }
        captured = {}
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": "comp-xyz"}]
            route_insert_result = MagicMock()
            route_insert_result.data = [AttrDict({"id": "new-route-id", "driver_id": FAKE_DRIVER_ID,
                                                  "name": "Ruta Empresa", "status": "pending",
                                                  "total_stops": 1, "stops": []})]
            stops_insert_result = MagicMock()
            stops_insert_result.data = [{"id": "s1"}]
            final_route = MagicMock()
            final_route.data = {"id": "new-route-id", "company_id": "comp-xyz", "stops": []}
            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] <= 1:
                        def capture_insert(data):
                            captured["route_data"] = data
                            m = MagicMock()
                            m.execute.return_value = route_insert_result
                            return m
                        chain.insert.side_effect = capture_insert
                    else:
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = final_route
                elif name == "stops":
                    chain.insert.return_value.execute.return_value = stops_insert_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            response = await client.post("/routes", json=route_payload)

        assert response.status_code == 200
        assert captured["route_data"].get("company_id") == "comp-xyz"

    @pytest.mark.asyncio
    async def test_create_route_no_company_id_for_solo_driver(self, client):
        """Un driver self-service (sin empresa) NO debe meter company_id en la ruta
        → comportamiento idéntico al actual, cero impacto para drivers sueltos."""
        route_payload = {
            "driver_id": FAKE_DRIVER_ID,
            "name": "Ruta Solo",
            "stops": [{"address": "Calle Gran Via 1", "lat": 40.420, "lng": -3.705, "position": 0}],
            "total_distance_km": 1.0,
        }
        captured = {}
        with patch("main.supabase") as mock_sb:
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]
            route_insert_result = MagicMock()
            route_insert_result.data = [AttrDict({"id": "new-route-id", "driver_id": FAKE_DRIVER_ID,
                                                  "name": "Ruta Solo", "status": "pending",
                                                  "total_stops": 1, "stops": []})]
            stops_insert_result = MagicMock()
            stops_insert_result.data = [{"id": "s1"}]
            final_route = MagicMock()
            final_route.data = {"id": "new-route-id", "company_id": None, "stops": []}
            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] <= 1:
                        def capture_insert(data):
                            captured["route_data"] = data
                            m = MagicMock()
                            m.execute.return_value = route_insert_result
                            return m
                        chain.insert.side_effect = capture_insert
                    else:
                        chain.select.return_value.eq.return_value.single.return_value.execute.return_value = final_route
                elif name == "stops":
                    chain.insert.return_value.execute.return_value = stops_insert_result
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            response = await client.post("/routes", json=route_payload)

        assert response.status_code == 200
        assert "company_id" not in captured["route_data"]

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
        """Completing a route flips status to 'completed' + stamps completed_at.
        The route stays visible in the history (NO deleted_at): UI / cold-start
        filters keep it off the home screen by status, not by soft-delete
        (Miguel, 12 may 2026: finalizada = en historial, no en pantalla)."""
        with patch("main.supabase") as mock_sb:
            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]

            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            update_result = MagicMock()
            update_result.data = [{
                "id": "route-1",
                "status": "completed",
                "completed_at": "2026-05-12T11:00:00+00:00",
                "deleted_at": None,
            }]

            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] == 1:
                        # verify_route_access → SELECT id, driver_id
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                    else:
                        # UPDATE chain: .neq('status','completed') for idempotency
                        chain.update.return_value.eq.return_value.neq.return_value.execute.return_value = update_result
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)

            response = await client.patch("/routes/route-1/complete")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["route"]["completed_at"] is not None
        # Finalised routes belong in history → no deleted_at.
        assert data["route"]["deleted_at"] is None

    @pytest.mark.asyncio
    async def test_complete_route_already_finalized_is_idempotent(self, client):
        """If the route was already completed, /complete returns 200 with
        already_finalized=true so the app cleans local state without
        raising. Prevents zombie routes from re-appearing on retry."""
        with patch("main.supabase") as mock_sb:
            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            empty_update = MagicMock()
            empty_update.data = []  # nothing matched (already completed)
            already_completed = MagicMock()
            already_completed.data = [{
                "id": "route-1",
                "status": "completed",
                "completed_at": "2026-05-12T09:00:00+00:00",
                "deleted_at": None,
            }]

            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                    elif call_count["routes"] == 2:
                        chain.update.return_value.eq.return_value.neq.return_value.execute.return_value = empty_update
                    else:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = already_completed
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            response = await client.patch("/routes/route-1/complete")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data.get("already_finalized") is True

    @pytest.mark.asyncio
    async def test_clear_route(self, client):
        """PATCH /routes/{id}/clear archives the route + cascades stops
        via trigger. Replaces the old client-side `supabase.update` that
        was hitting RLS 42501 when the JWT went stale (Sentry NATIVE-30)."""
        with patch("main.supabase") as mock_sb:
            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            update_result = MagicMock()
            update_result.data = [{
                "id": "route-1",
                "status": "cancelled",
                "deleted_at": "2026-05-12T11:30:00+00:00",
            }]
            stops_count = MagicMock()
            stops_count.count = 5

            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                    else:
                        chain.update.return_value.eq.return_value.is_.return_value.execute.return_value = update_result
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "stops":
                    chain.select.return_value.eq.return_value.not_.is_.return_value.execute.return_value = stops_count
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            response = await client.patch("/routes/route-1/clear")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["route"]["deleted_at"] is not None
        assert data["stops_cleared"] == 5

    @pytest.mark.asyncio
    async def test_clear_route_already_archived_is_idempotent(self, client):
        """Re-clearing an already-archived route returns 200 + already_archived=true."""
        with patch("main.supabase") as mock_sb:
            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]
            empty_update = MagicMock()
            empty_update.data = []
            already_archived = MagicMock()
            already_archived.data = [{
                "id": "route-1",
                "status": "cancelled",
                "deleted_at": "2026-05-12T08:00:00+00:00",
            }]
            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                    elif call_count["routes"] == 2:
                        chain.update.return_value.eq.return_value.is_.return_value.execute.return_value = empty_update
                    else:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = already_archived
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            response = await client.patch("/routes/route-1/clear")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data.get("already_archived") is True

    @pytest.mark.asyncio
    async def test_clear_route_requires_ownership(self, client):
        """clear_route must reject a driver who doesn't own the route."""
        with patch("main.supabase") as mock_sb:
            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": "different-driver"}]
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "routes":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            response = await client.patch("/routes/route-1/clear")

        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_delete_route_soft_deletes_never_hard(self, client):
        """DELETE /routes/{id} must SOFT-delete (set deleted_at), NEVER hard-delete.

        The old hard-delete wiped completed stops from the 'trabajadas' count
        (the canonical metric counts completed/failed INCLUDING deleted_at) and
        caused REACT-NATIVE-1E silent drops when an offline complete/fail op
        targeted a stop of the deleted route. trg_soft_delete_route_stops cascades
        deleted_at to the stops; proofs/tracking are kept."""
        with patch("main.supabase") as mock_sb:
            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]
            update_result = MagicMock()
            update_result.data = [{"id": "route-1", "deleted_at": "2026-05-29T11:00:00+00:00"}]
            stops_count = MagicMock()
            stops_count.count = 7

            hard_delete_called = {"any": False}
            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                # A .delete() on ANY table is a hard-delete → must NEVER happen.
                def _mark_hard_delete(*a, **k):
                    hard_delete_called["any"] = True
                    return MagicMock()
                chain.delete.side_effect = _mark_hard_delete
                if name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                    else:
                        chain.update.return_value.eq.return_value.is_.return_value.execute.return_value = update_result
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                elif name == "stops":
                    chain.select.return_value.eq.return_value.not_.is_.return_value.execute.return_value = stops_count
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            response = await client.delete("/routes/route-1")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["stops_deleted"] == 7
        assert hard_delete_called["any"] is False, "delete_route must NOT hard-delete any row"

    @pytest.mark.asyncio
    async def test_delete_route_idempotent(self, client):
        """Re-deleting an already soft-deleted route → 200 + already_deleted=true."""
        with patch("main.supabase") as mock_sb:
            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]
            empty_update = MagicMock()
            empty_update.data = []
            already = MagicMock()
            already.data = [{"id": "route-1", "deleted_at": "2026-05-29T08:00:00+00:00"}]
            call_count = {"routes": 0}

            def table_dispatch(name):
                chain = MagicMock()
                if name == "routes":
                    call_count["routes"] += 1
                    if call_count["routes"] == 1:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                    elif call_count["routes"] == 2:
                        chain.update.return_value.eq.return_value.is_.return_value.execute.return_value = empty_update
                    else:
                        chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = already
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            response = await client.delete("/routes/route-1")

        assert response.status_code == 200
        assert response.json().get("already_deleted") is True

    @pytest.mark.asyncio
    async def test_delete_route_requires_ownership(self, client):
        """delete_route must reject a driver who doesn't own the route."""
        with patch("main.supabase") as mock_sb:
            route_access = MagicMock()
            route_access.data = [{"id": "route-1", "driver_id": "different-driver"}]
            driver_lookup = MagicMock()
            driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

            def table_dispatch(name):
                chain = MagicMock()
                if name == "routes":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                elif name == "drivers":
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
                return chain

            mock_sb.table = MagicMock(side_effect=table_dispatch)
            response = await client.delete("/routes/route-1")

        assert response.status_code == 403


class TestReconcileOptimization:
    """Tests for PATCH /routes/{route_id}/reconcile-optimization."""

    def _mock_dispatch(self, existing_route: dict):
        """Build a Supabase table mock that yields existing_route on the
        first select+single chain (route lookup), and absorbs the UPDATE chain
        used to persist hash + polyline."""
        driver_lookup = MagicMock()
        driver_lookup.data = [{"id": FAKE_DRIVER_ID, "company_id": None}]

        route_access = MagicMock()
        route_access.data = [{"id": "route-1", "driver_id": FAKE_DRIVER_ID}]

        route_select = MagicMock()
        route_select.data = existing_route

        call_count = {"routes": 0}

        def table_dispatch(name):
            chain = MagicMock()
            if name == "routes":
                call_count["routes"] += 1
                if call_count["routes"] == 1:
                    chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = route_access
                elif call_count["routes"] == 2:
                    chain.select.return_value.eq.return_value.limit.return_value.single.return_value.execute.return_value = route_select
                else:
                    chain.update.return_value.eq.return_value.execute.return_value = MagicMock()
            elif name == "drivers":
                chain.select.return_value.eq.return_value.limit.return_value.execute.return_value = driver_lookup
            elif name == "stops":
                chain.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
            return chain

        return table_dispatch

    @pytest.mark.asyncio
    async def test_persists_when_bd_has_no_hash(self, client):
        """BD vacío + cliente envía hash → escribe, success=true."""
        with patch("main.supabase") as mock_sb:
            mock_sb.table = MagicMock(
                side_effect=self._mock_dispatch({"id": "route-1", "status": "in_progress", "optimized_hash": None})
            )
            response = await client.patch(
                "/routes/route-1/reconcile-optimization",
                json={"optimized_hash": "abc123", "polyline_points": [[1.0, 2.0]]},
            )
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_rejects_hash_mismatch_without_force(self, client):
        """BD ya tiene hash distinto + force ausente → success=false hash_mismatch."""
        with patch("main.supabase") as mock_sb:
            mock_sb.table = MagicMock(
                side_effect=self._mock_dispatch({"id": "route-1", "status": "in_progress", "optimized_hash": "old"})
            )
            response = await client.patch(
                "/routes/route-1/reconcile-optimization",
                json={"optimized_hash": "new"},
            )
        body = response.json()
        assert response.status_code == 200
        assert body["success"] is False
        assert body["reason"] == "hash_mismatch"
        assert body["current_hash"] == "old"

    @pytest.mark.asyncio
    async def test_force_overwrites_existing_hash(self, client):
        """BD tiene hash distinto + force=true (post-optimize) → sobrescribe."""
        with patch("main.supabase") as mock_sb:
            mock_sb.table = MagicMock(
                side_effect=self._mock_dispatch({"id": "route-1", "status": "in_progress", "optimized_hash": "old"})
            )
            response = await client.patch(
                "/routes/route-1/reconcile-optimization",
                json={"optimized_hash": "new", "polyline_points": [[1.0, 2.0]], "force": True},
            )
        assert response.status_code == 200
        assert response.json()["success"] is True

    @pytest.mark.asyncio
    async def test_refuses_completed_route(self, client):
        """Ruta completed no es reconciliable (evita reabrir histórico)."""
        with patch("main.supabase") as mock_sb:
            mock_sb.table = MagicMock(
                side_effect=self._mock_dispatch({"id": "route-1", "status": "completed", "optimized_hash": None})
            )
            response = await client.patch(
                "/routes/route-1/reconcile-optimization",
                json={"optimized_hash": "abc"},
            )
        assert response.status_code == 409
