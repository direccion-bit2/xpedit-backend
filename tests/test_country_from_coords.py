"""Tests for _country_iso_from_coords helper used by /places/autocomplete.

Bug 20 may 2026: Christian Yáñez (paying, La Serena Chile, BD country=AR) had
0 results in autocomplete because components=country:ar is a HARD FILTER in
Google Places. Helper added to detect GPS-vs-country mismatch and skip the
hard filter when GPS clearly disagrees.

These tests pin the bounding boxes used in production. If you tighten/loosen
a box, fix the test on purpose — silent shifts here cause user-visible
"no results" or wrong-country bias.
"""

import main


class TestCountryFromCoords:
    def test_none_when_no_coords(self):
        assert main._country_iso_from_coords(None, None) is None
        assert main._country_iso_from_coords(40.4, None) is None
        assert main._country_iso_from_coords(None, -3.7) is None

    def test_spain_madrid(self):
        # Madrid centro
        assert main._country_iso_from_coords(40.4168, -3.7038) == "ES"

    def test_spain_canarias(self):
        # Las Palmas
        assert main._country_iso_from_coords(28.1235, -15.4363) == "ES"
        # Tenerife
        assert main._country_iso_from_coords(28.4636, -16.2518) == "ES"

    def test_chile_la_serena_christian_case(self):
        # Caso canónico: Christian Yáñez en La Serena. Antes del fix, el flag
        # AR ganaba y app devolvía 0 resultados.
        assert main._country_iso_from_coords(-29.9264746, -71.2592614) == "CL"

    def test_chile_santiago(self):
        assert main._country_iso_from_coords(-33.4489, -70.6693) == "CL"

    def test_argentina_buenos_aires(self):
        assert main._country_iso_from_coords(-34.6037, -58.3816) == "AR"

    def test_argentina_cordoba(self):
        assert main._country_iso_from_coords(-31.4201, -64.1888) == "AR"

    def test_frontera_neuquen_returns_none(self):
        # Andrea Duimovich: Neuquén lng ≈ -68.04. Es Argentina REAL pero la
        # cordillera está justo al lado → preferimos None (mantener el filter
        # actual) que riesgo de falso positivo.
        assert main._country_iso_from_coords(-38.9454, -68.0450) is None

    def test_mexico_cdmx(self):
        assert main._country_iso_from_coords(19.4326, -99.1332) == "MX"

    def test_colombia_bogota(self):
        assert main._country_iso_from_coords(4.7110, -74.0721) == "CO"

    def test_ecuador_quito(self):
        assert main._country_iso_from_coords(-0.1807, -78.4678) == "EC"

    def test_peru_lima(self):
        assert main._country_iso_from_coords(-12.0464, -77.0428) == "PE"

    def test_uruguay_montevideo(self):
        assert main._country_iso_from_coords(-34.9011, -56.1645) == "UY"

    def test_mid_atlantic_returns_none(self):
        # Atlántico medio — fuera de cualquier país conocido
        assert main._country_iso_from_coords(0.0, -30.0) is None

    def test_north_pole_returns_none(self):
        assert main._country_iso_from_coords(85.0, 0.0) is None
