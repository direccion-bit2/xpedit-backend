"""Persistencia OCR (fix v1.1): el número de portal y el CP del modelo deben
guardarse de verdad en ocr_corrections, y la confianza debe ser un número real.

Blinda el bug raíz del OCR v1: model_extracted_parts omitía 'number' y usaba la
clave camelCase 'postalCode' (el modelo devuelve 'postal_code'), y model_confidence
leía 'extraction_confidence' (clave inexistente) → number/CP/confianza se perdían.
"""
from main import (
    _msi_choose_formatted,
    _msi_model_parts,
    _msi_numeric_confidence,
    _msi_should_retry_without_country,
    _msi_street_is_empty,
)


class TestMsiChooseFormatted:
    """Qué dirección mostrar: conservar lo leído si el geocoding degrada/falla."""

    def test_geocoding_usable_usa_formatted_de_google(self):
        out = _msi_choose_formatted(False, True, "Pl. de las Infantas, 24, 11540 Sanlúcar", "Plaza de las Infantas 24, 11540 Sanlucar")
        assert out == "Pl. de las Infantas, 24, 11540 Sanlúcar"

    def test_geocoding_degradado_conserva_lo_leido(self):
        # Google degradó a "11540 Sanlúcar" (sin calle) → usar el flat leído.
        out = _msi_choose_formatted(False, False, "11540 Sanlúcar de Barrameda, Cádiz, España", "CALLE VIDRIEROS 1, 11540 SANLUCAR DE BARRAMEDA")
        assert out == "CALLE VIDRIEROS 1, 11540 SANLUCAR DE BARRAMEDA"

    def test_zero_results_conserva_lo_leido(self):
        out = _msi_choose_formatted(False, False, "", "Colonia monte algaida c/H 16, 11540 Sanlucar")
        assert out == "Colonia monte algaida c/H 16, 11540 Sanlucar"

    def test_vacia_devuelve_cadena_vacia(self):
        assert _msi_choose_formatted(True, False, "", "") == ""


class TestMsiShouldRetryWithoutCountry:
    """Reintento de geocoding sin lock de país cuando el país del móvil viene mal."""

    def test_reintenta_si_cp_5_digitos_en_round1(self):
        assert _msi_should_retry_without_country({"postal_code": "24001"}, None) is True

    def test_no_reintenta_en_round2_con_bbox(self):
        # round 2 (bbox) ya es un reintento; no encadenar más llamadas.
        assert _msi_should_retry_without_country({"postal_code": "24001"}, {"sw_lat": 1}) is False

    def test_no_reintenta_sin_cp_valido(self):
        assert _msi_should_retry_without_country({"postal_code": ""}, None) is False
        assert _msi_should_retry_without_country({"postal_code": "ABC"}, None) is False
        assert _msi_should_retry_without_country({"postal_code": "123"}, None) is False
        assert _msi_should_retry_without_country({}, None) is False


class TestMsiStreetIsEmpty:
    """is_empty no debe descartar direcciones válidas (bug visto en staging)."""

    def test_rescata_lugar_sin_tipo_via_con_numero_cp_ciudad(self):
        # "LA CERAMICA 15, 11130 Chiclana": el modelo la leyó bien; sin el rescate
        # se marcaba vacía solo porque "La Cerámica" no empieza por Calle/Avda.
        stop = {"street": "LA CERAMICA", "number": "15", "postal_code": "11130", "city": "Chiclana"}
        assert _msi_street_is_empty(stop) is False

    def test_calle_con_tipo_via_no_es_vacia(self):
        assert _msi_street_is_empty({"street": "Calle Mayor"}) is False

    def test_calle_con_digito_no_es_vacia(self):
        assert _msi_street_is_empty({"street": "Gran Via 12"}) is False

    def test_street_vacio_es_vacia(self):
        assert _msi_street_is_empty({"street": ""}) is True
        assert _msi_street_is_empty({}) is True

    def test_nombre_lugar_sin_numero_sigue_vacia(self):
        # Solo ciudad/lugar en street, sin número → NO entregable, sigue vacía.
        stop = {"street": "San José del Valle", "postal_code": "11580", "city": "San José del Valle"}
        assert _msi_street_is_empty(stop) is True

    def test_lugar_con_cp_invalido_sigue_vacia(self):
        # Número pero CP no de 5 dígitos → no se rescata.
        stop = {"street": "La Cerámica", "number": "15", "postal_code": "111", "city": "Chiclana"}
        assert _msi_street_is_empty(stop) is True


class TestMsiModelParts:
    def test_incluye_number_y_postal_code_con_claves_correctas(self):
        stop = {
            "name": "Jesús Gregori",
            "street": "Virgilio Bernabéu",
            "number": "10",
            "postal_code": "46270",
            "city": "Castelló",
            "province": "Valencia",
            "floor_etc": "3 B",
            "confidence_per_field": {"street": 0.9, "number": 0.8, "postal_code": 0.95},
            "geocoding_status": "ok",
            "place_id": "abc123",
        }
        parts = _msi_model_parts(stop)
        # El número de portal AHORA se guarda (antes se omitía)
        assert parts["number"] == "10"
        # El CP se guarda bajo la clave correcta snake_case (antes buscaba 'postalCode' → se perdía)
        assert parts["postal_code"] == "46270"
        assert "postalCode" not in parts
        # Se persiste la confianza por campo y la traza de geocoding
        assert parts["confidence_per_field"]["number"] == 0.8
        assert parts["geocoding_status"] == "ok"
        assert parts["place_id"] == "abc123"

    def test_omite_campos_vacios_o_nulos(self):
        stop = {"street": "Calle Mayor", "number": "", "postal_code": None, "city": "Gijón"}
        parts = _msi_model_parts(stop)
        assert parts["street"] == "Calle Mayor"
        assert parts["city"] == "Gijón"
        assert "number" not in parts      # cadena vacía no se guarda
        assert "postal_code" not in parts  # None no se guarda


class TestMsiNumericConfidence:
    def test_usa_minimo_de_confianza_por_campo(self):
        stop = {"confidence_per_field": {"street": 0.9, "number": 0.4, "postal_code": 0.8}}
        # El campo más débil manda (el número, 0.4)
        assert _msi_numeric_confidence(stop) == 0.4

    def test_fallback_a_categoria_si_no_hay_por_campo(self):
        assert _msi_numeric_confidence({"confidence": "high"}) == 0.9
        assert _msi_numeric_confidence({"confidence": "medium"}) == 0.6
        assert _msi_numeric_confidence({"confidence": "low"}) == 0.3

    def test_extraccion_vacia_siempre_baja(self):
        stop = {"is_empty_extraction": True, "confidence_per_field": {"street": 0.9}}
        assert _msi_numeric_confidence(stop) == 0.1

    def test_sin_datos_devuelve_none_no_revienta(self):
        # Antes: float(stop['extraction_confidence']) → KeyError/None silencioso.
        assert _msi_numeric_confidence({}) is None
