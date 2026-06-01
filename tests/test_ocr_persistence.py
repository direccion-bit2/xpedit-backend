"""Persistencia OCR (fix v1.1): el número de portal y el CP del modelo deben
guardarse de verdad en ocr_corrections, y la confianza debe ser un número real.

Blinda el bug raíz del OCR v1: model_extracted_parts omitía 'number' y usaba la
clave camelCase 'postalCode' (el modelo devuelve 'postal_code'), y model_confidence
leía 'extraction_confidence' (clave inexistente) → number/CP/confianza se perdían.
"""
from main import _msi_model_parts, _msi_numeric_confidence


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
