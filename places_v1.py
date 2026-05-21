"""Google Places API v1 (New) — paralelo a la implementación Legacy en main.py.

Razón: Legacy NO devuelve distance_meters aunque se pase origin (verificado
21 may 2026 con curl). New SÍ. Además Legacy está congelado desde 1 mar 2025
con sunset inevitable.

Política coste: field mask SOLO Essentials. Cualquier campo Pro (displayName,
rating, regularOpeningHours, phoneNumber, websiteUri, businessStatus, priceLevel,
userRatingCount, primaryType...) sube SKU 3.4x — test test_field_mask_whitelist
falla si alguien añade uno.

Compatibilidad: este módulo devuelve diccionarios con EL MISMO FORMATO que la app
ya espera de Legacy (predictions[], result.geometry.location, address_components,
etc.). El mapper hace la traducción interna. La app NO se entera del cambio.
"""

from __future__ import annotations

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


# Endpoints v1
_V1_AUTOCOMPLETE_URL = "https://places.googleapis.com/v1/places:autocomplete"
_V1_DETAILS_BASE_URL = "https://places.googleapis.com/v1/places"


# ───────────────────────────── Field masks ─────────────────────────────────
# Cualquier cambio aquí lo bloquea el test test_field_mask_whitelist en
# tests/test_places_v1.py — defensa anti cost-spike silencioso.

# Autocomplete: solo lo necesario para pintar la lista + re-orden por proximidad.
# distanceMeters es la razón principal de migrar a v1.
_AUTOCOMPLETE_FIELD_MASK = (
    "suggestions.placePrediction.placeId,"
    "suggestions.placePrediction.text,"
    "suggestions.placePrediction.structuredFormat,"
    "suggestions.placePrediction.types,"
    "suggestions.placePrediction.distanceMeters"
)

# Place Details: TODOS los campos elegidos están en SKU "Essentials" ($5/1000).
# Whitelist explícita usada por el test:
DETAILS_ESSENTIALS_ALLOWLIST = frozenset({
    "id",
    "location",
    "formattedAddress",
    "viewport",
    "addressComponents",
    "types",
})
_DETAILS_FIELD_MASK = ",".join(sorted(DETAILS_ESSENTIALS_ALLOWLIST))


# ───────────────────────────── Public API ──────────────────────────────────

async def autocomplete_v1(
    client: httpx.AsyncClient,
    api_key: str,
    *,
    input: str,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    country: Optional[str] = None,
    sessiontoken: Optional[str] = None,
    origin_lat: Optional[float] = None,
    origin_lng: Optional[float] = None,
    timeout: float = 20.0,
) -> dict:
    """Llama Places API v1 autocomplete y devuelve formato Legacy compatible.

    Devuelve dict con shape Legacy: {status, predictions[], error_message?}
    para que el handler en main.py no necesite distinguir backend interno.
    """
    body: dict = {
        "input": input,
        "languageCode": "es",
    }

    # Dual bias strategy (21 may 2026 — replica Spoke/Circuit behaviour):
    #   - Si hay origin (= última stop conocida): bias estrecho 5 km centrado en
    #     esa stop. Caso "siguiente parada" típico: el driver ya está en una
    #     ciudad concreta y la próxima parada SUELE ser cerca. Bias estrecho
    #     evita que Google traiga matches exactos en otras ciudades cuando hay
    #     número específico ("calle X 12").
    #   - Si NO hay origin (= primera parada o búsqueda sin contexto): bias
    #     amplio 30 km centrado en GPS. Permite descubrir direcciones a través
    #     de la zona de reparto sin sesgar a una calle concreta.
    # Recomendación oficial Google: "consider specifying a smaller radius" para
    # mejorar el ranking de establishments cuando el bias point es preciso.
    if origin_lat is not None and origin_lng is not None:
        body["locationBias"] = {
            "circle": {
                "center": {
                    "latitude": float(origin_lat),
                    "longitude": float(origin_lng),
                },
                "radius": 5000.0,
            }
        }
        body["origin"] = {
            "latitude": float(origin_lat),
            "longitude": float(origin_lng),
        }
    elif lat is not None and lng is not None:
        body["locationBias"] = {
            "circle": {
                "center": {"latitude": float(lat), "longitude": float(lng)},
                "radius": 30000.0,
            }
        }

    cc = (country or "").strip().lower()
    if len(cc) == 2 and cc.isalpha():
        body["includedRegionCodes"] = [cc]
    if sessiontoken:
        body["sessionToken"] = sessiontoken

    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": _AUTOCOMPLETE_FIELD_MASK,
        "Content-Type": "application/json",
    }
    try:
        resp = await client.post(
            _V1_AUTOCOMPLETE_URL, json=body, headers=headers, timeout=timeout
        )
    except Exception as e:
        logger.warning(f"places_v1 autocomplete request error: {e}")
        return {
            "status": "ZERO_RESULTS",
            "predictions": [],
            "error_message": f"v1 request error: {e}",
        }

    if resp.status_code != 200:
        body_excerpt = resp.text[:200] if resp.text else ""
        logger.warning(
            f"places_v1 autocomplete http {resp.status_code}: {body_excerpt}"
        )
        return {
            "status": "ZERO_RESULTS",
            "predictions": [],
            "error_message": f"v1 http {resp.status_code}",
        }

    try:
        return _map_v1_autocomplete_to_legacy(resp.json())
    except Exception as e:
        logger.warning(f"places_v1 autocomplete mapper failed: {e}")
        return {
            "status": "ZERO_RESULTS",
            "predictions": [],
            "error_message": f"v1 mapper error: {e}",
        }


async def details_v1(
    client: httpx.AsyncClient,
    api_key: str,
    *,
    place_id: str,
    sessiontoken: Optional[str] = None,
    timeout: float = 10.0,
) -> dict:
    """Llama Places API v1 details y devuelve formato Legacy compatible."""
    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": _DETAILS_FIELD_MASK,
    }
    params: dict = {"languageCode": "es"}
    if sessiontoken:
        params["sessionToken"] = sessiontoken
    url = f"{_V1_DETAILS_BASE_URL}/{place_id}"

    try:
        resp = await client.get(url, params=params, headers=headers, timeout=timeout)
    except Exception as e:
        logger.warning(f"places_v1 details request error: {e}")
        return {"status": "UNKNOWN_ERROR", "error_message": f"v1 request error: {e}"}

    if resp.status_code != 200:
        body_excerpt = resp.text[:200] if resp.text else ""
        logger.warning(
            f"places_v1 details http {resp.status_code}: {body_excerpt}"
        )
        # 404 Google v1 = place_id no encontrado (equivale a INVALID_REQUEST Legacy)
        status = "NOT_FOUND" if resp.status_code == 404 else "UNKNOWN_ERROR"
        return {"status": status, "error_message": f"v1 http {resp.status_code}"}

    try:
        return _map_v1_details_to_legacy(resp.json())
    except Exception as e:
        logger.warning(f"places_v1 details mapper failed: {e}")
        return {"status": "UNKNOWN_ERROR", "error_message": f"v1 mapper error: {e}"}


# ───────────────────────────── Mappers ─────────────────────────────────────

def _map_v1_autocomplete_to_legacy(v1: dict) -> dict:
    """Convierte response v1 al formato Legacy que la app espera.

    v1: {suggestions: [{placePrediction: {placeId, text:{text}, structuredFormat:{mainText:{text}, secondaryText:{text}}, types[], distanceMeters?}}]}
    Legacy: {status, predictions: [{place_id, description, structured_formatting:{main_text, secondary_text}, types[], distance_meters?}]}
    """
    suggestions = v1.get("suggestions") or []
    predictions: list[dict] = []
    for s in suggestions:
        pp = s.get("placePrediction")
        if not pp:
            continue
        text_obj = pp.get("text") or {}
        sf = pp.get("structuredFormat") or {}
        main_text_obj = sf.get("mainText") or {}
        secondary_text_obj = sf.get("secondaryText") or {}
        pred: dict = {
            "place_id": pp.get("placeId"),
            "description": text_obj.get("text", ""),
            "structured_formatting": {
                "main_text": main_text_obj.get("text", ""),
                "secondary_text": secondary_text_obj.get("text", ""),
            },
            "types": pp.get("types") or [],
        }
        # ESTA es la razón principal de la migración a v1.
        if "distanceMeters" in pp:
            pred["distance_meters"] = pp["distanceMeters"]
        predictions.append(pred)

    return {
        "status": "OK" if predictions else "ZERO_RESULTS",
        "predictions": predictions,
    }


def _map_v1_details_to_legacy(v1: dict) -> dict:
    """Convierte response Details v1 al formato Legacy que la app espera.

    v1: {id, location:{latitude, longitude}, formattedAddress, viewport, addressComponents:[{longText, shortText, types[]}], types[]}
    Legacy: {status, result: {geometry:{location:{lat, lng}}, address_components:[{long_name, short_name, types[]}], formatted_address, types[], place_id}}
    """
    loc = v1.get("location") or {}
    addr_comps: list[dict] = []
    for c in v1.get("addressComponents") or []:
        addr_comps.append({
            "long_name": c.get("longText", ""),
            "short_name": c.get("shortText", ""),
            "types": c.get("types") or [],
        })

    result: dict = {
        "geometry": {
            "location": {
                "lat": loc.get("latitude"),
                "lng": loc.get("longitude"),
            }
        },
        "address_components": addr_comps,
        "formatted_address": v1.get("formattedAddress", ""),
        "types": v1.get("types") or [],
        "place_id": v1.get("id", ""),
    }
    # viewport opcional (no roto si no llega — la app no lo usa hoy)
    if v1.get("viewport"):
        result["geometry"]["viewport"] = v1["viewport"]

    return {"status": "OK", "result": result}
