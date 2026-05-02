"""Street closures scraper.

Reads official municipal websites that publish street closures, parses each
entry, geocodes both segment endpoints + the segment polyline using Google
Directions API, and upserts into the `street_closures` table on Supabase.
Runs every 30 minutes via APScheduler. Idempotent on (source, source_url).

Currently supports:
- Sanlúcar de Barrameda (https://www.sanlucardebarrameda.es/es/cortes-de-calle)

To add a new city, write a new `scrape_<city>()` returning a list of
ClosureRecord and register it in `ALL_SCRAPERS`.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------- Data shape ----------

@dataclass
class ClosureRecord:
    source: str
    source_url: str
    city: str
    street_name: str
    starts_at: datetime
    ends_at: datetime
    segment_from: str | None = None
    segment_to: str | None = None
    closure_type: str | None = None
    reason: str | None = None
    all_day: bool = False
    time_window_start: str | None = None
    time_window_end: str | None = None
    # Geocoding result, filled in after the scraper returns (or by the scraper itself).
    # `lat`/`lng` is always set (midpoint of segment OR street centroid).
    # `lat_from/to` are only set when we have a real two-endpoint segment.
    # `street_polyline` is the Google encoded polyline between the two endpoints,
    # following the actual road geometry (not a straight line).
    lat: float | None = None
    lng: float | None = None
    lat_from: float | None = None
    lng_from: float | None = None
    lat_to: float | None = None
    lng_to: float | None = None
    street_polyline: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


# ---------- Geocoding helpers ----------

async def _geocode(
    client: httpx.AsyncClient, google_api_key: str, query: str
) -> tuple[float, float] | None:
    """Geocode a free-form address string. Returns (lat, lng) or None."""
    try:
        resp = await client.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": query, "key": google_api_key, "language": "es",
                    "region": "es", "components": "country:ES"},
            timeout=10,
        )
        data = resp.json()
        if data.get("status") == "OK" and data.get("results"):
            loc = data["results"][0]["geometry"]["location"]
            return loc["lat"], loc["lng"]
    except Exception as e:
        logger.warning(f"Geocoding failed for '{query}': {e}")
    return None


async def _directions_polyline(
    client: httpx.AsyncClient, google_api_key: str,
    origin: tuple[float, float], destination: tuple[float, float],
) -> str | None:
    """Get encoded polyline of the actual road geometry between two points
    using Google Directions API. Returns None if the API fails. We use
    `mode=driving` so the polyline follows car-accessible roads (one-ways,
    pedestrian zones excluded).
    """
    try:
        resp = await client.get(
            "https://maps.googleapis.com/maps/api/directions/json",
            params={
                "origin": f"{origin[0]},{origin[1]}",
                "destination": f"{destination[0]},{destination[1]}",
                "mode": "driving",
                "key": google_api_key,
                "language": "es",
            },
            timeout=10,
        )
        data = resp.json()
        if data.get("status") == "OK" and data.get("routes"):
            return data["routes"][0]["overview_polyline"]["points"]
    except Exception as e:
        logger.warning(f"Directions failed {origin}→{destination}: {e}")
    return None


async def geocode_record(
    client: httpx.AsyncClient, google_api_key: str, record: ClosureRecord
) -> None:
    """Mutates `record` with geocoded coordinates and (if a segment) the polyline.

    Strategy:
    - If we have BOTH segment_from and segment_to: geocode each intersection
      separately and ask Directions API for the road geometry between them.
      Set lat_from/to, lng_from/to, street_polyline, and lat/lng=midpoint.
    - Else: geocode the street centroid only. Set lat/lng.
    """
    if record.segment_from and record.segment_to:
        # Try the two intersections in series so we don't double-bill if one fails.
        q_from = f"{record.street_name} y {record.segment_from}, {record.city}, España"
        q_to = f"{record.street_name} y {record.segment_to}, {record.city}, España"
        a = await _geocode(client, google_api_key, q_from)
        b = await _geocode(client, google_api_key, q_to)
        if a and b:
            record.lat_from, record.lng_from = a
            record.lat_to, record.lng_to = b
            record.lat = (a[0] + b[0]) / 2
            record.lng = (a[1] + b[1]) / 2
            # Real road geometry between the two endpoints
            record.street_polyline = await _directions_polyline(
                client, google_api_key, a, b
            )
            return
        # Partial: only one endpoint geocoded. Fall through to street centroid
        # so we at least have a marker.
        if a:
            record.lat, record.lng = a
            return
        if b:
            record.lat, record.lng = b
            return
    # No segment, or both intersections failed: geocode the street centroid
    coords = await _geocode(
        client, google_api_key,
        f"{record.street_name}, {record.city}, España",
    )
    if coords:
        record.lat, record.lng = coords


# ---------- Sanlúcar scraper ----------

SANLUCAR_LIST_URL = "https://www.sanlucardebarrameda.es/es/cortes-de-calle"
SANLUCAR_BASE = "https://www.sanlucardebarrameda.es"

# DD/MM/YYYY HH:MM
_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})(?:\s*(?:-|a las)?\s*(\d{1,2}):(\d{2}))?")

# "TRAMO DE [from] A [to]". Tolerates "TRAMO DE CALZADA DE", "TRAMO CALLE", etc.
_LOCALIZATION_TRAMO_RE = re.compile(
    r"TRAMO\s+(?:DE\s+)?(?:CALZADA\s+(?:DE\s+)?)?(.+?)\s+A\s+(.+?)$",
    re.IGNORECASE,
)


def _parse_dt_es(date_str: str, time_str: str | None) -> datetime | None:
    """Parse 'DD/MM/YYYY' + optional 'HH:MM'. Returns timezone-aware UTC."""
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if not m:
        return None
    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    hour = minute = 0
    if time_str:
        tm = re.match(r"(\d{1,2}):(\d{2})", time_str)
        if tm:
            hour, minute = int(tm.group(1)), int(tm.group(2))
    naive = datetime(year, month, day, hour, minute)
    # Approximate Madrid DST: months 4-9 → +2, else +1. Worst-case 1h drift on
    # the boundary days; acceptable for closures lasting hours/days.
    offset = 2 if naive.month in {4, 5, 6, 7, 8, 9} else 1
    return naive.replace(tzinfo=timezone(timedelta(hours=offset))).astimezone(timezone.utc)


def _drupal_field(article, field_name: str) -> str | None:
    """Read a Drupal field's value by its machine name suffix.
    Example: field_name='localizaci-n' → reads field--name-field-localizaci-n.
    """
    sel = f".field--name-field-{field_name} .field--item"
    el = article.select_one(sel)
    if not el:
        return None
    txt = el.get_text(strip=True)
    return txt or None


def _parse_sanlucar_detail(html: str) -> dict[str, Any]:
    """Parse a Sanlúcar closure detail page using Drupal field selectors.
    Returns a dict with keys: localizacion, segment_from, segment_to,
    closure_type, reason, franja, fecha_desde, fecha_hasta.
    """
    soup = BeautifulSoup(html, "lxml")
    article = soup.select_one("article") or soup
    out: dict[str, Any] = {}

    localizacion = _drupal_field(article, "localizaci-n")
    if localizacion:
        out["localizacion"] = localizacion
        m = _LOCALIZATION_TRAMO_RE.search(localizacion)
        if m:
            out["segment_from"] = m.group(1).strip().title()
            out["segment_to"] = m.group(2).strip().title()
        # else: full street, leave segment_from/to unset

    out["closure_type"] = _drupal_field(article, "tipo-de-corte")
    out["reason"] = _drupal_field(article, "motivo")
    out["franja"] = _drupal_field(article, "franja-horaria")
    out["fecha_desde"] = _drupal_field(article, "fecha-desde")
    out["fecha_hasta"] = _drupal_field(article, "fecha-hasta")

    return out


def _split_drupal_datetime(s: str | None) -> tuple[str | None, str | None]:
    """Drupal datetime fields render as 'Lun, 27/04/2026 - 12:00'.
    Returns (date_part, time_part)."""
    if not s:
        return None, None
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})\s*(?:-|a las)?\s*(\d{1,2}:\d{2})?", s)
    if not m:
        return None, None
    return m.group(1), m.group(2)


def _parse_franja(franja: str | None) -> tuple[bool, str | None, str | None]:
    """Returns (all_day, time_window_start, time_window_end).
    Examples:
      'TODO EL DIA' → (True, None, None)
      'DE 09:00 A 10:00 HORAS' → (False, '09:00', '10:00')
    """
    if not franja:
        return False, None, None
    if "TODO EL D" in franja.upper():
        return True, None, None
    m = re.search(r"(\d{1,2}):(\d{2})\s+A\s+(\d{1,2}):(\d{2})", franja, re.IGNORECASE)
    if m:
        return False, f"{int(m.group(1)):02d}:{m.group(2)}", f"{int(m.group(3)):02d}:{m.group(4)}"
    return False, None, None


async def scrape_sanlucar(
    *, google_api_key: str, max_items: int = 50
) -> list[ClosureRecord]:
    """Scrape Sanlúcar de Barrameda's official street closures page.
    Each detail page is parsed via Drupal field selectors (resilient to layout
    changes that don't rename the fields). Geocoding happens after parsing.
    """
    out: list[ClosureRecord] = []
    async with httpx.AsyncClient(
        timeout=20, follow_redirects=True,
        headers={"User-Agent": "XpeditClosuresBot/1.0 (+https://www.xpedit.es)"},
    ) as client:
        # 1. Fetch listing
        resp = await client.get(SANLUCAR_LIST_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Each listing row links to /es/node/<id>
        seen_urls: set[str] = set()
        anchors = soup.select("a[href^='/es/node/']")

        for a in anchors[:max_items]:
            href = a.get("href")
            if not href or href in seen_urls:
                continue
            seen_urls.add(href)
            detail_url = SANLUCAR_BASE + href

            # Street name from anchor text
            street_name = a.get_text(strip=True)
            if not street_name or len(street_name) < 3:
                continue

            # 2. Detail page parse (Drupal selectors)
            try:
                detail_resp = await client.get(detail_url, timeout=15)
                detail_resp.raise_for_status()
                detail = _parse_sanlucar_detail(detail_resp.text)
            except Exception as e:
                logger.warning(f"Sanlúcar detail failed {detail_url}: {e}")
                continue

            # 3. Dates
            d1, t1 = _split_drupal_datetime(detail.get("fecha_desde"))
            d2, t2 = _split_drupal_datetime(detail.get("fecha_hasta"))
            if not d1 or not d2:
                logger.warning(f"Sanlúcar: missing dates for {detail_url}")
                continue
            starts_at = _parse_dt_es(d1, t1)
            ends_at = _parse_dt_es(d2, t2)
            if not starts_at or not ends_at:
                logger.warning(f"Sanlúcar: cannot parse dates for {detail_url}: {d1} {t1} / {d2} {t2}")
                continue
            # Make sure ends_at >= starts_at; if not (rare), assume same-day end-of-day
            if ends_at < starts_at:
                ends_at = starts_at.replace(hour=23, minute=59)

            # 4. Time window
            all_day, tw_start, tw_end = _parse_franja(detail.get("franja"))
            # If this is a same-day closure (d1 == d2) and we have a time window,
            # set ends_at to the end of the window so /closures/near respects
            # active vs ended.
            if d1 == d2 and tw_end:
                eh, em = map(int, tw_end.split(":"))
                ends_at = starts_at.replace(hour=eh, minute=em)

            record = ClosureRecord(
                source="ayto_sanlucar",
                source_url=detail_url,
                city="Sanlúcar de Barrameda",
                street_name=street_name.title(),
                segment_from=detail.get("segment_from"),
                segment_to=detail.get("segment_to"),
                closure_type=detail.get("closure_type"),
                reason=detail.get("reason"),
                starts_at=starts_at,
                ends_at=ends_at,
                all_day=all_day,
                time_window_start=tw_start,
                time_window_end=tw_end,
                raw_payload={"localizacion": detail.get("localizacion"), "franja": detail.get("franja")},
            )
            out.append(record)

        # 5. Geocode + Directions polyline for each record (in series; ~10 records max)
        for rec in out:
            await geocode_record(client, google_api_key, rec)

    return out


# ---------- Upsert into Supabase ----------

def upsert_closures(supabase_admin, records: list[ClosureRecord]) -> dict[str, int]:
    """Insert or update closures. Skips records without lat/lng."""
    inserted = skipped = failed = 0
    for r in records:
        if r.lat is None or r.lng is None:
            skipped += 1
            continue
        try:
            payload = {
                "source": r.source,
                "source_url": r.source_url,
                "city": r.city,
                "street_name": r.street_name,
                "segment_from": r.segment_from,
                "segment_to": r.segment_to,
                "lat": r.lat,
                "lng": r.lng,
                "lat_from": r.lat_from,
                "lng_from": r.lng_from,
                "lat_to": r.lat_to,
                "lng_to": r.lng_to,
                "street_polyline": r.street_polyline,
                "closure_type": r.closure_type,
                "reason": r.reason,
                "starts_at": r.starts_at.isoformat(),
                "ends_at": r.ends_at.isoformat(),
                "all_day": r.all_day,
                "time_window_start": r.time_window_start,
                "time_window_end": r.time_window_end,
                "raw_payload": r.raw_payload,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            res = supabase_admin.table("street_closures").upsert(
                payload, on_conflict="source,source_url"
            ).execute()
            if res.data:
                inserted += 1
        except Exception as e:
            logger.warning(f"Upsert failed for {r.source_url}: {e}")
            failed += 1
    return {"inserted_or_updated": inserted, "skipped_no_coords": skipped, "failed": failed}


# ---------- Registry ----------

ALL_SCRAPERS = {
    "sanlucar": scrape_sanlucar,
}
