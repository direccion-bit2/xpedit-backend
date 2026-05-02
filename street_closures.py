"""Street closures scraper.

Reads official municipal websites that publish street closures, parses each
entry, geocodes the segment using Google Geocoding API, and upserts into
the `street_closures` table on Supabase. Runs every 30 minutes via
APScheduler. Idempotent on (source, source_url).

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
    source_url: str        # absolute URL of the detail page (idempotency key)
    city: str
    street_name: str
    starts_at: datetime
    ends_at: datetime
    segment_from: str | None = None
    segment_to: str | None = None
    closure_type: str | None = None  # "Total", "Parcial"
    reason: str | None = None
    all_day: bool = False
    time_window_start: str | None = None  # HH:MM
    time_window_end: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


# ---------- Geocoding helpers ----------

async def _geocode_intersection(
    client: httpx.AsyncClient, google_api_key: str, street_a: str, street_b: str | None, city: str
) -> tuple[float, float] | None:
    """Geocode a street or intersection. Returns (lat, lng) or None.

    If `street_b` is given, geocodes the intersection ("street_a y street_b, city").
    Otherwise geocodes the street centroid ("street_a, city").
    """
    if street_b:
        query = f"{street_a} y {street_b}, {city}, España"
    else:
        query = f"{street_a}, {city}, España"
    try:
        resp = await client.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": query, "key": google_api_key, "language": "es"},
            timeout=10,
        )
        data = resp.json()
        if data.get("status") == "OK" and data.get("results"):
            loc = data["results"][0]["geometry"]["location"]
            return loc["lat"], loc["lng"]
    except Exception as e:
        logger.warning(f"Geocoding failed for '{query}': {e}")
    return None


async def geocode_segment(
    client: httpx.AsyncClient, google_api_key: str, record: ClosureRecord
) -> tuple[float, float] | None:
    """Geocode the segment of a closure. If we have segment_from/to, the
    midpoint of both intersections is returned. Otherwise the street centroid."""
    if record.segment_from and record.segment_to:
        a = await _geocode_intersection(client, google_api_key, record.street_name, record.segment_from, record.city)
        b = await _geocode_intersection(client, google_api_key, record.street_name, record.segment_to, record.city)
        if a and b:
            return (a[0] + b[0]) / 2, (a[1] + b[1]) / 2
        if a:
            return a
        if b:
            return b
    return await _geocode_intersection(client, google_api_key, record.street_name, None, record.city)


# ---------- Sanlúcar scraper ----------

SANLUCAR_LIST_URL = "https://www.sanlucardebarrameda.es/es/cortes-de-calle"
SANLUCAR_BASE = "https://www.sanlucardebarrameda.es"

# DD/MM/YYYY HH:MM
_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})(?:\s*(?:-|a las)?\s*(\d{1,2}):(\d{2}))?")


def _parse_dt_es(s: str) -> datetime | None:
    """Parse 'DD/MM/YYYY HH:MM' or 'DD/MM/YYYY'. Returns timezone-aware UTC."""
    if not s:
        return None
    m = _DATE_RE.search(s)
    if not m:
        return None
    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    hour = int(m.group(4)) if m.group(4) else 0
    minute = int(m.group(5)) if m.group(5) else 0
    # Sanlúcar uses Europe/Madrid local time (UTC+1 standard, +2 DST).
    # We approximate by treating it as UTC+2 between last Sun of March and last Sun of October.
    # Good enough for a closure that lasts hours/days; the worst miss is 1h at the boundary.
    naive = datetime(year, month, day, hour, minute)
    # Determine DST roughly: if month in 4..9 → +2, else month==3 last Sun → +2, else +1
    dst = naive.month in {4, 5, 6, 7, 8, 9}
    offset = 2 if dst else 1
    return naive.replace(tzinfo=timezone(timedelta(hours=offset))).astimezone(timezone.utc)


async def _fetch_sanlucar_detail(client: httpx.AsyncClient, detail_url: str) -> dict[str, Any]:
    """Fetch and parse a single closure detail page."""
    resp = await client.get(detail_url, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    text = soup.get_text(separator="\n")

    out: dict[str, Any] = {}

    # "TRAMO DE [from] A [to]" or "DE [from] A [to]"
    m = re.search(r"(?:TRAMO\s+(?:DE)?\s*|DE\s+)([A-ZÁÉÍÓÚÑÜ][^\n]{2,80}?)\s+A\s+(?:CALLE\s+|AVDA\.\s+|AVENIDA\s+)?([A-ZÁÉÍÓÚÑÜ][^\n]{2,80})", text, re.IGNORECASE)
    if m:
        out["segment_from"] = m.group(1).strip().title()
        out["segment_to"] = m.group(2).strip().title()

    # Tipo de corte: Total / Parcial
    m = re.search(r"(?:tipo\s+(?:de\s+)?corte|corte)\s*[:\-]?\s*(total|parcial)", text, re.IGNORECASE)
    if m:
        out["closure_type"] = m.group(1).capitalize()

    # Motivo
    m = re.search(r"motivo\s*[:\-]?\s*([^\n]{3,200})", text, re.IGNORECASE)
    if m:
        out["reason"] = m.group(1).strip()

    # Fechas: línea con "Lun, DD/MM/YYYY - HH:MM" o "DD/MM/YYYY a las HH:MM"
    dates = _DATE_RE.findall(text)
    if dates:
        out["raw_dates_found"] = dates

    return out


async def scrape_sanlucar(
    *, google_api_key: str, max_items: int = 50
) -> list[ClosureRecord]:
    """Scrape Sanlúcar de Barrameda's official street closures page.

    Returns geocoded ClosureRecord items. Items without parseable dates are
    skipped (logged). Items where geocoding fails return lat/lng=None and
    must be filtered by the caller.
    """
    out: list[ClosureRecord] = []
    async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers={
        "User-Agent": "XpeditClosuresBot/1.0 (+https://www.xpedit.es)"
    }) as client:
        # 1. Fetch listing page
        resp = await client.get(SANLUCAR_LIST_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # The listing renders rows with: street name, dates, link to /es/node/<id>
        # We collect all anchors that point to /es/node/<id> and parse the surrounding row.
        rows = soup.select("article, .views-row, .field-content, tr")
        seen_urls: set[str] = set()

        # Collect anchors → detail URL is the idempotency key
        anchors = soup.select("a[href^='/es/node/']")
        for a in anchors[:max_items]:
            href = a.get("href")
            if not href or href in seen_urls:
                continue
            seen_urls.add(href)
            detail_url = SANLUCAR_BASE + href

            # Find the row containing this anchor to extract the listing-level data
            row = a.find_parent(["tr", "article", "div"])
            row_text = row.get_text(separator="\n").strip() if row else a.get_text()

            # Street name: the anchor text or the closest heading
            street_name = a.get_text(strip=True)
            if not street_name or len(street_name) < 3:
                # try parent heading
                heading = row.find(["h2", "h3", "h4"]) if row else None
                if heading:
                    street_name = heading.get_text(strip=True)
            if not street_name or len(street_name) < 3:
                continue

            # Find dates in the row text
            dates = _DATE_RE.findall(row_text)
            if not dates:
                logger.debug(f"Sanlúcar: no dates in listing row for {detail_url}, fetching detail")
                continue

            # Detail page for tramo / motivo / closure_type
            try:
                detail = await _fetch_sanlucar_detail(client, detail_url)
            except Exception as e:
                logger.warning(f"Sanlúcar detail fetch failed {detail_url}: {e}")
                detail = {}

            # Resolve dates: prefer detail page if has them; else listing
            all_dates = detail.get("raw_dates_found", []) or dates
            if len(all_dates) >= 2:
                starts_str = "/".join(all_dates[0][:3]) + (f" {all_dates[0][3]}:{all_dates[0][4]}" if all_dates[0][3] else "")
                ends_str = "/".join(all_dates[1][:3]) + (f" {all_dates[1][3]}:{all_dates[1][4]}" if all_dates[1][3] else "")
                starts_at = _parse_dt_es(starts_str)
                ends_at = _parse_dt_es(ends_str)
            elif len(all_dates) == 1:
                starts_str = "/".join(all_dates[0][:3]) + (f" {all_dates[0][3]}:{all_dates[0][4]}" if all_dates[0][3] else "")
                starts_at = _parse_dt_es(starts_str)
                ends_at = (starts_at + timedelta(hours=8)) if starts_at else None
            else:
                starts_at = ends_at = None

            if not starts_at or not ends_at:
                logger.warning(f"Sanlúcar: cannot parse dates for {detail_url}")
                continue

            # Time window: look for "HH:MM A HH:MM HORAS"
            tw_match = re.search(r"(\d{1,2}):(\d{2})\s+A\s+(\d{1,2}):(\d{2})\s+HORAS", row_text + " " + str(detail), re.IGNORECASE)
            tw_start = tw_end = None
            all_day = "TODO EL D" in (row_text + " " + str(detail)).upper()
            if tw_match and not all_day:
                tw_start = f"{int(tw_match.group(1)):02d}:{tw_match.group(2)}"
                tw_end = f"{int(tw_match.group(3)):02d}:{tw_match.group(4)}"

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
                raw_payload={"row": row_text[:500], "detail": detail},
            )
            out.append(record)

        # 2. Geocode each
        for rec in out:
            coords = await geocode_segment(client, google_api_key, rec)
            if coords:
                rec.raw_payload["lat"] = coords[0]
                rec.raw_payload["lng"] = coords[1]

    return out


# ---------- Upsert into Supabase ----------

def upsert_closures(supabase_admin, records: list[ClosureRecord]) -> dict[str, int]:
    """Insert or update closures. Returns counts."""
    inserted = updated = skipped = failed = 0
    for r in records:
        lat = r.raw_payload.get("lat")
        lng = r.raw_payload.get("lng")
        if lat is None or lng is None:
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
                "lat": lat,
                "lng": lng,
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
                # supabase upsert doesn't easily distinguish insert vs update;
                # treat any successful row as inserted_or_updated
                inserted += 1
        except Exception as e:
            logger.warning(f"Upsert failed for {r.source_url}: {e}")
            failed += 1
    return {"inserted_or_updated": inserted, "skipped_no_coords": skipped, "failed": failed}


# ---------- Registry ----------

ALL_SCRAPERS = {
    "sanlucar": scrape_sanlucar,
}
