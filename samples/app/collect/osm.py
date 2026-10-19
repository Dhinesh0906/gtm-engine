"""OpenStreetMap collector — primary lead source for appointment-driven local businesses.

Collects POIs with website OR phone, limited to an allow-list of call/appointment
verticals (dentists, clinics, salons, lawyers, trades, etc.). Captures phone number
in raw.phone. Replaces the old loose `_industry_tag` with a normalized vertical label.
"""

import logging
import os
import tempfile
from collections.abc import Iterator

import httpx
import osmium

from app.collect import staging
from app.config import settings

logger = logging.getLogger(__name__)

WEBSITE_KEYS = ("website", "contact:website")
PHONE_KEYS = ("phone", "contact:phone", "contact:mobile")
ADDR_KEYS = ("addr:housenumber", "addr:street", "addr:city", "addr:postcode")

# Allow-list of OSM tag values that are call/appointment-driven local businesses.
# Tag key -> set of allowed VALUES.  A POI must match at least one.
_VERTICALS: dict[str, set[str]] = {
    "amenity": {
        "dentist", "doctors", "clinic", "veterinary", "pharmacy",
        "restaurant", "cafe", "bar",
    },
    "healthcare": {
        "dentist", "physiotherapist", "psychotherapist", "optometrist",
        "podiatrist", "chiropractor", "midwife", "alternative",
    },
    "shop": {
        "hairdresser", "beauty", "massage", "nails", "tattoo",
        "optician", "barber", "car_repair",
    },
    "leisure": {
        "fitness_centre", "spa", "sports_centre",
    },
    "office": {
        "lawyer", "accountant", "estate_agent", "insurance", "financial",
        "tax_advisor", "employment_agency", "architect", "notary",
        "consulting", "travel_agent",
    },
    "craft": {
        "plumber", "electrician", "hvac", "carpenter", "painter",
        "roofer", "builder", "locksmith", "gardener",
    },
    "tourism": {
        "hotel", "guest_house", "bed_and_breakfast",
    },
}

_EXCLUDED_TAGS: dict[str, set[str]] = {
    "amenity": {"atm", "bench", "toilets", "parking", "fuel", "bus_station",
                "waste_basket", "post_box", "fountain", "bicycle_parking",
                "recycling", "drinking_water", "shelter", "telephone",
                "vending_machine", "fire_hydrant", "clock"},
}


def _build_address(tags: dict) -> str:
    return ", ".join(str(tags[k]) for k in ADDR_KEYS if tags.get(k))


def _classify_vertical(tags: dict) -> str | None:
    """Return a normalized vertical label (tag value, lowercased), or None."""
    for key, allowed in _VERTICALS.items():
        val = tags.get(key)
        if val and val.lower() in allowed:
            return val.lower()
    return None


def _is_excluded(tags: dict) -> bool:
    for key, excluded in _EXCLUDED_TAGS.items():
        val = tags.get(key)
        if val and val.lower() in excluded:
            return True
    return False


def _to_record(osm_type: str, osm_id: int, tags: dict) -> dict | None:
    """Map an OSM POI to a staging record, or None if not a valid prospect."""
    # Must have a name.
    name = tags.get("name", "").strip()
    if not name:
        return None

    # Must match allow-list vertical.
    vertical = _classify_vertical(tags)
    if not vertical:
        return None

    # Must NOT be on the exclude list.
    if _is_excluded(tags):
        return None

    # Get website.
    website = ""
    for key in WEBSITE_KEYS:
        val = tags.get(key)
        if val:
            website = str(val).strip()
            break
    domain = staging.normalize_domain(website) if website else ""

    # Get phone.
    phone = ""
    for key in PHONE_KEYS:
        val = tags.get(key)
        if val:
            phone = str(val).strip()
            break

    # Must have website OR phone (or both).
    if not domain and not phone:
        return None

    # Build raw dict with phone.
    raw = dict(tags)
    if phone:
        raw["phone"] = phone

    return {
        "source": "osm",
        "external_id": f"{osm_type}/{osm_id}",
        "name": name,
        "domain": domain,
        "country": str(tags.get("addr:country", "")).upper(),
        "address": _build_address(tags),
        "industry": vertical,
        "raw": raw,
        "fetched_at": staging.now(),
    }


class _StopParsing(Exception):
    pass


class _OSMHandler(osmium.SimpleHandler):
    """Collects nodes/ways matching the allow-list, deduped by name+domain+phone."""

    def __init__(self, limit: int | None, seen: set[str]):
        super().__init__()
        self.limit = limit
        self.seen = seen
        self.rows: list[dict] = []
        self._count = 0

    def _handle(self, osm_type: str, obj) -> None:
        tags = {t.k: t.v for t in obj.tags}
        record = _to_record(osm_type, obj.id, tags)
        if record is None:
            return

        # Dedupe key: domain if set, else phone, else name.
        dedupe_key = record["domain"] or record["raw"].get("phone", "") or record["name"]
        if dedupe_key in self.seen:
            return
        self.seen.add(dedupe_key)

        self.rows.append(record)
        self._count += 1
        if self._count % 100000 == 0:
            logger.info("osm: %d POIs matched...", self._count)

        if self.limit is not None and len(self.rows) >= self.limit:
            raise _StopParsing

    def node(self, n) -> None:
        self._handle("node", n)

    def way(self, w) -> None:
        self._handle("way", w)


def _regions() -> list[str]:
    return [u.strip() for u in (settings.osm_regions or "").split(",") if u.strip()]


def _download(url: str) -> str:
    fd, path = tempfile.mkstemp(suffix=".osm.pbf")
    os.close(fd)
    try:
        with httpx.stream(
            "GET", url,
            headers={"User-Agent": settings.user_agent},
            timeout=300,
            follow_redirects=True,
        ) as response:
            response.raise_for_status()
            with open(path, "wb") as fh:
                for chunk in response.iter_bytes(chunk_size=1 << 16):
                    fh.write(chunk)
    except Exception:
        try:
            os.remove(path)
        except OSError:
            pass
        raise
    return path


def fetch(limit: int | None = None) -> Iterator[dict]:
    """Yield OSM POIs matching the allow-list from configured Geofabrik regions.

    SAMPLE MODE (``limit`` set) uses OSM_SAMPLE_REGION — a tiny extract — so a
    sample never downloads the multi-GB continent files.
    """
    if limit is not None and settings.osm_sample_region:
        regions = [settings.osm_sample_region]
        logger.info("OSM sample mode: using %s", regions[0])
    else:
        regions = _regions()
    if not regions:
        logger.warning("OSM: no regions configured")
        return

    seen: set[str] = set()
    produced = 0
    for index, url in enumerate(regions):
        try:
            path = _download(url)
        except Exception as exc:
            kind = staging.classify_http_error(exc)
            if index == 0 and produced == 0 and kind:
                raise staging.SourceError(kind, str(exc)) from exc
            logger.warning("OSM region %s failed: %s", url, exc)
            continue

        remaining = None if limit is None else max(0, limit - produced)
        handler = _OSMHandler(remaining, seen)
        try:
            handler.apply_file(path)
        except _StopParsing:
            pass
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

        for record in handler.rows:
            yield record
            produced += 1

        if limit is not None and produced >= limit:
            return
