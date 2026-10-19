"""India Ministry of Corporate Affairs (MCA) company master data collector.

Discovers the data.gov.in catalog that groups all state/UT "Company Master Data"
datasets, then pages through that catalog. CIN is unique across states, so the
StagingWriter deduplicates automatically.

# Pulls all state/UT company-master datasets discovered from the data.gov.in catalog.
"""

import json
import logging
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from app.collect import staging
from app.config import settings

logger = logging.getLogger(__name__)

BASE_URL = "https://api.data.gov.in"
PAGE_SIZE = 1000
DISCOVERY_PAGE_SIZE = 1000
STATE_DISCOVERY_LIMIT = 1000
SEED_RESOURCE_ID = "77cdb9fb-a407-432b-881d-33d02074a6eb"
CACHE_TTL_DAYS = 7


def _cache_path() -> Path:
    """Path to the cached state/UT resource list."""
    return Path(settings.raw_dir) / "india_mca" / "_resources.json"


def _is_cache_stale(cache: dict) -> bool:
    """Return True if the cached resource list is missing or older than CACHE_TTL_DAYS."""
    updated_at = cache.get("discovered_at")
    if not updated_at:
        return True
    try:
        ts = datetime.fromisoformat(updated_at)
    except ValueError:
        return True
    return datetime.now(timezone.utc) - ts > timedelta(days=CACHE_TTL_DAYS)


def _load_cached_resources() -> list[dict] | None:
    """Load the cached resource list if it exists and is fresh."""
    path = _cache_path()
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            cache = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return None
    if _is_cache_stale(cache):
        return None
    return cache.get("resources")


def _save_cached_resources(resources: list[dict]) -> None:
    """Write the discovered resource list to disk with a timestamp."""
    path = _cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    cache = {
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        "resources": resources,
    }
    with path.open("w", encoding="utf-8") as fh:
        json.dump(cache, fh, ensure_ascii=False, indent=2)


def _discover_catalog_uuid(api_key: str) -> str:
    """Use the verified Karnataka seed resource to discover the parent catalog UUID."""
    url = f"{BASE_URL}/resource/{SEED_RESOURCE_ID}"
    r = httpx.get(
        url,
        params={"api-key": api_key, "format": "json", "limit": 1, "offset": 0},
        headers={"User-Agent": settings.user_agent},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    catalog_uuid = data.get("catalog_uuid")
    if not catalog_uuid:
        raise ValueError("seed resource did not return a catalog_uuid")
    return catalog_uuid


def _discover_states(api_key: str, catalog_uuid: str) -> list[str]:
    """Pull a sample from the catalog and collect every distinct state name."""
    url = f"{BASE_URL}/catalog/{catalog_uuid}"
    r = httpx.get(
        url,
        params={
            "api-key": api_key,
            "format": "json",
            "limit": STATE_DISCOVERY_LIMIT,
            "offset": 0,
        },
        headers={"User-Agent": settings.user_agent},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    states = set()
    for record in data.get("records", []):
        state = (record.get("registered_state") or "").strip()
        if state:
            states.add(state)
    if not states:
        raise ValueError("catalog sample did not return any state information")
    return sorted(states)


def _discover_resources(api_key: str) -> list[dict]:
    """Discover every state/UT in the MCA catalog and return a resource entry for each.

    Each entry is a "virtual" resource: a state filter applied to the single
    data.gov.in catalog that aggregates all state/UT company-master datasets.
    """
    catalog_uuid = _discover_catalog_uuid(api_key)
    states = _discover_states(api_key, catalog_uuid)
    resources = [
        {"catalog_uuid": catalog_uuid, "state": state, "resource_id": state}
        for state in states
    ]
    _save_cached_resources(resources)
    return resources


def _clean(value: Any) -> str:
    """Return a clean string, treating missing markers as empty."""
    if value is None:
        return ""
    s = str(value).strip()
    return "" if s.lower() in {"na", "n/a", "-"} else s


def _to_record(item: dict[str, Any]) -> dict:
    """Map a single MCA record to the common staging format."""
    return {
        "source": "india_mca",
        "external_id": _clean(item.get("corporate_identification_number")),
        "name": _clean(item.get("company_name")),
        "domain": "",
        "country": "IN",
        "address": _clean(item.get("registered_office_address")),
        "raw": dict(item),
        "fetched_at": staging.now(),
    }


def _fetch_page(resource: dict, api_key: str, limit: int, offset: int) -> list[dict]:
    """Fetch one page of a state/UT from the catalog."""
    url = f"{BASE_URL}/catalog/{resource['catalog_uuid']}"
    params = {
        "api-key": api_key,
        "format": "json",
        "limit": limit,
        "offset": offset,
        "filters[registered_state]": resource["state"],
    }
    r = httpx.get(
        url,
        params=params,
        headers={"User-Agent": settings.user_agent},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    return data.get("records", [])


def fetch(limit: int | None = None) -> Iterator[dict]:
    """Yield MCA company records in staging format.

    Discovers the MCA catalog and all state/UT entries, then pages through each
    one. If no API key is configured, logs a skip message and yields nothing so
    the runner can report SKIP_NO_KEY.

    Raises SourceError on the first request failure so the runner can classify
    the failure; later failures after partial success are swallowed and rows
    already gathered are kept.
    """
    api_key = settings.data_gov_in_key
    if not api_key:
        logger.info("India MCA skipped: no key")
        return

    resources = _load_cached_resources()
    if not resources:
        try:
            resources = _discover_resources(api_key)
        except Exception as exc:  # noqa: BLE001
            kind = staging.classify_http_error(exc)
            if kind:
                raise staging.SourceError(kind, str(exc)) from exc
            raise staging.SourceError(staging.SourceOutcome.FAIL, str(exc)) from exc

    yielded = 0
    produced_any = False

    # In sample mode, cap each state so the sample spans many states/UTs.
    state_limit = None
    if limit is not None and resources:
        state_limit = max(2, limit // len(resources))

    for resource in resources:
        if limit is not None and yielded >= limit:
            break

        state_yielded = 0
        offset = 0
        while (limit is None or yielded < limit) and (
            state_limit is None or state_yielded < state_limit
        ):
            page_limit = PAGE_SIZE
            if limit is not None:
                page_limit = min(page_limit, limit - yielded)
            if state_limit is not None:
                page_limit = min(page_limit, state_limit - state_yielded)
            if page_limit <= 0:
                break

            try:
                records = _fetch_page(resource, api_key, page_limit, offset)
            except Exception as exc:  # noqa: BLE001
                if not produced_any:
                    kind = staging.classify_http_error(exc)
                    if kind:
                        raise staging.SourceError(kind, str(exc)) from exc
                logger.warning(
                    "India MCA state %s failed after partial success: %s",
                    resource["state"],
                    exc,
                )
                break

            if not records:
                break

            for item in records:
                if limit is not None and yielded >= limit:
                    return
                if state_limit is not None and state_yielded >= state_limit:
                    break

                record = _to_record(item)
                if record["external_id"] and record["name"]:
                    yield record
                    yielded += 1
                    state_yielded += 1
                    produced_any = True

            offset += len(records)

            # Be polite to the free government API.
            if len(records) == page_limit:
                time.sleep(0.2)

    if not produced_any:
        raise staging.SourceError(staging.SourceOutcome.FAIL, "no MCA records produced")
