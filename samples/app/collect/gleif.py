"""GLEIF LEI Golden Copy bulk downloader. Phase 1 (Phase 13 scale fix).

Uses GLEIF's Golden Copy bulk CSV (~3.4M LEIs) instead of the paginated API.
The API was capped at ~10,000 rows; this stream has NO row limit.

Maps: LEI→external_id, Entity.LegalName→name, Entity.LegalAddress.Country→country,
Entity.LegalAddress.*→address, entity.LegalJurisdiction→country fallback, industry="" (none in GLEIF).
"""

import csv
import io
import logging
import os
import tempfile
import zipfile
from collections.abc import Iterator

import httpx

from app.collect import staging
from app.config import settings

logger = logging.getLogger(__name__)

GOLDENCOPY_LATEST = settings.gleif_goldencopy_url


def _build_address(parts: dict) -> str:
    addr = ", ".join(
        p.strip()
        for k in ("FirstAddressLine", "AdditionalAddressLine_1", "AdditionalAddressLine_2",
                    "AdditionalAddressLine_3", "City", "Region", "PostalCode", "Country")
        if (p := parts.get(f"Entity.LegalAddress.{k}", "")).strip()
    )
    return addr


def fetch(limit: int | None = None) -> Iterator[dict]:
    """Yield GLEIF Golden Copy CSV rows in staging format (~3.4M rows, no cap)."""
    logger.info("Discovering latest GLEIF Golden Copy...")
    try:
        r = httpx.get(GOLDENCOPY_LATEST, headers={"User-Agent": settings.user_agent}, timeout=30)
        r.raise_for_status()
        gc = r.json()
        csv_url = gc["data"]["lei2"]["full_file"]["csv"]["url"]
        total = gc["data"]["lei2"]["full_file"]["csv"]["record_count"]
        logger.info("GLEIF Golden Copy: %s rows, %s", total, csv_url)
    except Exception as exc:
        kind = staging.classify_http_error(exc)
        if kind:
            raise staging.SourceError(kind, str(exc)) from exc
        raise staging.SourceError(staging.SourceOutcome.FAIL, str(exc)) from exc

    fd, tmp_path = tempfile.mkstemp(suffix=".zip")
    os.close(fd)
    try:
        logger.info("Downloading GLEIF zip...")
        with httpx.stream("GET", csv_url, headers={"User-Agent": settings.user_agent}, timeout=600) as resp:
            resp.raise_for_status()
            with open(tmp_path, "wb") as fh:
                for chunk in resp.iter_bytes(1 << 20):
                    fh.write(chunk)

        yielded = 0
        produced_any = False

        with zipfile.ZipFile(tmp_path) as zf:
            names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not names:
                raise staging.SourceError(staging.SourceOutcome.FAIL, "No CSV in GLEIF zip")
            csv_name = names[0]
            logger.info("Reading GLEIF CSV: %s", csv_name)
            with zf.open(csv_name) as fh:
                reader = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8"))
                # Strip headers.
                reader.fieldnames = [fn.strip() for fn in (reader.fieldnames or [])]

                for row in reader:
                    lei = (row.get("LEI") or "").strip()
                    legal_name = (row.get("Entity.LegalName") or "").strip()
                    if not lei:
                        continue

                    country = (row.get("Entity.LegalAddress.Country") or
                               row.get("Entity.LegalJurisdiction") or "").strip()

                    yield {
                        "source": "gleif",
                        "external_id": lei,
                        "name": legal_name or "",
                        "domain": "",
                        "country": country.upper()[:3],
                        "address": _build_address(row),
                        "industry": "",
                        "raw": {k: v for k, v in list(row.items())[:20]},
                        "fetched_at": staging.now(),
                    }
                    yielded += 1
                    produced_any = True
                    if limit is not None and yielded >= limit:
                        return

        if not produced_any:
            raise staging.SourceError(staging.SourceOutcome.FAIL, "GLEIF CSV yielded 0 rows")
        logger.info("GLEIF: yielded %d rows", yielded)
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
