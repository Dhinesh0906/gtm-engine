"""UK Companies House bulk downloader. Phase 1 (Phase 13 scale rewrite).

Uses the free bulk Company Data Product (full monthly snapshot CSV) instead of
the scoped search API. Captures company name, country=GB, address, and SIC codes
as the industry signal. No API key required for the bulk product.
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


def _bulk_url() -> str:
    """Return the current-month bulk CSV URL, falling back up to 2 previous months."""
    now = __import__("datetime").datetime.now()
    guesses = [
        [now.year, now.month],
        [now.year - (1 if now.month == 1 else 0), 12 if now.month == 1 else now.month - 1],
    ]
    for yr, mo in guesses:
        url = f"http://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-{yr}-{mo:02d}-01.zip"
        try:
            r = httpx.head(url, headers={"User-Agent": settings.user_agent}, timeout=10)
            if r.status_code < 400:
                return url
        except Exception:
            continue
    # Fallback: try the configured URL.
    return settings.companies_house_bulk_url


def _strip_headers(reader):
    """Strip leading/trailing whitespace from every CSV header field."""
    reader.fieldnames = [fn.strip() for fn in (reader.fieldnames or [])]
    return reader


def _parse_row(row: dict) -> dict | None:
    """Map one CSV row to a staging record, or None if unparseable."""
    # Build a lookup with stripped keys explicitly
    row = {k.strip(): v for k, v in row.items()}
    name = (row.get("CompanyName") or "").strip()
    number = (row.get("CompanyNumber") or "").strip()
    if not number or not name:
        return None

    # Address.
    addr_parts = [
        row.get("RegAddress.AddressLine1", ""),
        row.get("RegAddress.AddressLine2", ""),
        row.get("RegAddress.PostTown", ""),
        row.get("RegAddress.County", ""),
        row.get("RegAddress.PostCode", ""),
    ]
    address = ", ".join(p.strip() for p in addr_parts if p and p.strip())

    # SIC codes as industry signal.
    sic_parts = []
    for i in range(1, 5):
        code = (row.get(f"SICCode.SicText_{i}") or "").strip()
        if code:
            sic_parts.append(code)
    industry = "; ".join(sic_parts[:2]) if sic_parts else ""

    return {
        "source": "companies_house",
        "external_id": number,
        "name": name,
        "domain": "",
        "country": "GB",
        "address": address,
        "industry": industry,
        "raw": {k: v for k, v in row.items()},
        "fetched_at": staging.now(),
    }


def fetch(limit: int | None = None) -> Iterator[dict]:
    """Yield Companies House bulk CSV rows in staging format.

    Downloads the full Company Data Product zip and streams the CSV inside.
    No query scoping, no numeric cap. --sample stops after 50.
    """
    url = _bulk_url()
    logger.info("Downloading Companies House bulk: %s", url)

    try:
        r = httpx.get(url, headers={"User-Agent": settings.user_agent}, timeout=300, follow_redirects=True)
        r.raise_for_status()
    except Exception as exc:
        kind = staging.classify_http_error(exc)
        if kind:
            raise staging.SourceError(kind, str(exc)) from exc
        raise staging.SourceError(staging.SourceOutcome.FAIL, str(exc)) from exc

    # Write zip to a temp file on disk so we can stream the CSV out of it.
    fd, tmp_path = tempfile.mkstemp(suffix=".zip")
    os.close(fd)
    try:
        with open(tmp_path, "wb") as fh:
            fh.write(r.content)

        yielded = 0
        produced_any = False

        with zipfile.ZipFile(tmp_path) as zf:
            names = zf.namelist()
            csv_name = names[0] if names else "BasicCompanyData.csv"
            logger.info("Companies House zip contains: %s", csv_name)
            with zf.open(csv_name) as fh:
                reader = _strip_headers(csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig")))
                for row in reader:
                    if limit is not None and yielded >= limit:
                        return
                    rec = _parse_row(row)
                    if rec is None:
                        continue
                    yield rec
                    yielded += 1
                    produced_any = True
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass

    if not produced_any:
        raise staging.SourceError(staging.SourceOutcome.FAIL, "bulk CSV yielded 0 rows")
    logger.info("Companies House: yielded %d rows", yielded)
