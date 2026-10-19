"""Belgium KBO/CBE (Crossroads Bank for Enterprises) Open Data collector.

The monthly full extract is a ZIP of CSVs. Company names live in `denomination.csv`,
keyed by `EnterpriseNumber`. A free registered account is required for download.

Access flow:
1. POST login to `/kbo-open-data/static/j_spring_security_check`.
2. POST license acceptance to `/kbo-open-data/affiliation/prexml?form=`.
3. Read the downloads list and GET the latest `KboOpenData_*_Full.zip`.

# TODO(harvest): full run joins denomination + contact + address CSVs; contact.csv has web/email for Phase 6.
"""

import csv
import io
import logging
import re
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx

from app.collect import staging
from app.config import settings

logger = logging.getLogger(__name__)

_LOGIN_URL = "https://kbopub.economie.fgov.be/kbo-open-data/static/j_spring_security_check"
_LICENSE_URL = "https://kbopub.economie.fgov.be/kbo-open-data/affiliation/prexml?form="
_FILES_LISTING_URL = "https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/?files"
DEFAULT_KBO_URL = _FILES_LISTING_URL
DENOMINATION_CSV_CANDIDATES = ("denomination.csv", "denomination_insert.csv")
ZIP_NAME_PATTERN = re.compile(
    r"KboOpenData_\d+_\d{4}_\d{2}_\d{2}_(?:Full|Update)\.zip", re.IGNORECASE
)


def _cache_dir() -> Path:
    """Return the directory used to cache the downloaded KBO zip."""
    return Path(settings.raw_dir) / "belgium_kbo" / "_cache"


def _has_credentials() -> bool:
    """Return True only when both KBO username and password are configured."""
    return bool(settings.kbo_username and settings.kbo_password)


def _client() -> httpx.Client:
    """Build an httpx client that follows redirects and preserves cookies."""
    return httpx.Client(
        follow_redirects=True,
        headers={"User-Agent": settings.user_agent},
        timeout=120,
    )


def _login(client: httpx.Client) -> None:
    """Authenticate via the KBO login form.

    Raises SourceError if the login page is returned (bad credentials) or the
    site is unreachable.
    """
    r = client.post(
        _LOGIN_URL,
        data={
            "j_username": settings.kbo_username,
            "j_password": settings.kbo_password,
        },
    )
    r.raise_for_status()
    if "/login" in str(r.url):
        raise staging.SourceError(
            staging.SourceOutcome.SKIP_AUTH,
            "KBO login failed: bad credentials",
        )


def _accept_license(client: httpx.Client) -> None:
    """Accept the Open Data licence to unlock the downloads page."""
    r = client.post(
        _LICENSE_URL,
        data={
            "productId": "1",
            "username": settings.kbo_username,
            "licenseAccepted": "true",
            "purpose": "GTM Engine Phase 1 open data collection",
        },
    )
    r.raise_for_status()


def _resolve_zip_url(client: httpx.Client, base_url: str) -> str:
    """Return the exact zip URL to download.

    If `base_url` points to a specific `.zip` file it is returned as-is.
    Otherwise the downloads listing page is fetched and the latest
    `KboOpenData_*_Full.zip` link is chosen.
    """
    if base_url.lower().endswith(".zip"):
        return base_url

    listing_url = base_url if base_url else _FILES_LISTING_URL
    r = client.get(listing_url)
    r.raise_for_status()
    candidates = sorted(set(ZIP_NAME_PATTERN.findall(r.text)), reverse=True)
    if not candidates:
        raise ValueError(f"no KboOpenData_*_Full.zip link found at {listing_url}")
    filename = candidates[0]
    # Links on the listing page are relative to .../xml/files/.
    return urljoin("https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/files/", filename)


def _download_zip(client: httpx.Client, url: str) -> Path:
    """Download the KBO zip to the cache directory, reusing an existing copy."""
    _cache_dir().mkdir(parents=True, exist_ok=True)
    filename = url.split("/")[-1] or "kbo.zip"
    path = _cache_dir() / filename

    if path.exists() and zipfile.is_zipfile(path):
        logger.info("Belgium KBO cache hit: %s", path)
        return path

    if path.exists():
        logger.info("Belgium KBO removing incomplete cache: %s", path)
        path.unlink()

    logger.info("Belgium KBO downloading: %s", url)
    with client.stream("GET", url, timeout=600) as response:
        response.raise_for_status()
        with path.open("wb") as fh:
            for chunk in response.iter_bytes(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)

    if not zipfile.is_zipfile(path):
        raise ValueError(f"downloaded file is not a valid zip: {path}")
    return path


def _stream_denomination_rows(zip_path: Path) -> Iterator[dict[str, Any]]:
    """Stream rows from the denomination CSV inside the cached zip.

    Full zips contain `denomination.csv`; daily update zips contain
    `denomination_insert.csv`. The first matching CSV is used.
    """
    with zipfile.ZipFile(zip_path) as zf:
        candidates = [
            n
            for n in zf.namelist()
            if any(n.lower().endswith(c) for c in DENOMINATION_CSV_CANDIDATES)
        ]
        if not candidates:
            raise ValueError(f"no denomination CSV found in {zip_path}")
        csv_name = candidates[0]

        with zf.open(csv_name) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            reader = csv.DictReader(text)
            for row in reader:
                yield row


def _clean(value: Any) -> str:
    """Return a clean string, treating None as empty."""
    if value is None:
        return ""
    return str(value).strip()


def _to_record(row: dict[str, Any]) -> dict:
    """Map a single denomination CSV row to the common staging format."""
    external_id = _clean(row.get("EntityNumber") or row.get("EnterpriseNumber"))
    return {
        "source": "belgium_kbo",
        "external_id": external_id,
        "name": _clean(row.get("Denomination")),
        "domain": "",
        "country": "BE",
        "address": "",
        "raw": dict(row),
        "fetched_at": staging.now(),
    }


def fetch(limit: int | None = None) -> Iterator[dict]:
    """Yield KBO company records in staging format.

    If KBO_USERNAME or KBO_PASSWORD is empty, logs a skip message and yields
    nothing so the runner can report SKIP_NO_KEY.

    Raises SourceError on the first download/streaming failure before any rows
    are produced. Later failures after partial success are swallowed and rows
    already gathered are kept.
    """
    if not _has_credentials():
        logger.info("Belgium KBO skipped: no credentials")
        return

    with _client() as client:
        try:
            _login(client)
            _accept_license(client)
            url = _resolve_zip_url(client, settings.kbo_url or DEFAULT_KBO_URL)
            zip_path = _download_zip(client, url)
        except staging.SourceError:
            raise
        except Exception as exc:  # noqa: BLE001
            kind = staging.classify_http_error(exc)
            if kind:
                raise staging.SourceError(kind, str(exc)) from exc
            raise staging.SourceError(staging.SourceOutcome.FAIL, str(exc)) from exc

        yielded = 0
        seen: set[str] = set()

        try:
            for row in _stream_denomination_rows(zip_path):
                if limit is not None and yielded >= limit:
                    return

                record = _to_record(row)
                if not record["external_id"] or not record["name"]:
                    continue
                if record["external_id"] in seen:
                    continue
                seen.add(record["external_id"])

                if limit is not None and yielded >= limit:
                    return
                yield record
                yielded += 1

        except Exception as exc:  # noqa: BLE001
            if yielded == 0:
                kind = staging.classify_http_error(exc)
                if kind:
                    raise staging.SourceError(kind, str(exc)) from exc
                raise staging.SourceError(staging.SourceOutcome.FAIL, str(exc)) from exc
            logger.warning("Belgium KBO stopped after partial success: %s", exc)
