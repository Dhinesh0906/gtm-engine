"""Canada Corporations Canada federal corporations open data collector.

The Federal Corporations dataset on open.canada.ca provides freely-downloadable CSV
files for federally incorporated companies (CBCA and non-CBCA, active and dissolved).
This collector streams the configured CSV without loading it into memory.

# TODO(harvest): full run streams the complete dataset; add provincial registries.
"""

import csv
import logging
from collections.abc import Iterator
from typing import Any

import httpx

from app.collect import staging
from app.config import settings

logger = logging.getLogger(__name__)

DEFAULT_CANADA_URL = "https://d4bf66bykfyaf.cloudfront.net/corporations-active-cbca-en.csv"


def _clean(value: Any) -> str:
    """Return a clean string, treating None as empty."""
    if value is None:
        return ""
    return str(value).strip()


def _format_address(row: dict[str, Any]) -> str:
    """Flatten Corporations Canada address columns into one line."""
    parts = [
        _clean(row.get("Street")),
        _clean(row.get("Street 2")),
        _clean(row.get("City/town")),
        _clean(row.get("Province/territory")),
        _clean(row.get("Country")),
        _clean(row.get("Postal code")),
    ]
    return ", ".join(part for part in parts if part)


def _to_record(row: dict[str, Any]) -> dict:
    """Map a single Corporations Canada CSV row to the common staging format."""
    name = _clean(row.get("Corporate name - form 1")) or _clean(
        row.get("Corporate name - form 2")
    )
    return {
        "source": "canada_corporations",
        "external_id": _clean(row.get("Corporation number")),
        "name": name,
        "domain": "",
        "country": "CA",
        "address": _format_address(row),
        "raw": dict(row),
        "fetched_at": staging.now(),
    }


def _line_iterator(response) -> Iterator[str]:
    """Yield complete text lines from an httpx stream, stripping a leading UTF-8 BOM."""
    buffer = ""
    first_chunk = True
    for chunk in response.iter_text():
        if first_chunk:
            chunk = chunk.lstrip("\ufeff")
            first_chunk = False
        buffer += chunk
        *lines, buffer = buffer.split("\n")
        for line in lines:
            if line:
                yield line
    if buffer:
        yield buffer


def _stream_csv_rows(url: str) -> Iterator[dict[str, Any]]:
    """Stream decoded CSV rows from the remote URL."""
    with httpx.stream(
        "GET",
        url,
        headers={"User-Agent": settings.user_agent},
        timeout=120,
    ) as response:
        response.raise_for_status()
        reader = csv.DictReader(_line_iterator(response), skipinitialspace=True)
        for row in reader:
            yield row


def fetch(limit: int | None = None) -> Iterator[dict]:
    """Yield Corporations Canada records in staging format.

    Streams the configured CSV from open.canada.ca. Raises SourceError on the
    first request or parse failure before any rows are produced. Later failures
    after partial success are swallowed and rows already gathered are kept.
    """
    url = settings.canada_url or DEFAULT_CANADA_URL
    yielded = 0

    try:
        for row in _stream_csv_rows(url):
            if limit is not None and yielded >= limit:
                return

            record = _to_record(row)
            if record["external_id"] and record["name"]:
                yield record
                yielded += 1
    except Exception as exc:  # noqa: BLE001
        if yielded == 0:
            kind = staging.classify_http_error(exc)
            if kind:
                raise staging.SourceError(kind, str(exc)) from exc
            raise staging.SourceError(staging.SourceOutcome.FAIL, str(exc)) from exc
        logger.warning("Canada Corporations CSV failed after partial success: %s", exc)

    if yielded == 0:
        raise staging.SourceError(staging.SourceOutcome.FAIL, "no Canada records produced")
