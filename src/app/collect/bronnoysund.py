"""Norway Brønnøysund Enhetsregisteret bulk downloader. Phase 1 (Phase 13 scale fix).

Uses the full download endpoint (~1.1M entities) instead of the paginated API.
The API was capped at ~10,000 rows; this stream has NO row limit.

Maps: organisasjonsnummer→external_id, navn→name, naeringskode1→industry,
forretningsadresse→address, country="NO", hjemmeside→domain.
"""

import gzip
import io
import json
import logging
import tempfile
from collections.abc import Iterator

import httpx

from app.collect import staging
from app.config import settings

logger = logging.getLogger(__name__)

BULK_URL = settings.bronnoysund_bulk_url


def _format_address(addr: dict) -> str:
    if not isinstance(addr, dict):
        return ""
    parts: list[str] = []
    for key in ("adresse", "postnummer", "poststed", "land"):
        val = addr.get(key, "")
        if isinstance(val, list):
            parts.extend(str(v) for v in val if v)
        elif val:
            parts.append(str(val))
    return ", ".join(parts)


def _naering_to_industry(naering: dict) -> str:
    if not isinstance(naering, dict):
        return ""
    kode = naering.get("kode", "")
    beskrivelse = naering.get("beskrivelse", "")
    if beskrivelse:
        return f"{kode}: {beskrivelse}" if kode else beskrivelse
    return kode or ""


def _to_record(item: dict) -> dict:
    """Map one Brønnøysund entity dict to a staging record."""
    naering = item.get("naeringskode1", {})
    website = item.get("hjemmeside", "")
    forretning = item.get("forretningsadresse", {})
    country = forretning.get("landkode", "NO") if isinstance(forretning, dict) else "NO"

    return {
        "source": "bronnoysund",
        "external_id": str(item.get("organisasjonsnummer", "")),
        "name": item.get("navn", ""),
        "domain": staging.normalize_domain(website) if website else "",
        "country": str(country).upper(),
        "address": _format_address(forretning),
        "industry": _naering_to_industry(naering),
        "raw": {k: v for k, v in list(item.items())[:30]},
        "fetched_at": staging.now(),
    }


def fetch(limit: int | None = None) -> Iterator[dict]:
    """Yield Brønnøysund entities from the bulk download (~1.1M rows, no cap)."""
    logger.info("Downloading Brønnøysund bulk: %s", BULK_URL)

    try:
        r = httpx.get(
            BULK_URL,
            headers={
                "User-Agent": settings.user_agent,
                "Accept": "application/vnd.brreg.enhetsregisteret.enhet.v2+gzip",
            },
            timeout=600,
            follow_redirects=True,
        )
        r.raise_for_status()
    except Exception as exc:
        kind = staging.classify_http_error(exc)
        if kind:
            raise staging.SourceError(kind, str(exc)) from exc
        raise staging.SourceError(staging.SourceOutcome.FAIL, str(exc)) from exc

    yielded = 0
    produced_any = False

    try:
        buf = io.BytesIO(r.content)
        with gzip.GzipFile(fileobj=buf) as fh:
            content = fh.read().decode("utf-8")
        data = json.loads(content)

        # Structure: {"_embedded": {"enheter": [...]}} or a list.
        if isinstance(data, dict):
            items = data.get("_embedded", {}).get("enheter", [])
        elif isinstance(data, list):
            items = data
        else:
            items = []

        if not items:
            raise staging.SourceError(staging.SourceOutcome.FAIL, "Brønnøysund bulk returned no entities")

        for item in items:
            if limit is not None and yielded >= limit:
                return

            yield _to_record(item)
            yielded += 1
            produced_any = True

    except Exception as exc:
        if not produced_any:
            raise staging.SourceError(staging.SourceOutcome.FAIL, str(exc)) from exc
        logger.warning("Brønnøysund bulk partial failure: %s", exc)

    logger.info("Brønnøysund: yielded %d rows", yielded)
