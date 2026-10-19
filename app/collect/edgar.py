"""SEC EDGAR downloader. Phase 1."""

import logging
from collections.abc import Iterator

import httpx

from app.collect import staging
from app.config import settings

logger = logging.getLogger(__name__)

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def fetch(limit: int | None = None) -> Iterator[dict]:
    """Yield SEC EDGAR ticker/CIK records in staging format.

    If the first request fails before any rows are produced, raises SourceError
    so the runner can classify the failure.
    """
    try:
        r = httpx.get(
            TICKERS_URL,
            headers={"User-Agent": settings.user_agent},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as exc:  # noqa: BLE001
        kind = staging.classify_http_error(exc)
        if kind:
            raise staging.SourceError(kind, str(exc)) from exc
        logger.warning("EDGAR fetch failed: %s", exc)
        return

    count = 0
    for _, item in data.items():
        if limit is not None and count >= limit:
            return

        yield {
            "source": "edgar",
            "external_id": str(item.get("cik_str", "")),
            "name": item.get("title", ""),
            "domain": "",
            "country": "US",
            "address": "",
            "raw": item,
            "fetched_at": staging.now(),
        }
        count += 1
