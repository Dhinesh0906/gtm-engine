"""Common Crawl Web-Graph downloader. Phase 1 (Phase 13 scale rewrite).

Ingests Common Crawl's DOMAIN-LEVEL web graph vertices (~134M domains)
instead of the CDX API. Each vertex line is <id>\t<reversed-domain> (SURT-ish).
We un-reverse the domain and stage one record per UNIQUE domain.

Full mode: NO page/row cap — streams all vertices files to exhaustion.
Sample mode (--sample): stops at 50 rows.

Uses the current release's domain-vertices.paths.gz to discover all parts.
"""

import gzip
import logging
import re
import time
from collections.abc import Iterator
from io import BytesIO

import httpx

from app.collect import staging
from app.config import settings

logger = logging.getLogger(__name__)

WEBGRAPH_BASE = settings.cc_webgraph_base
WEBGRAPH_RELEASE = settings.cc_webgraph_release


def _unreverse_domain(reversed_domain: str) -> str:
    """Un-reverse a SURT-ish domain: 'com,example' -> 'example.com'.

    Handles both comma-separated (SURT subset) and regular dot-separated forms.
    Also strips any leading scheme/port noise.
    """
    rd = reversed_domain.strip()
    # Strip leading scheme if present (e.g. "http://org,example").
    rd = re.sub(r"^https?://", "", rd)
    # Strip port.
    rd = re.sub(r":\d+$", "", rd)
    # If comma-separated, reverse. Otherwise treat as standard.
    if "," in rd:
        parts = rd.split(",")
    else:
        parts = rd.split(".")
    parts = [p.strip() for p in parts if p.strip()]
    parts.reverse()
    return ".".join(parts).lower()


def _domain_vertices_pathlisting() -> list[str]:
    """Download the path listing for all domain-vertices parts. Returns list of relative paths."""
    url = f"{WEBGRAPH_BASE}/{WEBGRAPH_RELEASE}/domain/{WEBGRAPH_RELEASE}-domain-vertices.paths.gz"
    try:
        r = httpx.get(url, headers={"User-Agent": settings.user_agent}, timeout=60)
        r.raise_for_status()
        with gzip.GzipFile(fileobj=BytesIO(r.content)) as fh:
            parts = fh.read().decode("utf-8").strip().split("\n")
        return [p.strip() for p in parts if p.strip()]
    except Exception:
        logger.warning("Could not fetch vertices path listing; falling back to single file")
        return [f"{WEBGRAPH_RELEASE}-domain-vertices.txt.gz"]


def _download_part(path: str) -> Iterator[str]:
    """Stream a single gzipped vertices part and yield lines."""
    url = f"{WEBGRAPH_BASE}/{WEBGRAPH_RELEASE}/domain/{path}"
    # Strip duplicate slashes.
    url = re.sub(r"([^:])//+", r"\1/", url)
    logger.info("Downloading webgraph part: %s", url)
    r = httpx.get(url, headers={"User-Agent": settings.user_agent}, timeout=120, follow_redirects=True)
    r.raise_for_status()
    try:
        with gzip.GzipFile(fileobj=BytesIO(r.content)) as fh:
            for line in fh:
                line = line.decode("utf-8", errors="replace").strip()
                if line:
                    yield line
    except gzip.BadGzipFile:
        # Maybe not gzipped — try plain text.
        for line in r.text.split("\n"):
            line = line.strip()
            if line:
                yield line


def fetch(limit: int | None = None) -> Iterator[dict]:
    """Yield Common Crawl domain-level web-graph vertices in staging format.

    Full mode: streams ALL parts to exhaustion (NO cap).
    Sample mode: stops after 50 rows.
    """
    try:
        parts = _domain_vertices_pathlisting()
        if not parts:
            raise RuntimeError("No domain-vertices parts found for this release")
    except Exception as exc:
        kind = staging.classify_http_error(exc)
        if kind:
            raise staging.SourceError(kind, str(exc)) from exc
        raise staging.SourceError(staging.SourceOutcome.FAIL, str(exc)) from exc

    seen: set[str] = set()
    yielded = 0
    produced_any = False

    for part in parts:
        if limit is not None and yielded >= limit:
            break
        try:
            for line in _download_part(part):
                # Vertex lines are tab-separated and may have >2 columns
                # (e.g. <id>\t<reversed_domain>\t<extra>). Split on ALL tabs and
                # pick the field that is the reversed domain: it contains dots and
                # is not the purely-numeric id. This avoids sweeping an extra
                # column into the domain (which produced tabs like "name\t1.com.au").
                fields = [f.strip() for f in line.rstrip("\n").split("\t") if f.strip()]
                if len(fields) < 2:
                    continue
                cc_id = fields[0]
                reversed_domain = ""
                for f in fields:
                    if "." in f and not f.replace(".", "").isdigit():
                        reversed_domain = f
                        break
                if not reversed_domain:
                    continue
                domain = _unreverse_domain(reversed_domain)
                if not domain or "." not in domain or any(c.isspace() for c in domain):
                    continue
                if domain in seen:
                    continue
                seen.add(domain)

                yield {
                    "source": "common_crawl",
                    "external_id": domain,
                    "name": "",
                    "domain": domain,
                    "country": "",
                    "address": "",
                    "industry": "",
                    "raw": {"cc_id": cc_id, "reversed": reversed_domain},
                    "fetched_at": staging.now(),
                }
                yielded += 1
                produced_any = True
                if limit is not None and yielded >= limit:
                    return
        except Exception as exc:
            logger.warning("Failed to process webgraph part %s: %s", part, exc)
            if not produced_any:
                continue

    logger.info("Common Crawl webgraph: %d unique domains from %d parts", yielded, len(parts))
