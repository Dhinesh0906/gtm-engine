"""Homepage text extraction and cleaning module. Phase 3."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import func
from sqlmodel import select

from app.collect.staging import normalize_domain
from app.config import settings
from app.db import Company, Session, engine, init_db, run_concurrent

logger = logging.getLogger(__name__)

# Informative paths we consider worth fetching from a homepage.
WORTHWHILE_SLUGS = {
    "about",
    "about-us",
    "aboutus",
    "services",
    "products",
    "solutions",
    "company",
    "what-we-do",
    "industries",
}


def _webtext_dir() -> Path:
    path = Path(settings.raw_dir) / "webtext"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _attempts_path() -> Path:
    return _webtext_dir() / "_attempts.jsonl"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _load_attempts() -> dict[str, dict[str, Any]]:
    """Return the latest attempt record per domain."""
    path = _attempts_path()
    attempts: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return attempts
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            domain = record.get("domain", "")
            if not domain:
                continue
            old = attempts.get(domain)
            if old is None or _parse_dt(record["attempted_at"]) >= _parse_dt(old["attempted_at"]):
                attempts[domain] = record
    return attempts


def _should_skip(attempt: dict[str, Any] | None) -> bool:
    """Return True if a cached attempt means we should skip fetching now."""
    if attempt is None:
        return False
    status = attempt.get("status", "")
    attempted_at = attempt.get("attempted_at", "")
    if not attempted_at:
        return False

    # Successful/empty attempts are never retried.
    if status in {"ok", "empty"}:
        return True

    # Retry transient failures only after WEBTEXT_RETRY_DAYS.
    try:
        age_days = (datetime.now(timezone.utc) - _parse_dt(attempted_at)).days
    except ValueError:
        return False
    return age_days < settings.webtext_retry_days


def _record_attempt(domain: str, status: str, chars: int = 0) -> None:
    path = _attempts_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "domain": domain,
        "status": status,
        "chars": chars,
        "attempted_at": _now(),
    }
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _compact_attempts() -> None:
    """Rewrite the attempts log so only the latest record per domain remains."""
    attempts = _load_attempts()
    if not attempts:
        return
    path = _attempts_path()
    with path.open("w", encoding="utf-8") as fh:
        for record in attempts.values():
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def html_to_text(html: str, max_chars: int = 8000) -> str:
    """Return cleaned, trimmed text from raw HTML.

    Drops script/style/noscript/svg/nav/footer/header tags and collapses
    whitespace. Result is capped at ``max_chars``.
    """
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "nav", "footer", "header"]):
        tag.decompose()
    text = " ".join(soup.stripped_strings)
    text = re.sub(r"\s+", " ", text).strip()
    if max_chars and len(text) > max_chars:
        text = text[:max_chars]
    return text


def _base_url(response: httpx.Response, original_scheme_domain: str) -> str:
    """Best-effort base URL for resolving relative links."""
    final_url = str(response.url)
    if final_url and final_url.startswith(("http://", "https://")):
        parsed = urlparse(final_url)
        return f"{parsed.scheme}://{parsed.netloc}"
    return original_scheme_domain


def _link_path_parts(path: str) -> list[str]:
    return [p for p in path.strip("/").split("/") if p]


def _slug_is_informative(slug: str) -> bool:
    base = slug.lower().split("?")[0].split("#")[0].strip("/")
    return base in WORTHWHILE_SLUGS


def pick_internal_links(html: str, base_url: str, max_links: int = 2) -> list[str]:
    """Pick up to ``max_links`` same-domain links that look informative."""
    if not html or not base_url:
        return []
    base_domain = normalize_domain(base_url)
    if not base_domain:
        return []
    soup = BeautifulSoup(html, "html.parser")
    found: list[str] = []
    seen = set()
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if not isinstance(href, str):
            continue
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        link_domain = normalize_domain(parsed.netloc)
        if link_domain != base_domain:
            continue
        parts = _link_path_parts(parsed.path)
        if not parts:
            continue
        # Accept the first informative segment of the path.
        informative = [p for p in parts if _slug_is_informative(p)]
        if not informative:
            continue
        # Skip fragments pointing to the same page, and duplicates.
        key = (parsed.scheme, parsed.netloc, parsed.path)
        if key in seen:
            continue
        seen.add(key)
        found.append(absolute)
        if len(found) >= max_links:
            break
    return found


def _classify_http_error(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        # Auth/rate-limit/block treated as "error" rather than unreachable.
        if code in (401, 403, 404, 410, 451):
            return "error"
        if code >= 500 or code in (408, 429, 502, 503, 504):
            return "unreachable"
        return "error"
    if isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.NetworkError,
            httpx.PoolTimeout,
        ),
    ):
        return "unreachable"
    return "error"


def _is_html_response(response: httpx.Response) -> bool:
    content_type = response.headers.get("content-type", "").lower()
    return "text/html" in content_type or "application/xhtml" in content_type


def _fetch_one(
    client: httpx.Client,
    url: str,
    timeout: float,
) -> httpx.Response:
    response = client.get(
        url,
        timeout=timeout,
        follow_redirects=True,
        headers={
            "User-Agent": settings.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        },
    )
    response.raise_for_status()
    return response


def fetch_domain_text(domain: str, ignore_cache: bool = False) -> tuple[str, str]:
    """Fetch homepage (+ 2 informative internal pages) and return cleaned text + status.

    Status is one of: "ok", "empty", "unreachable", "error".
    The function never propagates network exceptions.

    Pass ``ignore_cache=True`` to force a fresh fetch (used by domain verification).
    """
    if not domain:
        return "", "empty"

    attempts = _load_attempts()
    if not ignore_cache and _should_skip(attempts.get(domain)):
        return "", attempts[domain]["status"]

    timeout = settings.webtext_timeout
    last_status = "unreachable"
    homepage_html: str | None = None
    base_url = f"https://{domain}"

    try:
        with httpx.Client() as client:
            # Try HTTPS first.
            try:
                response = _fetch_one(client, base_url, timeout)
                if not _is_html_response(response):
                    _record_attempt(domain, "empty", 0)
                    return "", "empty"
                homepage_html = response.text
            except Exception as exc:  # noqa: BLE001
                # Fall back to HTTP for transport-level failures.
                try:
                    http_url = f"http://{domain}"
                    response = _fetch_one(client, http_url, timeout)
                    if not _is_html_response(response):
                        _record_attempt(domain, "empty", 0)
                        return "", "empty"
                    homepage_html = response.text
                    base_url = _base_url(response, http_url)
                except Exception as exc2:  # noqa: BLE001
                    last_status = _classify_http_error(exc2)
                    if last_status == "unreachable" and isinstance(exc, httpx.HTTPStatusError):
                        last_status = _classify_http_error(exc)
                    _record_attempt(domain, last_status, 0)
                    return "", last_status

            base_url = _base_url(response, base_url)
            all_text = html_to_text(homepage_html, max_chars=8000)

            if not all_text:
                _record_attempt(domain, "empty", 0)
                return "", "empty"

            # Fetch up to 2 informative internal pages, appending text up to ~12 k chars.
            links = pick_internal_links(homepage_html, base_url, max_links=2)
            remaining = 12000 - len(all_text)
            for link in links:
                if remaining <= 0:
                    break
                try:
                    page = _fetch_one(client, link, timeout)
                    if _is_html_response(page):
                        page_text = html_to_text(page.text, max_chars=remaining)
                        if page_text:
                            all_text += "\n\n" + page_text
                            remaining = 12000 - len(all_text)
                except Exception:  # noqa: BLE001
                    # Internal pages are best-effort; keep going.
                    continue
                time.sleep(0.5)

            chars = len(all_text)
            _record_attempt(domain, "ok", chars)
            return all_text, "ok"
    except Exception:  # noqa: BLE001
        # Last-resort catch so this function never crashes the batch.
        _record_attempt(domain, "error", 0)
        return "", "error"


# Companies worth crawling, MOST RELEVANT FIRST.
#
# The previous version selected `domain != '' AND web_text = ''` with no
# ordering at all. Against 13.1M eligible rows SQLite scans from Company.id 1,
# and this dataset was loaded alphabetically by domain - so id 1-3 are
# '+pentagon.do', '-essex-karate.com', '-no.website'. Every run re-ground the
# same front slice of junk and placeholder domains. A measured 500-company
# backfill fetched 291 sites and changed the campaign's coverage by exactly
# zero, because none of the 291 were leads anyone was going to email.
#
# Crawling is a scarce resource: ~58% success and real wall-clock cost per
# domain. Spend it on companies the sender is about to reach, not on the
# alphabet.
_TIER_ASSIGNED = """
    SELECT DISTINCT c.id FROM companies c
    JOIN people p          ON p.company_id = c.id
    JOIN contact_points cp ON cp.person_id = p.id
    JOIN assignments a     ON a.contact_point_id = cp.id
    WHERE c.domain != '' AND c.web_text = ''
      AND a.status NOT IN ('sent', 'suppressed')
    LIMIT :lim
"""

_TIER_IN_SEGMENT = """
    SELECT c.id FROM companies c
    WHERE c.domain != '' AND c.web_text = ''
      AND c.industry_nace IN (
            SELECT name FROM segments WHERE status != 'paused'
          UNION
            SELECT si.industry_nace FROM segment_industries si
            JOIN segments s ON s.id = si.segment_id
            WHERE s.status != 'paused')
      AND (:no_country OR c.country IN (SELECT code FROM campaign_countries))
    LIMIT :lim
"""

# Last resort, and the old behaviour: anything at all that still needs text.
_TIER_ANY = """
    SELECT c.id FROM companies c
    WHERE c.domain != '' AND c.web_text = ''
    LIMIT :lim
"""


def _iter_eligible(session: Session, attempts: dict[str, dict[str, Any]], limit: int | None):
    """Yield companies that need webtext, most relevant first.

    Three tiers, in order: companies with a live assignment (about to be
    emailed), companies in an active segment and target country, then anything.
    Each tier is exhausted before the next is touched, so a crawl budget is
    always spent on the most useful rows available.
    """
    from sqlalchemy import text as _sql

    count = 0
    seen_ids: set[int] = set()
    # Over-fetch: many candidates are skipped by the attempt cache, and a tier
    # that returned exactly `limit` rows would starve once they were filtered.
    fetch = (limit * 5) if limit else 50_000

    try:
        countries = [r[0] for r in session.execute(
            _sql("SELECT code FROM campaign_countries")).all()]
    except Exception:  # noqa: BLE001 - table may not exist on an old database
        countries = []

    for label, sql, params in (
        ("assigned", _TIER_ASSIGNED, {"lim": fetch}),
        ("in-segment", _TIER_IN_SEGMENT,
         {"lim": fetch, "no_country": 1 if not countries else 0}),
        ("any", _TIER_ANY, {"lim": fetch}),
    ):
        if limit is not None and count >= limit:
            return
        try:
            ids = [r[0] for r in session.execute(_sql(sql), params).all()]
        except Exception as exc:  # noqa: BLE001 - a missing table must not stop the crawl
            logger.warning("webtext: tier %s unavailable (%s), skipping", label, exc)
            continue
        if not ids:
            continue
        logger.info("webtext: tier '%s' offers %d candidate(s)", label, len(ids))
        for cid in ids:
            if cid in seen_ids:
                continue
            seen_ids.add(cid)
            company = session.get(Company, cid)
            if company is None or not company.domain or company.web_text:
                continue
            if _should_skip(attempts.get(company.domain)):
                continue
            yield company
            count += 1
            if limit is not None and count >= limit:
                return


def run(limit: int | None = None, workers: int | None = None) -> dict[str, Any]:
    """Fetch and cache homepage text for up to ``limit`` eligible companies, concurrently."""
    init_db()
    attempts = _load_attempts()
    if workers is None:
        workers = settings.webtext_concurrency or 1

    companies: list[Company] = []
    with Session(engine) as db:
        for company in _iter_eligible(db, attempts, limit):
            companies.append(company)

    if not companies:
        return {"attempted": 0, "success": 0, "failed": 0, "skipped": 0}

    counters: dict[str, int] = {"success": 0, "failed": 0, "skipped": 0}

    def _process(company: Company) -> dict:
        with Session(engine) as db:
            company = db.get(Company, company.id)
            if not company or company.web_text:
                return {"status": "skipped"}
            text, status = fetch_domain_text(company.domain)
            if status == "ok" and text:
                company.web_text = text
                db.add(company)
                db.commit()
                return {"status": "success"}
            return {"status": "failed"}

    results = run_concurrent(companies, _process, workers=workers)
    for r in results:
        status = r.get("status", "failed")
        if status in ("success", "failed", "skipped"):
            counters[status] += 1

    return {"attempted": len(companies), **counters}


def report() -> dict[str, Any]:
    """Return coverage numbers for web text."""
    init_db()
    with Session(engine) as db:
        total = db.exec(select(func.count()).select_from(Company)).one()
        with_domain = db.exec(select(func.count()).select_from(Company).where(Company.domain != "")).one()
        with_text = db.exec(select(func.count()).select_from(Company).where(Company.web_text != "")).one()

    attempts = _load_attempts()
    attempted_domains = len(attempts)
    ok_domains = sum(1 for a in attempts.values() if a.get("status") == "ok")
    success_rate = round(100 * ok_domains / attempted_domains, 2) if attempted_domains else 0.0

    return {
        "total": total,
        "with_domain": with_domain,
        "with_text": with_text,
        "attempted_domains": attempted_domains,
        "ok_domains": ok_domains,
        "success_rate": success_rate,
    }


def _print_report(report_data: dict[str, Any]) -> None:
    print(f"Companies:         {report_data['total']}")
    print(f"With domain:       {report_data['with_domain']}")
    print(f"With web_text:     {report_data['with_text']}")
    print(f"Attempted domains: {report_data['attempted_domains']}")
    print(f"OK domains:        {report_data['ok_domains']}")
    print(f"Success rate:      {report_data['success_rate']}%")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Raliess Phase 3 web-text fetcher")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Fetch homepage text for eligible companies")
    run_parser.add_argument("--limit", type=int, default=None, help="Maximum companies to fetch")
    run_parser.add_argument("--workers", type=int, default=None, help="Concurrent workers")

    subparsers.add_parser("report", help="Print web-text coverage report")

    args = parser.parse_args(argv)

    if args.command == "run":
        summary = run(limit=args.limit, workers=args.workers)
        print(f"Fetched: {summary['success']}  Failed: {summary['failed']}  Skipped: {summary['skipped']}")
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE3 OK: {report_data['with_text']}/{report_data['with_domain']} "
            f"companies have web text"
        )
        # Fail the gate if domains exist but none were fetched.
        return 0 if not (report_data["with_domain"] > 0 and report_data["with_text"] == 0) else 1

    if args.command == "report":
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE3 OK: {report_data['with_text']}/{report_data['with_domain']} "
            f"companies have web text"
        )
        return 0 if not (report_data["with_domain"] > 0 and report_data["with_text"] == 0) else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
