"""Phase 3b: free domain discovery for domain-less companies.

Two free levers:
1. Mine existing company_sources.raw_json for emails/website URLs.
2. Guess candidate domains from the company name and verify with the homepage + Brain.

A wrong domain is worse than no domain, so every write is conservative and cached.

# TODO(phase3b-scale): full run on the cloud; optional free SearXNG fallback for search.
"""

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
from urllib.parse import urlparse

from sqlalchemy import func
from sqlmodel import select, or_

from app.brain import cheap_json
from app.config import settings
from app.db import Company, CompanySource, Session, engine, init_db
from app.resolve import normalize_name, registrable_domain
from app.webtext import fetch_domain_text

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# Generic email providers and site builders: never treat these as a company domain.
GENERIC_EMAIL_BASES = {
    "gmail",
    "googlemail",
    "yahoo",
    "hotmail",
    "outlook",
    "live",
    "aol",
    "icloud",
    "proton",
    "protonmail",
    "gmx",
    "mail",
    "qq",
    "163",
    "126",
    "yandex",
    "zoho",
    "hey",
    "me",
    "msn",
    "foxmail",
    "ymail",
    "rocketmail",
}

# Common website-builder / placeholder domains that are not a real company domain.
SITE_BUILDER_DOMAINS = {
    "wixsite.com",
    "squarespace.com",
    "godaddysites.com",
    "business.site",
    "weebly.com",
    "webflow.io",
    "netlify.app",
    "vercel.app",
    "github.io",
    "wordpress.com",
    "blogspot.com",
    "medium.com",
    "substack.com",
}

# Country -> preferred ccTLD for guessed candidates.
COUNTRY_TLDS: dict[str, list[str]] = {
    "US": [".com", ".co", ".net"],
    "GB": [".co.uk", ".com"],
    "IN": [".in", ".com", ".co"],
    "BE": [".be", ".com"],
    "CA": [".ca", ".com"],
    "SG": [".sg", ".com"],
    "NO": [".no", ".com"],
    "DE": [".de", ".com"],
    "FR": [".fr", ".com"],
    "NL": [".nl", ".com"],
    "ES": [".es", ".com"],
    "IT": [".it", ".com"],
    "JP": [".jp", ".com"],
    "AU": [".com.au", ".com"],
    "BR": [".com.br", ".com"],
    "MX": [".mx", ".com"],
}

# Common filler words we drop when building slugs.
NAME_STOPWORDS = {
    "and",
    "the",
    "of",
    "for",
    "in",
    "at",
    "on",
    "with",
    "by",
    "a",
    "an",
    "to",
    "from",
    "co",
    "company",
    "corporation",
    "inc",
    "llc",
    "ltd",
    "limited",
    "plc",
    "gmbh",
    "srl",
    "sarl",
    "bv",
    "nv",
    "sa",
    "ag",
    "ab",
    "as",
    "oy",
    "pte",
    "pvt",
    "private",
    " Holding",
    "holdings",
    "group",
    "international",
    "global",
}

# Keys that signal an email/website value in raw JSON.
EMAIL_KEYS = {"email", "email_addr", "email_address", "correio_eletronico", "e_mail", "contact_email"}
WEBSITE_KEYS = {"website", "url", "homepage", "web", "site", "web_site", "contact_url", "link"}
CONTACT_KEYS = {"contact", "contacts", "email", "phone", "address"}


def _discover_dir() -> Path:
    path = Path(settings.raw_dir) / "discover"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _attempts_path() -> Path:
    return _discover_dir() / "_attempts.jsonl"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _is_generic_domain(domain: str) -> bool:
    """Return True for generic email hosts / site builders / placeholders."""
    if not domain:
        return True
    domain = domain.lower()
    if domain in SITE_BUILDER_DOMAINS or domain in {"example.com", "example.org", "localhost", "test.com"}:
        return True
    extracted = _extract_tld(domain)
    if extracted and extracted.domain in GENERIC_EMAIL_BASES:
        return True
    return False


def _extract_tld(domain: str) -> Any:
    import tldextract

    return tldextract.extract(domain)


def _load_attempts() -> dict[int, dict[str, Any]]:
    """Return the latest attempt record per company_id."""
    path = _attempts_path()
    attempts: dict[int, dict[str, Any]] = {}
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
            company_id = record.get("company_id")
            if not isinstance(company_id, int):
                continue
            old = attempts.get(company_id)
            if old is None or _parse_dt(record["attempted_at"]) >= _parse_dt(old["attempted_at"]):
                attempts[company_id] = record
    return attempts


def _should_skip(attempt: dict[str, Any] | None) -> bool:
    """Return True if a cached attempt means we should skip this company now."""
    if attempt is None:
        return False
    method = attempt.get("method", "")
    attempted_at = attempt.get("attempted_at", "")
    if not attempted_at:
        return False

    # Successful or empty discoveries are never retried.
    if method in {"mined", "guessed", "empty"}:
        return True

    # Retry failures only after DISCOVERY_RETRY_DAYS.
    try:
        age_days = (datetime.now(timezone.utc) - _parse_dt(attempted_at)).days
    except ValueError:
        return False
    return age_days < settings.discovery_retry_days


def _record_attempt(
    company_id: int,
    method: str,
    domain: str = "",
    confidence: int = 0,
) -> None:
    path = _attempts_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "company_id": company_id,
        "method": method,
        "domain": domain,
        "confidence": confidence,
        "attempted_at": _now(),
    }
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _compact_attempts() -> None:
    """Rewrite the attempts log so only the latest record per company_id remains."""
    attempts = _load_attempts()
    if not attempts:
        return
    path = _attempts_path()
    with path.open("w", encoding="utf-8") as fh:
        for record in attempts.values():
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _extract_domains_from_raw(raw: dict[str, Any]) -> dict[str, int]:
    """Scan a raw_json dict and return scored domain candidates."""
    candidates: dict[str, int] = {}

    def _add(value: str, bonus: int = 1) -> None:
        value = value.strip().lower()
        if not value:
            return
        # Email -> domain part.
        if "@" in value:
            for part in value.split(","):
                part = part.strip()
                if "@" not in part:
                    continue
                domain_part = part.rsplit("@", 1)[-1].strip()
                # Strip trailing punctuation.
                domain_part = re.sub(r"[^a-z0-9.-]+$", "", domain_part)
                domain = registrable_domain(domain_part)
                if domain and not _is_generic_domain(domain):
                    candidates[domain] = candidates.get(domain, 0) + bonus
            return

        # URL or bare domain.
        candidate = registrable_domain(value)
        if candidate and not _is_generic_domain(candidate):
            candidates[candidate] = candidates.get(candidate, 0) + bonus

    def _walk(obj: Any, key: str = "") -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                lower_key = k.lower().replace("-", "_")
                bonus = 1
                if lower_key in EMAIL_KEYS or "email" in lower_key:
                    bonus = 3
                elif lower_key in WEBSITE_KEYS or any(w in lower_key for w in ("url", "website", "web", "homepage")):
                    bonus = 3
                elif lower_key in CONTACT_KEYS:
                    bonus = 2
                _walk(v, k)
                if isinstance(v, str):
                    _add(v, bonus)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item, key)
        elif isinstance(obj, str):
            _add(obj, 1)

    _walk(raw)
    return candidates


def _best_mined_domain(candidates: dict[str, int]) -> str | None:
    """Return a single mined domain if we are confident; otherwise None."""
    if not candidates:
        return None
    # One strong candidate, or multiple candidates that are the same registrable root.
    if len(candidates) == 1:
        return next(iter(candidates))
    # If the same domain appears in multiple sources it will have a high score.
    best_score = max(candidates.values())
    best = [d for d, s in candidates.items() if s == best_score]
    if len(best) == 1 and best_score >= 2:
        return best[0]
    # Multiple different candidates: ambiguous, do not guess.
    return None


def mine_known_domains(limit: int | None = None) -> dict[str, Any]:
    """Lever 1: extract domains from existing company_sources.raw_json.

    Returns summary counters.
    """
    init_db()
    attempts = _load_attempts()
    checked = 0
    found = 0
    ambiguous = 0

    with Session(engine) as db:
        for company in db.exec(
            select(Company).where(or_(Company.domain == "", Company.domain.is_(None)))
        ):
            checked += 1
            if _should_skip(attempts.get(company.id)):
                continue

            candidates: dict[str, int] = {}
            for source in db.exec(
                select(CompanySource).where(CompanySource.company_id == company.id)
            ):
                try:
                    raw = json.loads(source.raw_json) if source.raw_json else {}
                except json.JSONDecodeError:
                    raw = {}
                for domain, score in _extract_domains_from_raw(raw).items():
                    candidates[domain] = candidates.get(domain, 0) + score

            domain = _best_mined_domain(candidates)
            if domain:
                company.domain = domain
                db.add(company)
                found += 1
                _record_attempt(company.id, "mined", domain=domain, confidence=100)
            else:
                # Ambiguous: do NOT cache a failure here so Lever 2 can still try.
                ambiguous += 1

            if limit is not None and checked >= limit:
                break

        db.commit()

    if checked > 1000:
        _compact_attempts()

    return {"checked": checked, "found": found, "ambiguous": ambiguous}


def candidate_domains(name: str, country: str) -> list[str]:
    """Build a conservative ranked list of candidate domains from a company name."""
    norm = normalize_name(name)
    if not norm:
        return []
    tokens = [t for t in norm.split() if t and t not in NAME_STOPWORDS]
    if not tokens:
        return []

    # Country TLDs, defaulting to .com/.co/.net if unknown.
    country = (country or "").strip().upper()
    tlds = COUNTRY_TLDS.get(country, [".com", ".co", ".net"])
    # Ensure global TLDs are present.
    for tld in (".com", ".co", ".net"):
        if tld not in tlds:
            tlds.append(tld)

    # Slugs from most to least specific.
    full = "".join(tokens)
    full_hyphen = "-".join(tokens)
    first_two = "".join(tokens[:2])
    first_two_hyphen = "-".join(tokens[:2])
    first = tokens[0]

    slugs = [full, full_hyphen, first_two, first_two_hyphen, first]

    candidates: list[str] = []
    seen = set()
    for tld in tlds:
        for slug in slugs:
            if not slug:
                continue
            domain = f"{slug}{tld}"
            if domain in seen:
                continue
            seen.add(domain)
            candidates.append(domain)
            if len(candidates) >= 8:
                return candidates
    return candidates


def _is_parked_page(text: str) -> bool:
    """Fast heuristic to detect common domain-parking / for-sale pages."""
    lowered = text.lower()
    markers = [
        "domain for sale",
        "buy this domain",
        "this domain is for sale",
        "sedo",
        "hugedomains",
        "domainmarket",
        "parked",
        "parking page",
        "domain name has been registered",
        "is owned and listed by",
        "godaddy auctions",
        "buy now",
        "make offer",
        "domain is available",
    ]
    return any(marker in lowered for marker in markers)


def _brain_verify(company: Company, domain: str, page_text: str) -> tuple[bool, int]:
    """Ask the cheap Brain whether the candidate domain is the company's real site."""
    if settings.demo:
        # Demo mode: never accept a guessed domain.
        return False, 0

    if _is_parked_page(page_text):
        return False, 0

    snippet = page_text[:600].replace('"', "'").strip()
    prompt = (
        f"Company name: {company.name}\n"
        f"Country: {company.country}\n"
        f"Candidate domain: {domain}\n"
        f"Homepage text snippet: {snippet}\n\n"
        "Does this candidate domain clearly belong to the company above as its official website? "
        "Reject parked pages, typosquats (e.g. example-com.com), unrelated matches, and any domain that does not look like the real corporate site. "
        "Answer ONLY with a JSON object: {\"match\": true/false, \"confidence\": 0-100, \"reason\": \"...\"}. "
        "A wrong yes is very costly, so be conservative."
    )
    result = cheap_json(
        prompt,
        system="You are a conservative official-domain verifier. Return only JSON.",
        max_tokens=2000,
        demo_value={"match": False, "confidence": 0},
        model=settings.model_discovery,
    )
    if not isinstance(result, dict):
        return False, 0
    match = bool(result.get("match"))
    confidence = int(result.get("confidence", 0))
    return match, confidence


def verify_company_site(company: Company, domain: str) -> tuple[bool, int]:
    """Fetch the candidate domain and ask the Brain if it belongs to the company.

    Returns (accepted, confidence).
    """
    text, status = fetch_domain_text(domain, ignore_cache=True)
    if status != "ok" or not text:
        return False, 0
    match, confidence = _brain_verify(company, domain, text)
    return match and confidence >= settings.discovery_confidence, confidence


def discover(limit: int | None = None) -> dict[str, Any]:
    """Lever 2: guess-and-verify domains for domain-less companies.

    Returns summary counters.
    """
    init_db()
    attempts = _load_attempts()
    checked = 0
    found = 0
    rejected = 0

    with Session(engine) as db:
        for company in db.exec(
            select(Company).where(or_(Company.domain == "", Company.domain.is_(None)))
        ):
            if _should_skip(attempts.get(company.id)):
                continue

            checked += 1
            candidates = candidate_domains(company.name, company.country)
            if not candidates:
                _record_attempt(company.id, "none", confidence=0)
                rejected += 1
                continue

            accepted: str | None = None
            confidence = 0
            for domain in candidates:
                accepted, confidence = verify_company_site(company, domain)
                if accepted:
                    accepted = domain
                    break
                time.sleep(0.5)

            if accepted:
                company.domain = accepted
                db.add(company)
                db.commit()
                found += 1
                _record_attempt(company.id, "guessed", domain=accepted, confidence=confidence)
            else:
                rejected += 1
                _record_attempt(company.id, "none", confidence=confidence)

            if checked % 10 == 0:
                db.commit()

            if limit is not None and (found + rejected) >= limit:
                break

        db.commit()

    return {"checked": checked, "found": found, "rejected": rejected}


def run(limit: int | None = None, workers: int | None = None, per_endpoint: int | None = None) -> dict[str, Any]:
    """Run both levers: mine all raw_json, then guess-and-verify up to ``limit``."""
    init_db()
    mined = mine_known_domains()
    if workers is not None:
        guessed = discover(limit=limit)
    elif per_endpoint is not None:
        from app.brain import _providers
        pool = _providers()
        endpoints = [u for p in pool for u in p._urls]
        if not endpoints:
            guessed = discover(limit=limit)
        else:
            guessed = _discover_sharded(limit, endpoints, per_endpoint or settings.discover_per_endpoint)
    else:
        guessed = discover(limit=limit)
    return {"mined": mined, "guessed": guessed}


def _discover_sharded(limit: int | None, endpoints: list[str], per_endpoint: int) -> dict[str, Any]:
    """Sharded discover: each endpoint gets per_endpoint workers pulling from queue."""
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    init_db()
    attempts = _load_attempts()
    with Session(engine) as db:
        candidates = []
        for company in db.exec(
            select(Company).where(or_(Company.domain == "", Company.domain.is_(None)))
        ):
            if _should_skip(attempts.get(company.id)):
                continue
            candidates.append(company.id)
            if limit and len(candidates) >= limit:
                break

    found = [0]
    rejected = [0]
    lock = threading.Lock()
    idx = [0]
    total_workers = len(endpoints) * per_endpoint

    def _worker(ei: int) -> None:
        while True:
            with lock:
                i = idx[0]
                if i >= len(candidates):
                    return
                idx[0] += 1
                cid = candidates[i]

            logger.info("discover: company %s (%d/%d, worker %d)", cid, i + 1, len(candidates), ei)
            with Session(engine) as db:
                company = db.get(Company, cid)
                if not company:
                    continue
                candidates_d = candidate_domains(company.name, company.country)
                if not candidates_d:
                    _record_attempt(cid, "none", confidence=0)
                    with lock:
                        rejected[0] += 1
                    continue

                accepted = None
                for domain in candidates_d:
                    a, conf = verify_company_site(company, domain)
                    if a:
                        accepted = domain
                        break

                if accepted:
                    company.domain = accepted
                    db.add(company)
                    db.commit()
                    _record_attempt(cid, "guessed", domain=accepted, confidence=conf)
                    with lock:
                        found[0] += 1
                else:
                    _record_attempt(cid, "none", confidence=0)
                    with lock:
                        rejected[0] += 1

    logger.info("discover sharded: %d candidates, %d endpoints x %d = %d workers",
                len(candidates), len(endpoints), per_endpoint, total_workers)
    with ThreadPoolExecutor(max_workers=total_workers) as pool:
        for i in range(total_workers):
            pool.submit(_worker, i)
        pool.shutdown(wait=True)

    return {"checked": len(candidates), "found": found[0], "rejected": rejected[0]}


def report() -> dict[str, Any]:
    """Return domain discovery coverage numbers."""
    init_db()
    with Session(engine) as db:
        total = db.exec(select(func.count()).select_from(Company)).one()
        with_domain = len(
            db.exec(select(Company).where(Company.domain != "").where(Company.domain.is_not(None))).all()
        )
        without_domain = db.exec(select(func.count()).select_from(Company).where(or_(Company.domain == "", Company.domain.is_(None)))).one()

    attempts = _load_attempts()
    mined = sum(1 for a in attempts.values() if a.get("method") == "mined")
    guessed = sum(1 for a in attempts.values() if a.get("method") == "guessed")

    return {
        "total": total,
        "with_domain": with_domain,
        "without_domain": without_domain,
        "domain_fill_pct": round(100 * with_domain / total, 2) if total else 0.0,
        "mined": mined,
        "guessed": guessed,
        "attempts": len(attempts),
    }


def _print_report(report_data: dict[str, Any]) -> None:
    print(f"Total companies:   {report_data['total']}")
    print(f"With domain:       {report_data['with_domain']} ({report_data['domain_fill_pct']}%)")
    print(f"Without domain:    {report_data['without_domain']}")
    print(f"Mined domains:     {report_data['mined']}")
    print(f"Guessed domains:   {report_data['guessed']}")
    print(f"Cached attempts:   {report_data['attempts']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Raliess Phase 3b free domain discovery")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("mine", help="Mine domains from existing raw_json")
    subparsers.add_parser("report", help="Print domain discovery report")

    run_parser = subparsers.add_parser("run", help="Mine + guess-and-verify domains")
    run_parser.add_argument("--limit", type=int, default=None, help="Max companies to guess-verify")
    run_parser.add_argument("--per-endpoint", type=int, default=None, help="Workers per endpoint (sharded)")
    run_parser.add_argument("--workers", type=int, default=None, help="Flat pool workers")

    args = parser.parse_args(argv)

    if args.command == "mine":
        summary = mine_known_domains()
        print(f"Mined: {summary['found']}  Checked: {summary['checked']}  Ambiguous: {summary['ambiguous']}")
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE3b OK: +{report_data['mined']} mined, +{report_data['guessed']} guessed, "
            f"{report_data['domain_fill_pct']}% companies now have a domain"
        )
        return 0 if report_data["with_domain"] > 0 or report_data["total"] == 0 else 1

    if args.command == "run":
        summary = run(limit=args.limit, per_endpoint=args.per_endpoint, workers=args.workers)
        print(f"Mined: {summary['mined']['found']}  Guessed: {summary['guessed']['found']}")
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE3b OK: +{report_data['mined']} mined, +{report_data['guessed']} guessed, "
            f"{report_data['domain_fill_pct']}% companies now have a domain"
        )
        return 0 if report_data["with_domain"] > 0 or report_data["total"] == 0 else 1

    if args.command == "report":
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE3b OK: +{report_data['mined']} mined, +{report_data['guessed']} guessed, "
            f"{report_data['domain_fill_pct']}% companies now have a domain"
        )
        return 0 if report_data["with_domain"] > 0 or report_data["total"] == 0 else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
