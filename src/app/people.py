"""People discovery, email extraction, and SMTP verification. Phase 6.

On-demand, free contact finding for companies that already have a domain. Emails are stored in
`contact_points` (type='email') so social channels can slot in later. A bounce-risk address is
never marked sendable: verification gates every write.

# TODO(phase6-scale): Companies House officers when the live key is set; per-campaign scoping in Phase 7.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import smtplib
import string
import sys
import time
from typing import Any

import dns.resolver
import httpx
from sqlalchemy import func
from sqlmodel import select

from app.brain import cheap_json
from app.collect.staging import normalize_domain
from app.config import settings
from app.db import (
    Company,
    CompanySource,
    ContactPoint,
    Person,
    Session,
    Suppression,
    engine,
    init_db,
    now,
)
from app.discover import GENERIC_EMAIL_BASES
from app.resolve import registrable_domain
from app.webtext import fetch_domain_text, html_to_text

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# Keep httpx from logging URLs that contain the Hunter API key.
logging.getLogger("httpx").setLevel(logging.WARNING)

# Disposable / burner email domains. Keep a small conservative blocklist.
DISPOSABLE_DOMAINS = {
    "mailinator.com",
    "tempmail.com",
    "temp-mail.org",
    "throwawaymail.com",
    "guerrillamail.com",
    "yopmail.com",
    "sharklasers.com",
    "getairmail.com",
    "10minutemail.com",
    "fakeinbox.com",
    "burnermail.io",
}

# Paths likely to list staff / contact details.
CONTACT_PATHS = ["contact", "about", "about-us", "team", "staff", "people", "leadership"]

# Keys that signal an email value in raw JSON.
EMAIL_KEYS = {"email", "email_addr", "email_address", "correio_eletronico", "e_mail", "contact_email"}
CONTACT_KEYS = {"contact", "contacts", "email", "phone", "address"}

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean_email(value: str) -> str | None:
    """Return a normalized email or None if it looks broken."""
    value = value.strip().lower()
    value = re.sub(r"^[\s\"'<>():]+|[\s\"'<>():,;.]+$", "", value)
    if not value or "@" not in value:
        return None
    if value.count("@") != 1:
        return None
    local, domain = value.rsplit("@", 1)
    if not local or not domain or "." not in domain:
        return None
    return f"{local}@{domain}"


def _syntax_ok(email: str) -> bool:
    """Basic RFC-ish syntax sanity check."""
    if not email or "@" not in email:
        return False
    local, domain = email.rsplit("@", 1)
    if not re.match(r"^[a-z0-9._%+\-]+$", local, re.IGNORECASE):
        return False
    if not re.match(r"^[a-z0-9.\-]+\.[a-z]{2,}$", domain, re.IGNORECASE):
        return False
    return True


def _is_generic_or_disposable(email: str) -> bool:
    """Return True for public providers, site builders, or disposable domains."""
    domain = email.rsplit("@", 1)[-1].lower()
    if domain in DISPOSABLE_DOMAINS:
        return True
    extracted = _extract_tld(domain)
    if extracted and extracted.domain in GENERIC_EMAIL_BASES:
        return True
    return False


def _extract_tld(domain: str) -> Any:
    import tldextract

    return tldextract.extract(domain)


def _matches_company_domain(email: str, company_domain: str) -> bool:
    """Return True when the email's registrable domain matches the company's."""
    email_domain = email.rsplit("@", 1)[-1]
    return registrable_domain(email_domain) == registrable_domain(company_domain)


def _helo_domain(domain: str) -> str:
    """Domain to use in the SMTP HELO/EHLO command."""
    return _mail_from(domain).rsplit("@", 1)[-1]


def _mail_from(domain: str) -> str:
    """Return a MAIL FROM address.

    Prefer the configured FROM_EMAIL. If it is not set, fall back to an address
    on the recipient domain so the receiving MTA does not reject us for SPF.
    """
    from_addr = settings.from_email or ""
    if "@" in from_addr:
        return from_addr
    return f"verify@{domain}"


def _random_address(domain: str) -> str:
    """Generate a random mailbox for catch-all detection."""
    local = "".join(random.choices(string.ascii_lowercase, k=12))
    return f"{local}@{domain}"


def _normalize_name(value: str) -> str:
    """Lowercase, strip, collapse whitespace for person-name dedup."""
    return re.sub(r"\s+", " ", value.strip().lower())


# ---------------------------------------------------------------------------
# SMTP verification
# ---------------------------------------------------------------------------


def verify_email(addr: str) -> str:
    """Return one of: valid | invalid | catch_all | unknown.

    First asks Hunter.io if an API key is configured; if it returns a clear verdict,
    we use that. Otherwise we fall back to a direct SMTP RCPT probe.

    IMPORTANT: many home/mobile ISPs block outbound SMTP port 25. On a laptop this
    function will legitimately return ``unknown`` for every address. That is expected
    and safe: ``unknown`` contacts are stored as unverified and rely on the outbound
    provider's bounce handling at send time. We never mark ``unknown`` as verified.
    """
    addr = (addr or "").strip().lower()
    if not _syntax_ok(addr):
        return "invalid"
    if _is_generic_or_disposable(addr):
        return "invalid"

    # Try Hunter email verifier first (API key provided).
    hunter_status = _hunter_verify_email(addr)
    if hunter_status:
        return hunter_status

    if not settings.smtp_verify_enabled:
        return "unknown"

    domain = addr.rsplit("@", 1)[-1]

    # MX lookup.
    try:
        answers = dns.resolver.resolve(domain, "MX")
        mx = sorted(answers, key=lambda r: r.preference)[0].exchange.to_text().rstrip(".")
    except dns.resolver.NoAnswer:
        # No MX but an A record may still receive mail; treat as unknown, not invalid.
        return "unknown"
    except dns.resolver.NXDOMAIN:
        return "invalid"
    except dns.exception.DNSException:
        return "unknown"
    except Exception:  # noqa: BLE001
        return "unknown"

    if not mx:
        return "unknown"

    # SMTP probe.
    try:
        with smtplib.SMTP(mx, 25, timeout=settings.smtp_verify_timeout) as server:
            server.ehlo(_helo_domain(domain))
            server.mail(_mail_from(domain))
            code, message = server.rcpt(addr)
            if code in (250, 251):
                # Catch-all probe.
                probe = _random_address(domain)
                code2, _ = server.rcpt(probe)
                server.quit()
                if code2 in (250, 251):
                    return "catch_all"
                return "valid"
            if code in (550, 551, 553, 554):
                msg = str(message).lower()
                # Rejections based on our sending IP/SPF are not mailbox verdicts.
                if any(x in msg for x in ("spf", "not allowed", "sender", "reputation", "blacklist", "ip", "rbl")):
                    return "unknown"
                return "invalid"
            return "unknown"
    except (smtplib.SMTPException, OSError, TimeoutError):
        return "unknown"
    except Exception:  # noqa: BLE001
        return "unknown"


# ---------------------------------------------------------------------------
# Contact extraction helpers
# ---------------------------------------------------------------------------


def _extract_emails_from_text(text: str) -> set[str]:
    """Pull normalized emails out of free text."""
    found: set[str] = set()
    for match in EMAIL_RE.findall(text or ""):
        email = _clean_email(match)
        if email:
            found.add(email)
    return found


def _extract_emails_from_raw(raw: dict[str, Any]) -> set[str]:
    """Walk a parsed raw_json object and collect emails."""
    found: set[str] = set()

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                lower = k.lower().replace("-", "_")
                if lower in EMAIL_KEYS or "email" in lower or lower in CONTACT_KEYS:
                    if isinstance(v, str):
                        for m in EMAIL_RE.findall(v):
                            email = _clean_email(m)
                            if email:
                                found.add(email)
                    elif isinstance(v, list):
                        _walk(v)
                else:
                    _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)
        elif isinstance(obj, str):
            for m in EMAIL_RE.findall(obj):
                email = _clean_email(m)
                if email:
                    found.add(email)

    _walk(raw)
    return found


# ---------------------------------------------------------------------------
# Web fetching for people pages
# ---------------------------------------------------------------------------


def _fetch_path_text(domain: str, path: str, timeout: float = 12.0) -> str:
    """Fetch a specific path on the domain and return cleaned text."""
    url = f"https://{domain}/{path.lstrip('/')}"
    try:
        with httpx.Client() as client:
            response = client.get(
                url,
                timeout=timeout,
                follow_redirects=True,
                headers={
                    "User-Agent": settings.user_agent,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
            response.raise_for_status()
            content_type = response.headers.get("content-type", "").lower()
            if "text/html" not in content_type and "application/xhtml" not in content_type:
                return ""
            return html_to_text(response.text, max_chars=6000)
    except Exception:  # noqa: BLE001
        return ""


def _site_text(domain: str) -> str:
    """Return combined homepage + contact/about/team text."""
    parts: list[str] = []
    homepage_text, status = fetch_domain_text(domain, ignore_cache=True)
    if status == "ok" and homepage_text:
        parts.append(homepage_text)
    for path in CONTACT_PATHS:
        page_text = _fetch_path_text(domain, path)
        if page_text:
            parts.append(page_text)
        time.sleep(0.3)
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Brain extraction of named people
# ---------------------------------------------------------------------------


def _extract_named_people(text: str) -> list[dict[str, str]]:
    """Ask the cheap Brain to return named people from website text.

    Returns a list of {first_name, last_name, title}. Never invents people.
    """
    if not text or len(text) < 50:
        return []
    snippet = text[:3500].replace('"', "'").strip()
    prompt = (
        "Extract the named people (team, leadership, staff) mentioned in the website text below. "
        "Return ONLY a JSON array of objects with keys: first_name, last_name, title. "
        "If no named people are found, return an empty array. Do NOT invent people. "
        "Be conservative; only include people explicitly named with a first and last name.\n\n"
        f"Text:\n{snippet}\n"
    )
    result = cheap_json(
        prompt,
        system="You extract named people from website text. Return only JSON.",
        max_tokens=1500,
        demo_value=[],
        model=settings.model_cheap,
    )
    if not isinstance(result, list):
        return []
    people: list[dict[str, str]] = []
    for item in result:
        if not isinstance(item, dict):
            continue
        first = str(item.get("first_name", "")).strip()
        last = str(item.get("last_name", "")).strip()
        title = str(item.get("title", "")).strip()
        # Require at least a last name to avoid single-name noise.
        if not last:
            continue
        people.append({"first_name": first, "last_name": last, "title": title})
    return people


# ---------------------------------------------------------------------------
# Hunter.io client (uses the endpoints provided by the user)
# ---------------------------------------------------------------------------

HUNTER_BASE_URL = "https://api.hunter.io/v2"


def _hunter_get(path: str, params: dict[str, Any]) -> dict[str, Any] | None:
    """Make a GET request to the Hunter API and return the response data."""
    key = settings.hunter_api_key
    if not key:
        return None
    params = {**params, "api_key": key}
    try:
        response = httpx.get(f"{HUNTER_BASE_URL}{path}", params=params, timeout=20)
        response.raise_for_status()
        return response.json().get("data", {})
    except Exception as exc:  # noqa: BLE001
        logger.warning("Hunter API call failed for %s: %s", path, exc)
        return None


def _hunter_status_to_internal(status: str | None) -> str | None:
    """Map Hunter verification statuses to our internal statuses."""
    if not status:
        return None
    status = status.lower()
    if status in ("valid",):
        return "valid"
    if status in ("invalid", "disposable", "webmail"):
        return "invalid"
    if status in ("accept_all", "catch_all"):
        return "catch_all"
    return "unknown"


def _hunter_verify_email(email: str) -> str | None:
    """Return Hunter's verdict for an email, or None if unavailable."""
    data = _hunter_get("/email-verifier", {"email": email})
    if not data:
        return None
    return _hunter_status_to_internal(data.get("status"))


def _hunter_domain_search(domain: str) -> dict[str, Any]:
    """Return Hunter domain-search data: pattern + known emails.

    Returns {"pattern": str|None, "emails": [{value, first_name, last_name, position, confidence, type}, ...]}.
    """
    data = _hunter_get("/domain-search", {"domain": domain})
    if not data:
        return {"pattern": None, "emails": []}
    pattern = data.get("pattern", "")
    emails = []
    for item in data.get("emails", []):
        if not isinstance(item, dict):
            continue
        value = (item.get("value") or "").lower().strip()
        if not value:
            continue
        emails.append(
            {
                "value": value,
                "first_name": (item.get("first_name") or "").strip(),
                "last_name": (item.get("last_name") or "").strip(),
                "title": (item.get("position") or "").strip(),
                "confidence": int(item.get("confidence", 0) or 0),
                "type": (item.get("type") or "").lower().strip(),
            }
        )
    _HAS_PATTERN = ("{first", "{last", "{f}", "{l}")
    is_pattern = bool(pattern) and any(marker in pattern for marker in _HAS_PATTERN)
    return {"pattern": pattern if is_pattern else None, "emails": emails}


def _hunter_email_finder(domain: str, first_name: str, last_name: str) -> str | None:
    """Ask Hunter to find a specific person's email."""
    params: dict[str, str] = {"domain": domain}
    if first_name:
        params["first_name"] = first_name
    if last_name:
        params["last_name"] = last_name
    if len(params) < 2:
        return None
    data = _hunter_get("/email-finder", params)
    if not data:
        return None
    email = (data.get("email") or "").strip().lower()
    return email if email else None


def _hunter_pattern(domain: str) -> str | None:
    """Return just the organization's email pattern from Hunter.io."""
    return _hunter_domain_search(domain).get("pattern")


def _pattern_to_email(first: str, last: str, domain: str, pattern: str) -> str | None:
    """Apply a Hunter-style pattern like '{first}.{last}' or '{first}.{last}@domain.com'."""
    first = first.lower().strip()
    last = last.lower().strip()
    fi = first[0] if first else ""
    li = last[0] if last else ""
    # Pattern may omit the domain; if so, append our company domain.
    if "@" in pattern:
        local = pattern.split("@", 1)[0]
    else:
        local = pattern
    local = local.replace("{first_name}", first)
    local = local.replace("{last_name}", last)
    local = local.replace("{first}", first)
    local = local.replace("{last}", last)
    local = local.replace("{f}", fi)
    local = local.replace("{l}", li)
    local = re.sub(r"[^a-z0-9._\-]", "", local)
    if not local:
        return None
    return f"{local}@{domain.lower()}"


def _ranked_guesses(first: str, last: str, domain: str) -> list[tuple[str, int]]:
    """Return ranked (email, confidence) guesses when no pattern is known."""
    first = re.sub(r"[^a-z0-9]", "", first.lower())
    last = re.sub(r"[^a-z0-9]", "", last.lower())
    if not first and not last:
        return []
    guesses: list[tuple[str, int]] = []
    if first and last:
        guesses.append((f"{first}.{last}@{domain}", 45))
        guesses.append((f"{first}{last}@{domain}", 40))
        guesses.append((f"{first}_{last}@{domain}", 35))
        guesses.append((f"{first[0]}{last}@{domain}", 35))
    if first:
        guesses.append((f"{first}@{domain}", 30))
    if last:
        guesses.append((f"{last}@{domain}", 25))
    return guesses


# ---------------------------------------------------------------------------
# Storage helpers (dedup + gate)
# ---------------------------------------------------------------------------


def _is_suppressed(db, value: str) -> bool:
    return bool(
        db.exec(
            select(Suppression).where(Suppression.channel == "email").where(Suppression.value == value)
        ).first()
    )


def _add_suppression(db, value: str, reason: str = "SMTP invalid") -> None:
    if _is_suppressed(db, value):
        return
    db.add(Suppression(channel="email", value=value, reason=reason, created_at=now()))


def _find_or_create_person(
    db, company_id: int, first_name: str, last_name: str, title: str, source: str
) -> Person:
    first = (first_name or "").strip()
    last = (last_name or "").strip()
    norm_first = _normalize_name(first)
    norm_last = _normalize_name(last)

    existing = None
    if norm_first or norm_last:
        existing = db.exec(
            select(Person)
            .where(Person.company_id == company_id)
            .where(Person.first_name == first)
            .where(Person.last_name == last)
        ).first()
    else:
        # Anonymous role-contact person: one per company.
        existing = db.exec(
            select(Person)
            .where(Person.company_id == company_id)
            .where(Person.first_name == "")
            .where(Person.last_name == "")
        ).first()

    if existing:
        if title and not existing.title:
            existing.title = title
        if source and not existing.source:
            existing.source = source
        db.add(existing)
        return existing

    person = Person(
        company_id=company_id,
        first_name=first,
        last_name=last,
        title=title,
        source=source,
        created_at=now(),
    )
    db.add(person)
    db.flush()
    return person


def _status_rank(status: str) -> int:
    return {"valid": 3, "catch_all": 2, "unknown": 1, "invalid": 0}.get(status, 0)


def _store_email(
    db,
    company_id: int,
    email: str,
    person_info: dict[str, str],
    status: str,
    confidence: int,
    source: str,
) -> str | None:
    """Store a verified email or suppress it. Returns the status or None if skipped."""
    email = _clean_email(email)
    if not email or _is_generic_or_disposable(email):
        return None
    if not _matches_company_domain(email, db.get(Company, company_id).domain or ""):
        return None
    if _is_suppressed(db, email):
        return None

    if status == "invalid":
        _add_suppression(db, email, reason=f"SMTP invalid ({source})")
        return "invalid"

    person = _find_or_create_person(
        db, company_id, person_info.get("first_name", ""), person_info.get("last_name", ""), person_info.get("title", ""), source
    )

    existing = db.exec(
        select(ContactPoint)
        .where(ContactPoint.type == "email")
        .where(ContactPoint.value == email)
        .where(ContactPoint.person_id == person.id)
    ).first()

    if existing:
        # Only upgrade status/confidence.
        if _status_rank(status) > _status_rank(existing.verify_status):
            existing.verify_status = status
            existing.verified = 1 if status == "valid" else 0
            existing.confidence = max(existing.confidence, confidence)
            existing.source = source
            db.add(existing)
        elif confidence > existing.confidence:
            existing.confidence = confidence
            db.add(existing)
        return status

    db.add(
        ContactPoint(
            person_id=person.id,
            type="email",
            value=email,
            verified=1 if status == "valid" else 0,
            verify_status=status,
            confidence=confidence,
            source=source,
            created_at=now(),
        )
    )
    return status


# ---------------------------------------------------------------------------
# Levers
# ---------------------------------------------------------------------------


def _lever1_mine_emails(db, company: Company) -> list[dict[str, Any]]:
    """Mine emails from company_sources.raw_json."""
    candidates: list[dict[str, Any]] = []
    for source in db.exec(select(CompanySource).where(CompanySource.company_id == company.id)):
        try:
            raw = json.loads(source.raw_json) if source.raw_json else {}
        except json.JSONDecodeError:
            raw = {}
        for email in _extract_emails_from_raw(raw):
            if _is_generic_or_disposable(email):
                continue
            if not _matches_company_domain(email, company.domain or ""):
                continue
            candidates.append(
                {
                    "email": email,
                    "person": {},
                    "confidence": 80,
                    "source": "mined",
                }
            )
    return candidates


def _lever2_site_emails_and_people(db, company: Company) -> list[dict[str, Any]]:
    """Crawl homepage + contact pages for emails and named people."""
    domain = company.domain
    if not domain:
        return []

    text = _site_text(domain)
    if not text:
        return []

    emails = _extract_emails_from_text(text)
    people = _extract_named_people(text)

    candidates: list[dict[str, Any]] = []
    # Emails we can attribute to a named person if the local part matches.
    people_by_email: dict[str, dict[str, str]] = {}
    for person in people:
        first = person.get("first_name", "").lower()
        last = person.get("last_name", "").lower()
        for email in list(emails):
            local = email.split("@", 1)[0].lower()
            if (first and last and (local == f"{first}.{last}" or local == f"{first}{last}")) or (
                last and local == last
            ):
                people_by_email[email] = person
                emails.discard(email)

    for email, person in people_by_email.items():
        candidates.append({"email": email, "person": person, "confidence": 75, "source": "site"})
    for email in emails:
        candidates.append({"email": email, "person": {}, "confidence": 70, "source": "site"})

    # Named people without a matching email: keep them for pattern guessing in lever 3.
    matched_names = {json.dumps(p, sort_keys=True) for p in people_by_email.values()}
    for person in people:
        if json.dumps(person, sort_keys=True) not in matched_names:
            candidates.append({"email": "", "person": person, "confidence": 0, "source": "named"})

    return candidates


def _lever3_hunter(db, company: Company, named_people: list[dict[str, str]]) -> list[dict[str, Any]]:
    """Build candidate emails from Hunter.io domain search + email finder + pattern guesses."""
    domain = company.domain
    if not domain:
        return []

    hunter_data = _hunter_domain_search(domain)
    pattern = hunter_data.get("pattern")
    candidates: list[dict[str, Any]] = []
    seen_emails: set[str] = set()

    # Known emails from Hunter domain search (highest confidence).
    for item in hunter_data.get("emails", []):
        email = item["value"]
        if not email or email in seen_emails:
            continue
        seen_emails.add(email)
        person = {}
        if item.get("first_name") or item.get("last_name"):
            person = {
                "first_name": item["first_name"],
                "last_name": item["last_name"],
                "title": item["title"],
            }
        candidates.append(
            {
                "email": email,
                "person": person,
                "confidence": max(item["confidence"], 90),
                "source": "hunter_domain",
            }
        )

    # Email finder for named people extracted from the website.
    for person in named_people:
        first = person.get("first_name", "")
        last = person.get("last_name", "")
        if not first and not last:
            continue
        email = _hunter_email_finder(domain, first, last)
        if email and email not in seen_emails:
            seen_emails.add(email)
            candidates.append({"email": email, "person": person, "confidence": 85, "source": "hunter_finder"})

    # Pattern-based guesses for named people still without an email.
    finder_names = {
        json.dumps(c["person"], sort_keys=True)
        for c in candidates
        if c.get("source") == "hunter_finder" and c.get("person")
    }
    for person in named_people:
        if json.dumps(person, sort_keys=True) in finder_names:
            continue
        first = person.get("first_name", "")
        last = person.get("last_name", "")
        if not first and not last:
            continue
        if pattern:
            email = _pattern_to_email(first, last, domain, pattern)
            if email and email not in seen_emails:
                seen_emails.add(email)
                candidates.append({"email": email, "person": person, "confidence": 70, "source": "hunter_pattern"})
        else:
            for email, confidence in _ranked_guesses(first, last, domain):
                if email not in seen_emails:
                    seen_emails.add(email)
                    candidates.append({"email": email, "person": person, "confidence": confidence, "source": "guess"})

    return candidates


# ---------------------------------------------------------------------------
# Main entry points
# ---------------------------------------------------------------------------


def find_people_for_company(company_id: int) -> dict[str, int]:
    """Run all three levers for one company, verify, store, and return counts."""
    init_db()
    counts: dict[str, int] = {
        "people": 0,
        "emails": 0,
        "valid": 0,
        "catch_all": 0,
        "invalid_suppressed": 0,
        "unknown": 0,
    }

    with Session(engine) as db:
        company = db.get(Company, company_id)
        if not company or not company.domain:
            return counts

        # Collect candidates.
        raw_candidates: list[dict[str, Any]] = []
        raw_candidates.extend(_lever1_mine_emails(db, company))
        raw_candidates.extend(_lever2_site_emails_and_people(db, company))

        # Named people from lever 2 for Hunter email finder / pattern guessing.
        named_people = [c["person"] for c in raw_candidates if c.get("source") == "named" and c.get("person")]
        raw_candidates.extend(_lever3_hunter(db, company, named_people))

        # Remove named-only placeholders and dedupe by email.
        email_map: dict[str, dict[str, Any]] = {}
        unnamed_people: list[dict[str, Any]] = []
        for cand in raw_candidates:
            email = (cand.get("email") or "").strip().lower()
            person = cand.get("person") or {}
            if not email:
                # A named person with no email is only useful for lever 3, already consumed.
                continue
            existing = email_map.get(email)
            if not existing:
                email_map[email] = cand
            else:
                # Prefer a named attribution over anonymous.
                if person.get("last_name") and not existing.get("person", {}).get("last_name"):
                    email_map[email] = cand
                elif cand.get("confidence", 0) > existing.get("confidence", 0):
                    email_map[email] = cand

        # Verify and store.
        for email, cand in email_map.items():
            status = verify_email(email)
            stored_status = _store_email(
                db,
                company.id,
                email,
                cand.get("person", {}),
                status,
                cand.get("confidence", 0),
                cand.get("source", "unknown"),
            )
            if stored_status:
                counts["emails"] += 1
                if stored_status == "valid":
                    counts["valid"] += 1
                elif stored_status == "catch_all":
                    counts["catch_all"] += 1
                elif stored_status == "invalid":
                    counts["invalid_suppressed"] += 1
                elif stored_status == "unknown":
                    counts["unknown"] += 1

        db.commit()

    # Count distinct people created/updated for this company.
    with Session(engine) as db:
        people_count = len(
            db.exec(select(Person).where(Person.company_id == company_id)).all()
        )
        counts["people"] = people_count

    return counts


def run(limit: int | None = None, targetable_only: bool = True) -> dict[str, int]:
    """Run people discovery for companies with a domain.

    Prefers targetable companies (those with a description) when ``targetable_only`` is True.
    """
    init_db()
    totals: dict[str, int] = {
        "companies": 0,
        "people": 0,
        "emails": 0,
        "valid": 0,
        "catch_all": 0,
        "invalid_suppressed": 0,
        "unknown": 0,
    }

    query = select(Company).where(Company.domain != "").where(Company.domain.is_not(None))  # type: ignore[attr-defined]
    if targetable_only:
        query = query.where(Company.description != "")

    with Session(engine) as db:
        for company in db.exec(query.order_by(Company.id)):
            counts = find_people_for_company(company.id)
            totals["companies"] += 1
            totals["people"] += counts["people"]
            totals["emails"] += counts["emails"]
            totals["valid"] += counts["valid"]
            totals["catch_all"] += counts["catch_all"]
            totals["invalid_suppressed"] += counts["invalid_suppressed"]
            totals["unknown"] += counts["unknown"]
            if limit is not None and totals["companies"] >= limit:
                break
            time.sleep(0.5)

    return totals


def report() -> dict[str, Any]:
    """Return people/contact coverage numbers."""
    init_db()
    with Session(engine) as db:
        # All aggregation happens in SQL. Materialising millions of ORM objects
        # just to len()/group them in Python stalls the report and can exhaust RAM.
        total_companies = db.exec(select(func.count()).select_from(Company)).one()
        with_domain = db.exec(
            select(func.count())
            .select_from(Company)
            .where(Company.domain != "")
            .where(Company.domain.is_not(None))  # type: ignore[attr-defined]
        ).one()
        people = db.exec(select(func.count()).select_from(Person)).one()
        emails = db.exec(
            select(func.count()).select_from(ContactPoint).where(ContactPoint.type == "email")
        ).one()
        by_status: dict[str, int] = {
            status: count
            for status, count in db.exec(
                select(ContactPoint.verify_status, func.count())
                .where(ContactPoint.type == "email")
                .group_by(ContactPoint.verify_status)
            )
        }
        companies_with_contact = db.exec(
            select(func.count(func.distinct(Person.company_id))).where(
                Person.company_id.is_not(None)  # type: ignore[attr-defined]
            )
        ).one()

    return {
        "total_companies": total_companies,
        "with_domain": with_domain,
        "people": people,
        "emails": emails,
        "by_status": by_status,
        "companies_with_contact": companies_with_contact,
    }


def _print_report(report_data: dict[str, Any]) -> None:
    print(f"Total companies:         {report_data['total_companies']}")
    print(f"With domain:             {report_data['with_domain']}")
    print(f"Companies with contact:  {report_data['companies_with_contact']}")
    print(f"People:                  {report_data['people']}")
    print(f"Emails:                  {report_data['emails']}")
    print("By verify_status:")
    for status, count in sorted(report_data["by_status"].items()):
        print(f"  {status:<16} {count}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GTM Engine Phase 6 people + email discovery")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Find people and emails for companies")
    run_parser.add_argument("--limit", type=int, default=None, help="Maximum companies to process")
    run_parser.add_argument("--all", action="store_true", help="Process all companies with a domain, not just targetable ones")

    company_parser = subparsers.add_parser("company", help="Process a single company by domain")
    company_parser.add_argument("domain", help="Company domain")

    subparsers.add_parser("report", help="Print people/email coverage report")

    args = parser.parse_args(argv)

    if args.command == "run":
        totals = run(limit=args.limit, targetable_only=not args.all)
        print(
            f"Companies: {totals['companies']}  People: {totals['people']}  Emails: {totals['emails']}  "
            f"Valid: {totals['valid']}  Catch-all: {totals['catch_all']}  "
            f"Invalid-suppressed: {totals['invalid_suppressed']}  Unknown: {totals['unknown']}"
        )
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE6 OK: {report_data['people']} people, {report_data['emails']} emails, "
            f"{report_data['by_status'].get('valid', 0)} valid, "
            f"{report_data['by_status'].get('catch_all', 0)} catch-all, "
            f"{report_data['by_status'].get('invalid', 0)} suppressed"
        )
        return 0

    if args.command == "company":
        with Session(engine) as db:
            company = db.exec(select(Company).where(Company.domain == args.domain)).first()
            if not company:
                print(f"No company found for domain {args.domain!r}")
                return 1
            counts = find_people_for_company(company.id)
        print(
            f"People: {counts['people']}  Emails: {counts['emails']}  "
            f"Valid: {counts['valid']}  Catch-all: {counts['catch_all']}  "
            f"Invalid-suppressed: {counts['invalid_suppressed']}  Unknown: {counts['unknown']}"
        )
        return 0

    if args.command == "report":
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE6 OK: {report_data['people']} people, {report_data['emails']} emails, "
            f"{report_data['by_status'].get('valid', 0)} valid, "
            f"{report_data['by_status'].get('catch_all', 0)} catch-all, "
            f"{report_data['by_status'].get('invalid', 0)} suppressed"
        )
        return 0 if report_data["emails"] > 0 or report_data["total_companies"] == 0 else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
