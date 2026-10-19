"""Listing-to-contact promotion module. Phase 2b.

Converts OSM/Overture listing data in company_sources.raw_json into
email + phone ContactPoints, creating a generic Person per company as
needed. Role addresses (info@, contact@) and free-provider emails
(gmail, etc.) are accepted for published listings. Idempotent.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from typing import Any

from sqlalchemy import func
from sqlmodel import select

from app.config import settings
from app.db import (
    Company, CompanySource, ContactPoint, Person,
    Session, engine, init_db, now, run_concurrent,
)

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# Reuse people.py's clean/syntax helpers.
from app.people import _clean_email, _syntax_ok, _extract_emails_from_text
DISPOSABLE_DOMAINS = {
    "mailinator.com", "tempmail.com", "temp-mail.org", "throwawaymail.com",
    "guerrillamail.com", "yopmail.com", "sharklasers.com", "getairmail.com",
    "10minutemail.com", "fakeinbox.com", "burnermail.io",
}

_GENERIC_EMAIL_BASES = {
    "gmail", "googlemail", "yahoo", "hotmail", "outlook", "live",
    "aol", "icloud", "proton", "protonmail", "gmx", "mail", "qq",
    "163", "126", "yandex", "zoho", "hey", "me", "msn", "foxmail",
    "ymail", "rocketmail", "fastmail", "tuta", "tutanota",
    "hushmail", "mailfence",
}


def _is_free_provider(email: str) -> bool:
    domain = email.rsplit("@", 1)[-1].lower()
    import tldextract
    extracted = tldextract.extract(domain)
    return extracted.domain in _GENERIC_EMAIL_BASES if extracted else False


def _is_disposable(email: str) -> bool:
    domain = email.rsplit("@", 1)[-1].lower()
    return domain in DISPOSABLE_DOMAINS


def promote_contacts(
    limit: int | None = None, workers: int = 8, batch: int = 2000
) -> dict[str, int]:
    """Promote listing raw_json into people + contact_points.

    SINGLE-PASS STREAMING implementation. The previous concurrent version:
      * submitted EVERY company as a task up-front (3M Future objects -> ~7.6 GB RSS),
      * opened a Session and ran ~4 queries PER company,
      * committed PER company — and SQLite serialises writers, so 8 threads just
        contended for the write lock and fsynced 3M times (~74 rows/sec => 11.5 h).

    Now: one session, one ordered scan of company_sources, in-memory lookups, and a
    commit every `batch` companies. `workers` is accepted for backwards
    compatibility but no longer used.

    Returns: {companies_scanned, emails_added, phones_added,
               persons_created, skipped, invalid, errors}.
    """
    init_db()
    sources = {s.strip().lower() for s in settings.listing_sources.split(",") if s.strip()}
    if not sources:
        return {"companies_scanned": 0, "emails_added": 0, "phones_added": 0,
                "persons_created": 0, "skipped": 0, "invalid": 0, "errors": 0}

    with Session(engine) as db:
        # Collect DISTINCT company ids in SQL and de-dupe with a SET.
        # The previous version did `if cs.company_id not in company_ids` against a
        # LIST — an O(n) scan per row, i.e. O(n^2) overall, which never completes
        # once there are millions of listing rows.
        company_ids: list[int] = []
        seen_ids: set[int] = set()
        for cid in db.exec(
            select(CompanySource.company_id)
            .where(CompanySource.source.in_(sources))
            .distinct()
        ):
            if cid in seen_ids:
                continue
            seen_ids.add(cid)
            company_ids.append(cid)
            if limit and len(company_ids) >= limit:
                break
        logger.info("listings: %d companies from listing sources", len(company_ids))

        # Pre-load idempotency + person lookups ONCE instead of querying per company.
        already_promoted: set[int] = {
            row for row in db.exec(
                select(Person.company_id)
                .join(ContactPoint, ContactPoint.person_id == Person.id)
                .where(ContactPoint.source.like("%_listing"))
                .distinct()
            ) if row is not None
        }
        logger.info("listings: %d companies already promoted (will skip)", len(already_promoted))

    if not company_ids:
        return {"companies_scanned": 0, "emails_added": 0, "phones_added": 0,
                "persons_created": 0, "skipped": 0, "invalid": 0, "errors": 0}

    counters = {"companies_scanned": 0, "emails_added": 0, "phones_added": 0,
                "persons_created": 0, "skipped": 0, "invalid": 0, "errors": 0}

    wanted: set[int] = set(company_ids)

    with Session(engine) as db:
        # Existing people, loaded once (avoids a per-company SELECT).
        person_by_company: dict[int, int] = {}
        for cid_, pid_ in db.exec(select(Person.company_id, Person.id)):
            if cid_ is not None and cid_ not in person_by_company:
                person_by_company[cid_] = pid_
        logger.info("listings: %d companies already have a person", len(person_by_company))

        # ONE ordered scan of the listing rows; rows for a company arrive together.
        rows = db.exec(
            select(CompanySource.company_id, CompanySource.source, CompanySource.raw_json)
            .where(CompanySource.source.in_(sources))
            .order_by(CompanySource.company_id)
        )

        cur_cid: int | None = None
        cur_source = ""
        emails: list[str] = []
        phones: list[str] = []
        category = ""
        pending = 0

        def _flush(cid: int, src: str, emails: list[str], phones: list[str], category: str) -> None:
            """Write one company's contacts. Assumes cid is wanted and not yet promoted."""
            nonlocal pending
            counters["companies_scanned"] += 1
            if cid in already_promoted:
                counters["skipped"] += 1
                return
            if not emails and not phones:
                counters["skipped"] += 1
                return

            pid = person_by_company.get(cid)
            if pid is None:
                person = Person(
                    company_id=cid, first_name="", last_name="",
                    title=category or "Business", source="listing",
                )
                db.add(person)
                db.flush()
                pid = person.id
                person_by_company[cid] = pid
                counters["persons_created"] += 1

            listing_source = f"{src}_listing" if src else "listing"
            seen_vals: set[tuple[str, str]] = set()

            for email in emails:
                if not _syntax_ok(email) or _is_disposable(email):
                    counters["invalid"] += 1
                    continue
                if not settings.listing_allow_free_providers and _is_free_provider(email):
                    counters["invalid"] += 1
                    continue
                email = email.lower()
                if ("email", email) in seen_vals:
                    continue
                seen_vals.add(("email", email))
                verify_status, verified = "unknown", 0
                if settings.listing_verify_emails:
                    from app.people import verify_email
                    verify_status = verify_email(email)
                    verified = 1 if verify_status == "valid" else 0
                db.add(ContactPoint(
                    person_id=pid, type="email", value=email,
                    verified=verified, verify_status=verify_status,
                    confidence=settings.listing_contact_confidence,
                    source=listing_source,
                ))
                counters["emails_added"] += 1

            for phone in phones:
                phone_norm = re.sub(r"[ \-\(\)\.]+", "", phone).strip()
                if not phone_norm or len(phone_norm) < 6:
                    continue
                if ("phone", phone_norm) in seen_vals:
                    continue
                seen_vals.add(("phone", phone_norm))
                db.add(ContactPoint(
                    person_id=pid, type="phone", value=phone_norm,
                    verified=0, verify_status="unknown",
                    confidence=settings.listing_contact_confidence,
                    source=listing_source,
                ))
                counters["phones_added"] += 1

            already_promoted.add(cid)
            pending += 1

        for cid, src, raw_json in rows:
            if cid != cur_cid:
                if cur_cid is not None and cur_cid in wanted:
                    _flush(cur_cid, cur_source, emails, phones, category)
                    if pending >= batch:
                        db.commit()
                        pending = 0
                        logger.info(
                            "listings: %d scanned, %d emails, %d phones",
                            counters["companies_scanned"], counters["emails_added"],
                            counters["phones_added"],
                        )
                cur_cid, cur_source = cid, src
                emails, phones, category = [], [], ""

            if cid not in wanted:
                continue
            try:
                raw = json.loads(raw_json) if raw_json else {}
            except json.JSONDecodeError:
                continue
            category = raw.get("category", "") or category or ""
            email_val = raw.get("email", "")
            if email_val:
                cleaned = _clean_email(str(email_val))
                if cleaned:
                    emails.append(cleaned)
            phone_val = raw.get("phone", "")
            if phone_val:
                phones.append(str(phone_val).strip())
            for e in _extract_emails_from_text(json.dumps(raw)):
                cleaned = _clean_email(e)
                if cleaned and cleaned not in emails:
                    emails.append(cleaned)

        if cur_cid is not None and cur_cid in wanted:
            _flush(cur_cid, cur_source, emails, phones, category)
        db.commit()

    logger.info(
        "listings done: %d scanned, %d emails, %d phones, %d persons",
        counters["companies_scanned"], counters["emails_added"],
        counters["phones_added"], counters["persons_created"],
    )
    return counters



def report() -> dict:
    """Return listing-promotion coverage: companies, email-accessible, phone, top industries."""
    init_db()
    sources = {s.strip().lower() for s in settings.listing_sources.split(",") if s.strip()}
    with Session(engine) as db:
        # ALL aggregation in SQL. The previous version called .all() on multi-million
        # row selects and counted in Python, which is slow and memory-hungry; it also
        # unpacked single-column rows as tuples (`for (ind,) in rows`), which raises
        # ValueError because SQLModel returns scalars for a single-column select.
        listing_company_ids = select(CompanySource.company_id).where(
            CompanySource.source.in_(sources)
        )

        def _companies_with(contact_type: str):
            return select(Person.company_id).where(
                Person.id.in_(
                    select(ContactPoint.person_id).where(
                        ContactPoint.type == contact_type,
                        ContactPoint.source.like("%_listing"),
                    )
                )
            )

        total = db.exec(
            select(func.count(func.distinct(CompanySource.company_id))).where(
                CompanySource.source.in_(sources)
            )
        ).one()
        with_email = db.exec(
            select(func.count(func.distinct(Company.id))).where(
                Company.id.in_(_companies_with("email"))
            )
        ).one()
        with_phone = db.exec(
            select(func.count(func.distinct(Company.id))).where(
                Company.id.in_(_companies_with("phone"))
            )
        ).one()

        # Top verticals by email-reachable company count — GROUP BY in SQL.
        top: list[dict] = []
        for industry, count in db.exec(
            select(Company.industry_nace, func.count())
            .where(Company.id.in_(_companies_with("email")))
            .where(Company.industry_nace != "")
            .group_by(Company.industry_nace)
            .order_by(func.count().desc())
            .limit(15)
        ):
            top.append({"industry": (industry or "").strip().lower(), "count": count})

    return {"total_mined": total, "with_email": with_email,
            "with_phone": with_phone, "top_industries": top}


def backfill_industry(limit: int | None = None, batch: int = 5000) -> dict[str, int]:
    """Copy the listing category into companies.industry_nace.

    Companies resolved before the industry fix have an empty industry_nace, which
    breaks vertical targeting and the report. The category is already stored in
    company_sources.raw_json, so we can backfill WITHOUT re-running resolve (a
    --full rebuild would delete the people/contact_points already created).
    """
    init_db()
    sources = {s.strip().lower() for s in settings.listing_sources.split(",") if s.strip()}
    updated = 0
    scanned = 0
    with Session(engine) as db:
        # Companies still missing an industry.
        missing: set[int] = {
            cid for cid in db.exec(
                select(Company.id).where(
                    (Company.industry_nace == "") | (Company.industry_nace.is_(None))
                )
            )
        }
        logger.info("backfill: %d companies missing an industry", len(missing))
        if not missing:
            return {"scanned": 0, "updated": 0}

        pending = 0
        for cid, raw_json in db.exec(
            select(CompanySource.company_id, CompanySource.raw_json)
            .where(CompanySource.source.in_(sources))
        ):
            scanned += 1
            if cid not in missing:
                continue
            try:
                raw = json.loads(raw_json) if raw_json else {}
            except json.JSONDecodeError:
                continue
            category = str(raw.get("category", "") or "").strip().lower()
            if not category:
                continue
            company = db.get(Company, cid)
            if company is None:
                continue
            company.industry_nace = category
            db.add(company)
            missing.discard(cid)
            updated += 1
            pending += 1
            if pending >= batch:
                db.commit()
                pending = 0
                logger.info("backfill: %d companies updated", updated)
            if limit and updated >= limit:
                break
        db.commit()
    logger.info("backfill done: %d scanned, %d updated", scanned, updated)
    return {"scanned": scanned, "updated": updated}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GTM Engine listing-to-contact promotion")
    subparsers = parser.add_subparsers(dest="command")

    p = subparsers.add_parser("promote", help="Promote listing contacts")
    p.add_argument("--limit", type=int, default=None, help="Max companies")
    p.add_argument("--workers", type=int, default=8, help="Concurrent workers")

    subparsers.add_parser("report", help="Listing contact coverage report")

    bf = subparsers.add_parser(
        "backfill-industry",
        help="Copy listing category into companies.industry_nace (no resolve rerun)",
    )
    bf.add_argument("--limit", type=int, default=None)

    args = parser.parse_args(argv)

    if args.command == "promote":
        c = promote_contacts(limit=args.limit, workers=args.workers)
        print(f"Scanned: {c['companies_scanned']}, Emails: {c['emails_added']}, "
              f"Phones: {c['phones_added']}, Persons: {c['persons_created']}, "
              f"Skipped: {c['skipped']}, Invalid: {c['invalid']}")
        return 0

    if args.command == "backfill-industry":
        r = backfill_industry(limit=args.limit)
        print(f"Backfilled industry on {r['updated']} companies (scanned {r['scanned']})")
        return 0

    if args.command == "report":
        r = report()
        print(f"Listing-source companies: {r['total_mined']}")
        print(f"  With email: {r['with_email']}")
        print(f"  With phone: {r['with_phone']}")
        print("  Top industries (email-reachable):")
        for item in r["top_industries"]:
            print(f"    {item['industry']:<30} {item['count']:>6}")
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
