"""Read-only query helpers for the data viewer.

All lists use LIMIT/OFFSET. Every count is a SELECT COUNT(*) — never
unbounded .all() or counting in Python.
"""

from __future__ import annotations

from sqlmodel import Session, select, func

from app.db import Company, ContactPoint, Person, CompanySource

DEFAULT_PAGE_SIZE = 100


def summary(db: Session) -> dict:
    """Total counts + top countries + top industries."""
    total = db.exec(select(func.count()).select_from(Company)).first() or 0
    with_email = _count_companies_with_contact(db, "email")
    with_phone = _count_companies_with_contact(db, "phone")

    top_countries = db.exec(
        select(Company.country, func.count()).where(Company.country != "")
        .group_by(Company.country).order_by(func.count().desc()).limit(20)
    ).all()
    top_industries = db.exec(
        select(Company.industry_nace, func.count()).where(Company.industry_nace != "")
        .group_by(Company.industry_nace).order_by(func.count().desc()).limit(30)
    ).all()

    return {
        "total": int(total),
        "with_email": with_email,
        "with_phone": with_phone,
        "top_countries": [{"name": r[0], "count": int(r[1])} for r in top_countries],
        "top_industries": [{"name": r[0], "count": int(r[1])} for r in top_industries],
    }


def _count_companies_with_contact(db: Session, ctype: str) -> int:
    sub = (
        select(func.count())
        .select_from(Company)
        .where(
            Company.id.in_(
                select(Person.company_id).where(
                    Person.id.in_(
                        select(ContactPoint.person_id).where(ContactPoint.type == ctype)
                    )
                )
            )
        )
    )
    result = db.exec(sub).first()
    return int(result) if result else 0


def data_page(
    db: Session,
    industry: str = "",
    country: str = "",
    has_email: bool = False,
    q: str = "",
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> dict:
    """Paginated company list with best email + phone. SQL-paginated."""
    base = select(Company)
    count_q = select(func.count()).select_from(Company)

    if industry:
        base = base.where(Company.industry_nace == industry)
        count_q = count_q.where(Company.industry_nace == industry)
    if country:
        base = base.where(Company.country == country)
        count_q = count_q.where(Company.country == country)
    if q:
        base = base.where(Company.name.like(f"%{q}%"))  # type: ignore[arg-type]
        count_q = count_q.where(Company.name.like(f"%{q}%"))  # type: ignore[arg-type]

    total = int(db.exec(count_q).first() or 0)
    offset = max(0, (page - 1) * page_size)
    companies = db.exec(base.limit(page_size).offset(offset)).all()

    rows: list[dict] = []
    for c in companies:
        email = _best_contact(db, c.id, "email")
        phone = _best_contact(db, c.id, "phone") or "-"
        desc = (c.description or "")[:80] if c.description else "-"
        addr = c.country or "-"

        if has_email and email == "-":
            continue

        rows.append({
            "company_id": c.id,
            "name": c.name or "-",
            "email": email,
            "phone": phone,
            "description": desc,
            "address": addr,
            "industry": c.industry_nace or "-",
            "country": c.country or "-",
        })

    if has_email:
        total = len(rows)

    return {
        "rows": rows,
        "total": total,
        "page": page,
        "pages": max(1, (total + page_size - 1) // page_size) if total else 1,
    }


def _best_contact(db: Session, company_id: int, ctype: str) -> str:
    cp = db.exec(
        select(ContactPoint).where(
            ContactPoint.person_id.in_(
                select(Person.id).where(Person.company_id == company_id)
            ),
            ContactPoint.type == ctype,
        ).order_by(ContactPoint.verified.desc(), ContactPoint.confidence.desc())
    ).first()
    return cp.value if cp else "-"


def company_detail(company_id: int, db: Session) -> dict | None:
    c = db.get(Company, company_id)
    if not c:
        return None
    contacts = db.exec(
        select(ContactPoint).where(
            ContactPoint.person_id.in_(
                select(Person.id).where(Person.company_id == company_id)
            )
        )
    ).all()
    sources = db.exec(
        select(CompanySource).where(CompanySource.company_id == company_id)
    ).all()
    return {
        "id": c.id, "name": c.name, "domain": c.domain,
        "country": c.country, "industry_nace": c.industry_nace,
        "description": c.description, "keywords": c.keywords,
        "web_text": (c.web_text or "")[:200],
        "size_range": c.size_range, "size_estimate": c.size_estimate,
        "b2b_score": c.b2b_score, "saas_score": c.saas_score,
        "startup_score": c.startup_score, "tech_score": c.tech_score,
        "source": c.source, "enriched_at": c.enriched_at, "created_at": c.created_at,
        "contacts": [
            {"type": cp.type, "value": cp.value, "verify_status": cp.verify_status,
             "confidence": cp.confidence, "source": cp.source}
            for cp in contacts
        ],
        "sources": [
            {"source": s.source, "external_id": s.external_id, "raw_json": s.raw_json[:500] if s.raw_json else ""}
            for s in sources
        ],
    }
