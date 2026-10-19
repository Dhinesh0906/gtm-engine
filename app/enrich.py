"""Company enrichment and scoring module. Phase 4.

Reads a company's cached web_text and asks the cheap Brain to fill structured
enrichment columns. Lazy, idempotent, and never crashes on bad model output.

# TODO(phase4-scale): batch enrichment on the cloud; embeddings for Phase 5 search.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from typing import Any

from sqlalchemy import func
from sqlmodel import select

from app.brain import cheap_json
from app.config import settings
from app.db import Company, CompanySource, Session, engine, init_db, now, run_concurrent


# ---------------------------------------------------------------------------
# No-LLM tag-based enrichment for OSM companies
# ---------------------------------------------------------------------------

_TAG_LABELS: dict[str, str] = {
    "dentist": "Dental practice", "doctors": "Medical practice", "clinic": "Medical clinic",
    "veterinary": "Veterinary clinic", "pharmacy": "Pharmacy",
    "physiotherapist": "Physiotherapy clinic", "psychotherapist": "Psychotherapy practice",
    "optometrist": "Optometry practice", "podiatrist": "Podiatry clinic",
    "chiropractor": "Chiropractic clinic", "midwife": "Midwifery practice",
    "alternative": "Alternative healthcare practice",
    "hairdresser": "Hair salon", "beauty": "Beauty salon", "massage": "Massage parlour",
    "nails": "Nail salon", "tattoo": "Tattoo studio", "optician": "Optician",
    "barber": "Barber shop", "car_repair": "Car repair shop",
    "fitness_centre": "Fitness centre", "spa": "Spa", "sports_centre": "Sports centre",
    "lawyer": "Law firm", "accountant": "Accounting firm", "estate_agent": "Real estate agency",
    "insurance": "Insurance office", "financial": "Financial services",
    "tax_advisor": "Tax advisory", "employment_agency": "Recruitment agency",
    "architect": "Architecture firm", "notary": "Notary office",
    "consulting": "Consulting firm", "travel_agent": "Travel agency",
    "plumber": "Plumbing service", "electrician": "Electrical contractor",
    "hvac": "HVAC contractor", "carpenter": "Carpentry service",
    "painter": "Painting contractor", "roofer": "Roofing contractor",
    "builder": "Building contractor", "locksmith": "Locksmith",
    "gardener": "Gardening service",
    "restaurant": "Restaurant", "cafe": "Cafe", "bar": "Bar",
    "hotel": "Hotel", "guest_house": "Guest house",
    "bed_and_breakfast": "Bed and breakfast",
}


def _build_tag_description(name: str, industry: str, country: str) -> str:
    label = _TAG_LABELS.get(industry, industry.replace("_", " ").title())
    parts = [label]
    if country and country.upper() not in ("", "XX"):
        parts.append(country.upper())
    return f"{' '.join(parts)}."


def enrich_from_tags(company_id: int) -> str:
    """No-LLM enrichment for OSM/Overture-sourced companies using their tag vertical.

    Status: "enriched" on success, "skipped" if already enriched or not eligible.
    """
    with Session(engine) as db:
        company = db.get(Company, company_id)
        if company is None:
            return "missing"
        if not _needs_enrichment(company):
            return "skipped"

        # Check source via company_sources.
        has_tag_source = bool(db.exec(
            select(CompanySource).where(
                CompanySource.company_id == company_id,
                CompanySource.source.in_(["osm", "overture"]),
            )
        ).first())

        industry = (company.industry_nace or "").strip()
        if not has_tag_source or not industry:
            return "skipped"

        company.description = _build_tag_description(
            company.name or "", industry, company.country or ""
        )
        company.keywords = industry
        company.industry_nace = industry
        company.b2b_score = 0.0
        company.saas_score = 0.0
        company.startup_score = 0.0
        company.tech_score = 0.0
        company.enriched_at = now()
        db.add(company)
        db.commit()
        return "enriched"

def enrich_all_from_tags(limit: int | None = None, batch: int = 5000) -> dict[str, int]:
    """Bulk no-LLM enrichment for every OSM/Overture company, in ONE session.

    The per-company helper opens a new Session, runs a source lookup and commits
    for EACH row — at millions of rows that is hours of fsyncs. Here we:
      1. load the set of tag-sourced company ids ONCE,
      2. stream the companies needing enrichment,
      3. commit every `batch` rows.
    """
    init_db()
    enriched = 0
    skipped = 0
    with Session(engine) as db:
        tag_ids = {
            row for row in db.exec(
                select(CompanySource.company_id).where(
                    CompanySource.source.in_(["osm", "overture"])
                )
            )
        }
        logger.info("Tag enrichment: %d companies come from listing sources", len(tag_ids))

        q = select(Company).where(
            (Company.enriched_at.is_(None)) | (Company.enriched_at == "")
        ).where(Company.industry_nace != "")
        processed = 0
        for company in db.exec(q):
            if limit is not None and processed >= limit:
                break
            processed += 1
            if company.id not in tag_ids:
                skipped += 1
                continue
            industry = (company.industry_nace or "").strip()
            if not industry:
                skipped += 1
                continue
            company.description = _build_tag_description(
                company.name or "", industry, company.country or ""
            )
            company.keywords = industry
            company.b2b_score = 0.0
            company.saas_score = 0.0
            company.startup_score = 0.0
            company.tech_score = 0.0
            company.enriched_at = now()
            db.add(company)
            enriched += 1
            if enriched % batch == 0:
                db.commit()
                logger.info("Tag enrichment: %d enriched", enriched)
        db.commit()
    logger.info("Tag enrichment done: %d enriched, %d skipped", enriched, skipped)
    return {"enriched": enriched, "skipped": skipped}


logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

DEMO_ENRICHMENT: dict[str, Any] = {
    "description": "",
    "keywords": "",
    "industry_nace": "",
    "size_estimate": None,
    "size_range": "",
    "b2b_score": 0.0,
    "saas_score": 0.0,
    "startup_score": 0.0,
    "tech_score": 0.0,
}


# Deterministic markers that identify parked/placeholder/thin pages. These are
# checked case-insensitively before the enrichment model is called, so we never
# pay for an LLM to hallucinate a business profile for a domain-for-sale page.
_PARKED_MARKERS = (
    "domain is for sale",
    "this domain is for sale",
    "buy this domain",
    "domain name has been registered",
    "domain name for sale",
    "buy this domain name",
    "this domain name is for sale",
    "for sale by owner",
    "domain for sale",
    "domain available",
    "domain parked",
    "parked domain",
    "this domain is parked",
    "domain parking",
    "parking page",
    "coming soon",
    "under construction",
    "website is coming",
    "future home of",
    "account suspended",
    "default web page",
    "apache2 default",
    "apache2 ubuntu default page",
    "welcome to nginx",
    "nginx welcome",
    "index of /",
    "hugedomains",
    "sedo",
    "godaddy",
    "namecheap parking",
    "shortcut links",
    "short links",
    "url shortener",
    "shorten",
    "404 error",
    "page not found",
)


def looks_parked_or_thin(web_text: str, name: str = "") -> bool:
    """Return True if the page has no real business signal to enrich.

    Rules are deliberately conservative and content-based, not score-based:
    - cleaned text is shorter than ENRICH_MIN_CHARS meaningful characters, or
    - text contains a known parked/for-sale/placeholder marker.
    """
    text = (web_text or "").strip()
    if not text:
        return True

    # Collapse whitespace to estimate meaningful text length.
    cleaned = re.sub(r"\s+", " ", text).strip()
    if len(cleaned) < settings.enrich_min_chars:
        return True

    lower = cleaned.lower()
    for marker in _PARKED_MARKERS:
        if marker in lower:
            return True

    # If the page is just a placeholder "Loading" (no other meaningful content),
    # it is not a real business homepage.
    if len(cleaned) <= 50 and ("loading" in lower or "load" in lower):
        return True

    return False


def build_enrichment_prompt(name: str, country: str, web_text: str) -> str:
    """Return a strict prompt for extracting enrichment fields from web_text."""
    snippet = (web_text or "").replace('"', "'").strip()
    _max = settings.enrich_text_chars
    if len(snippet) > _max:
        snippet = snippet[:_max] + "\n...[truncated]"

    return (
        f"Company name: {name}\n"
        f"Country: {country}\n"
        f"Homepage / website text:\n{snippet}\n\n"
        "Based ONLY on the text above, return a single STRICT JSON object with these keys:\n"
        "- description: 1-2 sentences on what the company does (\"\" if unknown)\n"
        "- keywords: 5-10 short comma-separated phrases (\"\" if unknown)\n"
        "- industry_nace: best-guess NACE-style label, e.g. \"J - Information and communication\" (\"\" if unknown)\n"
        "- size_estimate: integer employee count or null\n"
        "- size_range: one of \"1-10\", \"11-50\", \"51-200\", \"201-1000\", \"1000+\", or \"\"\n"
        "- b2b_score: 0.0-1.0 (how clearly B2B)\n"
        "- saas_score: 0.0-1.0 (how clearly SaaS / subscription software)\n"
        "- startup_score: 0.0-1.0 (startup signals)\n"
        "- tech_score: 0.0-1.0 (technology intensity)\n\n"
        "If the text is thin, parked, or unrelated, return empty/low-confidence values instead of inventing facts. "
        "If this is a domain-for-sale, parked, placeholder, or 'coming soon' page, "
        "return description and keywords as empty strings. "
        "Return ONLY the JSON object, no commentary."
    )


def _clamp_score(value: Any) -> float | None:
    """Coerce a score to a 0..1 float or None."""
    if value is None or value == "":
        return None
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if score != score:  # NaN
        return None
    return max(0.0, min(1.0, score))


def _coerce_int(value: Any) -> int | None:
    """Coerce an employee-count estimate to int or None."""
    if value is None or value == "":
        return None
    try:
        n = int(float(value))
    except (TypeError, ValueError):
        return None
    return n if n >= 0 else None


def _coerce_size_range(value: Any) -> str:
    """Normalize a size-range string to one of the allowed buckets."""
    if not value:
        return ""
    allowed = {"1-10", "11-50", "51-200", "201-1000", "1000+"}
    cleaned = re.sub(r"\s+", "", str(value)).strip()
    if cleaned in allowed:
        return cleaned
    # Accept some common variants.
    low = cleaned.lower()
    if "1-10" in low or "1to10" in low or "1 – 10" in str(value):
        return "1-10"
    if "11-50" in low or "11to50" in low:
        return "11-50"
    if "51-200" in low or "51to200" in low:
        return "51-200"
    if "201-1000" in low or "201to1000" in low or "201-500" in low:
        return "201-1000"
    if "1000" in low or "1000+" in low or "enterprise" in low:
        return "1000+"
    return ""


def _truncate(value: Any, max_chars: int) -> str:
    text = str(value) if value is not None else ""
    return text[:max_chars]


def coerce_enrichment(data: dict[str, Any] | None) -> dict[str, Any]:
    """Clamp, coerce and sanitize Brain enrichment output.

    Never raises. Returns a dict safe to write to the Company columns.
    """
    if not isinstance(data, dict):
        data = {}

    description = _truncate(data.get("description", ""), 1000)
    keywords = _truncate(data.get("keywords", ""), 500)
    industry_nace = _truncate(data.get("industry_nace", ""), 120)
    size_estimate = _coerce_int(data.get("size_estimate"))
    size_range = _coerce_size_range(data.get("size_range"))

    return {
        "description": description,
        "keywords": keywords,
        "industry_nace": industry_nace,
        "size_estimate": size_estimate,
        "size_range": size_range,
        "b2b_score": _clamp_score(data.get("b2b_score")),
        "saas_score": _clamp_score(data.get("saas_score")),
        "startup_score": _clamp_score(data.get("startup_score")),
        "tech_score": _clamp_score(data.get("tech_score")),
    }


def _needs_enrichment(company: Company) -> bool:
    return bool(company.web_text) and not company.enriched_at


_PRE_FILTER_MIN_WORDS = 20


def _is_pre_filter_parked(web_text: str, name: str = "") -> bool:
    """Return True if this page is junk — no LLM call needed."""
    if looks_parked_or_thin(web_text, name):
        return True
    text = (web_text or "").strip()
    if len(text) < settings.enrich_min_chars:
        return True
    words = re.findall(r"[a-zA-Z]{3,}", text.lower())
    if len(words) < _PRE_FILTER_MIN_WORDS:
        return True
    return False


def _mark_parked(company: Company) -> None:
    company.description = ""
    company.keywords = ""
    company.industry_nace = ""
    company.size_estimate = None
    company.size_range = ""
    company.b2b_score = None
    company.saas_score = None
    company.startup_score = None
    company.tech_score = None
    company.enriched_at = now()


def enrich_company(company_id: int) -> str:
    """Enrich a single company and return its new status.

    Status is one of: "enriched", "skipped", "failed", "parked".
    """
    init_db()
    with Session(engine) as db:
        company = db.get(Company, company_id)
        if company is None:
            return "missing"
        if not _needs_enrichment(company):
            return "skipped"

        if _is_pre_filter_parked(company.web_text or "", company.name or ""):
            logger.debug("Company %s (%s) pre-filtered: parked/thin", company.id, company.domain or company.name)
            _mark_parked(company)
            db.add(company)
            db.commit()
            return "parked"

        prompt = build_enrichment_prompt(company.name, company.country, company.web_text or "")
        try:
            result = cheap_json(
                prompt,
                system="You are a careful company analyst. Return only strict JSON.",
                max_tokens=1500,
                demo_value=DEMO_ENRICHMENT,
                model=settings.model_cheap,
            )
        except Exception:  # noqa: BLE001
            return "failed"
        enriched = coerce_enrichment(result)

        # Only mark enriched if we got at least one useful signal.
        if not any(
            [
                enriched["description"],
                enriched["keywords"],
                enriched["industry_nace"],
                enriched["size_estimate"] is not None,
                enriched["size_range"],
                enriched["b2b_score"] is not None,
                enriched["saas_score"] is not None,
                enriched["startup_score"] is not None,
                enriched["tech_score"] is not None,
            ]
        ):
            return "failed"

        for key, value in enriched.items():
            setattr(company, key, value)
        company.enriched_at = now()
        db.add(company)
        db.commit()
        return "enriched"


# ---------------------------------------------------------------------------
# Batched enrichment
# ---------------------------------------------------------------------------

_BATCH_PROMPT_PREAMBLE = (
    "Enrich each of the following companies. For each, return a strict JSON object "
    "with these keys:\n"
    "- description: 1-2 sentences on what the company does (empty if unknown)\n"
    "- keywords: 5-10 short comma-separated phrases (empty if unknown)\n"
    "- industry_nace: best-guess NACE label (empty if unknown)\n"
    "- size_estimate: integer or null\n"
    "- size_range: one of \"1-10\",\"11-50\",\"51-200\",\"201-1000\",\"1000+\",empty\n"
    "- b2b_score, saas_score, startup_score, tech_score: each 0.0-1.0\n\n"
    "Return ALL results as a JSON ARRAY, ONE object per company, each with an extra "
    "\"id\" field matching the id given. If text is thin/flat, return empty values. "
    "Never invent facts not supported by the text. Do NOT include commentary.\n\n"
    "Companies to enrich:\n"
)

_BATCH_SYSTEM = "You enrich many companies at once. Return only a JSON array. Be conservative."


def _build_batch_prompt(companies: list[dict]) -> str:
    lines = [_BATCH_PROMPT_PREAMBLE]
    for i, c in enumerate(companies, 1):
        snippet = (c["web_text"] or "").replace('"', "'").strip()
        if len(snippet) > settings.enrich_text_chars:
            snippet = snippet[:settings.enrich_text_chars] + "\n...[truncated]"
        lines.append(
            f"### COMPANY {i}\n"
            f"id: {c['id']}\n"
            f"name: {c['name']}\n"
            f"country: {c.get('country', '')}\n"
            f"text: {snippet}\n"
        )
    return "\n".join(lines)


def _parse_batch_result(raw: Any) -> dict[int, dict]:
    """Parse batch response: accept array, object wrapping array, or JSON in prose."""
    items: list[dict] = []
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        for v in raw.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                items = v
                break
        if not items and "id" in raw:
            items = [raw]
    elif isinstance(raw, str):
        cleaned = raw.strip()
        import re as _re
        match = _re.search(r"\[.*]", cleaned, _re.DOTALL)
        if match:
            try:
                items = json.loads(match.group())
            except json.JSONDecodeError:
                pass
    result: dict[int, dict] = {}
    for item in items:
        if isinstance(item, dict):
            cid = item.get("id")
            if cid is not None:
                result[int(cid)] = item
    return result


def _write_enrichment(company_id: int, enriched: dict) -> str:
    """Write enriched fields and return status. Same logic as the tail of enrich_company."""
    with Session(engine) as db:
        company = db.get(Company, company_id)
        if company is None:
            return "missing"
        if not _needs_enrichment(company):
            return "skipped"
        coerced = coerce_enrichment(enriched)
        if not any([
            coerced["description"], coerced["keywords"], coerced["industry_nace"],
            coerced["size_estimate"] is not None, coerced["size_range"],
            coerced["b2b_score"] is not None, coerced["saas_score"] is not None,
            coerced["startup_score"] is not None, coerced["tech_score"] is not None,
        ]):
            return "failed"
        for key, value in coerced.items():
            setattr(company, key, value)
        company.enriched_at = now()
        db.add(company)
        db.commit()
        return "enriched"


# ---------------------------------------------------------------------------
# CHANGE LOG - batch sizing (added Aug 2026)
#
# BEFORE: enrich_batch() sent whatever it was given in ONE call. Batch size came
#         from ENRICH_BATCH_SIZE (default 10) and nothing checked how big the
#         resulting prompt was, because the paid model's context was far larger
#         than any batch could reach.
#
# WHY IT  Switching to a local model moved every LLM call - not just email
# CHANGED drafting - onto gemma. At num_ctx 8192 a full batch measured ~8,970
#         tokens: 10 companies x 2,000 chars of site text, plus 3,000 reserved
#         for the JSON response. That does not raise an error. The model simply
#         never sees the end of the prompt, so the last companies in each batch
#         come back missing or invented, silently.
#
# NOW:    _prompt_char_budget() + _split_to_context() below size the batch
#         against the model's actual context window and split it if needed.
#
# CURRENT At LOCAL_LLM_NUM_CTX=65536 the budget is ~220,000 chars, a batch of 10
# EFFECT: needs ~21,500, so the split NEVER triggers and behaviour is identical
#         to BEFORE. This code is dormant at 64k. It only does anything if
#         num_ctx is lowered or ENRICH_BATCH_SIZE is raised a long way.
# ---------------------------------------------------------------------------


def _prompt_char_budget() -> int:
    """How many characters of prompt one enrichment call may carry.

    Cloud models have room to spare, so this returns a number no realistic
    batch will reach. A local 8k model does not: with the defaults - 10
    companies x 2000 chars of site text, plus 3000 tokens reserved for the JSON
    response - a full batch works out at roughly 8,970 tokens against an 8,192
    window. It does not raise. The model simply never sees the last companies,
    and they come back missing or fabricated.

    3.6 chars/token is deliberately conservative for English prose (the usual
    estimate is ~4), so the budget errs on the side of splitting.
    """
    try:
        from app.brain import get_llm_mode

        if get_llm_mode() != "local":
            return 1_000_000
    except Exception:  # noqa: BLE001 - never let sizing break enrichment
        return 1_000_000
    ctx = max(2048, int(settings.local_llm_num_ctx or 8192))
    reserve = 4000 + 400          # max response tokens + system prompt
    return int(max(1000, ctx - reserve) * 3.6)


def _split_to_context(candidates: list[dict]) -> list[list[dict]]:
    """Pack companies into sub-batches that each fit the prompt budget.

    A single company larger than the whole budget still goes out alone rather
    than being dropped - one oversized prompt is a visible failure, a silently
    skipped company is not.
    """
    budget = _prompt_char_budget()
    per_overhead = 90            # the "### COMPANY n / id / name / country" block
    chunks: list[list[dict]] = []
    cur: list[dict] = []
    size = len(_BATCH_PROMPT_PREAMBLE)
    for c in candidates:
        cost = min(len(c.get("web_text") or ""), settings.enrich_text_chars) + per_overhead
        if cur and size + cost > budget:
            chunks.append(cur)
            cur, size = [], len(_BATCH_PROMPT_PREAMBLE)
        cur.append(c)
        size += cost
    if cur:
        chunks.append(cur)
    # Never return [[]] for empty input - a chunk that exists but holds nothing
    # would build a prompt with no companies and max_tokens of 0.
    return chunks or ([candidates] if candidates else [])


def enrich_batch(company_ids: list[int]) -> dict[int, str]:
    """Enrich a batch of companies in ONE LLM call. Returns {id: status}.

    Companies pre-filtered as parked are handled locally (no LLM).
    Companies missing from the batch response are retried individually.
    """
    init_db()
    results: dict[int, str] = {}

    # Pre-filter: parked pages never go to the LLM.
    llm_candidates: list[dict] = []
    for cid in company_ids:
        with Session(engine) as db:
            company = db.get(Company, cid)
        if company is None:
            results[cid] = "missing"
            continue
        if not _needs_enrichment(company):
            results[cid] = "skipped"
            continue
        if _is_pre_filter_parked(company.web_text or "", company.name or ""):
            with Session(engine) as db:
                company = db.get(Company, cid)
                if company:
                    _mark_parked(company)
                    db.add(company)
                    db.commit()
            logger.debug("Batch: company %s pre-filtered as parked", cid)
            results[cid] = "parked"
            continue
        llm_candidates.append({
            "id": cid, "name": company.name, "country": company.country,
            "web_text": company.web_text or "",
        })

    if not llm_candidates:
        return results

    # A batch sized for a hosted model does not fit a local one. Split first,
    # and recurse - otherwise the tail of the prompt falls outside the context
    # window and those companies come back missing or invented, with no error
    # anywhere to tell you it happened.
    chunks = _split_to_context(llm_candidates)
    if len(chunks) > 1:
        logger.info(
            "Batch of %d split into %d to fit the model context window",
            len(llm_candidates), len(chunks),
        )
        for chunk in chunks:
            results.update(enrich_batch([c["id"] for c in chunk]))
        return results

    # Call the Brain ONCE for the whole batch.
    prompt = _build_batch_prompt(llm_candidates)
    max_tok = min(4000, 300 * len(llm_candidates))
    try:
        raw = cheap_json(
            prompt,
            system=_BATCH_SYSTEM,
            max_tokens=max_tok,
            model=settings.model_cheap,
        )
    except Exception:
        # Entire batch failed — mark all as failed, let caller retry.
        for cid in company_ids:
            if cid not in results:
                results[cid] = "failed"
        return results

    parsed = _parse_batch_result(raw)
    if not parsed:
        for cid in company_ids:
            if cid not in results:
                results[cid] = "failed"
        return results

    # Write results for companies found in the batch response.
    for cid, enriched in parsed.items():
        if cid in results:
            continue
        status = _write_enrichment(cid, enriched)
        results[cid] = status

    # Retry missing companies individually.
    for cid in company_ids:
        if cid not in results:
            logger.info("Batch: company %s missing from response — retrying individually", cid)
            status = enrich_company(cid)
            results[cid] = status

    return results


def run(
    limit: int | None = None,
    from_name: bool = False,
    workers: int | None = None,
    per_endpoint: int | None = None,
    batch_size: int | None = None,
) -> dict[str, int]:
    """Enrich eligible companies up to ``limit``, concurrently.

    Args:
        workers: Flat pool mode (overrides sharded).
        per_endpoint: Sharded workers per endpoint.
        batch_size: Companies per LLM call (default from config; 1 = per-company).
    """
    init_db()
    if batch_size is None:
        batch_size = settings.enrich_batch_size

    with Session(engine) as db:
        query = select(Company).where(Company.web_text != "").where(
            (Company.enriched_at.is_(None)) | (Company.enriched_at == "")
        )
        companies: list[int] = [c.id for c in db.exec(query)]
        if from_name:
            query2 = select(Company).where(Company.web_text == "").where(
                (Company.enriched_at.is_(None)) | (Company.enriched_at == "")
            ).where(Company.name != "")
            companies += [c.id for c in db.exec(query2)]

    if limit:
        companies = companies[:limit]

    if not companies:
        return {"enriched": 0, "skipped": 0, "failed": 0, "missing": 0, "parked": 0}

    counts: dict[str, int] = {"enriched": 0, "skipped": 0, "failed": 0, "missing": 0, "parked": 0}

    if batch_size <= 1:
        # Per-company path (original, backward-compatible).
        if workers is not None:
            def _process(cid):
                return {"status": enrich_company(cid)}
            results = run_concurrent(companies, _process, workers=workers)
        else:
            from app.brain import _providers
            pool = _providers()
            endpoints = [u for p in pool for u in p._urls] or [settings.go_base_url]
            results = _run_sharded(companies, endpoints, per_endpoint or settings.enrich_per_endpoint)
    else:
        # Batch mode: chunk companies, process batches concurrently.
        batches = [companies[i:i + batch_size] for i in range(0, len(companies), batch_size)]
        w = workers if workers is not None else max(1, per_endpoint or settings.enrich_per_endpoint)

        def _process_batch(batch):
            return enrich_batch(batch)

        results = run_concurrent(batches, _process_batch, workers=w)
        # Flatten dict-of-dicts into per-company results.
        flat_results: list[dict] = []
        for batch_res in results:
            if isinstance(batch_res, dict):
                for cid, status in batch_res.items():
                    flat_results.append({"status": status})
        results = flat_results

    for r in results:
        if isinstance(r, dict):
            status = r.get("status", "failed")
            counts[status] = counts.get(status, 0) + 1

    return counts


def build_name_only_prompt(name: str, country: str) -> str:
    """Cheaper fallback prompt for companies with no web_text."""
    return (
        f"Company name: {name}\n"
        f"Country: {country}\n\n"
        "Infer only from the company name and country. Return strict JSON:\n"
        "- industry_nace: best-guess NACE-style label (\"\" if unknown)\n"
        "- b2b_score, saas_score, startup_score, tech_score: each 0.0-1.0\n"
        "Be conservative; return low scores if the name is ambiguous."
    )


def enrich_from_name(company_id: int) -> str:
    """Light enrichment for companies that have a name but no web_text."""
    init_db()
    with Session(engine) as db:
        company = db.get(Company, company_id)
        if company is None:
            return "missing"
        if company.enriched_at or not company.name:
            return "skipped"

        prompt = build_name_only_prompt(company.name, company.country)
        try:
            result = cheap_json(
                prompt,
                system="You infer company attributes from a name. Return only strict JSON.",
                max_tokens=500,
                demo_value={
                    "industry_nace": "",
                    "b2b_score": 0.0,
                    "saas_score": 0.0,
                    "startup_score": 0.0,
                    "tech_score": 0.0,
                },
                model=settings.model_cheap,
            )
        except Exception:  # noqa: BLE001
            return "failed"
        if not isinstance(result, dict):
            return "failed"

        company.industry_nace = _truncate(result.get("industry_nace", ""), 120)
        company.b2b_score = _clamp_score(result.get("b2b_score"))
        company.saas_score = _clamp_score(result.get("saas_score"))
        company.startup_score = _clamp_score(result.get("startup_score"))
        company.tech_score = _clamp_score(result.get("tech_score"))
        company.enriched_at = now()
        db.add(company)
        db.commit()
        return "enriched"


def _run_sharded(companies: list[int], endpoints: list[str], per_endpoint: int) -> list[dict]:
    """Pin workers to endpoints. Each endpoint gets ``per_endpoint`` workers."""
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    n_endpoints = len(endpoints)
    total_workers = n_endpoints * per_endpoint
    results: list[dict] = [{"status": "skipped"}] * len(companies)
    lock = threading.Lock()
    idx = [0]
    done = [0]
    total = len(companies)
    logger.info("enrich sharded: %d companies, %d endpoints x %d = %d workers",
                total, n_endpoints, per_endpoint, total_workers)

    def _pinned_worker(ep: str, worker_id: int) -> None:
        while True:
            with lock:
                i = idx[0]
                if i >= len(companies):
                    return
                idx[0] += 1
                cid = companies[i]
            logger.info("enrich: start company %s (worker %d)", cid, worker_id)
            status = enrich_company(cid)
            results[i] = {"status": status}
            with lock:
                done[0] += 1
                d = done[0]
            logger.info("enrich: %d/%d company=%s -> %s", d, total, cid, status)

    with ThreadPoolExecutor(max_workers=total_workers) as pool:
        futures = []
        for ei, ep in enumerate(endpoints):
            for wi in range(per_endpoint):
                futures.append(pool.submit(_pinned_worker, ep, ei * per_endpoint + wi))
        for f in as_completed(futures):
            f.result()

    return results


def recheck() -> dict[str, int]:
    """Re-evaluate already-enriched companies and clear descriptions for parked/thin pages.

    Returns {"checked": n, "cleaned": n}.
    """
    init_db()
    checked = 0
    cleaned = 0
    with Session(engine) as db:
        query = (
            select(Company)
            .where(Company.enriched_at.is_not(None))
            .where(Company.description != "")
        )
        for company in db.exec(query):
            checked += 1
            if looks_parked_or_thin(company.web_text or "", company.name or ""):
                logger.info(
                    "Recheck: clearing description for company %s (%s) - parked/thin",
                    company.id,
                    company.domain or company.name,
                )
                company.description = ""
                company.keywords = ""
                db.add(company)
                cleaned += 1
        db.commit()
    return {"checked": checked, "cleaned": cleaned}


def report() -> dict[str, Any]:
    """Return enrichment coverage numbers."""
    init_db()
    with Session(engine) as db:
        total = db.exec(select(func.count()).select_from(Company)).one()
        with_web_text = db.exec(select(func.count()).select_from(Company).where(Company.web_text != "")).one()
        enriched = db.exec(select(func.count()).select_from(Company).where(Company.enriched_at.is_not(None))).one()

        score_sum: dict[str, float] = {}
        score_count: dict[str, int] = {}
        for company in db.exec(select(Company).where(Company.enriched_at.is_not(None))):
            for score_name in ("b2b_score", "saas_score", "startup_score", "tech_score"):
                value = getattr(company, score_name)
                if value is not None:
                    score_sum[score_name] = score_sum.get(score_name, 0.0) + value
                    score_count[score_name] = score_count.get(score_name, 0) + 1

    avg_scores = {
        name: round(score_sum.get(name, 0.0) / score_count[name], 3) if score_count.get(name) else 0.0
        for name in ("b2b_score", "saas_score", "startup_score", "tech_score")
    }

    return {
        "total": total,
        "with_web_text": with_web_text,
        "enriched": enriched,
        "eligible": with_web_text,
        "avg_scores": avg_scores,
    }


def _print_report(report_data: dict[str, Any]) -> None:
    print(f"Total companies:   {report_data['total']}")
    print(f"With web_text:     {report_data['with_web_text']}")
    print(f"Enriched:          {report_data['enriched']}")
    print("Avg scores:")
    for name, value in report_data["avg_scores"].items():
        print(f"  {name:<16} {value:.3f}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Raliess Phase 4 AI enrichment")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Enrich companies from web_text")
    run_parser.add_argument("--limit", type=int, default=None, help="Maximum companies to enrich")
    run_parser.add_argument("--workers", type=int, default=None, help="Flat pool workers (overrides sharded)")
    run_parser.add_argument("--per-endpoint", type=int, default=None, help="Workers per endpoint (sharded model)")
    run_parser.add_argument("--batch-size", type=int, default=None, help="Companies per LLM call (1=per-company)")
    run_parser.add_argument(
        "--from-tags",
        action="store_true",
        help="No-LLM: enrich OSM/Overture companies from their listing category (fast, free)",
    )
    run_parser.add_argument(
        "--from-name",
        action="store_true",
        help="Also run a light name-only fallback for companies without web_text",
    )

    subparsers.add_parser("recheck", help="Clear descriptions for already-enriched parked/thin pages")

    subparsers.add_parser("report", help="Print enrichment coverage report")

    args = parser.parse_args(argv)

    if args.command == "run":
        if args.from_tags:
            counts_t = enrich_all_from_tags(limit=args.limit)
            print(
                f"Tag-enriched: {counts_t['enriched']}  "
                f"Skipped: {counts_t['skipped']}   (0 LLM calls)"
            )
            _print_report(report())
            return 0

        counts = run(limit=args.limit, from_name=args.from_name, workers=args.workers,
                     per_endpoint=args.per_endpoint, batch_size=args.batch_size)
        print(
            f"Enriched: {counts['enriched']}  "
            f"Parked: {counts.get('parked', 0)}  "
            f"Skipped: {counts.get('skipped', 0)}  "
            f"Failed: {counts.get('failed', 0)}"
        )
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE4 OK: {report_data['enriched']}/{report_data['eligible']} enriched"
        )
        # Fail the gate if there are eligible companies but none were enriched.
        return 0 if not (report_data["eligible"] > 0 and report_data["enriched"] == 0) else 1

    if args.command == "recheck":
        summary = recheck()
        print(f"Rechecked: {summary['checked']}  Cleaned: {summary['cleaned']}")
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE4 OK: {report_data['enriched']}/{report_data['eligible']} enriched"
        )
        return 0

    if args.command == "report":
        report_data = report()
        _print_report(report_data)
        print(
            f"PHASE4 OK: {report_data['enriched']}/{report_data['eligible']} enriched"
        )
        return 0 if not (report_data["eligible"] > 0 and report_data["enriched"] == 0) else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
