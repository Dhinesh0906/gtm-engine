"""Map a free-form search term onto the verticals that actually exist.

The problem
-----------
Everything keys off `companies.industry_nace`, whose values come from the
source data - `dentist`, `physical_therapy`, `beauty_salon`. Typing "brain" or
"clinics" or "wellness" matches nothing, because those strings are not in the
data. Substring search only helps when the word happens to appear.

The fix
-------
Ask the model which of the EXISTING verticals a term refers to. "brain" is not
a vertical, but a model knows it points at neurology, physiotherapy and medical
clinics - and those may well exist in the data under other names.

Resolved mappings are written to vertical_aliases.json so the same term is
never resolved twice, and so you can edit or correct one by hand.

No new segments are created. This only decides which existing verticals a term
covers.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from sqlalchemy import text
from sqlmodel import Session

from app.config import settings
from app.db import engine, init_db

logger = logging.getLogger(__name__)

ALIAS_FILE = Path(settings.vertical_alias_file).expanduser()


def _load() -> dict:
    if not ALIAS_FILE.exists():
        return {}
    try:
        return json.loads(ALIAS_FILE.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def _save(data: dict) -> None:
    ALIAS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = ALIAS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(ALIAS_FILE)


SQL_VERTICALS = (
    "SELECT industry_nace, COUNT(*) c FROM companies "
    "WHERE industry_nace != '' GROUP BY industry_nace "
    "HAVING c >= :m ORDER BY c DESC"
)


def available_verticals(min_companies: int = 200) -> list[tuple[str, int]]:
    """Verticals in the data that are big enough to be worth targeting.

    The floor drops automatically on a small database. A hard 200 returned
    nothing at all on a 500-company dataset, which would have told the model
    there was nothing to choose from and made every term unmappable.
    """
    init_db()
    with Session(engine) as db:
        rows = db.execute(text(SQL_VERTICALS), {"m": min_companies}).all()
        if len(rows) < 10:
            rows = db.execute(text(SQL_VERTICALS), {"m": 1}).all()
    return [(str(r[0]), int(r[1])) for r in rows]


def cached(term: str) -> list[str] | None:
    return _load().get(term.strip().lower())


def forget(term: str) -> None:
    data = _load()
    data.pop(term.strip().lower(), None)
    _save(data)


def resolve(term: str, refresh: bool = False, verbose: bool = True) -> list[str]:
    """Return the existing verticals this term refers to.

    Order of attempts, cheapest first:
      1. the saved mapping
      2. exact match
      3. substring match
      4. the model, given the full list of verticals to choose from
    """
    t = (term or "").strip().lower()
    if not t:
        return []

    if not refresh:
        hit = cached(t)
        if hit is not None:
            if verbose:
                print(f"'{term}' -> {len(hit)} verticals (from {ALIAS_FILE.name})")
            return hit

    verticals = available_verticals()
    names = [v for v, _ in verticals]
    by_name = dict(verticals)

    # 2 + 3: free, so try them before spending a model call.
    if t in (n.lower() for n in names):
        found = [n for n in names if n.lower() == t]
    else:
        found = [n for n in names if t in n.lower()]

    if found:
        _remember(t, found)
        if verbose:
            _print(term, found, by_name, "matched directly")
        return found

    # 4: ask the model. It sees ONLY the verticals that exist, so it cannot
    # invent one that would match nothing.
    listing = "\n".join(f"{n} ({by_name[n]:,})" for n in names[:400])
    try:
        result = _ask(term, listing)
    except Exception as exc:  # noqa: BLE001
        # A dead model must not look like "this term does not exist" - that
        # would be a silent wrong answer, and you would go and invent a
        # segment you did not need.
        logger.warning("vertical mapping failed for %r: %s", term, exc)
        if verbose:
            print(f"Could not map {term!r}: the model is unreachable.")
            print(f"  {str(exc)[:160]}")
            print("  Start it with:  ./start_llms.sh")
            print(f"  Or map it by hand - add to {ALIAS_FILE}:")
            print(f'      {{"{t}": ["exact_vertical_name", "another"]}}')
            print("  Names to choose from:  python crawl_sites.py --list")
        return []

    picked: list[str] = []
    why = ""
    if isinstance(result, dict):
        why = str(result.get("why", ""))[:160]
        valid = {n.lower(): n for n in names}
        for v in result.get("verticals", []) or []:
            key = str(v).strip().lower()
            if key in valid and valid[key] not in picked:
                picked.append(valid[key])

    if picked:
        _remember(t, picked)
        if verbose:
            _print(term, picked, by_name, why or "mapped by the model")
    elif verbose:
        print(f"Nothing in your data corresponds to '{term}'.")
    return picked


def _ask(term: str, listing: str):
    """Ask the model which of the listed verticals the term covers."""
    from app.brain import smart_json

    return smart_json(
        f"A user is looking for businesses described as: \"{term}\"\n\n"
        f"Below is the COMPLETE list of business categories available, with "
        f"how many companies each holds. Choose every category that a "
        f"reasonable person searching for \"{term}\" would want included.\n\n"
        f"{listing}\n\n"
        f"Rules:\n"
        f"- Return ONLY names copied exactly from the list above.\n"
        f"- Include a category if it is a plausible fit, not only a perfect one.\n"
        f"- Prefer several related categories over a single narrow one.\n"
        f"- If nothing in the list fits, return an empty array.\n\n"
        f'Return JSON: {{"verticals": ["name", "name"], "why": "one sentence"}}',
        system=("You map a search term onto a fixed list of business "
                "categories. You never invent a category that is not in the "
                "list. Return only JSON."),
        max_tokens=600,
        model=settings.model_smart,
    )


def inject(term: str, campaign_id: int, limit: int = 50,
           refresh: bool = False, verbose: bool = True) -> dict:
    """Resolve a term, queue its leads, and put them AT THE FRONT.

    The existing queue is not touched. Each new assignment gets a
    `queue_priority` row, and next_unwritten_assignment() reads priority rows
    before the FIFO order - so the next cold email written comes from this set,
    and everything already waiting simply resumes behind it.

    Rank descends with each injection, so a term mapped later still lands
    ahead of one mapped earlier: the most recent instruction wins.
    """
    from sqlalchemy import text as _sql

    from app.agent import _POOL_SQL, _country_clause, _in_clause
    from app.db import Assignment, QueuePriority

    verticals = resolve(term, refresh=refresh, verbose=verbose)
    if not verticals:
        return {"term": term, "verticals": [], "created": 0,
                "error": f"nothing in your data corresponds to '{term}'"}

    init_db()
    blocked = settings.blocked_domain_list
    blocked_clause = ""
    if blocked:
        blocked_clause = "AND (" + " AND ".join(
            "LOWER(cp.value) NOT LIKE '%@" + d.replace("'", "") + "'"
            for d in blocked
        ) + ")"

    created = 0
    with Session(engine) as db:
        rows = db.execute(
            _sql(_POOL_SQL.format(
                industry_clause=_in_clause(verticals, "c.industry_nace"),
                country_clause=_country_clause(),
                blocked_clause="{blocked_clause}",
            ).format(blocked_clause=blocked_clause)),
            {"cid": campaign_id, "per": limit * 4},
        ).all()

        # Columns are (person_id, cp_id, src_rank, addr, country).
        # Interleave by country so a targeted push is spread across US/CA/GB
        # rather than being whichever country happens to sort first.
        by_country: dict[str, list[tuple[int, int, str]]] = {}
        for r in rows:
            country = (str(r[4]) or "??").strip().upper() or "??"
            by_country.setdefault(country, []).append(
                (int(r[0]), int(r[1]), str(r[3]).lower()))

        seen: set[str] = set()
        picked: list[tuple[int, int]] = []
        countries = sorted(by_country)
        idx = 0
        while len(picked) < limit and any(idx < len(by_country[c]) for c in countries):
            for c in countries:
                if idx < len(by_country[c]):
                    person_id, cp_id, addr = by_country[c][idx]
                    if addr not in seen:
                        seen.add(addr)
                        picked.append((person_id, cp_id))
                        if len(picked) >= limit:
                            break
            idx += 1

        # Sit above every existing priority row, so a term asked for now beats
        # one asked for an hour ago.
        top = db.execute(_sql("SELECT COALESCE(MAX(rank), 100) FROM queue_priority")).scalar()
        rank = int(top or 100) + 1

        for person_id, cp_id in picked:
            a = Assignment(campaign_id=campaign_id, person_id=person_id,
                           contact_point_id=cp_id, status="new")
            db.add(a)
            db.flush()                       # need a.id for the priority row
            db.add(QueuePriority(assignment_id=int(a.id), rank=rank,
                                 reason=term.strip().lower()))
            created += 1
        db.commit()

        waiting = db.execute(_sql(
            "SELECT COUNT(*) FROM assignments a "
            "WHERE a.campaign_id = :cid "
            "  AND a.status NOT IN ('sent','suppressed','hold') "
            "  AND NOT EXISTS (SELECT 1 FROM messages m "
            "                  WHERE m.assignment_id = a.id AND m.step = 1) "
            "  AND NOT EXISTS (SELECT 1 FROM queue_priority q "
            "                  WHERE q.assignment_id = a.id)"
        ), {"cid": campaign_id}).scalar() or 0

    if verbose:
        if created:
            print(f"\n{created} leads queued AT THE FRONT (rank {rank}).")
            print(f"{waiting:,} previously queued leads pushed behind them.")
            print("The next cold email the batch writes comes from this set.")
        else:
            print(f"\n0 leads queued. Every {term} lead is already emailed, "
                  f"already queued, suppressed, or has no address.")
            print("Build more supply:  python crawl_sites.py --industry "
                  f"{term!r}")
    return {"term": term, "verticals": verticals, "created": created,
            "rank": rank, "pushed_back": int(waiting)}


def _remember(term: str, verticals: list[str]) -> None:
    data = _load()
    data[term] = verticals
    _save(data)
    logger.info("vertical alias saved: %s -> %s", term, verticals)


def _print(term: str, found: list[str], sizes: dict, note: str) -> None:
    total = sum(sizes.get(f, 0) for f in found)
    print(f"'{term}' -> {len(found)} verticals ({note})")
    for f in found:
        print(f"    {f:<34}{sizes.get(f, 0):>10,} companies")
    print(f"    {'TOTAL':<34}{total:>10,}")
    print(f"    saved to {ALIAS_FILE.name} - edit that file to correct it")
