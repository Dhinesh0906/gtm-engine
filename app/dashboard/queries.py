"""Dashboard queries — pure reads, no mutations."""

from __future__ import annotations

from datetime import datetime as _dt, timezone as _tz

from sqlalchemy import text as _text
from sqlalchemy import text as _sqltext

from app import search_index as _si
from app.db import (
    Assignment, Campaign, Company, ContactPoint, Message, MessageMeta,
    Person, Segment, Session, engine,
)
from sqlmodel import select, func


def list_campaigns(db: Session) -> list[dict]:
    camps = db.exec(select(Campaign).order_by(Campaign.id.desc())).all()
    return [{"id": c.id, "name": c.name or c.domain or f"Campaign {c.id}", "domain": c.domain, "status": c.status} for c in camps]


def thread_messages(assignment_id: int, db: Session) -> list[dict]:
    msgs = db.exec(
        select(Message).where(Message.assignment_id == assignment_id).order_by(Message.created_at, Message.step)
    ).all()
    return [_msg_dict(m) for m in msgs]


def thread_context(assignment_id: int, db: Session) -> dict | None:
    a = db.get(Assignment, assignment_id)
    if not a:
        return None
    cp = db.get(ContactPoint, a.contact_point_id)
    person = db.get(Person, a.person_id) if cp else None
    campaign = db.get(Campaign, a.campaign_id)
    meta = db.exec(
        select(MessageMeta).where(
            MessageMeta.message_id.in_(
                select(Message.id).where(Message.assignment_id == assignment_id)
            )
        ).order_by(MessageMeta.id)
    ).first()
    company = db.get(Company, person.company_id) if person and person.company_id else None
    is_test = bool(company and company.source == "test")
    return {
        "assignment_id": a.id,
        "status": a.status,
        "is_test": is_test,
        "company": (company.name if company else ""),
        "persona_id": meta.persona_id if meta else "",
        "prospect": f"{person.first_name or ''} {person.last_name or ''}".strip() if person else "",
        "email": cp.value if cp else "",
        "campaign": campaign.name if campaign else "",
        "campaign_id": a.campaign_id,
    }


_INTENT_BADGE = {
    "interested": ("Hot", "hot"),
    "question": ("Hot", "hot"),
    "call": ("Hot", "hot"),
    "meeting": ("Hot", "hot"),
    "ready": ("Hot", "hot"),
    "pricing": ("Hot", "hot"),
    "partnership": ("Hot", "hot"),
    "negotiation": ("Hot", "hot"),
    "not_interested": ("Not Interested", "cold"),
    "unsubscribe": ("Unsubscribed", "cold"),
    "out_of_office": ("Out of Office", "neutral"),
    "auto_reply": ("Auto-reply", "neutral"),
    "other": ("Unqualified", "neutral"),
}


def inbox_data(
    db: Session,
    tab: str = "got_reply",
    campaign_id: int | None = None,
    status: str = "",
    selected_id: int | None = None,
    persona_id: str = "",
) -> dict:
    """Two-pane inbox: tab counts, the conversation list, and the selected thread.

    Tabs:
      needs_reply — a reply is waiting on YOU (escalated, or an AI draft pending)
      got_reply   — the prospect has replied at least once
      sent        — we have sent at least one email
    """
    inbound_ids = select(Message.assignment_id).where(Message.direction == "in")
    sent_ids = select(Message.assignment_id).where(Message.status.in_(["sent", "delivered"]))
    pending_reply_ids = select(Message.assignment_id).where(
        Message.status == "draft", Message.direction == "out", Message.step >= 99
    )

    def _scope(base):
        if campaign_id:
            base = base.where(Assignment.campaign_id == campaign_id)
        return base

    counts = {
        "needs_reply": db.exec(_scope(
            select(func.count()).select_from(Assignment).where(
                (Assignment.status == "escalated") | (Assignment.id.in_(pending_reply_ids))
            ))).one(),
        "got_reply": db.exec(_scope(
            select(func.count()).select_from(Assignment).where(Assignment.id.in_(inbound_ids)))).one(),
        "sent": db.exec(_scope(
            select(func.count()).select_from(Assignment).where(Assignment.id.in_(sent_ids)))).one(),
    }

    # ONE query for the whole list.
    #
    # This used to select assignments and then call thread_context() plus two
    # more queries per row - about 800 queries to draw 100 conversations. It
    # also ordered by Assignment.id, i.e. when the LEAD was created, so a reply
    # that arrived an hour ago sat below a thread that had been silent for a
    # month. Newest inbound first is the only order that makes sense in an
    # inbox.
    tab_clause = {
        "needs_reply": ("AND (a.status = 'escalated' OR EXISTS ("
                        "  SELECT 1 FROM messages d WHERE d.assignment_id = a.id "
                        "  AND d.status='draft' AND d.direction='out' AND d.step >= 99))"),
        "sent": ("AND EXISTS (SELECT 1 FROM messages s WHERE s.assignment_id = a.id "
                 "AND s.status IN ('sent','delivered'))"),
    }.get(tab, "AND EXISTS (SELECT 1 FROM messages i WHERE i.assignment_id = a.id "
                "AND i.direction='in')")

    rows = db.execute(_sqltext(f"""
        SELECT a.id, a.status, a.segment_id, a.campaign_id,
               COALESCE(co.name,'')                       AS company,
               TRIM(COALESCE(pe.first_name,'')||' '||COALESCE(pe.last_name,'')) AS prospect,
               COALESCE(cp.value,'')                      AS email,
               COALESCE(ca.name,'')                       AS campaign,
               COALESCE(co.source,'')                     AS company_source,
               COALESCE(sg.name,'')                       AS segment,
               li.last_in_at, li.intent,
               la.last_at, la.snippet,
               tr.last_read_at,
               (SELECT persona_id FROM message_meta mm
                JOIN messages m2 ON m2.id = mm.message_id
                WHERE m2.assignment_id = a.id ORDER BY mm.id LIMIT 1) AS persona_id
        FROM assignments a
        LEFT JOIN people    pe ON pe.id = a.person_id
        LEFT JOIN companies co ON co.id = pe.company_id
        LEFT JOIN contact_points cp ON cp.id = a.contact_point_id
        LEFT JOIN campaigns ca ON ca.id = a.campaign_id
        LEFT JOIN segments  sg ON sg.id = a.segment_id
        LEFT JOIN thread_read tr ON tr.assignment_id = a.id
        LEFT JOIN (
            SELECT assignment_id,
                   MAX(COALESCE(NULLIF(sent_at,''), created_at)) AS last_in_at,
                   MAX(intent) AS intent
            FROM messages WHERE direction='in' GROUP BY assignment_id
        ) li ON li.assignment_id = a.id
        LEFT JOIN (
            SELECT m.assignment_id,
                   MAX(COALESCE(NULLIF(m.sent_at,''), m.created_at)) AS last_at,
                   SUBSTR(MAX(COALESCE(NULLIF(m.sent_at,''), m.created_at) || '|' || COALESCE(m.body,'')), 21, 90) AS snippet
            FROM messages m GROUP BY m.assignment_id
        ) la ON la.assignment_id = a.id
        WHERE 1=1 {tab_clause}
          {"AND a.campaign_id = :cid" if campaign_id else ""}
          {'''AND EXISTS (SELECT 1 FROM message_meta mm2
                          JOIN messages m3 ON m3.id = mm2.message_id
                          WHERE m3.assignment_id = a.id
                            AND mm2.persona_id = :pid)''' if persona_id else ""}
        ORDER BY COALESCE(li.last_in_at, la.last_at, '') DESC, a.id DESC
        LIMIT 100
    """), {"cid": campaign_id, "pid": persona_id}).all()

    convos: list[dict] = []
    for r in rows:
        intent = (r.intent or "") if hasattr(r, "intent") else (r[11] or "")
        label, tone = _INTENT_BADGE.get(intent, ("", ""))
        last_in_at = r[10] or ""
        read_at = r[14] or ""
        ctx = {
            "assignment_id": int(r[0]),
            "status": r[1],
            "is_test": (r[8] == "test"),
            "company": r[4] or "",
            "persona_id": r[15] or "",
            "prospect": (r[5] or "").strip(),
            "email": r[6] or "",
            "campaign": r[7] or "",
            "campaign_id": r[3],
            "badge": label,
            "badge_tone": tone,
            "intent": intent,
            "segment": (r[9] or "").replace("_", " "),
            "date": (r[12] or "")[:10],
            "snippet": (r[13] or "")[:90],
            # Unread: a reply arrived that you have not opened since.
            "unread": bool(last_in_at) and (not read_at or last_in_at > read_at),
            "last_in_at": last_in_at,
        }
        if status and ctx["badge"].lower() != status.lower():
            continue
        convos.append(ctx)

    selected = None
    if selected_id is None and convos:
        selected_id = convos[0]["assignment_id"]
    if selected_id:
        sel_ctx = thread_context(selected_id, db)
        if sel_ctx:
            selected = {"ctx": sel_ctx, "msgs": thread_messages(selected_id, db)}

    return {"tab": tab, "counts": counts, "convos": convos, "selected": selected,
            "selected_id": selected_id,
            "unread": sum(1 for c in convos if c["unread"])}


def unread_count(db: Session, campaign_id: int | None = None) -> int:
    """Threads with a reply you have not opened yet. One query, for the nav.

    Counts inbound messages newer than the last time the thread was opened,
    so a SECOND reply on an already-read thread counts again.
    """
    return int(db.execute(_sqltext(f"""
        SELECT COUNT(*) FROM assignments a
        JOIN (SELECT assignment_id,
                     MAX(COALESCE(NULLIF(sent_at,''), created_at)) AS t
              FROM messages WHERE direction='in' GROUP BY assignment_id) li
          ON li.assignment_id = a.id
        LEFT JOIN thread_read tr ON tr.assignment_id = a.id
        WHERE (tr.last_read_at IS NULL OR li.t > tr.last_read_at)
          {"AND a.campaign_id = :cid" if campaign_id else ""}
    """), {"cid": campaign_id}).scalar() or 0)


def mark_thread_read(assignment_id: int, db: Session) -> None:
    """Record that the operator has just looked at this thread."""
    from app.db import ThreadRead

    stamp = _dt.now(_tz.utc).isoformat()
    row = db.exec(select(ThreadRead).where(
        ThreadRead.assignment_id == assignment_id)).first()
    if row:
        row.last_read_at = stamp
        db.add(row)
    else:
        db.add(ThreadRead(assignment_id=assignment_id, last_read_at=stamp))
    db.commit()


def drafts_list(db: Session, campaign_id: int | None = None) -> list[dict]:
    """REPLY drafts awaiting the operator.

    Step-1 messages are cold openers queued by the batch/autopilot and sent
    automatically - they are queue state, not decisions. Listing them here
    alongside real reply drafts meant a genuine reply to a warm prospect was
    buried among dozens of machine-managed cold emails, each with an
    "Approve & Send" button that should never have been offered.
    """
    q = (
        select(Message)
        .where(Message.status == "draft", Message.direction == "out",
               Message.step != 1)
        .order_by(Message.created_at.desc())
    )
    if campaign_id:
        q = q.where(Message.campaign_id == campaign_id)
    msgs = db.exec(q).all()
    return [_msg_dict(m, include_assignment=True, db=db) for m in msgs]


def queued_cold_count(db: Session, campaign_id: int | None = None) -> int:
    """How many cold openers are written and waiting for the sender."""
    stmt = select(func.count()).select_from(Message).where(
        Message.status == "draft", Message.direction == "out", Message.step == 1
    )
    if campaign_id:
        stmt = stmt.where(Message.campaign_id == campaign_id)
    return int(db.exec(stmt).first() or 0)


def escalations_list(db: Session, campaign_id: int | None = None) -> list[dict]:
    q = select(Assignment).where(Assignment.status == "escalated")
    if campaign_id:
        q = q.where(Assignment.campaign_id == campaign_id)
    assignments = db.exec(q).all()
    result: list[dict] = []
    for a in assignments:
        ctx = thread_context(a.id, db)
        if not ctx:
            continue
        # Find the suggested draft (most recent draft for this assignment).
        draft = db.exec(
            select(Message).where(Message.assignment_id == a.id, Message.status == "draft", Message.direction == "out")
            .order_by(Message.created_at.desc())
        ).first()
        ctx["draft_id"] = draft.id if draft else None
        ctx["draft_subject"] = draft.subject if draft else ""
        ctx["draft_body"] = (draft.body or "")[:200] if draft else ""
        result.append(ctx)
    return result


def segments_list(db: Session, campaign_id: int | None = None) -> list[dict]:
    q = select(Segment)
    if campaign_id:
        q = q.where(Segment.campaign_id == campaign_id)
    segs = db.exec(q.order_by(Segment.fit_score.desc())).all()
    _LABEL = {"testing": "ACTIVE", "working": "WORKING", "scaling": "SCALING", "paused": "PAUSED"}
    return [{"id": s.id, "name": s.name, "fit_score": s.fit_score, "status": s.status,
             "label": _LABEL.get(s.status, s.status.upper()),
             "angle": s.angle, "campaign_id": s.campaign_id} for s in segs]


def _msg_dict(m: Message, include_assignment: bool = False, db: Session | None = None) -> dict:
    d: dict = {
        "id": m.id, "assignment_id": m.assignment_id, "step": m.step,
        "direction": m.direction, "subject": m.subject, "body": m.body,
        "status": m.status, "intent": m.intent, "created_at": m.created_at,
        "sent_at": m.sent_at,
    }
    if include_assignment and db:
        a = db.get(Assignment, d["assignment_id"])
        if a:
            cp = db.get(ContactPoint, a.contact_point_id)
            person = db.get(Person, a.person_id) if cp else None
            d["prospect"] = f"{person.first_name or ''} {person.last_name or ''}".strip() if person else ""
            d["email"] = cp.value if cp else ""
            company = db.get(Company, person.company_id) if person and person.company_id else None
            d["is_test"] = bool(company and company.source == "test")
            d["company"] = company.name if company else ""
    return d


# ---------------------------------------------------------------------------
# Leads page helpers — all SQL-paginated, no unbounded .all()
# ---------------------------------------------------------------------------

DEFAULT_PAGE_SIZE = 50


# COUNT(*) over ~3M companies costs seconds. The leads page re-runs it on every
# page change AND on every 5s live poll, so cache it briefly per filter combo.
_COUNT_CACHE: dict[tuple, tuple[float, int]] = {}
_COUNT_TTL = 60.0

_FTS = _si.FTS_TABLE


def _rows(db, sql: str, params: dict | None = None) -> list:
    """Run raw SQL and return all rows.

    SQLModel's ``Session.exec`` is typed for select() constructs; raw text goes
    through SQLAlchemy's ``execute`` instead.
    """
    return db.execute(_text(sql), params or {}).all()


_FTS_CHECKED: dict[int, bool] = {}


def _fts_available(db) -> bool:
    """Whether the trigram search index exists. Probed once per engine."""
    key = id(db.get_bind())
    if key not in _FTS_CHECKED:
        try:
            _FTS_CHECKED[key] = _si.exists(db.connection())
        except Exception:
            _FTS_CHECKED[key] = False
    return _FTS_CHECKED[key]


def _cached_count(db, stmt, key: tuple) -> int:
    import time as _t
    hit = _COUNT_CACHE.get(key)
    nowt = _t.time()
    if hit and (nowt - hit[0]) < _COUNT_TTL:
        return hit[1]
    val = db.exec(stmt).first()
    val = int(val) if val else 0
    _COUNT_CACHE[key] = (nowt, val)
    return val


def _cached_count_sql(db, sql: str, params: dict, key: tuple) -> int:
    """Same TTL cache as _cached_count, for raw-SQL counts."""
    import time as _t
    hit = _COUNT_CACHE.get(key)
    nowt = _t.time()
    if hit and (nowt - hit[0]) < _COUNT_TTL:
        return hit[1]
    row = _rows(db, sql, params)
    val = int(row[0][0]) if row and row[0][0] else 0
    _COUNT_CACHE[key] = (nowt, val)
    return val


def invalidate_counts() -> None:
    """Drop cached counts — call after a send/tick changes assignment status."""
    _COUNT_CACHE.clear()


def _lead_filter_sql(industry: str, country: str, q: str, use_fts: bool) -> tuple[str, str, dict]:
    """Build the FROM/WHERE shared by the id-selection and count queries.

    Returns (from_clause, where_clause, params).
    """
    params: dict = {}
    if use_fts:
        frm = f"{_FTS} f JOIN companies c ON c.id = f.rowid"
        where = [f"f.name MATCH :fts"]
        params["fts"] = _si.escape(q)
    else:
        frm = "companies c"
        where = []
        if q:
            where.append("c.name LIKE :like")
            params["like"] = f"%{q}%"
    if industry:
        where.append("c.industry_nace = :ind")
        params["ind"] = industry
    if country:
        where.append("c.country = :cty")
        params["cty"] = country
    return frm, (" WHERE " + " AND ".join(where)) if where else "", params


def leads_page(
    db: Session,
    campaign_id: int,
    industry: str = "",
    country: str = "",
    state: str = "",
    q: str = "",
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    after: int = 0,
) -> dict:
    """Filtered, paginated leads.

    Performance notes — this page reads a ~3M row table whose rows are
    physically fat (``web_text`` is stored inline), so the shape of the query
    matters far more than the row count:

    * **Deferred join.** We first select only the 50 ``id`` values using an
      index that covers the filter columns, then fetch the 50 full rows by
      primary key. Selecting wide columns directly with ``LIMIT/OFFSET`` makes
      SQLite walk every skipped row's full payload; selecting ids walks index
      entries instead, which are ~20 bytes rather than ~10KB.
    * **Keyset pagination.** When ``after`` is supplied (the Next button),
      ``WHERE id > :after`` replaces ``OFFSET n`` entirely, so page 30,000
      costs the same as page 1. ``OFFSET`` is kept only for direct page jumps.
    * **No N+1.** Contacts, assignments and sent timestamps are fetched in one
      batched query each — 4 queries per page, not 3 per row.
    * **Correct totals.** ``sent``/``skipped`` are aggregated over the whole
      campaign, not counted from the 50 rows on screen (which is what the
      previous version did, making the header numbers change as you paged).
    """
    q = (q or "").strip()
    use_fts = bool(q) and _fts_available(db) and len(q) >= 3

    frm, where, params = _lead_filter_sql(industry, country, q, use_fts)

    # ---- total (cached per filter combo) --------------------------------
    total = _cached_count_sql(
        db, f"SELECT COUNT(*) FROM {frm}{where}", params,
        ("leads", industry, country, state, q, use_fts),
    )

    # ---- page of ids: covering-index walk, never touches the fat rows ----
    id_params = dict(params)
    id_params["lim"] = page_size
    if after:
        seek = (" AND " if where else " WHERE ") + "c.id > :after"
        id_params["after"] = after
        id_sql = f"SELECT c.id FROM {frm}{where}{seek} ORDER BY c.id LIMIT :lim"
    else:
        id_params["off"] = max(0, (page - 1) * page_size)
        id_sql = f"SELECT c.id FROM {frm}{where} ORDER BY c.id LIMIT :lim OFFSET :off"

    ids = [int(r[0]) for r in _rows(db, id_sql, id_params)]

    rows: list[dict] = []
    if ids:
        rows = _hydrate_leads(db, campaign_id, ids)

    # ---- campaign-wide status counts (bounded by assignment count) -------
    agg = _campaign_status_counts(db, campaign_id)
    sent_count = agg.get("sent", 0)
    skipped_count = agg.get("suppressed", 0)

    return {
        "counts": {
            "total": total,
            "sent": sent_count,
            "skipped": skipped_count,
            "remaining": max(0, total - sent_count - skipped_count),
        },
        "rows": rows,
        "page": page,
        "pages": max(1, (total + page_size - 1) // page_size) if total else 1,
        "next_after": ids[-1] if ids else 0,
        "first_id": ids[0] if ids else 0,
        "fts": use_fts,
    }


def _hydrate_leads(db: Session, campaign_id: int, ids: list[int]) -> list[dict]:
    """Turn 50 company ids into 50 display rows using 4 batched queries."""
    ph = ",".join(str(i) for i in ids)  # ints only — built from int() above

    base = {
        int(r[0]): (r[1], r[2], r[3], r[4])
        for r in _rows(db,
            f"SELECT id, name, country, industry_nace, description "
            f"FROM companies WHERE id IN ({ph})"
        )
    }

    # Best email + phone per company, one query for the whole page.
    contacts: dict[tuple[int, str], str] = {}
    for cid, ctype, value in _rows(db,
        f"SELECT p.company_id, cp.type, cp.value "
        f"FROM people p JOIN contact_points cp ON cp.person_id = p.id "
        f"WHERE p.company_id IN ({ph}) AND cp.type IN ('email','phone') "
        f"ORDER BY cp.verified DESC, cp.confidence DESC"
    ):
        contacts.setdefault((int(cid), ctype), value)  # first wins = best ranked

    # Assignment status per company for this campaign.
    assigns: dict[int, tuple[int, str]] = {}
    for cid, aid, status in _rows(db,
        f"SELECT p.company_id, a.id, a.status "
        f"FROM assignments a JOIN people p ON p.id = a.person_id "
        f"WHERE a.campaign_id = :camp AND p.company_id IN ({ph})",
        {"camp": campaign_id},
    ):
        assigns.setdefault(int(cid), (int(aid), status or ""))

    # Sent timestamps for just those assignments.
    sent_at_by_aid: dict[int, str] = {}
    if assigns:
        aids = ",".join(str(a[0]) for a in assigns.values())
        sent_at_by_aid = {
            int(r[0]): (r[1] or "")
            for r in _rows(db,
                f"SELECT assignment_id, MAX(sent_at) FROM messages "
                f"WHERE assignment_id IN ({aids}) AND status = 'sent' "
                f"GROUP BY assignment_id"
            )
        }

    out: list[dict] = []
    for cid in ids:  # preserve id order from the paged query
        name, cty, _ind, desc = base.get(cid, ("", "", "", ""))
        aid_status = assigns.get(cid)
        status, sent_at = "pending", ""
        if aid_status:
            aid, raw = aid_status
            if raw == "sent":
                status, sent_at = "sent", sent_at_by_aid.get(aid, "")
            elif raw == "suppressed":
                status = "skipped"
        out.append({
            "company_id": cid,
            "name": name or "-",
            "email": contacts.get((cid, "email"), "-"),
            "phone": contacts.get((cid, "phone"), "-"),
            "description": ((desc or "")[:80] or "-"),
            "address": cty or "-",
            "status": status,
            "sent_at": sent_at,
        })
    return out


def _campaign_status_counts(db: Session, campaign_id: int) -> dict[str, int]:
    """sent/suppressed/new counts for the whole campaign, cached briefly."""
    key = ("campaign_status", campaign_id)
    import time as _t
    hit = _COUNT_CACHE.get(key)
    if hit and (_t.time() - hit[0]) < _COUNT_TTL:
        return hit[1]  # type: ignore[return-value]
    agg = {
        str(r[0]): int(r[1])
        for r in _rows(db,
            "SELECT status, COUNT(*) FROM assignments "
            "WHERE campaign_id = :c GROUP BY status",
            {"c": campaign_id},
        )
    }
    _COUNT_CACHE[key] = (_t.time(), agg)  # type: ignore[assignment]
    return agg


def _best_contact(db: Session, company_id: int, ctype: str) -> str:
    """Return the best contact value for this company or '-'.

    Retained for callers outside the leads page. The leads page itself now
    batches this — see ``_hydrate_leads``.
    """
    cp = db.exec(
        select(ContactPoint).where(
            ContactPoint.person_id.in_(
                select(Person.id).where(Person.company_id == company_id)
            ),
            ContactPoint.type == ctype,
        ).order_by(ContactPoint.verified.desc(), ContactPoint.confidence.desc())
    ).first()
    return cp.value if cp else "-"


def leads_industries(db: Session) -> list[dict]:
    """Distinct industries with total count and email-reachable count — SQL GROUP BY."""
    rows = db.exec(
        select(
            Company.industry_nace,
            func.count().label("total"),
            func.count(
                func.nullif(
                    select(ContactPoint.id).where(
                        ContactPoint.person_id.in_(
                            select(Person.id).where(Person.company_id == Company.id)
                        ),
                        ContactPoint.type == "email",
                        ContactPoint.source.like("%_listing"),
                    ).exists().select_from(func.true()).correlate(Company),
                    0,
                )
            ).label("email_reachable"),
        ).where(Company.industry_nace != "").group_by(Company.industry_nace)
        .order_by(func.count().desc())
    ).all()
    return [
        {"industry": r[0], "total": int(r[1]), "email_reachable": int(r[2])}
        for r in rows
    ]
