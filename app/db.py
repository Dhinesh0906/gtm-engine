"""SQLModel models, engine, and session helpers for the frozen Raliess schema."""

from datetime import datetime, timezone
from weakref import WeakSet

from sqlalchemy import UniqueConstraint, event, text
from sqlmodel import Field, Session, SQLModel, create_engine

from app.config import settings


def now() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _set_sqlite_pragmas(dbapi_conn, _connection_record):
    """Apply SQLite concurrency + durability pragmas on every new connection."""
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA busy_timeout=30000")
    cursor.execute("PRAGMA foreign_keys=ON")
    # --- Apple Silicon / large-RAM tuning -------------------------------
    # The companies table is ~3M rows and carries web_text inline, so it is
    # several GB on disk. The defaults (2MB page cache, no mmap, temp b-trees
    # spilled to disk) make every GROUP BY and every deep scan hit the SSD.
    # An M-series box has the RAM and the memory bandwidth to keep the hot
    # index pages resident, which is where nearly all the read time goes.
    cursor.execute("PRAGMA cache_size=-262144")   # 256 MB page cache (negative = KiB)
    cursor.execute("PRAGMA mmap_size=4294967296")  # up to 4 GB mapped, avoids read() syscalls
    cursor.execute("PRAGMA temp_store=MEMORY")     # GROUP BY temp b-trees stay in RAM
    cursor.close()


engine = create_engine(
    settings.db_url,
    echo=False,
    connect_args={"check_same_thread": False, "timeout": 30},
    pool_size=50,
    max_overflow=150,
    pool_timeout=60,
)
event.listen(engine, "connect", _set_sqlite_pragmas)


def reset_engine(db_url: str | None = None) -> None:
    """Reset the module-level engine to point at a new DB. Called by test fixtures."""
    global engine
    target = db_url or settings.db_url
    event.remove(engine, "connect", _set_sqlite_pragmas)
    engine = create_engine(target, echo=False, connect_args={"check_same_thread": False})
    event.listen(engine, "connect", _set_sqlite_pragmas)
    # The database behind this URL may have been recreated/emptied, so forget any
    # cached "already initialized" state before creating the schema again.
    _initialized_engines.clear()
    SQLModel.metadata.create_all(engine)
    create_indexes()
    _initialized_engines.add(engine)


# Idempotent additive indexes for scale. Column definitions in the models/schema
# are intentionally left untouched.
_INDEXES = [
    ("ix_companies_country", "CREATE INDEX IF NOT EXISTS ix_companies_country ON companies(country)"),
    ("ix_companies_industry_nace", "CREATE INDEX IF NOT EXISTS ix_companies_industry_nace ON companies(industry_nace)"),
    ("ix_companies_source", "CREATE INDEX IF NOT EXISTS ix_companies_source ON companies(source)"),
    ("ix_companies_enriched_at", "CREATE INDEX IF NOT EXISTS ix_companies_enriched_at ON companies(enriched_at)"),
    (
        "ix_companies_need_webtext",
        "CREATE INDEX IF NOT EXISTS ix_companies_need_webtext ON companies(id) WHERE domain != '' AND web_text = ''",
    ),
    (
        "ix_companies_need_enrich",
        "CREATE INDEX IF NOT EXISTS ix_companies_need_enrich ON companies(id) WHERE web_text != '' AND enriched_at IS NULL",
    ),
    (
        "ix_companies_domainless",
        "CREATE INDEX IF NOT EXISTS ix_companies_domainless ON companies(id) WHERE domain = ''",
    ),
    # Leads page: filter by (country, industry) and order by id. In SQLite an
    # index implicitly carries the rowid, and `id` IS the rowid here, so this is
    # a COVERING index for "give me the ids on page N" — the planner can walk it
    # without ever touching the fat companies rows (which hold web_text inline).
    (
        "ix_companies_country_industry",
        "CREATE INDEX IF NOT EXISTS ix_companies_country_industry ON companies(country, industry_nace)",
    ),
    ("ix_company_sources_company_id", "CREATE INDEX IF NOT EXISTS ix_company_sources_company_id ON company_sources(company_id)"),
    ("ix_company_sources_source", "CREATE INDEX IF NOT EXISTS ix_company_sources_source ON company_sources(source)"),
    # Exact-match lookup used by Pass C to skip a source row it already has.
    # That check used to be an in-memory set of every (company_id, source,
    # external_id) tuple in the database - about 3.7 GB of RAM at 25M
    # companies, and growing with the import. An index does the same job in
    # microseconds and costs nothing resident.
    ("ix_company_sources_lookup",
     "CREATE INDEX IF NOT EXISTS ix_company_sources_lookup "
     "ON company_sources(company_id, source, external_id)"),
    # Pass C resolves a staged row to an existing company by domain. Same
    # story: it was a dict of every domain in the database (~2 GB).
    #
    # NOT a partial index. `... WHERE domain != ''` looks tidier and is
    # smaller, but SQLite will only use a partial index when the query repeats
    # its condition - and this lookup is a plain `WHERE domain = ?`, so the
    # planner ignored it and went back to scanning every company.
    ("ix_companies_domain_lookup",
     "CREATE INDEX IF NOT EXISTS ix_companies_domain_lookup "
     "ON companies(domain)"),
    ("ix_people_company_id", "CREATE INDEX IF NOT EXISTS ix_people_company_id ON people(company_id)"),
    ("ix_contact_points_person_id", "CREATE INDEX IF NOT EXISTS ix_contact_points_person_id ON contact_points(person_id)"),
    # (person_id, type) as ONE seek. Without this, `WHERE person_id IN (...)
    # AND type IN ('email','phone')` tempts the planner into using the
    # type-only index — which has 2 distinct values over 7.1M rows, so it
    # walks essentially the whole table on every leads page render.
    # `value` rides along to make the index covering for the leads query.
    (
        "ix_contact_points_person_type",
        "CREATE INDEX IF NOT EXISTS ix_contact_points_person_type "
        "ON contact_points(person_id, type, verified DESC, confidence DESC, value)",
    ),
    ("ix_contact_points_value", "CREATE INDEX IF NOT EXISTS ix_contact_points_value ON contact_points(value)"),
    ("ix_contact_points_type", "CREATE INDEX IF NOT EXISTS ix_contact_points_type ON contact_points(type)"),
    ("ix_assignments_campaign_id", "CREATE INDEX IF NOT EXISTS ix_assignments_campaign_id ON assignments(campaign_id)"),
    ("ix_assignments_person_id", "CREATE INDEX IF NOT EXISTS ix_assignments_person_id ON assignments(person_id)"),
    ("ix_assignments_segment_id", "CREATE INDEX IF NOT EXISTS ix_assignments_segment_id ON assignments(segment_id)"),
    ("ix_assignments_contact_point_id", "CREATE INDEX IF NOT EXISTS ix_assignments_contact_point_id ON assignments(contact_point_id)"),
    ("ix_messages_campaign_id", "CREATE INDEX IF NOT EXISTS ix_messages_campaign_id ON messages(campaign_id)"),
    ("ix_messages_assignment_id", "CREATE INDEX IF NOT EXISTS ix_messages_assignment_id ON messages(assignment_id)"),
    ("ix_messages_status", "CREATE INDEX IF NOT EXISTS ix_messages_status ON messages(status)"),
    ("ix_messages_direction", "CREATE INDEX IF NOT EXISTS ix_messages_direction ON messages(direction)"),
    ("ix_contact_history_person_id", "CREATE INDEX IF NOT EXISTS ix_contact_history_person_id ON contact_history(person_id)"),
    ("ix_contact_history_contacted_at", "CREATE INDEX IF NOT EXISTS ix_contact_history_contacted_at ON contact_history(contacted_at)"),
    ("ix_suppressions_value", "CREATE INDEX IF NOT EXISTS ix_suppressions_value ON suppressions(value)"),
    ("ix_suppressions_channel", "CREATE INDEX IF NOT EXISTS ix_suppressions_channel ON suppressions(channel)"),
    ("ix_send_log_day_channel", "CREATE INDEX IF NOT EXISTS ix_send_log_day_channel ON send_log(day, channel)"),
    ("ix_segments_campaign_id", "CREATE INDEX IF NOT EXISTS ix_segments_campaign_id ON segments(campaign_id)"),
    ("ix_message_meta_message_id", "CREATE UNIQUE INDEX IF NOT EXISTS ix_message_meta_message_id ON message_meta(message_id)"),
    ("ix_message_meta_rfc_message_id", "CREATE INDEX IF NOT EXISTS ix_message_meta_rfc_message_id ON message_meta(rfc_message_id)"),
    ("ix_message_meta_from_addr", "CREATE INDEX IF NOT EXISTS ix_message_meta_from_addr ON message_meta(from_addr)"),
    # EXPRESSION indexes. The pool query and is_sendable() both compare
    # LOWER(to_addr) and LOWER(suppressions.value); a plain index on the column
    # cannot serve a function call, so those NOT EXISTS clauses were scanning.
    # That is why building a pool for ONE segment took 60 seconds - a cost paid
    # again per segment once several are active.
    (
        "ix_message_meta_to_addr_lower",
        "CREATE INDEX IF NOT EXISTS ix_message_meta_to_addr_lower "
        "ON message_meta(LOWER(to_addr))",
    ),
    (
        "ix_suppressions_value_lower",
        "CREATE INDEX IF NOT EXISTS ix_suppressions_value_lower "
        "ON suppressions(LOWER(value))",
    ),
    (
        "ix_contact_points_value_lower",
        "CREATE INDEX IF NOT EXISTS ix_contact_points_value_lower "
        "ON contact_points(LOWER(value))",
    ),
    ("ix_email_events_ses_message_id", "CREATE INDEX IF NOT EXISTS ix_email_events_ses_message_id ON email_events(ses_message_id)"),
    ("ix_email_events_event_type", "CREATE INDEX IF NOT EXISTS ix_email_events_event_type ON email_events(event_type)"),
    ("ix_segment_industries_segment", "CREATE INDEX IF NOT EXISTS ix_segment_industries_segment ON segment_industries(segment_id)"),
    ("ix_segment_industries_pair", "CREATE UNIQUE INDEX IF NOT EXISTS ix_segment_industries_pair ON segment_industries(segment_id, industry_nace)"),
    ("ix_site_crawl_company_id", "CREATE UNIQUE INDEX IF NOT EXISTS ix_site_crawl_company_id ON site_crawl(company_id)"),
    ("ix_site_crawl_status", "CREATE INDEX IF NOT EXISTS ix_site_crawl_status ON site_crawl(status)"),
    ("ix_inbound_other_s3_key", "CREATE UNIQUE INDEX IF NOT EXISTS ix_inbound_other_s3_key ON inbound_other(s3_key)"),
    ("ix_inbound_other_to_addr", "CREATE INDEX IF NOT EXISTS ix_inbound_other_to_addr ON inbound_other(to_addr)"),
    ("ix_inbound_other_is_read", "CREATE INDEX IF NOT EXISTS ix_inbound_other_is_read ON inbound_other(is_read)"),
]


def create_indexes(conn_engine=None, analyze: bool = False) -> None:
    """Create all additive scale indexes idempotently.

    Pass analyze=True after a bulk load. Without sqlite_stat1 the query planner
    has no cardinality information and falls back to heuristics — which is how
    a 2-distinct-value index on contact_points(type) ends up preferred over a
    selective person_id lookup.
    """
    target = conn_engine or engine
    # Make sure every table exists first. A new model (segment_industries,
    # site_crawl, inbound_other) has no table until create_all runs, and its
    # index DDL then fails with "no such table".
    SQLModel.metadata.create_all(target)

    # Each index in its own transaction, and a failure is logged rather than
    # aborting the rest. One bad DDL used to take every remaining index with
    # it, which is how a missing table stopped the leads page being indexed.
    for name, ddl in _INDEXES:
        try:
            with target.begin() as conn:
                conn.execute(text(ddl))
        except Exception as exc:  # noqa: BLE001
            import logging

            logging.getLogger(__name__).warning(
                "index %s skipped: %s", name, str(exc)[:120]
            )
    if analyze:
        analyze_db(conn_engine)


def analyze_db(conn_engine=None) -> None:
    """Refresh planner statistics. Run after a harvest or a new index."""
    target = conn_engine or engine
    with target.begin() as conn:
        conn.execute(text("ANALYZE"))


def optimize_db(conn_engine=None) -> None:
    """Cheap incremental stats refresh — safe to call often."""
    target = conn_engine or engine
    with target.begin() as conn:
        conn.execute(text("PRAGMA optimize"))


class Company(SQLModel, table=True):
    """A company. Global and deduped by domain. Shared across all products."""

    __tablename__ = "companies"

    id: int | None = Field(default=None, primary_key=True)
    domain: str | None = Field(default=None)
    name: str = Field(default="")
    country: str = Field(default="")
    industry_nace: str = Field(default="")
    description: str = Field(default="")
    keywords: str = Field(default="")
    web_text: str = Field(default="")
    size_estimate: int | None = Field(default=None)
    size_range: str = Field(default="")
    b2b_score: float | None = Field(default=None)
    saas_score: float | None = Field(default=None)
    startup_score: float | None = Field(default=None)
    tech_score: float | None = Field(default=None)
    source: str = Field(default="")
    enriched_at: str | None = Field(default=None)
    created_at: str = Field(default_factory=now)


class SegmentIndustry(SQLModel, table=True):
    """Maps ONE segment to MANY industry_nace values.

    Without this, a segment matched companies by exact string equality on its
    own name - so a segment called "clinics" found nothing, because the data
    says dentist, physical_therapy, chiropractor. Segments only worked at all
    because they were generated from the raw strings in the first place.

    With a mapping row per vertical, one segment can cover a whole category,
    and the same vertical can belong to several segments.

    Backwards compatible: a segment with NO mapping rows still falls back to
    matching on its own name, so nothing existing breaks.

    Additive table; the frozen schema is untouched.
    """

    __tablename__ = "segment_industries"

    id: int | None = Field(default=None, primary_key=True)
    segment_id: int = Field(foreign_key="segments.id")
    industry_nace: str = Field(default="")
    created_at: str = Field(default_factory=now)


class ThreadRead(SQLModel, table=True):
    """When the operator last opened a conversation.

    Messages have no read flag, so every thread in the inbox looked identical
    whether it held a reply from an hour ago or nothing since last month. A
    thread is UNREAD when its newest inbound message is newer than
    `last_read_at`, or when there is no row here at all.

    Storing a timestamp rather than a boolean means a second reply on an
    already-read thread correctly makes it unread again.

    Additive table; the frozen 13-table schema is untouched.
    """

    __tablename__ = "thread_read"
    __table_args__ = (UniqueConstraint("assignment_id", name="uq_thread_read"),)

    id: int | None = Field(default=None, primary_key=True)
    assignment_id: int = Field(foreign_key="assignments.id", index=True)
    last_read_at: str = Field(default_factory=now)


class CampaignCountry(SQLModel, table=True):
    """Which countries THIS product mails. Ticked in the dashboard.

    The database holds every country we can harvest; this decides which of them
    a given campaign is allowed to email. Kept separate on purpose - collecting
    data and choosing a market are different decisions, and conflating them
    meant re-importing to change who you were selling to.

    Per campaign, not global, so two products can work different markets from
    the same company table without interfering.

    NO ROWS = fall back to TARGET_COUNTRIES in .env. Every existing campaign
    therefore keeps working untouched until someone ticks a box.

    Additive table; the frozen 13-table schema is untouched.
    """

    __tablename__ = "campaign_countries"
    __table_args__ = (UniqueConstraint("campaign_id", "code",
                                       name="uq_campaign_country"),)

    id: int | None = Field(default=None, primary_key=True)
    campaign_id: int = Field(foreign_key="campaigns.id", index=True)
    code: str = Field(default="")          # ISO-2, uppercase
    created_at: str = Field(default_factory=now)


class FollowupQueue(SQLModel, table=True):
    """Cold emails that are old enough to deserve one nudge.

    Eligibility used to be computed on the fly by a six-clause SQL query every
    time the follow-up batch ran. That worked, but it was invisible: there was
    no way to look at what was due, no record of why something dropped out, and
    no way to tell "never qualified" apart from "qualified and was cancelled".

    A row lands here automatically once a cold email is FOLLOWUP_DAYS old with
    no reply, and then moves in one direction only:

        pending  -> sent       the follow-up went out
        pending  -> cancelled  they replied, unsubscribed or bounced

    Nothing is ever deleted, so the history stays readable. `assignment_id` is
    unique, which is what enforces "one follow-up per person, ever" at the
    database level rather than by remembering to write the right WHERE clause.

    Additive table; the frozen 13-table schema is untouched.
    """

    __tablename__ = "followup_queue"
    __table_args__ = (UniqueConstraint("assignment_id",
                                       name="uq_followup_assignment"),)

    id: int | None = Field(default=None, primary_key=True)
    campaign_id: int = Field(foreign_key="campaigns.id", index=True)
    assignment_id: int = Field(foreign_key="assignments.id")
    original_message_id: int = Field(foreign_key="messages.id")
    to_addr: str = Field(default="", index=True)
    country: str = Field(default="")
    original_sent_at: str = Field(default="")
    eligible_at: str = Field(default="")     # when it became due
    status: str = Field(default="pending", index=True)  # pending|sent|cancelled
    cancel_reason: str = Field(default="")   # replied | suppressed | assignment_closed
    followup_message_id: int | None = Field(default=None)
    created_at: str = Field(default_factory=now)
    updated_at: str = Field(default_factory=now)


class QueuePriority(SQLModel, table=True):
    """Assignments that must be drawn BEFORE anything already waiting.

    The send queue is FIFO - `ORDER BY assignments.id` - so leads sourced today
    sit behind every lead sourced last week. That is right for normal running
    and wrong when you have just told the engine "go after brain clinics now":
    those would not be reached for days.

    A row here lifts an assignment above the FIFO order. Higher `rank` goes
    first; ties fall back to id, so within one injection the usual order holds.
    Nothing is deleted or renumbered - the existing queue is untouched and
    simply resumes once the prioritised rows are spent.

    Additive table; the frozen 13-table schema is untouched.
    """

    __tablename__ = "queue_priority"

    id: int | None = Field(default=None, primary_key=True)
    assignment_id: int = Field(foreign_key="assignments.id", index=True)
    rank: int = Field(default=100)
    reason: str = Field(default="")      # the term that caused the injection
    created_at: str = Field(default_factory=now)


class SiteCrawl(SQLModel, table=True):
    """One row per company whose website we have tried to scrape for an email.

    Exists so a site is NEVER crawled twice. Without it, every batch would
    re-fetch the same pages and rediscover the same address, which is the
    slowest possible way to learn nothing new.

    A row is written even when the crawl finds nothing - "we looked and there
    was no email" is exactly the fact worth remembering.

    Additive table: the frozen 13-table schema is untouched.
    """

    __tablename__ = "site_crawl"

    id: int | None = Field(default=None, primary_key=True)
    company_id: int = Field(foreign_key="companies.id")
    domain: str = Field(default="")
    status: str = Field(default="")      # ok | no_email | unreachable | no_domain
    emails_found: int = Field(default=0)
    best_email: str = Field(default="")
    crawled_at: str = Field(default_factory=now)


class InboundOther(SQLModel, table=True):
    """Mail sent to a NON-persona address on our domain.

    hello@, johnson@, support@ and friends are not sending identities, so mail
    arriving there is not part of any sales conversation - it is ordinary
    business mail and vendor spam. Keeping it out of the sales Inbox matters,
    but throwing it away is worse, so it lands here and gets its own page.

    Additive table: the frozen 13-table schema is untouched.
    """

    __tablename__ = "inbound_other"

    id: int | None = Field(default=None, primary_key=True)
    s3_key: str = Field(default="")
    to_addr: str = Field(default="")
    from_addr: str = Field(default="")
    from_name: str = Field(default="")
    subject: str = Field(default="")
    body: str = Field(default="")
    received_at: str = Field(default="")
    is_read: int = Field(default=0)
    created_at: str = Field(default_factory=now)


class CompanySource(SQLModel, table=True):
    """Where each company's data came from. One row per source that mentioned it."""

    __tablename__ = "company_sources"

    id: int | None = Field(default=None, primary_key=True)
    company_id: int = Field(foreign_key="companies.id")
    source: str = Field(default="")
    external_id: str = Field(default="")
    raw_json: str = Field(default="")


class Person(SQLModel, table=True):
    """A person at a company. Global and deduped."""

    __tablename__ = "people"

    id: int | None = Field(default=None, primary_key=True)
    company_id: int | None = Field(default=None, foreign_key="companies.id")
    first_name: str = Field(default="")
    last_name: str = Field(default="")
    title: str = Field(default="")
    source: str = Field(default="")
    created_at: str = Field(default_factory=now)


class ContactPoint(SQLModel, table=True):
    """A way to reach a person on a channel. Polymorphic."""

    __tablename__ = "contact_points"

    id: int | None = Field(default=None, primary_key=True)
    person_id: int = Field(foreign_key="people.id")
    type: str = Field(default="")
    value: str = Field(default="")
    verified: int = Field(default=0)
    verify_status: str = Field(default="unknown")
    confidence: int = Field(default=0)
    source: str = Field(default="")
    created_at: str = Field(default_factory=now)


class Campaign(SQLModel, table=True):
    """One product you are promoting."""

    __tablename__ = "campaigns"

    id: int | None = Field(default=None, primary_key=True)
    domain: str = Field(default="")
    name: str = Field(default="")
    summary: str = Field(default="")
    value_props: str = Field(default="")
    proof_points: str = Field(default="")
    status: str = Field(default="new")
    created_at: str = Field(default_factory=now)


class Segment(SQLModel, table=True):
    """A customer hypothesis within a campaign."""

    __tablename__ = "segments"

    id: int | None = Field(default=None, primary_key=True)
    campaign_id: int = Field(foreign_key="campaigns.id")
    name: str = Field(default="")
    description: str = Field(default="")
    fit_score: int = Field(default=0)
    angle: str = Field(default="")
    titles: str = Field(default="")
    search_queries: str = Field(default="")
    status: str = Field(default="testing")
    created_at: str = Field(default_factory=now)


class Assignment(SQLModel, table=True):
    """Links a person to a campaign+segment as a target."""

    __tablename__ = "assignments"

    id: int | None = Field(default=None, primary_key=True)
    campaign_id: int = Field(foreign_key="campaigns.id")
    segment_id: int | None = Field(default=None, foreign_key="segments.id")
    person_id: int = Field(foreign_key="people.id")
    contact_point_id: int = Field(foreign_key="contact_points.id")
    status: str = Field(default="new")
    created_at: str = Field(default_factory=now)


class Message(SQLModel, table=True):
    """Every message in or out, any channel."""

    __tablename__ = "messages"

    id: int | None = Field(default=None, primary_key=True)
    assignment_id: int = Field(foreign_key="assignments.id")
    campaign_id: int = Field(foreign_key="campaigns.id")
    channel: str = Field(default="email")
    step: int = Field(default=1)
    direction: str = Field(default="out")
    subject: str = Field(default="")
    body: str = Field(default="")
    status: str = Field(default="draft")
    intent: str = Field(default="")
    provider_id: str = Field(default="")
    scheduled_at: str | None = Field(default=None)
    sent_at: str | None = Field(default=None)
    created_at: str = Field(default_factory=now)


class Suppression(SQLModel, table=True):
    """Never contact again. Global, per channel."""

    __tablename__ = "suppressions"

    id: int | None = Field(default=None, primary_key=True)
    channel: str = Field(default="email")
    value: str = Field(default="")
    reason: str = Field(default="")
    created_at: str = Field(default_factory=now)


class SendLog(SQLModel, table=True):
    """Powers the warmup ramp and the daily cap. One row per channel per day."""

    __tablename__ = "send_log"

    id: int | None = Field(default=None, primary_key=True)
    channel: str = Field(default="email")
    day: str = Field(default="")
    count: int = Field(default=0)


class ContactHistory(SQLModel, table=True):
    """Enforces the GLOBAL frequency cap: one person, across ALL products and channels."""

    __tablename__ = "contact_history"

    id: int | None = Field(default=None, primary_key=True)
    person_id: int = Field(foreign_key="people.id")
    campaign_id: int = Field(foreign_key="campaigns.id")
    channel: str = Field(default="email")
    contacted_at: str = Field(default_factory=now)


# ---------------------------------------------------------------------------
# Phase 8: persona-sending metadata (ADDITIVE — 2 new tables)
# ---------------------------------------------------------------------------


class MessageMeta(SQLModel, table=True):
    """Per-message sending metadata: persona, Message-ID, routing."""

    __tablename__ = "message_meta"

    id: int | None = Field(default=None, primary_key=True)
    message_id: int = Field(foreign_key="messages.id")
    rfc_message_id: str = Field(default="")
    in_reply_to: str = Field(default="")
    references_ids: str = Field(default="")
    s3_key: str = Field(default="")
    from_addr: str = Field(default="")
    to_addr: str = Field(default="")
    persona_id: str = Field(default="")
    created_at: str = Field(default_factory=now)


class EmailEvent(SQLModel, table=True):
    """SES email events (send, delivery, bounce, complaint, open, click)."""

    __tablename__ = "email_events"

    id: int | None = Field(default=None, primary_key=True)
    ses_message_id: str = Field(default="")
    message_id: int | None = Field(default=None, foreign_key="messages.id")
    event_type: str = Field(default="")
    occurred_at: str = Field(default="")
    raw_json: str = Field(default="")
    created_at: str = Field(default_factory=now)


TABLE_MODELS = [
    Company,
    CompanySource,
    Person,
    ContactPoint,
    Campaign,
    Segment,
    Assignment,
    Message,
    Suppression,
    SendLog,
    ContactHistory,
    MessageMeta,
    EmailEvent,
]


_initialized_engines: "WeakSet" = WeakSet()


def init_db() -> None:
    """Create all tables + indexes ONCE per engine (idempotent).

    Workers call this per company; without a guard it re-runs create_all and
    checks out a pool connection every time, exhausting the pool under
    concurrency.

    The guard is keyed on the ENGINE OBJECT, not its URL: several distinct
    in-memory databases all share the URL "sqlite:///:memory:", so URL-keying
    would skip creating tables for a freshly rebound engine.
    """
    if engine in _initialized_engines:
        return
    SQLModel.metadata.create_all(engine)
    create_indexes()
    _initialized_engines.add(engine)


def session() -> Session:
    """Return a new database session."""
    return Session(engine)


from concurrent.futures import ThreadPoolExecutor, as_completed
import time as _time


def run_concurrent(items: list, fn, workers: int = 4) -> list[dict]:
    """Run fn(item) across workers threads. Returns list of fn(item) results.

    Each call gets its own Session. Failures in one worker don't abort others.
    """
    if not items:
        return []
    import logging as _logging
    _log = _logging.getLogger("app.concurrent")
    total = len(items)
    _log.info("run_concurrent: starting %d items across %d workers", total, max(1, workers))
    results: list[dict] = [{}] * len(items)
    done = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(fn, item): i for i, item in enumerate(items)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception:
                results[idx] = {"error": True}
            done += 1
            if done == 1 or done % 5 == 0 or done == total:
                _log.info("run_concurrent: %d/%d done", done, total)
    return results
