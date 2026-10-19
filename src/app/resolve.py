"""Phase 2: entity resolution (disk-backed, incremental, 200M-safe).

Uses DuckDB for on-disk grouping/joining and SQLite for the frozen companies
schema. Only new/changed staging rows are reprocessed each run (content-hash
incremental). No pass holds more than a bounded batch in RAM.

Deterministic first pass only:
- registrable-domain exact match
- normalized-name + country exact match
- cross-key backfill when a name+country has exactly one unambiguous domain

# TODO(phase2-fuzzy): embedding + address matching
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import unicodedata
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import tldextract
from sqlalchemy import delete as sa_delete
from sqlalchemy import func
from sqlalchemy import func as sa_func
from sqlmodel import select

from app.collect import staging
from app.config import settings
from app.db import Company, CompanySource, init_db, now, session

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# Sources trusted for legal name/address (lower number = higher trust).
REGISTRY_SOURCES = {
    "gleif",
    "edgar",
    "companies_house",
    "bronnoysund",
    "singapore_acra",
    "india_mca",
    "belgium_kbo",
    "canada_corporations",
}
WEB_SOURCES = {"common_crawl", "osm"}

# Trust values per field: lower = preferred.
SOURCE_TRUST: dict[str, dict[str, int]] = {
    "gleif": {"name": 0, "domain": 10},
    "edgar": {"name": 0, "domain": 10},
    "companies_house": {"name": 0, "domain": 10},
    "bronnoysund": {"name": 0, "domain": 10},
    "singapore_acra": {"name": 0, "domain": 10},
    "india_mca": {"name": 0, "domain": 10},
    "belgium_kbo": {"name": 0, "domain": 10},
    "canada_corporations": {"name": 0, "domain": 10},
    "common_crawl": {"name": 10, "domain": 0},
    "osm": {"name": 10, "domain": 0},
}

LEGAL_SUFFIXES = (
    "limited",
    "ltd",
    "ltee",
    "incorporated",
    "inc",
    "llc",
    "plc",
    "gmbh",
    "srl",
    "sarl",
    "bv",
    "nv",
    "pvt",
    "private",
    "pte",
    "company",
    "co",
    "corporation",
    "corp",
    "sa",
    "ag",
    "oy",
    "as",
    "ab",
)

RE_LEGAL_SUFFIX = re.compile(
    r"\b(" + "|".join(re.escape(s) for s in LEGAL_SUFFIXES) + r")\b",
    re.IGNORECASE,
)

JUNK_NAMES = {"n/a", "na", "none", "null", "unknown", "test", "tbd", "-", ".", "n a"}

PLACEHOLDER_DOMAINS = {
    "example.com",
    "example.org",
    "example.net",
    "localhost",
    "test.com",
    "wixsite.com",
    "squarespace.com",
    "godaddysites.com",
    "business.site",
}

BATCH_SIZE = 50000


def normalize_name(name: str) -> str:
    """Return a normalized name suitable for exact matching."""
    if not name:
        return ""
    text = str(name).lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = RE_LEGAL_SUFFIX.sub("", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def blocking_key(record: dict[str, Any]) -> str:
    """Return country + first token of normalized name for blocking."""
    country = str(record.get("country", "")).strip().upper()
    nname = normalize_name(record.get("name", ""))
    first = nname.split()[0] if nname else ""
    return f"{country}|{first}"


def registrable_domain(value: str) -> str:
    """Reduce any URL/domain to its registrable root (e.g. shop.acme.co.uk -> acme.co.uk)."""
    if not value:
        return ""
    value = str(value).strip().lower()
    # Accept bare domains; normalize_domain strips scheme/path if a URL was given.
    value = staging.normalize_domain(value)
    if not value or value.count(".") < 1:
        return ""
    extracted = tldextract.extract(value)
    if not extracted.domain or not extracted.suffix:
        return ""
    return f"{extracted.domain}.{extracted.suffix}".lower()


def is_junk_name(norm_name: str) -> bool:
    """Return True for names too short, numeric-only, or on the junk blocklist."""
    if not norm_name:
        return True
    if len(norm_name) <= 2:
        return True
    if norm_name.isdigit():
        return True
    return norm_name in JUNK_NAMES


def is_placeholder_domain(domain: str) -> bool:
    """Return True for well-known placeholder / website-builder domains and bare TLDs."""
    if not domain or "." not in domain:
        return True
    return domain.lower() in PLACEHOLDER_DOMAINS


def _hash(record: dict[str, Any]) -> str:
    """Stable content hash for incremental detection."""
    raw = record.get("raw") or {}
    payload = {
        "name": record.get("name", ""),
        "domain": record.get("domain", ""),
        "country": record.get("country", ""),
        "address": record.get("address", ""),
        "industry": record.get("industry", ""),
        "raw": sorted(raw.items()),
    }
    data = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(data.encode("utf-8")).hexdigest()


def _trust(source: str) -> tuple[int, int]:
    """Return (name_trust, domain_trust) for a source."""
    trust = SOURCE_TRUST.get(source, {})
    return int(trust.get("name", 10)), int(trust.get("domain", 10))


def _match_key(domain: str, norm_name: str, country: str, content_hash: str) -> str:
    """Return the deterministic identity key for a processed staging record."""
    if domain and not is_placeholder_domain(domain):
        return f"domain|{domain}"
    if not is_junk_name(norm_name) and country:
        return f"name|{norm_name}|{country}"
    return f"unmatchable|{content_hash}"


def _duckdb_conn() -> duckdb.DuckDBPyConnection:
    """Open the on-disk DuckDB scratch database, creating parent dirs if needed."""
    path = Path(settings.duckdb_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def _prepare_duckdb(conn: duckdb.DuckDBPyConnection, full: bool = False) -> None:
    """Create (or reset for --full) the DuckDB working tables."""
    if full:
        conn.execute("DROP TABLE IF EXISTS stage_keys")
        conn.execute("DROP TABLE IF EXISTS resolved_hashes")
        conn.execute("DROP TABLE IF EXISTS incoming_batch")
        conn.execute("DROP TABLE IF EXISTS company_groups")
        conn.execute("DROP TABLE IF EXISTS group_mapping")
        conn.execute("DROP TABLE IF EXISTS merged_companies")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stage_keys (
            source TEXT,
            external_id TEXT,
            orig_name TEXT,
            norm_name TEXT,
            country TEXT,
            domain TEXT,
            industry TEXT,
            match_key TEXT,
            content_hash TEXT PRIMARY KEY,
            raw_json TEXT,
            name_trust INTEGER,
            domain_trust INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS resolved_hashes (content_hash TEXT PRIMARY KEY)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS incoming_batch (
            source TEXT,
            external_id TEXT,
            orig_name TEXT,
            norm_name TEXT,
            country TEXT,
            domain TEXT,
            industry TEXT,
            match_key TEXT,
            content_hash TEXT,
            raw_json TEXT,
            name_trust INTEGER,
            domain_trust INTEGER
        )
    """)


def _insert_batch(conn: duckdb.DuckDBPyConnection, batch: list[tuple]) -> int:
    """Insert a batch into stage_keys, skipping rows already known or already staged."""
    conn.execute("DELETE FROM incoming_batch")
    # batch is 12-tuples with industry now.
    try:
        conn.executemany(
            """
            INSERT INTO incoming_batch VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            batch,
        )
    except Exception:
        # Fallback if batch is still 11-tuples (old format without industry).
        # Pad with empty industry.
        padded = []
        for row in batch:
            if len(row) == 12:
                padded.append(row)
            else:
                padded.append(row + ("",))
        conn.executemany(
            """
            INSERT INTO incoming_batch VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            padded,
        )
    # Drop rows we have already fully resolved in a previous run.
    conn.execute("""
        DELETE FROM incoming_batch
        WHERE content_hash IN (SELECT content_hash FROM resolved_hashes)
    """)
    # Dedupe against stage_keys via its PRIMARY KEY index instead of
    # `content_hash IN (SELECT ... FROM stage_keys)`. That subquery re-scanned the
    # WHOLE growing stage_keys table on EVERY batch — thousands of scans over
    # millions of rows, which stalls a multi-million-row ingest completely.
    before = conn.execute("SELECT COUNT(*) FROM stage_keys").fetchone()[0]  # type: ignore[index]
    try:
        conn.execute(
            "INSERT INTO stage_keys BY NAME SELECT * FROM incoming_batch ON CONFLICT DO NOTHING"
        )
    except Exception:  # noqa: BLE001 — older DuckDB without ON CONFLICT support
        conn.execute("""
            DELETE FROM incoming_batch
            WHERE content_hash IN (SELECT content_hash FROM stage_keys)
        """)
        conn.execute("INSERT INTO stage_keys BY NAME SELECT * FROM incoming_batch")
    after = conn.execute("SELECT COUNT(*) FROM stage_keys").fetchone()[0]  # type: ignore[index]
    return int(after) - int(before)


def _stream_staging_records() -> Iterator[dict[str, Any]]:
    """Yield all validated staging records across every source directory."""
    raw_dir = Path(settings.raw_dir)
    if not raw_dir.exists():
        return
    for source_dir in sorted(raw_dir.iterdir()):
        if not source_dir.is_dir():
            continue
        data_path = source_dir / "data.jsonl"
        if not data_path.exists():
            continue
        with data_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if staging.validate_record(record):
                    yield record


def pass_a_ingest(conn: duckdb.DuckDBPyConnection, limit: int | None = None) -> tuple[int, int]:
    """Stream staging files into DuckDB, skipping already-resolved rows.

    Returns (rows_read, rows_added).
    """
    rows_read = 0
    rows_added = 0
    batch: list[tuple] = []

    for record in _stream_staging_records():
        if limit is not None and rows_read >= limit:
            break
        rows_read += 1

        source = record.get("source", "")
        external_id = record.get("external_id", "")
        orig_name = str(record.get("name", "")).strip()
        norm_name = normalize_name(orig_name)
        country = str(record.get("country", "")).strip().upper()
        domain = registrable_domain(record.get("domain", ""))
        content_hash = _hash(record)
        match_key = _match_key(domain, norm_name, country, content_hash)
        name_trust, domain_trust = _trust(source)
        raw_json = json.dumps(record.get("raw", {}), ensure_ascii=False)

        batch.append(
            (
                source,
                external_id,
                orig_name,
                norm_name,
                country,
                domain,
                str(record.get("industry", "")).strip(),
                match_key,
                content_hash,
                raw_json,
                name_trust,
                domain_trust,
            )
        )

        if len(batch) >= BATCH_SIZE:
            rows_added += _insert_batch(conn, batch)
            batch = []
            # Progress: Pass A on millions of rows used to run completely silent,
            # which is indistinguishable from being hung.
            if rows_read % 250_000 < BATCH_SIZE:
                logger.info("Pass A: %d rows read, %d new so far", rows_read, rows_added)

    if batch:
        rows_added += _insert_batch(conn, batch)

    return rows_read, rows_added


def pass_b_resolve(conn: duckdb.DuckDBPyConnection) -> int:
    """Run the DuckDB grouping/backfill/merge pipeline.

    Returns the number of resolved company groups.
    """
    conn.execute("""
        CREATE OR REPLACE TABLE company_groups AS
        SELECT
            match_key,
            arg_min(domain, domain_trust) FILTER (WHERE domain IS NOT NULL AND domain <> '') AS domain,
            arg_min(orig_name, name_trust) FILTER (WHERE orig_name IS NOT NULL AND orig_name <> '') AS orig_name,
            arg_min(norm_name, name_trust) FILTER (WHERE norm_name IS NOT NULL AND norm_name <> '') AS norm_name,
            arg_min(country, name_trust) FILTER (WHERE country IS NOT NULL AND country <> '') AS country,
            arg_min(industry, name_trust) FILTER (WHERE industry IS NOT NULL AND industry <> '') AS industry,
            arg_min(source, name_trust) AS source,
            MIN(name_trust) AS name_trust,
            LIST(DISTINCT source) AS sources,
            COUNT(*) AS row_count
        FROM stage_keys
        GROUP BY match_key
    """)

    logger.info("Pass B 1/5: grouped by match_key")

    # Unambiguous name+country -> domain backfill map (built from rows carrying both).
    conn.execute("""
        CREATE OR REPLACE TABLE domain_votes AS
        SELECT norm_name, country, domain
        FROM (
            SELECT norm_name, country, domain,
                COUNT(DISTINCT domain) OVER (PARTITION BY norm_name, country) AS domain_count
            FROM stage_keys
            WHERE domain IS NOT NULL AND domain <> ''
              AND norm_name IS NOT NULL AND norm_name <> ''
              AND country IS NOT NULL AND country <> ''
        )
        WHERE domain_count = 1
        GROUP BY norm_name, country, domain
    """)

    logger.info("Pass B 2/5: built domain votes")

    # Backfill domain onto name-keyed groups when exactly one domain is available.
    conn.execute("""
        UPDATE company_groups
        SET domain = domain_votes.domain
        FROM domain_votes
        WHERE company_groups.domain IS NULL
          AND company_groups.match_key LIKE 'name|%'
          AND company_groups.norm_name = domain_votes.norm_name
          AND company_groups.country = domain_votes.country
    """)

    logger.info("Pass B 3/5: backfilled domains")

    # Build final group_key. Domain groups collapse together; name-only groups keep
    # name|country; unmatchable rows stay singletons.
    conn.execute("""
        CREATE OR REPLACE TABLE merged_companies AS
        SELECT
            CASE
                WHEN domain IS NOT NULL AND domain <> '' THEN domain
                WHEN norm_name IS NOT NULL AND norm_name <> '' AND country IS NOT NULL AND country <> ''
                    THEN norm_name || '|' || country
                ELSE match_key
            END AS group_key,
            MAX(domain) AS domain,
            arg_min(orig_name, name_trust) FILTER (WHERE orig_name IS NOT NULL AND orig_name <> '') AS orig_name,
            arg_min(norm_name, name_trust) FILTER (WHERE norm_name IS NOT NULL AND norm_name <> '') AS norm_name,
            arg_min(country, name_trust) FILTER (WHERE country IS NOT NULL AND country <> '') AS country,
            arg_min(industry, name_trust) FILTER (WHERE industry IS NOT NULL AND industry <> '') AS industry,
            arg_min(source, name_trust) AS source,
            MIN(name_trust) AS name_trust,
            LIST_DISTINCT(FLATTEN(LIST(sources))) AS sources,
            SUM(row_count) AS row_count
        FROM company_groups
        GROUP BY group_key
    """)

    logger.info("Pass B 4/5: merged companies")

    # Map each original match_key to its final merged group_key.
    #
    # PERFORMANCE: this used to be ONE join with OR'd conditions. A join predicate
    # containing OR cannot be hash-joined, so DuckDB degrades to a nested-loop —
    # ~5M x ~5M row comparisons, which never completes. Splitting it into three
    # single-equality joins lets each one hash-join; UNION removes duplicate pairs
    # so the result set is identical.
    conn.execute("""
        CREATE OR REPLACE TABLE group_mapping AS
        SELECT cg.match_key, mc.group_key
        FROM company_groups cg
        JOIN merged_companies mc ON mc.group_key = cg.domain
        WHERE cg.domain IS NOT NULL AND cg.domain <> ''

        UNION

        SELECT cg.match_key, mc.group_key
        FROM company_groups cg
        JOIN merged_companies mc ON mc.group_key = cg.norm_name || '|' || cg.country
        WHERE cg.norm_name IS NOT NULL AND cg.norm_name <> ''
          AND cg.country IS NOT NULL AND cg.country <> ''

        UNION

        SELECT cg.match_key, mc.group_key
        FROM company_groups cg
        JOIN merged_companies mc ON mc.group_key = cg.match_key
    """)

    logger.info("Pass B 5/5: built group mapping")
    count = conn.execute("SELECT COUNT(*) FROM merged_companies").fetchone()[0]  # type: ignore[index]
    return int(count)


# ---------------------------------------------------------------------------
# In-memory lookup caches for Pass C.
#
# The previous implementation queried SQLite once per company AND loaded EVERY
# domain-less company of a country into Python to compare normalized names —
# an O(n^2) scan that made a 1.5M-row resolve effectively never finish.
# We now load the existing keys ONCE and keep them in dicts/sets.
# ---------------------------------------------------------------------------
_CACHE_BY_NAME_COUNTRY: dict[tuple[str, str], int] = {}


def _load_caches(db) -> None:
    """Load ONLY what an index cannot serve.

    This used to pull every company and every company_source into Python
    dicts. At 3.4M companies that was tolerable; on a full global import it
    reached ~6 GB resident and kept climbing, because the cache grows with the
    very rows the pass is inserting. A machine that runs out of memory here
    loses hours of work.

    Two of the three caches are now indexed SQL lookups instead:

      by domain        -> ix_companies_domain_lookup
      source-exists    -> ix_company_sources_lookup

    The name/country cache STAYS in memory, because its key is
    normalize_name(), a Python function SQLite cannot index. It only holds
    companies with NO domain, which is a small minority.
    """
    _CACHE_BY_NAME_COUNTRY.clear()
    # NULL *or* empty. Company.domain is `str | None` with default None, so a
    # domain-less company is stored as NULL - and `domain == ""` matched none
    # of them. That silently emptied this cache, so every domain-less company
    # was re-created on the next run instead of being matched by name.
    for cid, name, country in db.exec(
        select(Company.id, Company.name, Company.country).where(
            (Company.domain == None) | (Company.domain == "")  # noqa: E711
        )
    ):
        key = (country or "", normalize_name(name or ""))
        if key[1]:
            _CACHE_BY_NAME_COUNTRY.setdefault(key, cid)
    logger.info(
        "Pass C caches: %d name/country keys held in memory "
        "(domain and source lookups use indexes)",
        len(_CACHE_BY_NAME_COUNTRY),
    )


def _find_company_by_domain(db, domain: str) -> Company | None:
    if not domain:
        return None
    # Indexed point lookup (ix_companies_domain_lookup). Was a dict of every
    # domain in the database.
    return db.exec(select(Company).where(Company.domain == domain)).first()


def _find_company_by_name_country(db, norm_name: str, country: str) -> Company | None:
    if not norm_name or not country:
        return None
    cid = _CACHE_BY_NAME_COUNTRY.get((country, norm_name))
    return db.get(Company, cid) if cid else None


def _company_source_exists(db, company_id: int, source: str, external_id: str) -> bool:
    # Indexed lookup (ix_company_sources_lookup). Was a set holding every
    # source row in the database - the single biggest memory user in Pass C.
    return db.exec(
        select(CompanySource.id).where(
            CompanySource.company_id == company_id,
            CompanySource.source == source,
            CompanySource.external_id == external_id,
        ).limit(1)
    ).first() is not None


def _upsert_company(
    db,
    domain: str | None,
    norm_name: str,
    country: str,
    orig_name: str,
    sources: list[str],
    industry: str = "",
) -> Company:
    """Create or update a company from a resolved group."""
    company = None
    if domain:
        company = _find_company_by_domain(db, domain)
    if company is None:
        company = _find_company_by_name_country(db, norm_name, country)

    if company is None:
        company = Company(
            domain=domain,
            name=orig_name or "",
            country=country,
            created_at=now(),
        )
        db.add(company)
        db.flush()
        # The domain lookup is served by an index now, and db.flush() above
        # has already made this row visible to it - so only the in-memory
        # name/country map (domain-less companies) needs updating by hand.
        if not domain and norm_name and country:
            _CACHE_BY_NAME_COUNTRY[(country, norm_name)] = company.id
    else:
        if domain and not company.domain:
            company.domain = domain
        if orig_name and not company.name:
            company.name = orig_name
        if country and not company.country:
            company.country = country

    # Carry the listing vertical (Overture/OSM category) onto the company. Without
    # this, industry_nace stays empty and vertical targeting is impossible.
    if industry and not (company.industry_nace or "").strip():
        company.industry_nace = industry

    if sources:
        company.source = ",".join(sorted({s for s in sources if s}))

    db.add(company)
    db.flush()
    return company


def pass_c_upsert(conn: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    """Stream resolved groups and upsert into the frozen SQLite tables.

    Returns (companies_count, sources_added_count).
    """
    init_db()

    query = """
        SELECT
            gm.group_key,
            mc.domain,
            mc.orig_name,
            mc.norm_name,
            mc.country,
            mc.industry,
            mc.sources,
            sk.source AS src_source,
            sk.external_id,
            sk.raw_json,
            sk.content_hash
        FROM group_mapping gm
        JOIN merged_companies mc ON gm.group_key = mc.group_key
        JOIN stage_keys sk ON sk.match_key = gm.match_key
        ORDER BY gm.group_key
    """

    result = conn.execute(query)
    companies = 0
    sources_added = 0
    groups_since_commit = 0

    # Stream processed hashes to a temp file so we never run another statement
    # on the open SELECT result cursor.
    hashes_path = Path(settings.duckdb_path).parent / "_resolve_hashes_pending.txt"
    hashes_path.parent.mkdir(parents=True, exist_ok=True)
    hashes_file = hashes_path.open("w", encoding="utf-8")

    try:
        with session() as db:
            _load_caches(db)
            current_group_key: str | None = None
            group_company: Company | None = None
            pending_sources: list[tuple[str, str, str]] = []

            def flush_group() -> None:
                nonlocal group_company, groups_since_commit
                if current_group_key is None or group_company is None:
                    return
                for src_source, external_id, raw_json in pending_sources:
                    if _company_source_exists(db, group_company.id, src_source, external_id):
                        continue
                    cs = CompanySource(
                        company_id=group_company.id,
                        source=src_source,
                        external_id=external_id,
                        raw_json=raw_json,
                    )
                    db.add(cs)
                pending_sources.clear()
                groups_since_commit += 1
                if groups_since_commit >= 2000:
                    db.commit()
                    groups_since_commit = 0
                    if companies and companies % 20000 == 0:
                        logger.info("Pass C: %d companies upserted", companies)

            while True:
                rows = result.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                for row in rows:
                    (
                        group_key,
                        domain,
                        orig_name,
                        norm_name,
                        country,
                        industry,
                        sources_json,
                        src_source,
                        external_id,
                        raw_json,
                        content_hash,
                    ) = row

                    if isinstance(sources_json, list):
                        sources: list[str] = sources_json
                    elif sources_json:
                        sources = json.loads(sources_json)
                    else:
                        sources = []

                    if group_key != current_group_key:
                        flush_group()
                        group_company = _upsert_company(
                            db, domain, norm_name, country, orig_name, sources, industry
                        )
                        current_group_key = group_key
                        companies += 1

                    pending_sources.append((src_source, external_id, raw_json))
                    hashes_file.write(content_hash + "\n")
                    sources_added += 1

            flush_group()
            db.commit()
    finally:
        hashes_file.close()

    # Bulk-load the resolved hashes now that the SELECT result is closed.
    _load_hashes_from_file(conn, hashes_path)

    return companies, sources_added


def _load_hashes_from_file(conn: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Bulk-load processed content hashes from a text file into resolved_hashes."""
    if not path.exists() or path.stat().st_size == 0:
        return
    batch: list[tuple[str]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            h = line.strip()
            if h:
                batch.append((h,))
            if len(batch) >= BATCH_SIZE:
                conn.executemany("INSERT OR IGNORE INTO resolved_hashes VALUES (?)", batch)
                batch = []
    if batch:
        conn.executemany("INSERT OR IGNORE INTO resolved_hashes VALUES (?)", batch)
    path.unlink()


def resolve_all(full: bool = False, limit: int | None = None) -> dict[str, Any]:
    """Run the full disk-backed resolution pipeline."""
    # Ensure the SQLite schema exists BEFORE any code path queries it. The
    # "nothing new to resolve" early-exit skips pass_c_upsert (which used to be
    # the only caller of init_db) and then reports on `companies` — crashing with
    # "no such table" on a fresh database.
    init_db()
    conn = _duckdb_conn()
    try:
        _prepare_duckdb(conn, full=full)

        if full:
            # Also clear the frozen SQLite companies/provenance so the rebuild is clean.
            # BULK delete: loading millions of ORM objects just to delete them one at
            # a time exhausts memory and takes hours at 5M+ rows.
            init_db()
            with session() as db:
                db.exec(sa_delete(CompanySource))
                db.exec(sa_delete(Company))
                db.commit()
            logger.info("Full rebuild: cleared existing companies + sources")

        rows_read, rows_added = pass_a_ingest(conn, limit=limit)
        logger.info("Pass A: read %d staging rows, added %d new", rows_read, rows_added)

        groups: int | None = None
        if rows_added == 0 and not full:
            logger.info("No new/changed staging rows to resolve.")
        else:
            groups = pass_b_resolve(conn)
            logger.info("Pass B: resolved %d company groups", groups)

            companies, sources_added = pass_c_upsert(conn)
            logger.info("Pass C: upserted %d companies, %d source rows", companies, sources_added)

            # Mark every current stage row as resolved so future runs are incremental.
            conn.execute("""
                INSERT OR IGNORE INTO resolved_hashes
                SELECT content_hash FROM stage_keys
            """)

        with session() as db:
            company_count = db.exec(select(func.count()).select_from(Company)).one()
            with_domain = len(db.exec(select(Company).where(Company.domain.is_not(None))).all())  # type: ignore[attr-defined]

        try:
            junk = conn.execute("SELECT COUNT(*) FROM stage_keys WHERE match_key LIKE 'unmatchable|%'").fetchone()[0]  # type: ignore[index]
        except duckdb.CatalogException:
            junk = 0

        return {
            "staging_read": rows_read,
            "staging_added": rows_added,
            "groups": groups if rows_added or full else None,
            "companies": company_count,
            "with_domain": with_domain,
            "junk": int(junk),
            "domain_fill_pct": round(100 * with_domain / company_count, 2) if company_count else 0.0,
        }
    finally:
        conn.close()


def resolution_report() -> dict[str, Any]:
    """Return a resolution summary from the frozen SQLite database + DuckDB."""
    init_db()
    with session() as db:
        # All aggregation happens in SQL. Materialising millions of ORM objects
        # just to count them stalls the report for many minutes and can exhaust RAM.
        company_count = db.exec(select(func.count()).select_from(Company)).one()
        with_domain = db.exec(
            select(func.count()).select_from(Company).where(Company.domain.is_not(None))  # type: ignore[attr-defined]
        ).one()
        source_counts: dict[str, int] = {
            source: count
            for source, count in db.exec(
                select(CompanySource.source, func.count()).group_by(CompanySource.source)
            )
        }

    conn = _duckdb_conn()
    try:
        junk = conn.execute(
            "SELECT COUNT(*) FROM stage_keys WHERE match_key LIKE 'unmatchable|%'"
        ).fetchone()[0]  # type: ignore[index]
    except duckdb.CatalogException:
        junk = 0
    finally:
        conn.close()

    return {
        "companies": company_count,
        "with_domain": with_domain,
        "domain_fill_pct": round(100 * with_domain / company_count, 2) if company_count else 0.0,
        "source_counts": source_counts,
        "junk": int(junk),
    }


def verify() -> tuple[bool, dict[str, Any]]:
    """Run invariants and return (ok, report)."""
    report = resolution_report()
    ok = report["companies"] > 0
    if ok:
        logger.info(
            "PHASE2 OK: %d companies, %.2f%% domains, %d junk-skipped",
            report["companies"],
            report["domain_fill_pct"],
            report["junk"],
        )
    else:
        logger.error("PHASE2 FAIL: no companies resolved")
    return ok, report


def print_report(report: dict[str, Any]) -> None:
    print(f"Companies:       {report['companies']}")
    print(f"With domain:     {report['with_domain']} ({report['domain_fill_pct']}%)")
    print(f"Junk skipped:    {report['junk']}")
    print("Per-source contributions:")
    for source, count in sorted(report["source_counts"].items()):
        print(f"  {source:<18} {count:>6}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GTM Engine Phase 2 entity resolution")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Resolve staging records into companies")
    run_parser.add_argument("--full", action="store_true", help="Clear all state and rebuild from scratch")
    run_parser.add_argument("--limit", type=int, default=None, help="Limit staging rows processed")

    subparsers.add_parser("report", help="Print resolution report")
    subparsers.add_parser("verify", help="Run resolution invariants")

    args = parser.parse_args(argv)

    if args.command == "run":
        summary = resolve_all(full=args.full, limit=args.limit)
        report = resolution_report()
        print_report(report)
        print(
            f"PHASE2 OK: {report['companies']} companies, {report['domain_fill_pct']}% domains, "
            f"{report['junk']} junk-skipped"
        )
        return 0 if report["companies"] > 0 else 1

    if args.command == "report":
        print_report(resolution_report())
        return 0

    if args.command == "verify":
        ok, report = verify()
        print(
            f"PHASE2 OK: {report['companies']} companies, {report['domain_fill_pct']}% domains, "
            f"{report['junk']} junk-skipped"
        )
        return 0 if ok else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
