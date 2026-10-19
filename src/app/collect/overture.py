"""Overture Maps Places collector. Phase 1 expansion.

Reads the Overture places theme (Parquet, ~72M businesses) either from a LOCAL
directory or from S3 via DuckDB. Filters by confidence, operating status, country,
and contact presence. Respects an optional category allow-list for the ICP.

Maps each place to a staging record with name, domain, country, address, and
industry (the Overture category). Raw includes phone/email/website for downstream
mining — so contacts are discoverable with no crawling.
"""

import logging
import os
from collections.abc import Iterator
from pathlib import Path

import duckdb

from app.collect import staging
from app.config import settings

logger = logging.getLogger(__name__)

# Optional category filter (empty = all).  Same verticals as osm.py's allow-list.
_TARGET_CATEGORIES: frozenset[str] = frozenset(
    c.strip().lower()
    for c in (settings.overture_categories or "").split(",")
    if c.strip()
)


def _resolve_paths() -> tuple[list[str], bool]:
    """Return (parquet glob paths, is_remote).

    Local wins over S3. Overture stores files in a hive-partitioned SUBDIRECTORY
    (``places/type=place/*.parquet``), so the local check must be RECURSIVE —
    a non-recursive listdir finds no parquet and would wrongly re-download from S3.
    """
    local = os.path.expanduser(settings.overture_local_path or "")
    if local and "*" in local:
        return [local], local.startswith("s3://")
    if local and os.path.isdir(local):
        # Recursive: matches both places/*.parquet and places/type=place/*.parquet
        if next(Path(local).rglob("*.parquet"), None) is not None:
            logger.info("Overture: reading LOCAL parquet from %s", local)
            return [os.path.join(local, "**", "*.parquet")], False
        logger.warning("Overture: %s exists but contains no .parquet files", local)
    s3 = settings.overture_s3_path
    if s3:
        logger.info("Overture: falling back to S3: %s", s3)
        return [s3], True
    raise staging.SourceError(
        staging.SourceOutcome.SKIP_UNREACHABLE, "No Overture data found (local or S3)"
    )


def _country_filter() -> str:
    raw = (settings.overture_countries or "").strip()
    if not raw:
        return ""
    countries = [f"'{c.strip().upper()}'" for c in raw.split(",") if c.strip()]
    if not countries:
        return ""
    # Strict: do NOT also let country-IS-NULL rows through, or a "US,GB" filter
    # would still pull in every country-less place on the planet.
    return f"AND addresses[1].country IN ({','.join(countries)})"


def _category_filter() -> str:
    if not _TARGET_CATEGORIES:
        return ""
    cats = ", ".join(f"'{c}'" for c in sorted(_TARGET_CATEGORIES))
    return f"AND (LOWER(categories.primary) IN ({cats}))"


def _contact_filter() -> str:
    if not settings.overture_require_contact:
        return ""
    return "AND (websites IS NOT NULL OR phones IS NOT NULL OR emails IS NOT NULL)"


def fetch(limit: int | None = None) -> Iterator[dict]:
    paths, is_remote = _resolve_paths()
    limit_clause = f" LIMIT {limit}" if limit is not None else ""

    sql = f"""
    SELECT
        id,
        names.primary               AS name,
        categories.primary          AS category,
        websites[1]                 AS website,
        emails[1]                   AS email,
        phones[1]                   AS phone,
        addresses[1].country        AS country,
        addresses[1].locality       AS locality,
        addresses[1].postcode       AS postcode,
        addresses[1].freeform       AS street,
        confidence,
        operating_status
    FROM read_parquet({_path_list(paths)})
    WHERE names.primary IS NOT NULL
      AND (operating_status IS NULL OR operating_status <> 'permanently_closed')
      AND (confidence IS NULL OR confidence >= {settings.overture_min_confidence})
      {_country_filter()}
      {_category_filter()}
      {_contact_filter()}
    {limit_clause}
    """

    logger.debug("Overture SQL:\n%s", sql)
    conn = duckdb.connect()
    try:
        if is_remote:
            # Reading from S3 needs httpfs; without this the query fails outright.
            conn.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-west-2';")
        try:
            cursor = conn.execute(sql)
        except Exception as exc:  # noqa: BLE001
            kind = staging.classify_http_error(exc) or (
                staging.SourceOutcome.SKIP_UNREACHABLE if is_remote else staging.SourceOutcome.FAIL
            )
            raise staging.SourceError(kind, str(exc)[:300]) from exc
        yielded = 0
        produced_any = False
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                oid = str(row[0] or "")
                name = str(row[1] or "").strip()
                category = (row[2] or "").strip().lower()
                website = str(row[3] or "").strip()
                email = str(row[4] or "").strip()
                phone = str(row[5] or "").strip()
                country = str(row[6] or "").strip().upper()
                locality = str(row[7] or "").strip()
                postcode = str(row[8] or "").strip()
                street = str(row[9] or "").strip()

                domain = staging.normalize_domain(website) if website else ""
                address = ", ".join(p for p in [street, locality, postcode] if p)

                raw = {
                    "overture_id": oid,
                    "category": category,
                    "website": website,
                    "confidence": row[10],
                    "locality": locality,
                }
                if email:
                    raw["email"] = email
                if phone:
                    raw["phone"] = phone

                yield {
                    "source": "overture",
                    "external_id": oid,
                    "name": name,
                    "domain": domain,
                    "country": country,
                    "address": address,
                    "industry": category,
                    "raw": raw,
                    "fetched_at": staging.now(),
                }
                yielded += 1
                produced_any = True

        logger.info("Overture: yielded %d rows", yielded)
    finally:
        conn.close()


def _path_list(paths: list[str]) -> str:
    """Build a DuckDB-friendly path expression.

    read_parquet() takes a single path OR a LIST of paths — passing several bare
    strings is a syntax error, so multiple paths must be wrapped in [ ... ].
    """
    if len(paths) == 1:
        return f"'{paths[0]}'"
    return "[" + ", ".join(f"'{p}'" for p in paths) + "]"
