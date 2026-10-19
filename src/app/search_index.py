"""FTS5 trigram index over company names.

Why this exists
---------------
The leads page searched with ``name LIKE '%query%'``. A leading wildcard can
never use a B-tree index, so SQLite's plan for that is a bare ``SCAN companies``
— every one of the ~3M rows, and those rows are physically fat because
``web_text`` is stored inline in the same table. That is the single slowest
thing the dashboard does.

FTS5's ``trigram`` tokenizer indexes every 3-character sequence, which makes
arbitrary substring matching an index lookup instead of a scan.

We use an *external content* table (``content='companies'``), so the names are
not duplicated — the FTS table stores only the trigram postings and points back
at ``companies.id``.

Sync policy
-----------
Deliberately NO triggers. Triggers would fire 3M times during a harvest and
make Loop A crawl. The index is rebuilt explicitly instead:

    python -m app.search_index --rebuild

``is_stale()`` compares row counts so the dashboard can warn when a harvest has
landed since the last rebuild. Search silently falls back to LIKE whenever the
index is missing, so nothing breaks if you never build it.
"""

from __future__ import annotations

import time

from sqlalchemy import text

from app.db import engine


FTS_TABLE = "companies_fts"


def exists(conn) -> bool:
    """True if the FTS table has been created."""
    row = conn.execute(
        text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:n"),
        {"n": FTS_TABLE},
    ).first()
    return row is not None


def has_trigram(conn) -> bool:
    """True if this SQLite build supports the trigram tokenizer (3.34+)."""
    try:
        conn.execute(text(
            "CREATE VIRTUAL TABLE IF NOT EXISTS _trigram_probe "
            "USING fts5(x, tokenize='trigram')"
        ))
        conn.execute(text("DROP TABLE IF EXISTS _trigram_probe"))
        return True
    except Exception:
        return False


def create(conn) -> None:
    """Create the external-content FTS table. Cheap; does not populate."""
    conn.execute(text(
        f"CREATE VIRTUAL TABLE IF NOT EXISTS {FTS_TABLE} USING fts5("
        "  name,"
        "  content='companies',"
        "  content_rowid='id',"
        "  tokenize='trigram'"
        ")"
    ))


def counts(conn) -> tuple[int, int]:
    """Return (companies_rows, fts_rows)."""
    c = conn.execute(text("SELECT COUNT(*) FROM companies")).scalar() or 0
    try:
        f = conn.execute(text(f"SELECT COUNT(*) FROM {FTS_TABLE}")).scalar() or 0
    except Exception:
        f = 0
    return int(c), int(f)


def is_stale(conn, tolerance: int = 1000) -> bool:
    """True if companies has drifted meaningfully ahead of the index."""
    c, f = counts(conn)
    return abs(c - f) > tolerance


def rebuild(verbose: bool = True) -> dict:
    """(Re)build the trigram index from scratch. One-time, minutes not seconds."""
    started = time.time()
    with engine.begin() as conn:
        if not has_trigram(conn):
            return {"ok": False, "error": "This SQLite build lacks the FTS5 trigram tokenizer (needs 3.34+)."}
        conn.execute(text(f"DROP TABLE IF EXISTS {FTS_TABLE}"))
        create(conn)
        if verbose:
            print("Populating trigram index over company names...")
        # 'rebuild' is the documented way to fill an external-content table from
        # its source in one pass. Much faster than row-by-row INSERT.
        conn.execute(text(f"INSERT INTO {FTS_TABLE}({FTS_TABLE}) VALUES('rebuild')"))
        conn.execute(text(f"INSERT INTO {FTS_TABLE}({FTS_TABLE}) VALUES('optimize')"))
        c, f = counts(conn)
    elapsed = time.time() - started
    if verbose:
        print(f"Indexed {f:,} names from {c:,} companies in {elapsed:.1f}s")
    return {"ok": True, "companies": c, "indexed": f, "seconds": round(elapsed, 1)}


def drop(verbose: bool = True) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {FTS_TABLE}"))
    if verbose:
        print("Dropped", FTS_TABLE)


def escape(term: str) -> str:
    """Quote a user string so FTS5 treats it as one literal phrase."""
    return '"' + term.replace('"', '""') + '"'


def usable(conn, term: str) -> bool:
    """Trigram needs >= 3 characters; shorter terms must fall back to LIKE."""
    return len(term.strip()) >= 3 and exists(conn)


def main() -> int:
    import sys

    args = sys.argv[1:]
    if "--drop" in args:
        drop()
        return 0
    if "--status" in args:
        with engine.connect() as conn:
            if not exists(conn):
                print("FTS index: NOT BUILT  (search falls back to a full table scan)")
                return 1
            c, f = counts(conn)
            print(f"FTS index: built — {f:,} names indexed, {c:,} companies "
                  f"({'STALE, rerun --rebuild' if is_stale(conn) else 'in sync'})")
        return 0
    res = rebuild()
    return 0 if res.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
