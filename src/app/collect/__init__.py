"""Data collection module. Phase 1.

CLI entry point for downloading company data from free web sources into local
JSONL staging files under raw/<source>/data.jsonl.

Usage:
    python -m app.collect <source> [--sample]
    python -m app.collect all [--sample]
    python -m app.collect --report
"""

import argparse
import logging
import sys
from collections.abc import Iterator

from app.collect import (
    belgium_kbo,
    bronnoysund,
    canada_corporations,
    common_crawl,
    companies_house,
    edgar,
    gleif,
    india_mca,
    osm,
    overture,
    singapore_acra,
    staging,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

SOURCES = {
    "gleif": gleif,
    "edgar": edgar,
    "common_crawl": common_crawl,
    "osm": osm,
    "overture": overture,
    "bronnoysund": bronnoysund,
    "companies_house": companies_house,
    "singapore_acra": singapore_acra,
    "india_mca": india_mca,
    "belgium_kbo": belgium_kbo,
    "canada_corporations": canada_corporations,
}


def _read_outcome(source: str) -> str:
    """Return the persisted outcome for a source, defaulting to FAIL."""
    data = staging.read_outcome(source)
    if data:
        return data.get("outcome", staging.SourceOutcome.FAIL)
    return staging.SourceOutcome.FAIL


def evaluate(outcomes: dict[str, str]) -> tuple[bool, str]:
    """Pure gate: decide whether the Phase 1 collection run passes.

    PASS if no source is FAIL and at least one source is OK.
    FAIL if any source is FAIL or zero sources are OK.
    """
    ok_sources = [name for name, outcome in outcomes.items() if outcome == staging.SourceOutcome.OK]
    fail_sources = [
        name
        for name, outcome in outcomes.items()
        if outcome == staging.SourceOutcome.FAIL
    ]

    if fail_sources:
        details = ", ".join(f"{name}: fail" for name in fail_sources)
        return False, f"PHASE1 FAIL: {details}"

    if not ok_sources:
        return False, "PHASE1 FAIL: no source produced rows"

    skipped = [
        (name, outcome)
        for name, outcome in outcomes.items()
        if outcome != staging.SourceOutcome.OK
    ]
    skip_note = ""
    if skipped:
        reasons = ", ".join(
            f"{name}: {outcome.replace('skip_', '')}"
            for name, outcome in skipped
        )
        skip_note = f" ({len(skipped)} skipped: {reasons})"

    total_ok_rows = sum(staging.count_rows(name) for name in ok_sources)
    return True, f"PHASE1 OK: {len(ok_sources)} sources ok, {total_ok_rows} rows{skip_note}"


def _run_source(name: str, sample: bool) -> tuple[int, int, int, str]:
    """Fetch and stage one source.

    Returns (rows_written, new_count, updated_count, outcome).
    """
    module = SOURCES[name]
    writer = staging.StagingWriter(name)
    limit = 50 if sample else None

    # Detect a clean skip due to missing API key before running.
    if name == "companies_house":
        # Bulk product doesn't need a key.
        pass  # no SKIP_NO_KEY — bulk is keyless
    if name == "india_mca" and not staging.settings.data_gov_in_key:
        staging.write_outcome(name, staging.SourceOutcome.SKIP_NO_KEY)
        return 0, 0, 0, staging.SourceOutcome.SKIP_NO_KEY
    if name == "belgium_kbo" and not (
        staging.settings.kbo_username and staging.settings.kbo_password
    ):
        staging.write_outcome(name, staging.SourceOutcome.SKIP_NO_KEY)
        return 0, 0, 0, staging.SourceOutcome.SKIP_NO_KEY

    outcome = staging.SourceOutcome.FAIL
    try:
        records: Iterator[dict] = module.fetch(limit=limit)
        for record in records:
            writer.write(record)
    except staging.SourceError as exc:
        outcome = exc.kind
        logging.getLogger(__name__).warning("Source %s outcome: %s", name, outcome)
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).warning("Source %s unexpected error: %s", name, exc)
        outcome = staging.SourceOutcome.FAIL

    rows_written = writer.new_count + writer.updated_count
    if writer.count > 0 or rows_written > 0:
        outcome = staging.SourceOutcome.OK

    staging.write_outcome(name, outcome)
    return rows_written, writer.new_count, writer.updated_count, outcome


def report() -> int:
    """Print a per-source staging report and return 0 on pass, 1 on fail."""
    counts = {name: staging.count_rows(name) for name in SOURCES}
    outcomes = {name: _read_outcome(name) for name in SOURCES}
    total = sum(counts.values())

    print(f"{'source':<18} {'rows':>8} {'status'}")
    print("-" * 40)
    for name in SOURCES:
        count = counts[name]
        outcome = outcomes[name]

        if outcome == staging.SourceOutcome.OK:
            status = "ok"
        elif outcome == staging.SourceOutcome.SKIP_NO_KEY:
            status = "skipped: no key"
        elif outcome == staging.SourceOutcome.SKIP_AUTH:
            data = staging.read_outcome(name)
            code = data.get("message", "") if data else ""
            status = f"skipped: auth ({code})" if code else "skipped: auth"
        elif outcome == staging.SourceOutcome.SKIP_UNREACHABLE:
            status = "skipped: unreachable"
        else:
            status = "FAIL"
        print(f"{name:<18} {count:>8} {status}")
    print("-" * 40)
    print(f"{'total':<18} {total:>8}")

    ok, message = evaluate(outcomes)
    print(message)
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GTM Engine Phase 1 data collection")
    parser.add_argument("source", nargs="?", help="Source to run or 'all'")
    parser.add_argument("--sample", action="store_true", help="Limit each source to 50 rows")
    parser.add_argument("--report", action="store_true", help="Print staging report")
    args = parser.parse_args(argv)

    if args.report:
        return report()

    if not args.source:
        parser.print_help()
        return 1

    if args.source == "all":
        names = list(SOURCES.keys())
    elif args.source in SOURCES:
        names = [args.source]
    else:
        print(f"Unknown source: {args.source}", file=sys.stderr)
        print(f"Known sources: {', '.join(SOURCES)}", file=sys.stderr)
        return 1

    for name in names:
        written, new_count, updated_count, outcome = _run_source(name, sample=args.sample)
        if outcome == staging.SourceOutcome.SKIP_NO_KEY:
            print(f"{name}: skipped: no key")
        elif outcome == staging.SourceOutcome.SKIP_AUTH:
            print(f"{name}: skipped: auth")
        elif outcome == staging.SourceOutcome.SKIP_UNREACHABLE:
            print(f"{name}: skipped: unreachable")
        else:
            update_note = f" ({updated_count} updated)" if updated_count else ""
            print(f"{name}: wrote {new_count} new rows{update_note}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
