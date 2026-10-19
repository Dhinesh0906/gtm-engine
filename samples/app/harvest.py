"""Harvest orchestrator: 24/7 driver that runs every collector at full scale.

The orchestrator REUSES app.collect._run_source and persists per-source state in
raw/<source>/_state.json. A global round counter is kept in raw/_harvest_state.json.
Round 2+ automatically catches content changes because StagingWriter now records a
content hash per external_id and appends updated records.

The orchestrator only fills staging files; loading into the database is Phase 2.
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.collect import SOURCES, _run_source
from app.collect import staging
from app.config import settings

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def _now() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _state_path(source: str) -> Path:
    """Return the path to a source's state file."""
    return Path(settings.raw_dir) / source / "_state.json"


def _global_state_path() -> Path:
    """Return the path to the global harvest state file."""
    return Path(settings.raw_dir) / "_harvest_state.json"


def _default_state() -> dict[str, Any]:
    """Return a fresh source state dict."""
    return {
        "status": "pending",
        "round": 0,
        "rows_total": 0,
        "new": 0,
        "updated": 0,
        "last_started": "",
        "last_completed": "",
        "last_error": "",
    }


def read_state(source: str) -> dict[str, Any]:
    """Read a source's state file, defaulting to a fresh pending state."""
    path = _state_path(source)
    if not path.exists():
        return _default_state()
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return _default_state()


def write_state(source: str, state: dict[str, Any]) -> None:
    """Persist a source's state file."""
    path = _state_path(source)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)


def read_global_state() -> dict[str, Any]:
    """Read the global harvest state file."""
    path = _global_state_path()
    if not path.exists():
        return {"round": 0, "last_updated": ""}
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {"round": 0, "last_updated": ""}


def write_global_state(state: dict[str, Any]) -> None:
    """Persist the global harvest state file."""
    path = _global_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)


def global_round() -> int:
    """Return the current global harvest round number."""
    return int(read_global_state().get("round", 0))


def set_global_round(round_number: int) -> None:
    """Set the global harvest round number."""
    write_global_state({"round": round_number, "last_updated": _now()})


def _source_names() -> list[str]:
    """Return the list of source names to harvest."""
    if settings.harvest_sources:
        configured = [n.strip() for n in settings.harvest_sources.split(",") if n.strip()]
        return [n for n in configured if n in SOURCES]
    return list(SOURCES.keys())


def run_source_full(name: str, sample: bool = False) -> dict[str, Any]:
    """Run a single source at full scale (or sample) and persist its state.

    Because the StagingWriter dedups by external_id and hashes content, re-running a
    source resumes from where it left off: new external_ids are added, changed records
    are appended, and unchanged records are skipped.
    """
    state = read_state(name)
    state["status"] = "running"
    state["last_started"] = _now()
    state["last_error"] = ""
    state["round"] = global_round()
    write_state(name, state)

    try:
        rows_written, new_count, updated_count, outcome = _run_source(name, sample=sample)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error running %s", name)
        rows_written, new_count, updated_count, outcome = 0, 0, 0, staging.SourceOutcome.FAIL

    state["rows_total"] = staging.count_rows(name)
    state["new"] = new_count
    state["updated"] = updated_count
    state["last_completed"] = _now()

    if outcome in (
        staging.SourceOutcome.SKIP_NO_KEY,
        staging.SourceOutcome.SKIP_AUTH,
        staging.SourceOutcome.SKIP_UNREACHABLE,
    ):
        state["status"] = "skipped"
    elif outcome == staging.SourceOutcome.OK:
        state["status"] = "complete"
    else:
        state["status"] = "error"
        state["last_error"] = str(outcome)

    write_state(name, state)
    return state


def run_round(sources: list[str] | None = None, sample: bool = False) -> tuple[bool, str]:
    """Run each configured source once and update its state.

    Returns (all_done, summary_message).
    """
    if sources is None:
        sources = _source_names()

    invalid = [n for n in sources if n not in SOURCES]
    if invalid:
        logger.warning("Unknown sources ignored: %s", invalid)
        sources = [n for n in sources if n in SOURCES]

    round_number = global_round()
    logger.info("Starting harvest round %s for %d sources", round_number, len(sources))

    for name in sources:
        try:
            run_source_full(name, sample=sample)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to run source %s", name)
            state = read_state(name)
            state["status"] = "error"
            state["last_error"] = str(exc)
            state["last_completed"] = _now()
            write_state(name, state)

    done = all_done(sources)
    counts = {n: read_state(n) for n in sources}
    total_rows = sum(s["rows_total"] for s in counts.values())
    total_new = sum(s["new"] for s in counts.values())
    total_updated = sum(s["updated"] for s in counts.values())
    status = "OK" if done else "INCOMPLETE"
    message = (
        f"HARVEST {status}: {len(sources)} sources processed, round {round_number}, "
        f"{total_rows} rows staged ({total_new} new, {total_updated} updated)"
    )
    logger.info(message)
    print(message)
    return done, message


def all_done(sources: list[str] | None = None) -> bool:
    """Return True when every non-skipped source in the list is complete for the current round."""
    if sources is None:
        sources = _source_names()
    current_round = global_round()
    for name in sources:
        state = read_state(name)
        if state.get("status") == "skipped":
            continue
        if state.get("status") != "complete" or state.get("round") != current_round:
            return False
    return True


def status() -> int:
    """Print a per-source harvest state table and return 0 if the round is complete."""
    sources = _source_names()
    round_number = global_round()

    print(f"{'source':<18} {'status':<10} {'round':>5} {'rows':>8} {'new':>6} {'updated':>8} {'last_error'}")
    print("-" * 80)
    for name in sources:
        state = read_state(name)
        print(
            f"{name:<18} "
            f"{state.get('status','pending'):<10} "
            f"{state.get('round',0):>5} "
            f"{state.get('rows_total',0):>8} "
            f"{state.get('new',0):>6} "
            f"{state.get('updated',0):>8} "
            f"{state.get('last_error','')[:40]}"
        )
    print("-" * 80)

    done = all_done(sources)
    complete_count = sum(1 for n in sources if read_state(n).get("status") == "complete")
    if done:
        message = f"HARVEST OK: {complete_count} sources complete, round {round_number}"
    else:
        message = f"HARVEST INCOMPLETE: {complete_count} sources complete, round {round_number}"
    print(message)
    return 0 if done else 1


def loop(rest_hours: int | None = None) -> int:
    """Run harvest rounds forever.

    When every non-skipped source completes a round, increment the global round
    counter, sleep, and start the next round.
    """
    if rest_hours is None:
        rest_hours = settings.harvest_rest_hours
    rest_seconds = rest_hours * 3600

    logger.info("Starting harvest loop with %s-hour rest", rest_hours)
    try:
        while True:
            done, _ = run_round()
            if done:
                round_number = global_round() + 1
                set_global_round(round_number)
                logger.info("Round complete. Sleeping %s hours before round %s.", rest_hours, round_number)
                time.sleep(rest_seconds)
            else:
                logger.warning("Round incomplete; sleeping %s hours before retry.", rest_hours)
                time.sleep(rest_seconds)
    except KeyboardInterrupt:
        logger.info("Harvest loop interrupted by user.")
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Raliess harvest orchestrator")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run one harvest round")
    run_parser.add_argument("--sample", action="store_true", help="Limit each source to 50 rows")
    run_parser.add_argument("--only", type=str, default="", help="Comma-separated source names")

    subparsers.add_parser("loop", help="Run harvest rounds forever")
    subparsers.add_parser("status", help="Print harvest state table")

    args = parser.parse_args(argv)

    if args.command == "run":
        sources = [n.strip() for n in args.only.split(",") if n.strip()] if args.only else None
        done, _ = run_round(sources=sources, sample=args.sample)
        return 0 if done else 1

    if args.command == "loop":
        return loop()

    if args.command == "status":
        return status()

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
