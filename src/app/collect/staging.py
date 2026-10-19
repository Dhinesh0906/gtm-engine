"""Staging format, domain normalization, and idempotent writer."""

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import settings


# Keys required in every staging record.
STAGING_KEYS = {
    "source": str,
    "external_id": str,
    "name": str,
    "domain": str,
    "country": str,
    "address": str,
    "industry": str,
    "raw": dict,
    "fetched_at": str,
}


def now() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def normalize_domain(value: str) -> str:
    """Return a bare, lowercase domain with no scheme, www, path, or query.

    Examples:
        https://www.Example.com/path -> example.com
        www.ACME.org                 -> acme.org
        http://localhost:8080        -> localhost
        already.bare.io              -> already.bare.io
    """
    if not value:
        return ""

    domain = value.strip().lower()

    # Strip scheme.
    domain = re.sub(r"^[a-z][a-z0-9+.-]*://", "", domain)

    # Strip optional username/password.
    domain = re.sub(r"^[^@]+@", "", domain)

    # Strip path, query, and fragment.
    domain = domain.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]

    # Strip port.
    domain = re.sub(r":\d+$", "", domain)

    # Strip www.
    if domain.startswith("www."):
        domain = domain[4:]

    # Strip a single trailing dot.
    if domain.endswith("."):
        domain = domain[:-1]

    return domain


def validate_record(record: dict) -> bool:
    """Return True only if record has all required keys with correct types.

    'industry' is optional — defaults to "" if missing (Phase 1 scale addition).
    """
    if not isinstance(record, dict):
        return False
    for key, expected_type in STAGING_KEYS.items():
        if key == "industry":
            continue  # optional, defaults to ""
        if key not in record:
            return False
        if not isinstance(record[key], expected_type):
            return False
    return True


def count_rows(source: str) -> int:
    """Return the number of JSONL lines in raw/<source>/data.jsonl without creating anything.

    Read-only helper for reporting.
    """
    path = Path(settings.raw_dir) / source / "data.jsonl"
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                count += 1
    return count


class SourceOutcome:
    """Persisted outcome of a source run."""

    OK = "ok"
    SKIP_NO_KEY = "skip_no_key"
    SKIP_AUTH = "skip_auth"
    SKIP_UNREACHABLE = "skip_unreachable"
    FAIL = "fail"


class SourceError(Exception):
    """Raised by a source fetcher when it fails before producing any rows."""

    def __init__(self, kind: str, message: str = ""):
        self.kind = kind
        self.message = message
        super().__init__(message or kind)


class WriteResult:
    """Three-way result of a StagingWriter write."""

    NEW = "new"
    DUP = "dup"
    UPDATED = "updated"

    def __init__(self, status: str):
        self.status = status

    def __bool__(self) -> bool:
        return self.status != WriteResult.DUP

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return self.status == other
        return isinstance(other, WriteResult) and self.status == other.status

    def __repr__(self) -> str:
        return f"WriteResult({self.status!r})"


def classify_http_error(exc: Exception) -> str | None:
    """Map an HTTP/network exception to a SourceOutcome, or None if not applicable."""
    import httpx

    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        if code in (401, 403):
            return SourceOutcome.SKIP_AUTH
        if code in (406, 408, 429, 500, 502, 503, 504):
            return SourceOutcome.SKIP_UNREACHABLE
        return None
    if isinstance(
        exc,
        (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.NetworkError),
    ):
        return SourceOutcome.SKIP_UNREACHABLE
    return None


def write_outcome(source: str, outcome: str, message: str = "") -> None:
    """Write the outcome of a source run to raw/<source>/_outcome.txt.

    Called only during writes, not during read-only reporting.
    """
    dir_path = Path(settings.raw_dir) / source
    dir_path.mkdir(parents=True, exist_ok=True)
    path = dir_path / "_outcome.txt"
    payload = {
        "outcome": outcome,
        "message": message,
        "updated_at": now(),
    }
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)


def read_outcome(source: str) -> dict | None:
    """Read the last outcome file for a source, or None if absent."""
    path = Path(settings.raw_dir) / source / "_outcome.txt"
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return None


class StagingWriter:
    """Idempotent JSONL writer for a single source.

    Loads existing external_ids and content hashes once on init. New records are
    written, duplicates are skipped, and changed records are appended (updates).
    """

    def __init__(self, source: str):
        self.source = source
        self.dir = Path(settings.raw_dir) / source
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / "data.jsonl"
        self._ids: set[str] = set()
        self._hashes: dict[str, str] = {}
        self._count = 0
        self._new = 0
        self._updated = 0
        self._dup = 0
        self._load_existing()

    def _hash(self, record: dict[str, Any]) -> str:
        """Return a stable content hash for the record."""
        raw = record.get("raw") or {}
        payload = {
            "name": record.get("name", ""),
            "domain": record.get("domain", ""),
            "country": record.get("country", ""),
            "address": record.get("address", ""),
            "raw": sorted(raw.items()),
        }
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha1(data.encode("utf-8")).hexdigest()

    def _load_existing(self) -> None:
        """Index external_ids and content hashes already on disk."""
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                eid = record.get("external_id")
                if isinstance(eid, str):
                    self._ids.add(eid)
                    self._hashes[eid] = self._hash(record)
                    self._count += 1

    def write_detailed(self, record: dict[str, Any]) -> WriteResult:
        """Write a validated record and return whether it is new, a dup, or updated."""
        if not validate_record(record):
            return WriteResult(WriteResult.DUP)
        if record["source"] != self.source:
            return WriteResult(WriteResult.DUP)

        eid = record["external_id"]
        new_hash = self._hash(record)

        if eid in self._ids:
            if self._hashes.get(eid) == new_hash:
                self._dup += 1
                return WriteResult(WriteResult.DUP)
            # Content changed: append updated record and store latest hash.
            self._hashes[eid] = new_hash
            self._updated += 1
            status = WriteResult.UPDATED
        else:
            self._ids.add(eid)
            self._hashes[eid] = new_hash
            self._count += 1
            self._new += 1
            status = WriteResult.NEW

        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        return WriteResult(status)

    def write(self, record: dict[str, Any]) -> bool:
        """Write a validated record if its external_id is new or changed.

        Returns True if the record was newly written or appended as an update,
        False if it was a duplicate.
        """
        return bool(self.write_detailed(record))

    @property
    def count(self) -> int:
        """Total number of unique records staged for this source."""
        return self._count

    @property
    def new_count(self) -> int:
        """Records written as new during this writer session."""
        return self._new

    @property
    def updated_count(self) -> int:
        """Records appended as updates during this writer session."""
        return self._updated

    @property
    def dup_count(self) -> int:
        """Duplicate records skipped during this writer session."""
        return self._dup
