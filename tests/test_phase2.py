"""Offline tests for Phase 2 entity resolution."""

import json
import os
import random
import resource
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from sqlmodel import create_engine, select

from app import db, resolve
from app.collect import staging


def _write_staging(raw_dir: Path, source: str, records: list[dict]) -> None:
    """Write staging records to raw/<source>/data.jsonl."""
    source_dir = raw_dir / source
    source_dir.mkdir(parents=True, exist_ok=True)
    path = source_dir / "data.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _fresh_db(monkeypatch):
    """Replace app.db.engine with a fresh in-memory SQLite DB."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    monkeypatch.setattr(db, "engine", engine)
    db.init_db()


def _setup_test_env(tmp_path: Path, monkeypatch):
    """Isolate both the SQLite DB and the DuckDB scratch database for a test."""
    raw_dir = tmp_path / "raw"
    monkeypatch.setattr(resolve.settings, "raw_dir", str(raw_dir))
    monkeypatch.setattr(staging.settings, "raw_dir", str(raw_dir))
    duckdb_path = tmp_path / "resolve.duckdb"
    monkeypatch.setattr(resolve.settings, "duckdb_path", str(duckdb_path))
    if duckdb_path.exists():
        duckdb_path.unlink()
    _fresh_db(monkeypatch)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("ACME Ltd.", "acme"),
        ("Café Söhne GmbH", "cafe sohne"),
        ("  The Private Pte. Co.  ", "the"),
        ("", ""),
        ("!!!", ""),
        ("AIRMEC CLIMATISATION LTEE", "airmec climatisation"),
    ],
)
def test_normalize_name(name, expected):
    assert resolve.normalize_name(name) == expected


def test_blocking_key_groups_by_country_and_first_token():
    rec1 = {"name": "Acme Ltd", "country": "GB"}
    rec2 = {"name": "Acme Analytics Inc", "country": "GB"}
    rec3 = {"name": "Acme Corp", "country": "US"}
    assert resolve.blocking_key(rec1) == resolve.blocking_key(rec2) == "GB|acme"
    assert resolve.blocking_key(rec3) == "US|acme"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("https://shop.acme.co.uk/path", "acme.co.uk"),
        ("http://www.acme.com", "acme.com"),
        ("acme.com.", "acme.com"),
        ("sub.sub.acme.io", "acme.io"),
        ("", ""),
        ("com", ""),
        ("localhost", ""),
    ],
)
def test_registrable_domain(value, expected):
    assert resolve.registrable_domain(value) == expected


def test_is_junk_name_rejects_junk():
    assert resolve.is_junk_name("") is True
    assert resolve.is_junk_name("a") is True
    assert resolve.is_junk_name("123") is True
    assert resolve.is_junk_name("n/a") is True
    assert resolve.is_junk_name("Acme") is False


def test_is_placeholder_domain_rejects_junk():
    assert resolve.is_placeholder_domain("example.com") is True
    assert resolve.is_placeholder_domain("localhost") is True
    assert resolve.is_placeholder_domain("acme.com") is False
    assert resolve.is_placeholder_domain("com") is True


def test_resolve_cross_key_backfill(tmp_path, monkeypatch):
    """A registry name-only record and a web domain record merge into one company with a domain."""
    _setup_test_env(tmp_path, monkeypatch)
    raw_dir = Path(resolve.settings.raw_dir)

    _write_staging(
        raw_dir,
        "canada_corporations",
        [
            {
                "source": "canada_corporations",
                "external_id": "R1",
                "name": "Acme Ltd",
                "domain": "",
                "country": "GB",
                "address": "1 Acme Way",
                "raw": {"id": "R1"},
                "fetched_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )
    _write_staging(
        raw_dir,
        "osm",
        [
            {
                "source": "osm",
                "external_id": "O1",
                "name": "Acme",
                "domain": "https://acme.co.uk",
                "country": "GB",
                "address": "",
                "raw": {"id": "O1"},
                "fetched_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )

    summary = resolve.resolve_all()
    assert summary["companies"] == 1
    assert summary["with_domain"] == 1

    with db.session() as session:
        company = session.exec(select(db.Company)).first()
        assert company is not None
        assert company.domain == "acme.co.uk"
        assert company.name == "Acme Ltd"
        assert company.country == "GB"
        sources = session.exec(select(db.CompanySource)).all()
        assert len(sources) == 2


def test_resolve_does_not_merge_different_companies_same_first_token(tmp_path, monkeypatch):
    """Records sharing a blocking key but with different full names stay separate."""
    _setup_test_env(tmp_path, monkeypatch)
    raw_dir = Path(resolve.settings.raw_dir)

    _write_staging(
        raw_dir,
        "canada_corporations",
        [
            {
                "source": "canada_corporations",
                "external_id": "R1",
                "name": "Acme Ltd",
                "domain": "",
                "country": "GB",
                "address": "",
                "raw": {"id": "R1"},
                "fetched_at": "2024-01-01T00:00:00+00:00",
            },
            {
                "source": "canada_corporations",
                "external_id": "R2",
                "name": "Acme Analytics Ltd",
                "domain": "",
                "country": "GB",
                "address": "",
                "raw": {"id": "R2"},
                "fetched_at": "2024-01-01T00:00:00+00:00",
            },
        ],
    )

    summary = resolve.resolve_all()
    assert summary["companies"] == 2


def test_resolve_all_is_idempotent(tmp_path, monkeypatch):
    """Running resolution twice produces the same company count and no duplicate sources."""
    _setup_test_env(tmp_path, monkeypatch)
    raw_dir = Path(resolve.settings.raw_dir)

    _write_staging(
        raw_dir,
        "canada_corporations",
        [
            {
                "source": "canada_corporations",
                "external_id": "R1",
                "name": "Acme Ltd",
                "domain": "",
                "country": "GB",
                "address": "",
                "raw": {"id": "R1"},
                "fetched_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )

    first = resolve.resolve_all()
    second = resolve.resolve_all()
    assert first["companies"] == second["companies"] == 1

    with db.session() as session:
        sources = session.exec(select(db.CompanySource)).all()
        assert len(sources) == 1


def test_no_junk_mega_merge(tmp_path, monkeypatch):
    """100 junk-name rows must not collapse into one merged blob."""
    _setup_test_env(tmp_path, monkeypatch)
    raw_dir = Path(resolve.settings.raw_dir)

    junk = [
        {
            "source": "common_crawl",
            "external_id": f"J{i}",
            "name": "-",
            "domain": "",
            "country": "US",
            "address": "",
            "raw": {"id": f"J{i}"},
            "fetched_at": "2024-01-01T00:00:00+00:00",
        }
        for i in range(100)
    ]
    real = [
        {
            "source": "canada_corporations",
            "external_id": "R1",
            "name": "Real One Ltd",
            "domain": "",
            "country": "US",
            "address": "",
            "raw": {"id": "R1"},
            "fetched_at": "2024-01-01T00:00:00+00:00",
        },
        {
            "source": "canada_corporations",
            "external_id": "R2",
            "name": "Real Two Ltd",
            "domain": "",
            "country": "US",
            "address": "",
            "raw": {"id": "R2"},
            "fetched_at": "2024-01-01T00:00:00+00:00",
        },
    ]
    _write_staging(raw_dir, "common_crawl", junk)
    _write_staging(raw_dir, "canada_corporations", real)

    summary = resolve.resolve_all()
    assert summary["companies"] == 102


def test_domain_variant_merge(tmp_path, monkeypatch):
    """Subdomain variants of the same registrable domain merge."""
    _setup_test_env(tmp_path, monkeypatch)
    raw_dir = Path(resolve.settings.raw_dir)

    _write_staging(
        raw_dir,
        "common_crawl",
        [
            {
                "source": "common_crawl",
                "external_id": "D1",
                "name": "Acme",
                "domain": "https://shop.acme.co.uk/checkout",
                "country": "GB",
                "address": "",
                "raw": {"id": "D1"},
                "fetched_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )
    _write_staging(
        raw_dir,
        "osm",
        [
            {
                "source": "osm",
                "external_id": "D2",
                "name": "Acme",
                "domain": "www.acme.co.uk",
                "country": "GB",
                "address": "",
                "raw": {"id": "D2"},
                "fetched_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )

    summary = resolve.resolve_all()
    assert summary["companies"] == 1
    assert summary["with_domain"] == 1


def test_order_independence(tmp_path, monkeypatch):
    """Shuffling the input records yields the same resolved company."""
    _setup_test_env(tmp_path, monkeypatch)
    raw_dir = Path(resolve.settings.raw_dir)

    records = [
        {
            "source": "common_crawl",
            "external_id": "C1",
            "name": "Acme",
            "domain": "https://acme.co.uk",
            "country": "GB",
            "address": "",
            "raw": {"id": "C1"},
            "fetched_at": "2024-01-01T00:00:00+00:00",
        },
        {
            "source": "canada_corporations",
            "external_id": "R1",
            "name": "Acme Ltd",
            "domain": "",
            "country": "GB",
            "address": "",
            "raw": {"id": "R1"},
            "fetched_at": "2024-01-01T00:00:00+00:00",
        },
    ]

    _write_staging(raw_dir, "all", records)
    first = resolve.resolve_all()

    _setup_test_env(tmp_path, monkeypatch)
    raw_dir = Path(resolve.settings.raw_dir)
    shuffled = list(records)
    random.shuffle(shuffled)
    _write_staging(raw_dir, "all", shuffled)
    second = resolve.resolve_all()

    assert first["companies"] == second["companies"] == 1
    assert first["with_domain"] == second["with_domain"] == 1

    with db.session() as session:
        company = session.exec(select(db.Company)).first()
        assert company.domain == "acme.co.uk"
        assert company.name == "Acme Ltd"


def test_incremental_resolves_only_new_rows(tmp_path, monkeypatch):
    """Adding one new staging row after a run resolves only that row."""
    _setup_test_env(tmp_path, monkeypatch)
    raw_dir = Path(resolve.settings.raw_dir)

    _write_staging(
        raw_dir,
        "canada_corporations",
        [
            {
                "source": "canada_corporations",
                "external_id": "R1",
                "name": "First Ltd",
                "domain": "",
                "country": "GB",
                "address": "",
                "raw": {"id": "R1"},
                "fetched_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )

    first = resolve.resolve_all()
    assert first["companies"] == 1
    assert first["staging_added"] == 1

    _write_staging(
        raw_dir,
        "canada_corporations",
        [
            {
                "source": "canada_corporations",
                "external_id": "R2",
                "name": "Second Ltd",
                "domain": "",
                "country": "GB",
                "address": "",
                "raw": {"id": "R2"},
                "fetched_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )

    second = resolve.resolve_all()
    assert second["companies"] == 2
    assert second["staging_added"] == 1


@pytest.mark.slow
@pytest.mark.skipif(os.environ.get("CI") == "true", reason="skip 100k scale smoke in CI")
def test_scale_smoke_stays_under_memory_bound(tmp_path, monkeypatch):
    """Generate 100k staging rows and assert bounded memory / completion."""
    _setup_test_env(tmp_path, monkeypatch)
    raw_dir = Path(resolve.settings.raw_dir)

    records = [
        {
            "source": "canada_corporations",
            "external_id": f"C{i}",
            "name": f"Company {i % 1000} Ltd",
            "domain": "",
            "country": "CA",
            "address": "",
            "raw": {"id": f"C{i}"},
            "fetched_at": "2024-01-01T00:00:00+00:00",
        }
        for i in range(100_000)
    ]
    _write_staging(raw_dir, "canada_corporations", records)

    before_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    summary = resolve.resolve_all()
    after_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    # Allow some headroom; macOS ru_maxrss is in bytes, Linux in kilobytes.
    delta_mb = (after_rss - before_rss) / (1024 * 1024)
    assert summary["companies"] == 1000
    assert delta_mb < 512, f"memory delta too large: {delta_mb:.1f} MB"
