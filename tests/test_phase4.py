"""Offline tests for Phase 4 AI enrichment."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app import enrich as enrich_module
from app.config import settings
from app.db import Company, Session, engine


@pytest.fixture
def fresh_db(tmp_path: Path, monkeypatch):
    """Create an isolated SQLite DB for tests that mutate companies."""
    from sqlmodel import create_engine
    from app import db

    db_path = tmp_path / "test.db"
    new_engine = create_engine(f"sqlite:///{db_path}", echo=False)
    db.SQLModel.metadata.create_all(new_engine)
    monkeypatch.setattr(db, "engine", new_engine)
    monkeypatch.setattr(enrich_module, "engine", new_engine)
    monkeypatch.setattr(settings, "db_url", f"sqlite:///{db_path}")
    return new_engine


class TestCoerceEnrichment:
    def test_clamps_scores(self):
        data = {
            "b2b_score": 1.7,
            "saas_score": -0.2,
            "startup_score": "x",
            "tech_score": 0.5,
        }
        out = enrich_module.coerce_enrichment(data)
        assert out["b2b_score"] == 1.0
        assert out["saas_score"] == 0.0
        assert out["startup_score"] is None
        assert out["tech_score"] == 0.5

    def test_coerces_size_estimate(self):
        assert enrich_module.coerce_enrichment({"size_estimate": "50"})["size_estimate"] == 50
        assert enrich_module.coerce_enrichment({"size_estimate": ""})["size_estimate"] is None
        assert enrich_module.coerce_enrichment({"size_estimate": None})["size_estimate"] is None
        assert enrich_module.coerce_enrichment({"size_estimate": "abc"})["size_estimate"] is None

    def test_size_range_normalization(self):
        assert enrich_module.coerce_enrichment({"size_range": "11-50"})["size_range"] == "11-50"
        assert enrich_module.coerce_enrichment({"size_range": "51 to 200"})["size_range"] == "51-200"
        assert enrich_module.coerce_enrichment({"size_range": "1000+"})["size_range"] == "1000+"
        assert enrich_module.coerce_enrichment({"size_range": "unknown"})["size_range"] == ""

    def test_missing_keys_never_raise(self):
        out = enrich_module.coerce_enrichment({})
        assert out["description"] == ""
        assert out["b2b_score"] is None

    def test_truncates_long_text(self):
        data = {"description": "x" * 2000}
        out = enrich_module.coerce_enrichment(data)
        assert len(out["description"]) == 1000


class TestEnrichCompany:
    def test_enriches_and_sets_timestamp(self, fresh_db, monkeypatch):
        from sqlmodel import select

        monkeypatch.setattr(
            enrich_module,
            "cheap_json",
            lambda *a, **kw: {
                "description": "Cloud accounting software.",
                "keywords": "accounting, cloud, saas",
                "industry_nace": "J - Information and communication",
                "size_estimate": 250,
                "size_range": "201-1000",
                "b2b_score": 0.9,
                "saas_score": 1.0,
                "startup_score": 0.3,
                "tech_score": 0.8,
            },
        )

        with Session(fresh_db) as db:
            company = Company(
                name="Acme",
                country="US",
                web_text="We make cloud accounting software for small businesses. " * 20,
            )
            db.add(company)
            db.commit()
            company_id = company.id

        status = enrich_module.enrich_company(company_id)
        assert status == "enriched"

        with Session(fresh_db) as db:
            company = db.get(Company, company_id)
            assert company.enriched_at is not None
            assert company.description == "Cloud accounting software."
            assert company.industry_nace == "J - Information and communication"
            assert company.size_estimate == 250
            assert company.size_range == "201-1000"
            assert company.b2b_score == 0.9
            assert company.saas_score == 1.0
            assert company.startup_score == 0.3
            assert company.tech_score == 0.8

    def test_skips_already_enriched(self, fresh_db, monkeypatch):
        monkeypatch.setattr(enrich_module, "cheap_json", lambda *a, **kw: {"description": "x"})

        with Session(fresh_db) as db:
            company = Company(
                name="Acme",
                country="US",
                web_text="...",
                enriched_at="2026-01-01T00:00:00+00:00",
            )
            db.add(company)
            db.commit()
            company_id = company.id

        status = enrich_module.enrich_company(company_id)
        assert status == "skipped"

    def test_skips_no_web_text(self, fresh_db, monkeypatch):
        monkeypatch.setattr(enrich_module, "cheap_json", lambda *a, **kw: {"description": "x"})

        with Session(fresh_db) as db:
            company = Company(name="Acme", country="US", web_text="")
            db.add(company)
            db.commit()
            company_id = company.id

        status = enrich_module.enrich_company(company_id)
        assert status == "skipped"

    def test_junk_response_does_not_set_enriched_at(self, fresh_db, monkeypatch):
        monkeypatch.setattr(enrich_module, "cheap_json", lambda *a, **kw: {})

        with Session(fresh_db) as db:
            company = Company(
                name="Acme",
                country="US",
                web_text="This is a real company homepage with plenty of meaningful text. " * 20,
            )
            db.add(company)
            db.commit()
            company_id = company.id

        status = enrich_module.enrich_company(company_id)
        assert status == "failed"

        with Session(fresh_db) as db:
            company = db.get(Company, company_id)
            assert company.enriched_at is None

    def test_brain_failure_does_not_crash(self, fresh_db, monkeypatch):
        def boom(*a, **kw):
            raise RuntimeError("api down")

        monkeypatch.setattr(enrich_module, "cheap_json", boom)

        with Session(fresh_db) as db:
            company = Company(
                name="Acme",
                country="US",
                web_text="We sell high quality widgets to customers worldwide. " * 20,
            )
            db.add(company)
            db.commit()
            company_id = company.id

        status = enrich_module.enrich_company(company_id)
        assert status == "failed"

        with Session(fresh_db) as db:
            company = db.get(Company, company_id)
            assert company.enriched_at is None


class TestParkedDetector:
    def test_for_sale_page_is_parked(self):
        assert enrich_module.looks_parked_or_thin("This domain is for sale. Buy it now!") is True

    def test_parked_page_with_domain_parking(self):
        assert enrich_module.looks_parked_or_thin("This domain is parked at Sedo.") is True

    def test_short_body_is_parked(self):
        assert enrich_module.looks_parked_or_thin("Welcome to our site.") is True

    def test_nginx_welcome_page_is_parked(self):
        assert enrich_module.looks_parked_or_thin("Welcome to nginx! If you see this page...") is True

    def test_real_product_page_is_not_parked(self):
        text = (
            "0--o - Code Optimization & Protection - Minify, Optimize, Obfuscate. "
            "Trusted by 1,000+ developers worldwide. Protect & Optimize Your Code. "
            "Upload your website, choose your protection level, and download production-ready code. "
            "Reduce file size by up to 70%. Start Free Documentation. No credit card required. "
        ) * 20
        assert len(text) > 600
        assert enrich_module.looks_parked_or_thin(text) is False

    def test_url_shortener_is_parked(self):
        assert enrich_module.looks_parked_or_thin("Shortcut links. GenerateIt! Shorten your URL.") is True


class TestParkedEnrichment:
    def test_enrich_company_does_not_call_brain_for_parked_page(self, fresh_db, monkeypatch):
        calls: list[Any] = []

        def fake_brain(*a, **kw):
            calls.append(1)
            return {"description": "Should not be written"}

        monkeypatch.setattr(enrich_module, "cheap_json", fake_brain)

        with Session(fresh_db) as db:
            company = Company(name="", country="US", domain="junk.example", web_text="This domain is for sale.")
            db.add(company)
            db.commit()
            company_id = company.id

        status = enrich_module.enrich_company(company_id)
        assert status == "parked"
        assert len(calls) == 0

        with Session(fresh_db) as db:
            company = db.get(Company, company_id)
            assert company.enriched_at is not None
            assert company.description == ""
            assert company.keywords == ""
            assert company.b2b_score is None
            assert company.saas_score is None
            assert company.startup_score is None
            assert company.tech_score is None

    def test_enrich_company_calls_brain_for_real_page(self, fresh_db, monkeypatch):
        calls: list[Any] = []

        def fake_brain(*a, **kw):
            calls.append(1)
            return {
                "description": "Cloud accounting software.",
                "keywords": "accounting, cloud",
                "b2b_score": 0.8,
                "saas_score": 0.9,
            }

        monkeypatch.setattr(enrich_module, "cheap_json", fake_brain)

        with Session(fresh_db) as db:
            company = Company(name="Acme", country="US", web_text="We make cloud accounting software. " * 50)
            db.add(company)
            db.commit()
            company_id = company.id

        status = enrich_module.enrich_company(company_id)
        assert status == "enriched"
        assert len(calls) == 1

        with Session(fresh_db) as db:
            company = db.get(Company, company_id)
            assert company.description == "Cloud accounting software."


class TestRecheck:
    def test_recheck_clears_description_for_parked_page(self, fresh_db, monkeypatch):
        with Session(fresh_db) as db:
            company = Company(
                name="",
                country="US",
                domain="junk.example",
                web_text="This domain is for sale.",
                description="A company that does amazing things.",
                keywords="amazing, things",
                enriched_at="2026-01-01T00:00:00+00:00",
            )
            db.add(company)
            db.commit()

        summary = enrich_module.recheck()
        assert summary["checked"] == 1
        assert summary["cleaned"] == 1

        with Session(fresh_db) as db:
            company = db.get(Company, 1)
            assert company.description == ""
            assert company.keywords == ""
            assert company.enriched_at is not None

    def test_recheck_keeps_real_description(self, fresh_db, monkeypatch):
        with Session(fresh_db) as db:
            company = Company(
                name="Acme",
                country="US",
                web_text="We make cloud accounting software. " * 50,
                description="Cloud accounting software.",
                keywords="accounting, cloud",
                enriched_at="2026-01-01T00:00:00+00:00",
            )
            db.add(company)
            db.commit()

        summary = enrich_module.recheck()
        assert summary["checked"] == 1
        assert summary["cleaned"] == 0

        with Session(fresh_db) as db:
            company = db.get(Company, 1)
            assert company.description == "Cloud accounting software."


class TestRunAndReport:
    def test_run_enriches_up_to_limit(self, fresh_db, monkeypatch):
        from sqlmodel import select

        monkeypatch.setattr(settings, "enrich_delay", 0.0)
        monkeypatch.setattr(
            enrich_module,
            "cheap_json",
            lambda *a, **kw: {
                "description": "A company.",
                "b2b_score": 0.5,
                "saas_score": 0.5,
                "startup_score": 0.5,
                "tech_score": 0.5,
            },
        )

        with Session(fresh_db) as db:
            for i in range(5):
                db.add(Company(name=f"C{i}", country="US", web_text=f"Company {i} does real business things. " * 20))
            db.commit()

        counts = enrich_module.run(limit=2, batch_size=1)
        assert counts["enriched"] == 2

        with Session(fresh_db) as db:
            enriched = len(db.exec(select(Company).where(Company.enriched_at.is_not(None))).all())
            assert enriched == 2

    def test_run_counts_parked_pages(self, fresh_db, monkeypatch):
        from sqlmodel import select

        monkeypatch.setattr(settings, "enrich_delay", 0.0)
        calls: list[Any] = []

        def fake_brain(*a, **kw):
            calls.append(1)
            return {"description": "A company.", "b2b_score": 0.5}

        monkeypatch.setattr(enrich_module, "cheap_json", fake_brain)

        with Session(fresh_db) as db:
            db.add(Company(name="Real", country="US", web_text="We do real things. " * 50))
            db.add(Company(name="Parked", country="US", web_text="This domain is for sale."))
            db.commit()

        counts = enrich_module.run(batch_size=1)
        assert counts["enriched"] == 1
        assert counts["parked"] == 1
        assert len(calls) == 1  # Brain called only for the real page

    def test_idempotent_second_run(self, fresh_db, monkeypatch):
        from sqlmodel import select

        monkeypatch.setattr(settings, "enrich_delay", 0.0)
        calls: list[Any] = []

        def fake_brain(*a, **kw):
            calls.append(1)
            return {"description": "A company.", "b2b_score": 0.5}

        monkeypatch.setattr(enrich_module, "cheap_json", fake_brain)

        with Session(fresh_db) as db:
            db.add(Company(name="C", country="US", web_text="Company C does real business things. " * 20))
            db.commit()

        enrich_module.run(limit=1, batch_size=1)
        assert len(calls) == 1

        enrich_module.run(limit=1, batch_size=1)
        assert len(calls) == 1  # not called again

        with Session(fresh_db) as db:
            enriched = len(db.exec(select(Company).where(Company.enriched_at.is_not(None))).all())
            assert enriched == 1

    def test_report_outputs_numbers(self, fresh_db):
        data = enrich_module.report()
        assert "total" in data
        assert "with_web_text" in data
        assert "enriched" in data
        assert "avg_scores" in data
