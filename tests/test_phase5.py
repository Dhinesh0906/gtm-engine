"""Offline tests for Phase 5 semantic search / lookalike."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from app import search as search_module
from app.config import settings
from app.db import Company, Session, engine


@pytest.fixture
def fresh_db(tmp_path: Path, monkeypatch):
    """Create an isolated SQLite DB for tests that mutate companies."""
    from sqlmodel import SQLModel, create_engine
    from app import db

    db_path = tmp_path / "test.db"
    new_engine = create_engine(f"sqlite:///{db_path}", echo=False)
    db.SQLModel.metadata.create_all(new_engine)
    monkeypatch.setattr(db, "engine", new_engine)
    monkeypatch.setattr(search_module, "engine", new_engine)
    monkeypatch.setattr(settings, "db_url", f"sqlite:///{db_path}")
    # Keep index files inside temp dir as well.
    monkeypatch.setattr(settings, "vector_index_path", str(tmp_path / "vectors.usearch"))
    monkeypatch.setattr(settings, "vector_metadata_path", str(tmp_path / "vectors_metadata.jsonl"))
    return new_engine


class TestEmbedText:
    def test_enriched_company_uses_description_and_keywords(self, fresh_db):
        c = Company(
            name="Acme",
            country="US",
            description="Cloud accounting software.",
            keywords="accounting, cloud, saas",
            enriched_at="2026-01-01T00:00:00+00:00",
        )
        text = search_module.company_embed_text(c)
        assert "Cloud accounting software" in text
        assert "accounting, cloud, saas" in text

    def test_unenriched_company_uses_name_and_web_text(self, fresh_db):
        c = Company(name="Acme", country="US", web_text="We make widgets and gadgets.")
        text = search_module.company_embed_text(c)
        assert "Acme" in text
        assert "We make widgets" in text

    def test_fallback_never_empty(self, fresh_db):
        c = Company(name="", country="")
        text = search_module.company_embed_text(c)
        assert text and text.startswith("company")


class TestIndexAndSearch:
    def _make_deterministic_embedder(self, monkeypatch):
        """Replace embed_texts so each text maps to a deterministic unit vector."""
        vectors: dict[str, np.ndarray] = {}

        def fake_embed(texts: list[str]) -> np.ndarray:
            out = np.zeros((len(texts), search_module.DIM), dtype=np.float32)
            for i, t in enumerate(texts):
                if t not in vectors:
                    vec = np.zeros(search_module.DIM, dtype=np.float32)
                    # Use a hash of the text to seed a deterministic sparse vector.
                    h = hash(t) % search_module.DIM
                    vec[h] = 1.0
                    # Also spread a little to avoid collisions.
                    vec[(h + 1) % search_module.DIM] = 0.5
                    vec /= np.linalg.norm(vec)
                    vectors[t] = vec
                out[i] = vectors[t]
            return out

        monkeypatch.setattr(search_module, "embed_texts", fake_embed)

    def test_build_index_and_search_ranking(self, fresh_db, monkeypatch):
        self._make_deterministic_embedder(monkeypatch)

        with Session(fresh_db) as db:
            for i, name in enumerate(["Alpha", "Beta", "Gamma"]):
                db.add(
                    Company(
                        name=name,
                        country="US",
                        description=f"Company {name} does machine learning.",
                        keywords="ml, ai",
                        enriched_at="2026-01-01T00:00:00+00:00",
                    )
                )
            db.commit()

        counts = search_module.build_index()
        assert counts["indexed"] == 3
        assert counts["skipped"] == 0

        results = search_module.search("machine learning", k=3)
        assert len(results) == 3
        # Results should be sorted by descending similarity.
        scores = [r["score"] for r in results]
        assert scores == sorted(scores, reverse=True)
        names = {r["name"] for r in results}
        assert names == {"Alpha", "Beta", "Gamma"}

    def test_country_filter(self, fresh_db, monkeypatch):
        self._make_deterministic_embedder(monkeypatch)

        with Session(fresh_db) as db:
            db.add(
                Company(
                    name="Paris Cafe",
                    country="FR",
                    description="A cozy cafe in Paris serving coffee.",
                    web_text="Coffee in Paris",
                )
            )
            db.add(
                Company(
                    name="Berlin Cafe",
                    country="DE",
                    description="A cafe in Berlin serving coffee.",
                    web_text="Coffee in Berlin",
                )
            )
            db.commit()

        search_module.build_index()
        results = search_module.search("coffee", k=10, country="FR")
        assert len(results) == 1
        assert results[0]["name"] == "Paris Cafe"

    def test_lookalike_excludes_seed(self, fresh_db, monkeypatch):
        self._make_deterministic_embedder(monkeypatch)

        with Session(fresh_db) as db:
            db.add(Company(name="Alpha", country="US", description="Machine learning platform"))
            db.add(Company(name="Beta", country="US", description="Machine learning tools"))
            db.add(Company(name="Gamma", country="US", description="Cooking recipes app"))
            db.commit()
            # IDs are 1, 2, 3.

        search_module.build_index()
        results = search_module.lookalike(1, k=2)
        ids = {r["company_id"] for r in results}
        assert 1 not in ids
        assert len(results) <= 2

    def test_incremental_build_skips_existing(self, fresh_db, monkeypatch):
        self._make_deterministic_embedder(monkeypatch)

        with Session(fresh_db) as db:
            db.add(
                Company(
                    name="Only",
                    country="US",
                    description="A company that does something",
                    web_text="something",
                )
            )
            db.commit()

        first = search_module.build_index()
        assert first["indexed"] == 1

        second = search_module.build_index()
        assert second["indexed"] == 0
        assert second["skipped"] == 1


class TestTargetable:
    def test_description_only_is_targetable(self, fresh_db):
        c = Company(name="Acme", description="Cloud accounting software.")
        assert search_module.is_targetable(c) is True

    def test_web_text_only_is_not_targetable_by_default(self, fresh_db):
        c = Company(name="Acme", web_text="We make widgets.")
        assert search_module.is_targetable(c) is False

    def test_web_text_only_is_targetable_in_text_mode(self, fresh_db, monkeypatch):
        monkeypatch.setattr(settings, "index_min_signal", "text")
        c = Company(name="Acme", web_text="We make widgets.")
        assert search_module.is_targetable(c) is True

    def test_name_only_is_not_targetable(self, fresh_db):
        c = Company(name="Acme Inc")
        assert search_module.is_targetable(c) is False

    def test_empty_company_is_not_targetable(self, fresh_db):
        c = Company(name="")
        assert search_module.is_targetable(c) is False

    def test_enriched_mode_requires_enriched_at(self, fresh_db, monkeypatch):
        monkeypatch.setattr(settings, "index_min_signal", "enriched")
        enriched = Company(name="Acme", description="Cloud accounting software.", enriched_at="2026-01-01T00:00:00+00:00")
        plain = Company(name="Beta", description="Cloud accounting software.")
        assert search_module.is_targetable(enriched) is True
        assert search_module.is_targetable(plain) is False


class TestBuildIndexTargetable:
    def _make_deterministic_embedder(self, monkeypatch):
        vectors: dict[str, np.ndarray] = {}

        def fake_embed(texts: list[str]) -> np.ndarray:
            out = np.zeros((len(texts), search_module.DIM), dtype=np.float32)
            for i, t in enumerate(texts):
                if t not in vectors:
                    vec = np.zeros(search_module.DIM, dtype=np.float32)
                    h = hash(t) % search_module.DIM
                    vec[h] = 1.0
                    vec[(h + 1) % search_module.DIM] = 0.5
                    vec /= np.linalg.norm(vec)
                    vectors[t] = vec
                out[i] = vectors[t]
            return out

        monkeypatch.setattr(search_module, "embed_texts", fake_embed)

    def test_build_index_skips_name_only_and_web_text_only_companies(self, fresh_db, monkeypatch):
        self._make_deterministic_embedder(monkeypatch)

        with Session(fresh_db) as db:
            db.add(Company(name="NameOnly", country="US"))
            db.add(Company(name="Described", country="US", description="Machine learning SaaS"))
            db.add(Company(name="WebOnly", country="US", web_text="We sell coffee online."))
            db.commit()

        counts = search_module.build_index()
        assert counts["indexed"] == 1
        assert counts["skipped"] == 2

        index, meta = search_module._load_index_and_meta()
        assert index is not None
        assert len(index) == 1
        indexed_ids = set(meta.keys())
        assert 2 in indexed_ids  # Described is id 2
        assert 1 not in indexed_ids
        assert 3 not in indexed_ids

    def test_incremental_promotion_when_company_gains_description(self, fresh_db, monkeypatch):
        self._make_deterministic_embedder(monkeypatch)

        with Session(fresh_db) as db:
            db.add(Company(name="Later", country="US"))
            db.commit()

        first = search_module.build_index()
        assert first["indexed"] == 0
        assert first["skipped"] == 1

        # Promote the company by adding a real description (enrichment result).
        with Session(fresh_db) as db:
            company = db.get(Company, 1)
            company.description = "Machine learning platform for retail."
            company.web_text = "Now we have text about machine learning."
            company.enriched_at = "2026-01-01T00:00:00+00:00"
            db.commit()

        second = search_module.build_index()
        assert second["indexed"] == 1
        assert second["skipped"] == 0

        index, meta = search_module._load_index_and_meta()
        assert len(index) == 1
        assert 1 in meta

    def test_text_mode_indexes_web_text_only_companies(self, fresh_db, monkeypatch):
        self._make_deterministic_embedder(monkeypatch)
        monkeypatch.setattr(settings, "index_min_signal", "text")

        with Session(fresh_db) as db:
            db.add(Company(name="NameOnly", country="US"))
            db.add(Company(name="WebOnly", country="US", web_text="We sell coffee online."))
            db.commit()

        counts = search_module.build_index(rebuild=True)
        assert counts["indexed"] == 1
        assert counts["skipped"] == 1

        index, meta = search_module._load_index_and_meta()
        assert len(index) == 1
        indexed_ids = set(meta.keys())
        assert 2 in indexed_ids  # WebOnly
        assert 1 not in indexed_ids  # NameOnly

    def test_all_signal_mode_indexes_everyone(self, fresh_db, monkeypatch):
        self._make_deterministic_embedder(monkeypatch)
        monkeypatch.setattr(settings, "index_min_signal", "all")

        with Session(fresh_db) as db:
            db.add(Company(name="NameOnly", country="US"))
            db.add(Company(name="Described", country="US", description="ML"))
            db.commit()

        counts = search_module.build_index(rebuild=True)
        assert counts["indexed"] == 2
        assert counts["skipped"] == 0


class TestReport:
    def test_report_outputs_numbers(self, fresh_db):
        data = search_module.report()
        assert data == {"total": 0, "enriched": 0, "targetable": 0, "indexed": 0}
