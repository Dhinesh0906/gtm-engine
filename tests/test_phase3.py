"""Offline tests for Phase 3 web-text fetcher."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from app import webtext as webtext_module
from app.config import settings


@pytest.fixture
def webtext_dir(tmp_path: Path, monkeypatch):
    """Point the webtext attempt cache at a temp directory."""
    monkeypatch.setattr(settings, "raw_dir", str(tmp_path / "raw"))
    # Ensure cache directory exists for the tests that write directly.
    (tmp_path / "raw" / "webtext").mkdir(parents=True, exist_ok=True)
    return tmp_path / "raw" / "webtext"


@pytest.fixture
def fresh_db(tmp_path: Path, monkeypatch):
    """Create an isolated SQLite DB for tests that mutate companies."""
    from sqlmodel import create_engine
    from app import db
    from app import webtext as wt

    db_path = tmp_path / "test.db"
    new_engine = create_engine(f"sqlite:///{db_path}", echo=False)
    db.SQLModel.metadata.create_all(new_engine)
    monkeypatch.setattr(db, "engine", new_engine)
    monkeypatch.setattr(wt, "engine", new_engine)
    monkeypatch.setattr(settings, "db_url", f"sqlite:///{db_path}")
    return new_engine


class TestHtmlToText:
    def test_strips_unwanted_tags(self):
        html = """
        <html><head><script>alert('x')</script><style>.x{}</style></head>
        <body>
        <nav>Nav</nav><header>Header</header>
        <main><p>Hello world</p></main>
        <footer>Footer</footer>
        </body></html>
        """
        text = webtext_module.html_to_text(html)
        assert "Hello world" in text
        assert "Nav" not in text
        assert "Header" not in text
        assert "Footer" not in text
        assert "alert" not in text
        assert ".x{}" not in text

    def test_collapses_whitespace(self):
        html = "<p>hello\n\n   world\t!</p>"
        assert webtext_module.html_to_text(html) == "hello world !"

    def test_caps_at_max_chars(self):
        html = "<p>" + "word " * 2000 + "</p>"
        text = webtext_module.html_to_text(html, max_chars=100)
        assert len(text) <= 100

    def test_empty_input(self):
        assert webtext_module.html_to_text("") == ""


class TestPickInternalLinks:
    def test_finds_informative_same_domain_links(self):
        html = """
        <a href="https://example.com/about-us">About</a>
        <a href="/services">Services</a>
        <a href="https://other.com/company">External</a>
        <a href="/contact">Contact</a>
        """
        links = webtext_module.pick_internal_links(html, "https://example.com")
        assert "https://example.com/about-us" in links
        assert "https://example.com/services" in links
        assert "https://other.com/company" not in links
        assert "https://example.com/contact" not in links
        assert len(links) <= 2

    def test_ignores_junk_and_duplicates(self):
        html = """
        <a href="/about">About</a>
        <a href="/about">About again</a>
        <a href="/login">Login</a>
        """
        links = webtext_module.pick_internal_links(html, "https://example.com", max_links=5)
        assert links == ["https://example.com/about"]


class TestAttemptCache:
    def test_ok_attempt_is_skipped(self, webtext_dir):
        domain = "example.com"
        webtext_module._record_attempt(domain, "ok", 1234)
        attempts = webtext_module._load_attempts()
        assert webtext_module._should_skip(attempts[domain]) is True

    def test_unreachable_is_retried_after_retry_days(self, webtext_dir, monkeypatch):
        domain = "dead.example"
        old = datetime.now(timezone.utc) - timedelta(days=settings.webtext_retry_days + 1)
        record = {
            "domain": domain,
            "status": "unreachable",
            "chars": 0,
            "attempted_at": old.isoformat(),
        }
        path = webtext_dir / "_attempts.jsonl"
        with path.open("w") as fh:
            fh.write(json.dumps(record) + "\n")

        attempts = webtext_module._load_attempts()
        assert webtext_module._should_skip(attempts[domain]) is False

    def test_unreachable_is_skipped_within_retry_days(self, webtext_dir):
        domain = "dead.example"
        recent = datetime.now(timezone.utc) - timedelta(days=1)
        record = {
            "domain": domain,
            "status": "unreachable",
            "chars": 0,
            "attempted_at": recent.isoformat(),
        }
        path = webtext_dir / "_attempts.jsonl"
        with path.open("w") as fh:
            fh.write(json.dumps(record) + "\n")

        attempts = webtext_module._load_attempts()
        assert webtext_module._should_skip(attempts[domain]) is True

    def test_compact_keeps_latest(self, webtext_dir):
        domain = "dup.example"
        for i in range(3):
            webtext_module._record_attempt(domain, "unreachable", 0)
        webtext_module._record_attempt(domain, "ok", 100)
        webtext_module._compact_attempts()
        attempts = webtext_module._load_attempts()
        assert attempts[domain]["status"] == "ok"
        assert attempts[domain]["chars"] == 100


class MockClient:
    """Simple httpx.Client mock for offline fetch tests."""

    def __init__(self, pages: dict[str, Any] | None = None, exc_on: list[str] | None = None):
        self.pages = pages or {}
        self.exc_on = set(exc_on or [])

    def get(self, url: str, **kwargs):
        if url in self.exc_on:
            raise httpx.ConnectError(f"Failed to connect to {url}")
        page = self.pages.get(url, {"status_code": 404, "text": "not found", "headers": {}})
        response = MagicMock()
        response.status_code = page.get("status_code", 200)
        response.text = page.get("text", "")
        response.headers = page.get("headers", {})
        response.url = page.get("url", url)
        if response.status_code >= 400:
            response.raise_for_status.side_effect = httpx.HTTPStatusError(
                "error",
                request=MagicMock(),
                response=response,
            )
        else:
            response.raise_for_status = MagicMock()
        return response

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class TestFetchDomainText:
    def test_successful_homepage_and_internal_pages(self, webtext_dir, monkeypatch):
        domain = "acme.example"
        homepage = """
        <html><head><title>Acme</title></head>
        <body><p>Welcome to Acme!</p>
        <a href="/about-us">About</a>
        <a href="/services">Services</a>
        </body></html>
        """
        about = "<html><body><p>We build things.</p></body></html>"
        services = "<html><body><p>Our services are great.</p></body></html>"
        pages = {
            f"https://{domain}": {
                "status_code": 200,
                "text": homepage,
                "headers": {"content-type": "text/html"},
                "url": f"https://{domain}/",
            },
            f"https://{domain}/about-us": {
                "status_code": 200,
                "text": about,
                "headers": {"content-type": "text/html"},
                "url": f"https://{domain}/about-us",
            },
            f"https://{domain}/services": {
                "status_code": 200,
                "text": services,
                "headers": {"content-type": "text/html"},
                "url": f"https://{domain}/services",
            },
        }
        monkeypatch.setattr(httpx, "Client", lambda: MockClient(pages=pages))

        text, status = webtext_module.fetch_domain_text(domain)
        assert status == "ok"
        assert "Welcome to Acme" in text
        assert "We build things" in text
        assert "Our services are great" in text

    def test_unreachable_does_not_propagate(self, webtext_dir, monkeypatch):
        domain = "dead.example"
        monkeypatch.setattr(
            httpx, "Client", lambda: MockClient(pages={}, exc_on=[f"https://{domain}", f"http://{domain}"])
        )
        text, status = webtext_module.fetch_domain_text(domain)
        assert status == "unreachable"
        assert text == ""

    def test_non_html_returns_empty(self, webtext_dir, monkeypatch):
        domain = "pdf.example"
        pages = {
            f"https://{domain}": {
                "status_code": 200,
                "text": "PDF content",
                "headers": {"content-type": "application/pdf"},
                "url": f"https://{domain}/",
            }
        }
        monkeypatch.setattr(httpx, "Client", lambda: MockClient(pages=pages))
        text, status = webtext_module.fetch_domain_text(domain)
        assert status == "empty"
        assert text == ""


class TestRunAndReport:
    def test_run_updates_companies_and_skips_cached(self, webtext_dir, fresh_db, monkeypatch):
        from app.db import Session
        from sqlmodel import select

        monkeypatch.setattr(settings, "webtext_delay", 0.0)

        homepage = "<html><body><p>We do cool things.</p></body></html>"
        pages = {
            "https://updated.example": {
                "status_code": 200,
                "text": homepage,
                "headers": {"content-type": "text/html"},
                "url": "https://updated.example/",
            }
        }
        monkeypatch.setattr(httpx, "Client", lambda: MockClient(pages=pages))

        with Session(fresh_db) as db:
            company = webtext_module.Company(domain="updated.example", name="Updated", country="US")
            db.add(company)
            db.commit()

        summary = webtext_module.run(limit=1)
        assert summary["attempted"] == 1
        assert summary["success"] == 1

        with Session(fresh_db) as db:
            company = db.exec(select(webtext_module.Company).where(webtext_module.Company.domain == "updated.example")).first()
            assert company is not None
            assert "cool things" in company.web_text

        # Second run should skip because web_text is no longer empty.
        summary2 = webtext_module.run(limit=1)
        assert summary2["attempted"] == 0

    def test_report_outputs_numbers(self, webtext_dir):
        data = webtext_module.report()
        assert "total" in data
        assert "with_domain" in data
        assert "with_text" in data
        assert data["with_text"] <= data["with_domain"]
