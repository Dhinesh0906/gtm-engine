"""Per-campaign extended product brief (fields with no DB column).

Stored as JSON at BRIEF_DIR/<campaign_id>.json. The campaigns table keeps the
core brief (name/summary/value_props/proof_points); this holds the rest.
Backward-compatible: load_brief() returns {} when no file exists, so every
caller keeps working on campaigns that were never interviewed.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from app.config import settings


def _brief_dir() -> Path:
    """Return the brief storage directory, creating it if needed."""
    path = Path(settings.brief_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def brief_path(campaign_id: int) -> str:
    """Return the filesystem path for a campaign's brief file."""
    return str(_brief_dir() / f"{campaign_id}.json")


def load_brief(campaign_id: int) -> dict[str, Any]:
    """Load a campaign's brief. Returns {} if missing or unreadable (never raises)."""
    path = Path(brief_path(campaign_id))
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return {}
        return data
    except (json.JSONDecodeError, OSError):
        return {}


def save_brief(campaign_id: int, data: dict[str, Any]) -> None:
    """Persist a campaign's brief atomically (write .tmp then rename)."""
    directory = _brief_dir()
    tmp_path = directory / f"{campaign_id}.json.tmp"
    final_path = directory / f"{campaign_id}.json"
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    os.replace(str(tmp_path), str(final_path))


def brief_context(campaign_id: int) -> str:
    """Render the brief into a compact text block for prompts.

    Only includes non-empty fields. Returns "" when the brief is empty or
    has no meaningful content.
    """
    brief = load_brief(campaign_id)
    if not brief:
        return ""

    lines: list[str] = []

    mapping: list[tuple[str, str]] = [
        ("pricing", "Pricing"),
        ("icp", "Ideal customer"),
        ("differentiators", "What makes it different"),
        # The objections a real prospect raises, and the honest answer to each.
        # Kept as brief DATA rather than prompt text so they can be corrected
        # from experience without touching code - and so a wrong rebuttal can
        # never become a claim the product does not support.
        ("objections", "Objections you will get, and the honest answer"),
        ("cta", "Call to action"),
        ("tone", "Tone"),
        ("sender_name", "From"),
    ]

    for key, label in mapping:
        value = str(brief.get(key, "")).strip()
        if not value:
            continue
        if key in ("differentiators", "objections"):
            lines.append(f"{label}:")
            for item in value.split("\n"):
                item = item.strip()
                if item:
                    lines.append(f"  - {item}")
        elif key == "sender_name":
            sender_title = str(brief.get("sender_title", "")).strip()
            if sender_title:
                lines.append(f"{label}: {value}, {sender_title}")
            else:
                lines.append(f"{label}: {value}")
        elif key == "cta":
            cta_link = str(brief.get("cta_link", "")).strip()
            if cta_link:
                lines.append(f"{label}: {value} (link: {cta_link})")
            else:
                lines.append(f"{label}: {value}")
        else:
            lines.append(f"{label}: {value}")

    locked = brief.get("locked_facts")
    if locked and isinstance(locked, list) and len(locked) > 0:
        lines.append("Approved facts you MAY use:")
        for fact in locked:
            fact = str(fact).strip()
            if fact:
                lines.append(f"  - {fact}")

    must_not = brief.get("must_not_claim")
    if must_not and isinstance(must_not, list) and len(must_not) > 0:
        lines.append("NEVER claim / must avoid:")
        for item in must_not:
            item = str(item).strip()
            if item:
                lines.append(f"  - {item}")

    if not lines:
        return ""

    return "\n".join(lines)


def violates_guardrail(text: str, campaign_id: int) -> list[str]:
    """Return any must_not_claim entries that appear as case-insensitive
    substrings in text. Returns [] when clean or no brief exists.
    """
    brief = load_brief(campaign_id)
    must_not = brief.get("must_not_claim")
    if not isinstance(must_not, list):
        return []
    lowered = text.lower()
    hits: list[str] = []
    for phrase in must_not:
        phrase = str(phrase).strip()
        if phrase and phrase.lower() in lowered:
            hits.append(phrase)
    return hits
