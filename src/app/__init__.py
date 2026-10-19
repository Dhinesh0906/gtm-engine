"""Raliess application package."""

from sqlmodel import select

from app.brain import cheap
from app.db import (
    Assignment,
    Campaign,
    Company,
    CompanySource,
    ContactHistory,
    ContactPoint,
    Message,
    Person,
    Segment,
    SendLog,
    Suppression,
    init_db,
    session,
)


TABLE_MODELS = [
    Company,
    CompanySource,
    Person,
    ContactPoint,
    Campaign,
    Segment,
    Assignment,
    Message,
    Suppression,
    SendLog,
    ContactHistory,
]


def selftest() -> None:
    """Run the Phase 0 self-test.

    Creates the database tables, verifies all 11 tables are queryable, and
    confirms the brain interface is wired for demo mode.
    """
    from app.config import settings

    init_db()

    with session() as db:
        for model in TABLE_MODELS:
            db.exec(select(model).limit(1))

    assert settings.demo, "Phase 0 expects demo mode (no OPENCODE_GO_API_KEY set)"
    assert cheap("", demo_value="ok") == "ok"

    print("PHASE0 OK: 11 tables, brain interface live, demo mode")
