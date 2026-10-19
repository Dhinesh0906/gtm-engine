"""Read-only SQLite engine + Session factory for the data viewer.

Constructs a SEPARATE engine in true read-only mode (uri=file:...?mode=ro)
so no bug in the viewer can ever write to the production database.
"""

from __future__ import annotations

from sqlmodel import Session, create_engine

from app.config import settings


def _ro_db_url() -> str:
    """Build a read-only SQLite URI from settings.db_url."""
    url = settings.db_url
    # Strip the sqlite:/// prefix, then rebuild as a URI.
    if url.startswith("sqlite:///"):
        path = url[len("sqlite:///"):]
    elif url.startswith("sqlite://"):
        path = url[len("sqlite://"):]
    else:
        path = url
    return f"sqlite:///file:{path}?mode=ro&uri=true"


ro_engine = create_engine(_ro_db_url(), echo=False)


def get_db():
    db = Session(ro_engine)
    try:
        yield db
    finally:
        db.close()
