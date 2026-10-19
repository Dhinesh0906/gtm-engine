"""Database utilities for the dashboard module."""

from app.db import Session, engine


def get_db():
    db = Session(engine)
    try:
        yield db
    finally:
        db.close()
