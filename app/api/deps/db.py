"""Database session dependencies."""

from collections.abc import Generator

from fastapi import Depends

from models import SessionLocal


def get_db() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

