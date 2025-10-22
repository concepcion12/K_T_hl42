"""Utilities shared across connector implementations."""

from __future__ import annotations

import re
from typing import Iterable

DISCIPLINE_SPLIT_RE = re.compile(r"[,/;&]+")


def normalize_phone(phone: str | None) -> str | None:
    """Normalize phone numbers to digits-only form."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    return digits or None


def contact_key(email: str | None, phone: str | None) -> tuple[str | None, str | None] | None:
    """Generate a deduplication key based on email/phone."""
    email_norm = email.lower().strip() if email else None
    phone_norm = normalize_phone(phone)
    if not email_norm and not phone_norm:
        return None
    return (email_norm, phone_norm)


def discipline_tokens(value: str | None) -> list[str]:
    """Split discipline strings into a normalized list of tokens."""
    if not value:
        return []
    if isinstance(value, str):
        parts: Iterable[str] = DISCIPLINE_SPLIT_RE.split(value)
    else:
        parts = [str(value)]
    return [part.strip() for part in parts if part.strip()]
