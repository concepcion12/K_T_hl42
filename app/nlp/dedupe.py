"""Simple dedupe utilities."""

from __future__ import annotations

from difflib import SequenceMatcher


def exact_match(a: str, b: str) -> bool:
    return a.strip().lower() == b.strip().lower()


def fuzzy_match(a: str, b: str, threshold: float = 0.85) -> bool:
    ratio = SequenceMatcher(None, a.lower(), b.lower()).ratio()
    return ratio >= threshold

