"""Basic taggers for disciplines and themes."""

from __future__ import annotations

from typing import Iterable, Set


DISCIPLINE_KEYWORDS = {
    "weaving": {"weaver", "weaving", "banig"},
    "music": {"band", "dj", "singer", "music"},
    "visual": {"art", "exhibit", "gallery", "painter"},
    "activist": {"decolonize", "activist", "advocate"},
}

THEME_KEYWORDS = {
    "decolonization": {"decolonize", "self-determination"},
    "weaving": {"weaving", "banig"},
    "sinahi": {"sinahi"},
}


def detect_disciplines(text: str) -> Set[str]:
    lowered = text.lower()
    tags = {discipline for discipline, keywords in DISCIPLINE_KEYWORDS.items() if any(kw in lowered for kw in keywords)}
    return tags


def detect_themes(text: str) -> Set[str]:
    lowered = text.lower()
    tags = {theme for theme, keywords in THEME_KEYWORDS.items() if any(kw in lowered for kw in keywords)}
    return tags

