"""Basic taggers for disciplines and themes."""

from __future__ import annotations

from typing import Mapping, Set


# Discipline keywords intentionally cast a wide net so that we can discover
# market-ready creative talent (music, visual art, craft, culinary, etc.) and
# community organizers who may not explicitly label themselves as activists.
DISCIPLINE_KEYWORDS: Mapping[str, Set[str]] = {
    "music": {
        "band",
        "choir",
        "composer",
        "dj",
        "guitarist",
        "instrumentalist",
        "musician",
        "music",
        "producer",
        "rapper",
        "singer",
        "songwriter",
        "vocalist",
    },
    "visual": {
        "art exhibit",
        "artist",
        "canvas",
        "gallery",
        "illustrator",
        "muralist",
        "painter",
        "photographer",
        "photography",
        "printmaker",
        "sculptor",
        "visual artist",
    },
    "craft": {
        "artisan",
        "carver",
        "ceramic",
        "craft",
        "handmade",
        "jeweler",
        "maker",
        "potter",
        "textile",
        "weaving",
        "woodwork",
    },
    "culinary": {
        "baker",
        "chef",
        "culinary",
        "food truck",
        "pastry",
        "pop-up dinner",
        "private chef",
    },
    "performing": {
        "actor",
        "choreographer",
        "dance",
        "dancer",
        "performer",
        "poet",
        "storyteller",
        "theatre",
        "theater",
    },
    "activist": {
        "advocate",
        "community organizer",
        "environmental justice",
        "food security",
        "grassroots",
        "land defender",
        "mutual aid",
        "organizer",
        "prutehi",
        "social justice",
        "sustainability",
    },
    "weaving": {"weaver", "weaving", "banig"},
}

THEME_KEYWORDS: Mapping[str, Set[str]] = {
    "decolonization": {
        "decolonize",
        "land back",
        "self-determination",
    },
    "weaving": {"weaving", "banig"},
    "sinahi": {"sinahi"},
    "food_security": {"food security", "food sovereignty", "community garden"},
    "sustainability": {"sustainability", "climate justice", "regenerative", "zero waste"},
    "cultural_preservation": {
        "cultural preservation",
        "culture keeper",
        "heritage",
        "language revitalization",
        "tradition bearer",
    },
    "community_wellness": {"mutual aid", "community care", "wellness workshop"},
}


def detect_disciplines(text: str) -> Set[str]:
    lowered = text.lower()
    tags = {
        discipline
        for discipline, keywords in DISCIPLINE_KEYWORDS.items()
        if any(kw in lowered for kw in keywords)
    }
    return tags


def detect_themes(text: str) -> Set[str]:
    lowered = text.lower()
    tags = {
        theme
        for theme, keywords in THEME_KEYWORDS.items()
        if any(kw in lowered for kw in keywords)
    }
    return tags

