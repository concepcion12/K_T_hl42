"""Scoring utilities for candidates and talent."""

from __future__ import annotations

from datetime import datetime
from typing import Any


INSTITUTIONAL_ANCHORS = {"caha_pdf", "guma", "artspace", "uog"}
COMMUNITY_CHANNELS = {"reddit", "events"}
SOCIAL_CHANNELS = {"instagram_stub", "tiktok_stub"}


def score_candidate(candidate_channel: str, metadata: dict[str, Any]) -> tuple[float, dict[str, float]]:
    breakdown = {"institutional": 0.0, "community": 0.0, "social": 0.0, "recency": 0.0}
    if candidate_channel in INSTITUTIONAL_ANCHORS or metadata.get("institutional_anchor"):
        breakdown["institutional"] = 40.0
    if candidate_channel in COMMUNITY_CHANNELS or metadata.get("community_signal"):
        breakdown["community"] = 30.0
    if candidate_channel in SOCIAL_CHANNELS:
        breakdown["social"] = 20.0
    breakdown["recency"] = 10.0
    total = sum(breakdown.values())
    return total, breakdown


def recency_weight(first_seen: datetime, now: datetime | None = None) -> float:
    now = now or datetime.utcnow()
    delta_days = (now - first_seen).days
    if delta_days <= 30:
        return 1.0
    if delta_days <= 90:
        return 0.5
    return 0.2

