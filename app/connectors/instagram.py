"""Instagram connector implementation using fixture-backed payloads."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from connectors.base import Candidate, SourcePayload, registry


HANDLE_PATTERN = re.compile(r"@([\w.\u00C0-\u024F]{2,64})", flags=re.UNICODE)
HASHTAG_PATTERN = re.compile(r"#([\w.\u00C0-\u024F]+)", flags=re.UNICODE)


class InstagramConnector:
    """Connector that normalizes Instagram Graph API style responses."""

    name = "instagram"
    default_cadence = "0 8 * * *"  # daily

    _FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "instagram_sample.json"
    _SEED_ACCOUNTS = ["@theguamguide", "@artspaceguahan", "@eifestival"]
    _SEED_TAGS = ["#GuamArt", "#GuamMusic", "#ShopLocal", "#Guåhan"]

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        fetched_at = datetime.now(timezone.utc)
        return [
            SourcePayload(
                channel=self.name,
                url="https://graph.instagram.com/me/media",
                kind="json",
                fetched_at=fetched_at,
                raw_blob_ptr=str(self._FIXTURE_PATH),
                meta={
                    "seed_accounts": self._SEED_ACCOUNTS,
                    "seed_tags": self._SEED_TAGS,
                    "rate_limit": {
                        "window_seconds": 3600,
                        "requests_remaining": 500,
                        "policy": "Instagram Basic Display API sampled fixtures",
                    },
                },
            )
        ]

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        payload_path = Path(source.raw_blob_ptr or self._FIXTURE_PATH)
        if not payload_path.exists():
            return []

        try:
            with payload_path.open("r", encoding="utf-8") as fp:
                payload: dict[str, Any] = json.load(fp)
        except (OSError, json.JSONDecodeError):
            return []

        posts = payload.get("data", [])
        candidates: list[Candidate] = []

        for post in posts:
            candidate = self._post_to_candidate(post, source)
            if candidate:
                candidates.append(candidate)

        return candidates

    def _post_to_candidate(self, post: dict[str, Any], source: SourcePayload) -> Candidate | None:
        caption = (post.get("caption") or "").strip()
        permalink = post.get("permalink")
        post_id = post.get("id") or permalink or "instagram-post"

        timestamp_raw = post.get("timestamp")
        timestamp_iso: str | None
        if isinstance(timestamp_raw, str):
            try:
                timestamp_iso = (
                    datetime.fromisoformat(timestamp_raw.replace("Z", "+00:00"))
                    .astimezone(timezone.utc)
                    .isoformat()
                )
            except ValueError:
                timestamp_iso = None
        else:
            timestamp_iso = None

        handles = self._normalize_handles(self._extract_handles(caption))
        hashtags = self._normalize_hashtags(self._extract_hashtags(caption))

        location = post.get("location")
        geotags: list[dict[str, Any]] = []
        if isinstance(location, dict) and location:
            geotags.append(
                {
                    "name": location.get("name"),
                    "id": location.get("id"),
                    "latitude": location.get("latitude"),
                    "longitude": location.get("longitude"),
                }
            )

        metadata = {
            "post_id": post_id,
            "caption": caption,
            "permalink": permalink,
            "timestamp": timestamp_iso,
            "engagement": {
                "like_count": post.get("like_count", 0),
                "comments_count": post.get("comments_count", 0),
            },
            "geotags": geotags,
            "handles": handles,
            "hashtags": hashtags,
            "themes": [tag.lstrip("#").lower() for tag in hashtags],
            "seed_account": post.get("username"),
            "media_type": post.get("media_type"),
            "raw_payload_ptr": source.raw_blob_ptr or str(self._FIXTURE_PATH),
        }

        evidence = caption or f"Instagram post {post_id}"

        return Candidate(
            name=f"Instagram post {post_id}",
            evidence=evidence,
            channel=self.name,
            metadata=metadata,
        )

    @staticmethod
    def _extract_handles(text: str) -> list[str]:
        return HANDLE_PATTERN.findall(text)

    @staticmethod
    def _extract_hashtags(text: str) -> list[str]:
        return HASHTAG_PATTERN.findall(text)

    @staticmethod
    def _normalize_handles(handles: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for handle in handles:
            formatted = f"@{handle.lstrip('@')}"
            if formatted not in seen:
                seen.add(formatted)
                ordered.append(formatted)
        return ordered

    @staticmethod
    def _normalize_hashtags(hashtags: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for tag in hashtags:
            formatted = f"#{tag.lstrip('#')}"
            if formatted not in seen:
                seen.add(formatted)
                ordered.append(formatted)
        return ordered


registry.register(InstagramConnector())
