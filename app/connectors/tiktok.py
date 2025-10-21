"""TikTok connector implementation using fixture-backed payloads."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from connectors.base import Candidate, SourcePayload, registry


HANDLE_PATTERN = re.compile(r"@([\w.\u00C0-\u024F]{2,64})", flags=re.UNICODE)
HASHTAG_PATTERN = re.compile(r"#([\w.\u00C0-\u024F]+)", flags=re.UNICODE)


class TikTokConnector:
    """Connector that normalizes TikTok API-style responses."""

    name = "tiktok"
    default_cadence = "0 9 * * *"  # daily

    _FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "tiktok_sample.json"
    _SEED_HANDLES = ["@artspaceguahan", "@theguamguide"]
    _SEED_HASHTAGS = ["#GuamMusic", "#GuamArt", "#Guåhan", "#Decolonize"]

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        fetched_at = datetime.now(timezone.utc)
        return [
            SourcePayload(
                channel=self.name,
                url="https://www.tiktok.com/api/aweme/v1/feed/",
                kind="json",
                fetched_at=fetched_at,
                raw_blob_ptr=str(self._FIXTURE_PATH),
                meta={
                    "seed_handles": self._SEED_HANDLES,
                    "seed_hashtags": self._SEED_HASHTAGS,
                    "rate_limit": {
                        "window_seconds": 60,
                        "requests_remaining": 120,
                        "policy": "TikTok approved client sampled fixtures",
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

        videos = payload.get("aweme_list", [])
        candidates: list[Candidate] = []
        for video in videos:
            candidate = self._video_to_candidate(video, source)
            if candidate:
                candidates.append(candidate)
        return candidates

    def _video_to_candidate(self, video: dict[str, Any], source: SourcePayload) -> Candidate | None:
        description = (video.get("desc") or "").strip()
        aweme_id = video.get("aweme_id") or "tiktok-video"
        create_time = video.get("create_time")
        timestamp_iso: str | None = None
        if isinstance(create_time, (int, float)):
            timestamp_iso = datetime.fromtimestamp(create_time, tz=timezone.utc).isoformat()

        handles = self._normalize_handles(self._collect_handles(video, description))
        hashtags = self._normalize_hashtags(self._collect_hashtags(video, description))

        statistics = video.get("statistics") or {}
        engagement = {
            "digg_count": statistics.get("digg_count", 0),
            "comment_count": statistics.get("comment_count", 0),
            "share_count": statistics.get("share_count", 0),
            "play_count": statistics.get("play_count", 0),
        }

        location_info = video.get("location") or {}
        geotags: list[dict[str, Any]] = []
        if isinstance(location_info, dict) and location_info:
            geotags.append(
                {
                    "name": location_info.get("name"),
                    "latitude": location_info.get("latitude"),
                    "longitude": location_info.get("longitude"),
                }
            )

        metadata = {
            "aweme_id": aweme_id,
            "caption": description,
            "timestamp": timestamp_iso,
            "permalink": video.get("share_url"),
            "engagement": engagement,
            "geotags": geotags,
            "handles": handles,
            "hashtags": hashtags,
            "themes": [tag.lstrip("#").lower() for tag in hashtags],
            "seed_handle": video.get("author", {}).get("unique_id"),
            "raw_payload_ptr": source.raw_blob_ptr or str(self._FIXTURE_PATH),
        }

        evidence = description or f"TikTok video {aweme_id}"

        return Candidate(
            name=f"TikTok video {aweme_id}",
            evidence=evidence,
            channel=self.name,
            metadata=metadata,
        )

    def _collect_handles(self, video: dict[str, Any], description: str) -> list[str]:
        handles = HANDLE_PATTERN.findall(description)
        text_extra = video.get("text_extra")
        if isinstance(text_extra, list):
            for item in text_extra:
                handle = item.get("user_unique_id") or item.get("@user_id")
                if handle:
                    handles.append(handle)
        author = video.get("author", {})
        if isinstance(author, dict):
            author_id = author.get("unique_id")
            if author_id:
                handles.append(author_id)
        return handles

    def _collect_hashtags(self, video: dict[str, Any], description: str) -> list[str]:
        hashtags = HASHTAG_PATTERN.findall(description)
        text_extra = video.get("text_extra")
        if isinstance(text_extra, list):
            for item in text_extra:
                hashtag = item.get("hashtag_name")
                if hashtag:
                    hashtags.append(hashtag)
        return hashtags

    @staticmethod
    def _normalize_handles(handles: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for handle in handles:
            formatted = f"@{str(handle).lstrip('@')}"
            if formatted not in seen:
                seen.add(formatted)
                ordered.append(formatted)
        return ordered

    @staticmethod
    def _normalize_hashtags(hashtags: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for tag in hashtags:
            formatted = f"#{str(tag).lstrip('#')}"
            if formatted not in seen:
                seen.add(formatted)
                ordered.append(formatted)
        return ordered


registry.register(TikTokConnector())
