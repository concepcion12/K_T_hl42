"""TikTok connector stub for future implementation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

from connectors.base import Candidate, Connector, SourcePayload, registry


class TikTokStubConnector:
    name = "tiktok_stub"
    default_cadence = "0 9 * * *"  # daily

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        fetched_at = datetime.now(timezone.utc)
        return [
            SourcePayload(
                channel=self.name,
                url="https://www.tiktok.com/@artspaceguahan",
                kind="json",
                fetched_at=fetched_at,
                raw_blob_ptr=None,
                meta={
                    "status": "stub",
                    "note": "Cadence planned for daily pulls once compliant API access is available",
                    "seed_hashtags": [
                        "#GuamMusic",
                        "#GuamArt",
                        "#Guåhan",
                        "#Decolonize",
                    ],
                },
            )
        ]

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        return []


registry.register(TikTokStubConnector())

