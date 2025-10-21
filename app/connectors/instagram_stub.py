"""Instagram connector stub for future implementation."""

from __future__ import annotations

from datetime import datetime
from typing import Sequence

from connectors.base import Candidate, Connector, SourcePayload, registry


class InstagramStubConnector:
    name = "instagram_stub"
    default_cadence = "0 8 * * *"  # daily

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        fetched_at = datetime.utcnow()
        return [
            SourcePayload(
                channel=self.name,
                url="https://www.instagram.com/theguamguide",
                kind="json",
                fetched_at=fetched_at,
                raw_blob_ptr=None,
            )
        ]

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        return []


registry.register(InstagramStubConnector())

