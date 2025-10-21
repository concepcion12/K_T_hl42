"""Events and festival connectors."""

from __future__ import annotations

from datetime import datetime
from typing import Sequence

from connectors.base import Candidate, Connector, SourcePayload, registry


class EventsConnector:
    name = "events"
    default_cadence = "0 5 * * 1"  # weekly Monday

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        fetched_at = datetime.utcnow()
        return [
            SourcePayload(
                channel=self.name,
                url="https://www.guamtime.net",
                kind="html",
                fetched_at=fetched_at,
                raw_blob_ptr="connectors/fixtures/events_sample.html",
            )
        ]

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        candidates: list[Candidate] = []
        path = source.raw_blob_ptr
        if not path:
            return candidates
        try:
            with open(path, "r", encoding="utf-8") as fp:
                content = fp.readlines()
        except FileNotFoundError:
            return candidates

        for line in content:
            line = line.strip()
            if not line:
                continue
            candidates.append(
                Candidate(
                    name=line,
                    evidence=line,
                    channel=self.name,
                    metadata={
                        "source": source.url,
                        "event_signal": True,
                    },
                )
            )
        return candidates


registry.register(EventsConnector())

