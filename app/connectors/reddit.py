"""Reddit r/guam connector for community endorsements."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Sequence

from connectors.base import Candidate, Connector, SourcePayload, registry


HANDLE_REGEX = re.compile(r"@([A-Za-z0-9_.]{2,30})")
URL_REGEX = re.compile(r"(https?://\S+)")


class RedditConnector:
    name = "reddit"
    default_cadence = "0 6 * * *"  # daily morning

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        fetched_at = datetime.utcnow()
        return [
            SourcePayload(
                channel=self.name,
                url="https://www.reddit.com/r/guam",
                kind="html",
                fetched_at=fetched_at,
                raw_blob_ptr="connectors/fixtures/reddit_sample.html",
            )
        ]

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        candidates: list[Candidate] = []
        fixture_path = source.raw_blob_ptr
        if not fixture_path:
            return candidates

        try:
            with open(fixture_path, "r", encoding="utf-8") as fp:
                content = fp.read()
        except FileNotFoundError:
            return candidates

        handles = HANDLE_REGEX.findall(content)
        urls = URL_REGEX.findall(content)
        for handle in handles:
            candidates.append(
                Candidate(
                    name=f"@{handle}",
                    evidence=f"Mentioned in Reddit thread: @{handle}",
                    channel=self.name,
                    metadata={
                        "handle": handle,
                        "source": source.url,
                        "community_signal": "reddit",
                    },
                )
            )
        for url in urls:
            candidates.append(
                Candidate(
                    name=url,
                    evidence=f"URL shared on Reddit: {url}",
                    channel=self.name,
                    metadata={
                        "url": url,
                        "source": source.url,
                        "community_signal": "reddit",
                    },
                )
            )

        return candidates


registry.register(RedditConnector())

