"""Reddit r/guam connector for community endorsements."""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import httpx
from bs4 import BeautifulSoup

from connectors.base import Candidate, Connector, SourcePayload, registry
from connectors.storage import ObjectRecord, ObjectStore


HANDLE_REGEX = re.compile(r"@([A-Za-z0-9_.]{2,30})")
URL_REGEX = re.compile(r"(https?://\S+)")


def _default_fetcher(url: str) -> bytes:
    response = httpx.get(
        url,
        headers={"User-Agent": "hl42-collector/1.0"},
        follow_redirects=True,
        timeout=10,
    )
    response.raise_for_status()
    return response.content


class RedditConnector:
    name = "reddit"
    default_cadence = "0 6 * * *"  # daily morning

    def __init__(
        self,
        fetcher: Callable[[str], bytes] | None = None,
        object_store: ObjectStore | None = None,
    ) -> None:
        self._fetcher = fetcher or _default_fetcher
        self._object_store = object_store or ObjectStore(Path(".object_store") / self.name)
        self._target_url = "https://www.reddit.com/r/guam"

    def _store_or_get(self, since: datetime | None) -> ObjectRecord | None:
        cached = self._object_store.get(self._target_url)
        if cached and since and cached.fetched_at > since:
            return cached

        payload = self._fetcher(self._target_url)
        fetched_at = datetime.now(timezone.utc)
        return self._object_store.store(self._target_url, payload, fetched_at=fetched_at)

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        record = self._store_or_get(since)
        if not record:
            return []

        payload_path = record.path_within(self._object_store.root)
        fetched_at = record.fetched_at
        return [
            SourcePayload(
                channel=self.name,
                url=self._target_url,
                kind="html",
                fetched_at=fetched_at,
                content_hash=record.content_hash,
                raw_blob_ptr=str(payload_path),
                meta={
                    "community_signal": "reddit",
                    "saved_searches": [
                        "artist",
                        "band",
                        "market vendor",
                        "tattoo",
                        "weaver",
                    ],
                },
            )
        ]

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        path = source.raw_blob_ptr
        if not path:
            return []

        try:
            html = Path(path).read_text(encoding="utf-8")
        except FileNotFoundError:
            return []

        soup = BeautifulSoup(html, "html.parser")
        candidates: list[Candidate] = []
        for article in soup.select("article[data-permalink]") or [soup]:
            permalink = article.get("data-permalink") or source.url or self._target_url
            post_upvotes = _parse_int(article.get("data-upvotes"))
            post_text = " ".join(article.stripped_strings)
            post_urls = set(_extract_urls(article)) | set(URL_REGEX.findall(post_text))
            post_handles = set(HANDLE_REGEX.findall(post_text))

            candidates.extend(
                _handle_candidates(
                    post_handles,
                    context="post",
                    permalink=permalink,
                    upvotes=post_upvotes,
                    source_url=source.url or self._target_url,
                    channel=self.name,
                )
            )
            candidates.extend(
                _url_candidates(
                    post_urls,
                    context="post",
                    permalink=permalink,
                    upvotes=post_upvotes,
                    source_url=source.url or self._target_url,
                    channel=self.name,
                )
            )

            for comment in article.select("[data-comment-id]"):
                comment_permalink = comment.get("data-permalink") or permalink
                comment_upvotes = _parse_int(comment.get("data-upvotes"))
                comment_text = " ".join(comment.stripped_strings)
                comment_handles = set(HANDLE_REGEX.findall(comment_text))
                comment_urls = set(_extract_urls(comment)) | set(URL_REGEX.findall(comment_text))

                candidates.extend(
                    _handle_candidates(
                        comment_handles,
                        context="comment",
                        permalink=comment_permalink,
                        upvotes=comment_upvotes,
                        source_url=source.url or self._target_url,
                        channel=self.name,
                    )
                )
                candidates.extend(
                    _url_candidates(
                        comment_urls,
                        context="comment",
                        permalink=comment_permalink,
                        upvotes=comment_upvotes,
                        source_url=source.url or self._target_url,
                        channel=self.name,
                    )
                )

        return candidates


registry.register(RedditConnector())


def _parse_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _extract_urls(element) -> set[str]:  # type: ignore[no-untyped-def]
    urls: set[str] = set()
    for anchor in element.find_all("a"):
        href = (anchor.get("href") or "").strip()
        if href.startswith("http://") or href.startswith("https://"):
            urls.add(href)
    return urls


def _handle_candidates(
    handles: set[str],
    *,
    context: str,
    permalink: str,
    upvotes: int | None,
    source_url: str,
    channel: str,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    for handle in handles:
        candidates.append(
            Candidate(
                name=f"@{handle}",
                evidence=f"Reddit {context} mention of @{handle}",
                channel=channel,
                metadata={
                    "handle": handle,
                    "source": source_url,
                    "community_signal": "reddit",
                    "thread_permalink": permalink,
                    "context": context,
                    "endorsement_score": upvotes,
                },
            )
        )
    return candidates


def _url_candidates(
    urls: set[str],
    *,
    context: str,
    permalink: str,
    upvotes: int | None,
    source_url: str,
    channel: str,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    for url in urls:
        candidates.append(
            Candidate(
                name=url,
                evidence=f"Reddit {context} shared URL: {url}",
                channel=channel,
                metadata={
                    "url": url,
                    "source": source_url,
                    "community_signal": "reddit",
                    "thread_permalink": permalink,
                    "context": context,
                    "endorsement_score": upvotes,
                },
            )
        )
    return candidates

