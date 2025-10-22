"""Events and festival connectors."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import httpx
from bs4 import BeautifulSoup

from connectors.base import Candidate, Connector, SourcePayload, registry
from connectors.storage import ObjectRecord, ObjectStore


def _default_fetcher(url: str) -> bytes:
    response = httpx.get(url, follow_redirects=True, timeout=10)
    response.raise_for_status()
    return response.content


class EventsConnector:
    name = "events"
    default_cadence = "0 5 * * 1"  # weekly Monday

    def __init__(
        self,
        fetcher: Callable[[str], bytes] | None = None,
        object_store: ObjectStore | None = None,
    ) -> None:
        self._fetcher = fetcher or _default_fetcher
        self._object_store = object_store or ObjectStore(Path(".object_store") / self.name)
        self._target_url = "https://www.guamtime.net"

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
        return [
            SourcePayload(
                channel=self.name,
                url=self._target_url,
                kind="html",
                fetched_at=record.fetched_at,
                content_hash=record.content_hash,
                raw_blob_ptr=str(payload_path),
                meta={
                    "cadence": "weekly",
                    "channels": [
                        "GuamTime",
                        "The Guam Guide",
                        "Visit Guam",
                        "Festival rosters",
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
        for event in soup.select("[data-event-id]"):
            event_name = (event.get("data-event-name") or "").strip()
            if not event_name:
                title_el = event.select_one(".event-title") or event.find("h2")
                if title_el:
                    event_name = title_el.get_text(strip=True)
            event_date = _normalise_date(event.get("data-event-date"))
            if not event_date:
                time_tag = event.find("time")
                if time_tag and time_tag.get("datetime"):
                    event_date = _normalise_date(time_tag.get("datetime"))
            venue = (event.get("data-venue") or "").strip()
            if not venue:
                venue_el = event.select_one(".venue")
                if venue_el:
                    venue = venue_el.get_text(strip=True)
            source_url = event.get("data-source-url") or source.url or self._target_url

            evidence_base = f"{event_name or 'Event'} at {venue}".strip()
            context_meta = {
                "event_name": event_name,
                "event_date": event_date,
                "venue": venue,
                "source": source.url or self._target_url,
                "source_url": source_url,
                "event_signal": True,
            }

            performer_list = event.select("ul[data-role='performer'] li, .performers li")
            vendor_list = event.select("ul[data-role='vendor'] li, .vendors li")

            candidates.extend(
                _event_candidates(
                    performer_list,
                    role="performer",
                    evidence_base=evidence_base,
                    metadata=context_meta,
                    channel=self.name,
                )
            )
            candidates.extend(
                _event_candidates(
                    vendor_list,
                    role="vendor",
                    evidence_base=evidence_base,
                    metadata=context_meta,
                    channel=self.name,
                )
            )

        return candidates


registry.register(EventsConnector())


def _normalise_date(value: str | None) -> str | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y"):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        else:
            return raw
    if parsed.tzinfo:
        parsed = parsed.astimezone(timezone.utc)
    if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0 and parsed.microsecond == 0:
        return parsed.date().isoformat()
    return parsed.isoformat()


def _event_candidates(
    elements,  # type: ignore[no-untyped-def]
    *,
    role: str,
    evidence_base: str,
    metadata: dict[str, object],
    channel: str,
) -> list[Candidate]:
    results: list[Candidate] = []
    for element in elements:
        name = element.get_text(strip=True)
        if not name:
            continue
        meta = dict(metadata)
        meta.update({"role": role})
        results.append(
            Candidate(
                name=name,
                evidence=f"{name} listed as {role} for {evidence_base}",
                channel=channel,
                metadata=meta,
            )
        )
    return results

