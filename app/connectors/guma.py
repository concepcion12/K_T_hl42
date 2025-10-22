"""Guam Unique Merchandise & Artisans (GUMA) connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from connectors.base import Candidate, Connector, SourcePayload, registry
from connectors.utils import contact_key, discipline_tokens


def _fixtures_path() -> Path:
    return Path(__file__).resolve().parent / "fixtures"


class GumaConnector(Connector):
    name = "guma"
    default_cadence = "0 2 * * 1"  # weekly Monday

    def __init__(self, data_path: Path | None = None) -> None:
        self.data_path = data_path or _fixtures_path() / "guma_directory.json"

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        fetched_at = datetime.now(timezone.utc)
        content_hash = None
        if self.data_path.exists():
            content_hash = f"guma:{int(self.data_path.stat().st_mtime)}"
        return [
            SourcePayload(
                channel=self.name,
                url="https://guma.guam.gov/directory",
                kind="json",
                fetched_at=fetched_at,
                raw_blob_ptr=str(self.data_path),
                content_hash=content_hash,
                meta={
                    "institutional_anchor": True,
                    "description": "GUMA member roster",
                },
            )
        ]

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        path = Path(source.raw_blob_ptr or "")
        if not path.exists():
            return []

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []

        artists = data.get("artists", [])
        seen_contacts: set[tuple[str | None, str | None]] = set()
        candidates: list[Candidate] = []

        for artist in artists:
            name = (artist.get("name") or "").strip()
            if not name:
                continue
            email = artist.get("email")
            phone = artist.get("phone")
            key = contact_key(email, phone)
            if key and key in seen_contacts:
                continue
            if key:
                seen_contacts.add(key)

            disciplines_value = artist.get("disciplines") or artist.get("discipline")
            if isinstance(disciplines_value, list):
                disciplines_list = [str(item).strip() for item in disciplines_value if str(item).strip()]
                discipline_primary = disciplines_list[0] if disciplines_list else None
            else:
                discipline_primary = str(disciplines_value).strip() if disciplines_value else None
                disciplines_list = discipline_tokens(discipline_primary)

            metadata: dict[str, object] = {
                "source": source.url,
                "institutional_anchor": True,
                "program": artist.get("program"),
                "disciplines": disciplines_list,
            }
            if discipline_primary:
                metadata["discipline"] = discipline_primary
            if email:
                metadata["email"] = email
            if phone:
                metadata["phone"] = phone

            evidence_parts = [name]
            if discipline_primary:
                evidence_parts.append(discipline_primary)
            if email:
                evidence_parts.append(email)

            candidates.append(
                Candidate(
                    name=name,
                    evidence=" | ".join(evidence_parts),
                    channel=self.name,
                    metadata=metadata,
                )
            )

        return candidates


registry.register(GumaConnector())
