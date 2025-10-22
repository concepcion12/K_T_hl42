"""Guam Artspace residency connector."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from bs4 import BeautifulSoup

from connectors.base import Candidate, Connector, SourcePayload, registry
from connectors.utils import contact_key, discipline_tokens


def _fixtures_path() -> Path:
    return Path(__file__).resolve().parent / "fixtures"


class ArtspaceConnector(Connector):
    name = "artspace"
    default_cadence = "30 1 * * 0"  # weekly Sunday night

    def __init__(self, html_path: Path | None = None) -> None:
        self.html_path = html_path or _fixtures_path() / "artspace_residents.html"

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        fetched_at = datetime.now(timezone.utc)
        content_hash = None
        if self.html_path.exists():
            content_hash = f"artspace:{int(self.html_path.stat().st_mtime)}"
        return [
            SourcePayload(
                channel=self.name,
                url="https://artspaceguam.org/residents",
                kind="html",
                fetched_at=fetched_at,
                raw_blob_ptr=str(self.html_path),
                content_hash=content_hash,
                meta={
                    "institutional_anchor": True,
                    "description": "Artspace residency roster",
                },
            )
        ]

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        html_file = Path(source.raw_blob_ptr or "")
        if not html_file.exists():
            return []

        soup = BeautifulSoup(html_file.read_text(encoding="utf-8"), "html.parser")
        cards = soup.select("article.artist-card")

        seen_contacts: set[tuple[str | None, str | None]] = set()
        candidates: list[Candidate] = []

        for card in cards:
            name_node = card.find("h2")
            discipline_node = card.find(class_="discipline")
            email_node = card.find("a", class_="email")
            phone_node = card.find(class_="phone")

            name = name_node.get_text(strip=True) if name_node else ""
            if not name:
                continue

            discipline_text = discipline_node.get_text(strip=True) if discipline_node else None
            email = None
            if email_node and email_node.has_attr("href"):
                href = email_node["href"]
                if href.lower().startswith("mailto:"):
                    email = href.split(":", 1)[1]
                else:
                    email = email_node.get_text(strip=True) or None
            phone = phone_node.get_text(strip=True) if phone_node else None

            key = contact_key(email, phone)
            if key and key in seen_contacts:
                continue
            if key:
                seen_contacts.add(key)

            disciplines = discipline_tokens(discipline_text)
            metadata: dict[str, object] = {
                "source": source.url,
                "institutional_anchor": True,
                "program": card.get("data-program"),
                "disciplines": disciplines,
            }
            if discipline_text:
                metadata["discipline"] = discipline_text
            if email:
                metadata["email"] = email
            if phone:
                metadata["phone"] = phone

            evidence_bits = [name]
            if discipline_text:
                evidence_bits.append(discipline_text)
            if email:
                evidence_bits.append(email)

            candidates.append(
                Candidate(
                    name=name,
                    evidence=" | ".join(evidence_bits),
                    channel=self.name,
                    metadata=metadata,
                )
            )

        return candidates


registry.register(ArtspaceConnector())
