"""CAHA Artist Directory connector.

Parses the institutional CAHA PDF to seed talent and affiliations.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import pdfplumber

from connectors.base import Candidate, Connector, SourcePayload, registry
from connectors.utils import contact_key, discipline_tokens, normalize_phone

HEADER_MARKERS = (
    "caha artist directory",
    "name - discipline - email - phone",
    "discipline",
)
EMAIL_REGEX = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_REGEX = re.compile(
    r"(?:\+?\d{1,3}[-.\s]*)?(?:\(?\d{3}\)?[-.\s]*)?\d{3}[-.\s]*\d{4}"
)
SPLIT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\s+[\u2013\u2014\u2015\-]\s+"),
    re.compile(r"\s+\|\s+"),
    re.compile(r"\s{2,}"),
)


def _fixtures_path() -> Path:
    return Path(__file__).resolve().parent / "fixtures"


class CAHAPDFConnector(Connector):
    name = "caha_pdf"
    default_cadence = "0 0 1 */6 *"  # biannual

    def __init__(self, pdf_path: Path | None = None) -> None:
        self.pdf_path = pdf_path or _fixtures_path() / "caha_directory.pdf"

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        fetched_at = datetime.now(timezone.utc)
        content_hash = None
        if self.pdf_path.exists():
            content_hash = f"caha:{int(self.pdf_path.stat().st_mtime)}"
        return [
            SourcePayload(
                channel=self.name,
                url="https://www.guamcaha.org/artist-directory",
                kind="pdf",
                fetched_at=fetched_at,
                raw_blob_ptr=str(self.pdf_path),
                content_hash=content_hash,
                meta={
                    "institutional_anchor": True,
                    "description": "CAHA Artist Directory",
                },
            )
        ]

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        pdf_file = Path(source.raw_blob_ptr or "")
        if not pdf_file.exists():
            return []

        seen_contacts: set[tuple[str | None, str | None]] = set()
        candidates: list[Candidate] = []

        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for line in text.splitlines():
                    record = self._parse_line(line)
                    if not record:
                        continue
                    key = contact_key(record.get("email"), record.get("phone"))
                    if key and key in seen_contacts:
                        continue
                    if key:
                        seen_contacts.add(key)
                    metadata = {
                        "source": source.url,
                        "institutional_anchor": True,
                        "discipline": record.get("discipline"),
                        "disciplines": record.get("disciplines", []),
                    }
                    if record.get("email"):
                        metadata["email"] = record["email"]
                    if record.get("phone"):
                        metadata["phone"] = record["phone"]
                    if record.get("phone_normalized"):
                        metadata["phone_normalized"] = record["phone_normalized"]
                    candidates.append(
                        Candidate(
                            name=record["name"],
                            evidence=line.strip(),
                            channel=self.name,
                            metadata=metadata,
                        )
                    )

        return candidates

    def _parse_line(self, raw_line: str) -> dict[str, object] | None:
        line = raw_line.strip()
        if not line:
            return None
        lowered = line.lower()
        if any(marker in lowered for marker in HEADER_MARKERS):
            return None

        email_match = EMAIL_REGEX.search(line)
        email = email_match.group(0) if email_match else None
        phone_match = PHONE_REGEX.search(line)
        phone = phone_match.group(0) if phone_match else None

        cleaned = line
        for match in (email_match, phone_match):
            if match:
                cleaned = cleaned.replace(match.group(0), " ")
        cleaned = cleaned.strip(" -\u2013\u2014\u2015|,;")

        name, discipline = self._split_name_discipline(cleaned)
        if not name or not discipline:
            return None

        return {
            "name": name,
            "discipline": discipline,
            "disciplines": discipline_tokens(discipline),
            "email": email,
            "phone": phone,
            "phone_normalized": normalize_phone(phone),
        }

    def _split_name_discipline(self, value: str) -> tuple[str | None, str | None]:
        for pattern in SPLIT_PATTERNS:
            parts = [part.strip(" -\u2013\u2014\u2015|,;") for part in pattern.split(value) if part.strip()]
            if len(parts) >= 2:
                return parts[0], parts[1]
        if " - " in value:
            parts = [part.strip() for part in value.split(" - ") if part.strip()]
            if len(parts) >= 2:
                return parts[0], parts[1]
        return None, None


registry.register(CAHAPDFConnector())

