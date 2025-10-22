"""University of Guam fine arts & faculty connector."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from connectors.base import Candidate, Connector, SourcePayload, registry
from connectors.utils import contact_key, discipline_tokens


def _fixtures_path() -> Path:
    return Path(__file__).resolve().parent / "fixtures"


class UOGConnector(Connector):
    name = "uog"
    default_cadence = "15 3 1 * *"  # monthly

    def __init__(self, csv_path: Path | None = None) -> None:
        self.csv_path = csv_path or _fixtures_path() / "uog_directory.csv"

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        fetched_at = datetime.now(timezone.utc)
        content_hash = None
        if self.csv_path.exists():
            content_hash = f"uog:{int(self.csv_path.stat().st_mtime)}"
        return [
            SourcePayload(
                channel=self.name,
                url="https://www.uog.edu/fine-arts/directory",
                kind="csv",
                fetched_at=fetched_at,
                raw_blob_ptr=str(self.csv_path),
                content_hash=content_hash,
                meta={
                    "institutional_anchor": True,
                    "description": "UOG College of Liberal Arts directory",
                },
            )
        ]

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        csv_file = Path(source.raw_blob_ptr or "")
        if not csv_file.exists():
            return []

        seen_contacts: set[tuple[str | None, str | None]] = set()
        candidates: list[Candidate] = []

        with csv_file.open("r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                name = (row.get("Name") or "").strip()
                if not name:
                    continue
                email = (row.get("Email") or "").strip() or None
                phone = (row.get("Phone") or "").strip() or None
                key = contact_key(email, phone)
                if key and key in seen_contacts:
                    continue
                if key:
                    seen_contacts.add(key)

                department = (row.get("Department") or "").strip() or None
                campus = (row.get("Campus") or "").strip() or None

                metadata: dict[str, object] = {
                    "source": source.url,
                    "institutional_anchor": True,
                    "department": department,
                    "campus": campus,
                    "disciplines": discipline_tokens(department),
                }
                if email:
                    metadata["email"] = email
                if phone:
                    metadata["phone"] = phone

                evidence_bits = [name]
                if department:
                    evidence_bits.append(department)
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


registry.register(UOGConnector())
