"""CAHA Artist Directory connector.

Parses the institutional CAHA PDF to seed talent and affiliations.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Sequence

import pdfplumber

from connectors.base import Candidate, Connector, SourcePayload, registry


class CAHAPDFConnector:
    name = "caha_pdf"
    default_cadence = "0 0 1 */6 *"  # biannual

    def __init__(self, pdf_path: Path | None = None) -> None:
        self.pdf_path = pdf_path or Path("connectors/fixtures/caha_sample.pdf")

    def fetch(self, since: datetime | None) -> Sequence[SourcePayload]:
        fetched_at = datetime.utcnow()
        content_hash = f"caha:{int(self.pdf_path.stat().st_mtime)}"
        return [
            SourcePayload(
                channel=self.name,
                url=str(self.pdf_path),
                kind="pdf",
                fetched_at=fetched_at,
                raw_blob_ptr=str(self.pdf_path),
                content_hash=content_hash,
            )
        ]

    def extract(self, source: SourcePayload) -> Sequence[Candidate]:
        candidates: list[Candidate] = []
        pdf_file = Path(source.raw_blob_ptr or "")
        if not pdf_file.exists():
            return candidates

        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for line in text.splitlines():
                    if "Discipline" in line:
                        continue
                    if line.strip():
                        candidates.append(
                            Candidate(
                                name=line.strip(),
                                evidence=line.strip(),
                                channel=self.name,
                                metadata={
                                    "source": source.url,
                                    "institutional_anchor": True,
                                },
                            )
                        )
        return candidates


registry.register(CAHAPDFConnector())

