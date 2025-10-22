"""Simple local object store for connector artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Dict


@dataclass
class ObjectRecord:
    """Metadata describing a stored object."""

    filename: str
    fetched_at: datetime
    content_hash: str

    def path_within(self, root: Path) -> Path:
        return root / self.filename


class ObjectStore:
    """A lightweight file-backed object store.

    The store keeps a manifest (JSON) alongside the stored payloads. Objects are
    addressed by an arbitrary key (typically a URL). Subsequent lookups can
    return the cached record without re-fetching remote content.
    """

    def __init__(self, root: str | Path | None = None) -> None:
        self.root = Path(root) if root else Path(".object_store")
        self.root.mkdir(parents=True, exist_ok=True)
        self._manifest_path = self.root / "manifest.json"
        self._manifest: Dict[str, ObjectRecord] = {}
        if self._manifest_path.exists():
            self._load_manifest()

    def _load_manifest(self) -> None:
        raw = json.loads(self._manifest_path.read_text(encoding="utf-8"))
        manifest: Dict[str, ObjectRecord] = {}
        for key, entry in raw.items():
            fetched_at = datetime.fromisoformat(entry["fetched_at"])
            manifest[key] = ObjectRecord(
                filename=entry["filename"],
                fetched_at=fetched_at,
                content_hash=entry["content_hash"],
            )
        self._manifest = manifest

    def _write_manifest(self) -> None:
        serialised = {
            key: {
                "filename": record.filename,
                "fetched_at": record.fetched_at.isoformat(),
                "content_hash": record.content_hash,
            }
            for key, record in self._manifest.items()
        }
        self._manifest_path.write_text(
            json.dumps(serialised, indent=2, sort_keys=True), encoding="utf-8"
        )

    def get(self, key: str) -> ObjectRecord | None:
        return self._manifest.get(key)

    def store(
        self, key: str, payload: bytes, fetched_at: datetime | None = None
    ) -> ObjectRecord:
        timestamp = fetched_at or datetime.now(timezone.utc)
        digest = sha256(payload).hexdigest()
        filename = f"{digest}.html"
        path = self.root / filename
        path.write_bytes(payload)
        record = ObjectRecord(filename=filename, fetched_at=timestamp, content_hash=digest)
        self._manifest[key] = record
        self._write_manifest()
        return record

