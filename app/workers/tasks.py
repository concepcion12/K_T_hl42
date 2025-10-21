"""RQ task definitions."""

from __future__ import annotations

from datetime import datetime, timezone

from connectors.base import registry
from models import SessionLocal, Source, Run


def run_connector(connector_name: str) -> None:
    connector = registry.get(connector_name)
    started_at = datetime.now(timezone.utc)

    with SessionLocal() as session:
        run = Run(connector=connector_name, started_at=started_at, status="running")
        session.add(run)
        session.commit()

        sources = connector.fetch(None)
        item_total = 0
        for source_payload in sources:
            source = Source(
                channel=source_payload.channel,
                url=source_payload.url,
                kind=source_payload.kind,
                fetched_at=source_payload.fetched_at,
                content_hash=source_payload.content_hash,
                raw_blob_ptr=source_payload.raw_blob_ptr,
                meta=source_payload.meta,
            )
            session.add(source)
            session.flush()

            candidates = connector.extract(source_payload)
            item_total += len(candidates)
        run.item_count = item_total
        run.status = "success"
        run.finished_at = datetime.now(timezone.utc)
        session.commit()

