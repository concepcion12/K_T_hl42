"""RQ task definitions."""

from __future__ import annotations

import logging
import traceback
from datetime import datetime, timezone

from connectors import caha_pdf, events, instagram_stub, reddit, tiktok_stub  # noqa: F401
from connectors.base import registry
from models import (
    Candidate as CandidateModel,
    Run,
    Schedule,
    SessionLocal,
    Source,
)
from workers.scheduler import calculate_next_due


logger = logging.getLogger(__name__)


def run_connector(connector_name: str) -> None:
    connector = registry.get(connector_name)
    started_at = datetime.now(timezone.utc)

    with SessionLocal() as session:
        run = Run(connector=connector_name, started_at=started_at, status="running")
        session.add(run)
        session.flush()

        item_total = 0

        try:
            sources = connector.fetch(None)
            for payload in sources:
                if payload.content_hash:
                    existing = (
                        session.query(Source)
                        .filter(Source.content_hash == payload.content_hash)
                        .one_or_none()
                    )
                    if existing:
                        logger.info("Skipping duplicate source %s", payload.content_hash)
                        continue

                source = Source(
                    channel=payload.channel,
                    url=payload.url,
                    kind=payload.kind,
                    fetched_at=payload.fetched_at,
                    content_hash=payload.content_hash,
                    raw_blob_ptr=payload.raw_blob_ptr,
                    meta=payload.meta,
                )
                session.add(source)
                session.flush()

                candidates = connector.extract(payload)
                for candidate in candidates:
                    session.add(
                        CandidateModel(
                            source_id=source.id,
                            name=candidate.name,
                            channel=candidate.channel,
                            evidence=candidate.evidence,
                            metadata_json=candidate.metadata,
                        )
                    )
                item_total += len(candidates)

            run.status = "success"
            run.item_count = item_total
        except Exception as exc:  # pragma: no cover - safety
            logger.exception("Connector %s failed", connector_name)
            run.status = "error"
            run.error_log = f"{exc}\n{traceback.format_exc()}"
        finally:
            run.finished_at = datetime.now(timezone.utc)

            schedule = session.get(Schedule, connector_name)
            if run.status == "success":
                next_due = calculate_next_due(
                    schedule.cadence_cron if schedule else connector.default_cadence,
                    run.finished_at,
                    run.finished_at,
                )
                if schedule:
                    schedule.last_run_at = run.finished_at
                    schedule.next_due_at = next_due
                else:
                    session.add(
                        Schedule(
                            connector=connector_name,
                            cadence_cron=connector.default_cadence,
                            last_run_at=run.finished_at,
                            next_due_at=next_due,
                        )
                    )
            elif schedule is None:
                # Ensure the schedule row exists even on failure so cadence can be tracked.
                session.add(
                    Schedule(
                        connector=connector_name,
                        cadence_cron=connector.default_cadence,
                        last_run_at=None,
                        next_due_at=None,
                    )
                )

            session.commit()

