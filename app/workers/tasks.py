"""RQ task definitions."""

from __future__ import annotations

import hashlib
import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Any

from connectors import caha_pdf, events, instagram_stub, reddit, tiktok_stub  # noqa: F401
from connectors.base import Candidate as CandidatePayload
from connectors.base import SourcePayload, registry
from models import (
    Candidate as CandidateModel,
    Embedding,
    Run,
    Schedule,
    SessionLocal,
    Source,
)
from nlp import dedupe, scoring
from redis import Redis
from sqlalchemy import select
from sqlalchemy.orm import Session

from workers.scheduler import calculate_next_due


logger = logging.getLogger(__name__)


def _hash_embedding_key(value: str) -> int:
    digest = hashlib.sha1(value.encode("utf-8"), usedforsecurity=False)
    return int(digest.hexdigest(), 16) % (2**31)


def _embedding_duplicate(session: Session, namespace: str, key: str | None) -> bool:
    if not key:
        return False
    hashed = _hash_embedding_key(key)
    existing = session.execute(
        select(Embedding).where(
            Embedding.object_type == namespace,
            Embedding.object_id == hashed,
        )
    ).scalar_one_or_none()
    return existing is not None


def _mark_embedding_key(session: Session, namespace: str, key: str | None) -> None:
    if not key:
        return
    hashed = _hash_embedding_key(key)
    exists = session.execute(
        select(Embedding).where(
            Embedding.object_type == namespace,
            Embedding.object_id == hashed,
        )
    ).scalar_one_or_none()
    if exists is None:
        session.add(Embedding(object_type=namespace, object_id=hashed, vector=None))


def _payload_embedding_key(meta: dict[str, Any] | None) -> str | None:
    if not meta:
        return None
    raw = meta.get("embedding_key")
    if raw is None:
        return None
    return str(raw)


def _is_duplicate_source(session: Session, payload: SourcePayload) -> bool:
    if payload.content_hash:
        existing = (
            session.query(Source)
            .filter(Source.content_hash == payload.content_hash)
            .one_or_none()
        )
        if existing:
            logger.info("Skipping duplicate source %s", payload.content_hash)
            return True

    embedding_key = _payload_embedding_key(payload.meta if isinstance(payload.meta, dict) else None)
    if _embedding_duplicate(session, "dedupe:source", embedding_key):
        logger.info("Skipping source via embedding key %s", embedding_key)
        return True

    recent_sources = (
        session.query(Source)
        .filter(Source.channel == payload.channel)
        .order_by(Source.fetched_at.desc())
        .limit(25)
        .all()
    )
    for existing in recent_sources:
        if payload.url and existing.url and dedupe.exact_match(existing.url, payload.url):
            logger.info("Skipping duplicate source url %s", payload.url)
            return True
        title = None
        existing_title = None
        if isinstance(payload.meta, dict):
            title = payload.meta.get("title")
        if isinstance(existing.meta, dict):
            existing_title = existing.meta.get("title")
        if title and existing_title and dedupe.fuzzy_match(str(existing_title), str(title)):
            logger.info("Skipping source with matching title %s", title)
            return True
    return False


def _is_duplicate_candidate(session: Session, candidate: CandidatePayload) -> bool:
    metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
    embedding_key = _payload_embedding_key(metadata)
    if _embedding_duplicate(session, "dedupe:candidate", embedding_key):
        logger.info("Skipping candidate via embedding key %s", embedding_key)
        return True

    recent_candidates = (
        session.query(CandidateModel)
        .filter(CandidateModel.channel == candidate.channel)
        .order_by(CandidateModel.created_at.desc())
        .limit(50)
        .all()
    )
    for existing in recent_candidates:
        if dedupe.exact_match(existing.name, candidate.name):
            logger.info("Skipping duplicate candidate %s", candidate.name)
            return True
        if dedupe.fuzzy_match(existing.name, candidate.name):
            logger.info("Skipping fuzzy duplicate candidate %s", candidate.name)
            return True
    return False


def run_connector(connector_name: str) -> None:
    connector = registry.get(connector_name)
    started_at = datetime.now(timezone.utc)
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = Redis.from_url(redis_url)
    lock_key = f"connector:{connector_name}:lock"

    with SessionLocal() as session:
        schedule = session.get(Schedule, connector_name)
        since = schedule.last_run_at if schedule else None

        run = Run(connector=connector_name, started_at=started_at, status="running")
        session.add(run)
        session.flush()

        item_total = 0

        try:
            sources = connector.fetch(since)
            for payload in sources:
                if _is_duplicate_source(session, payload):
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

                _mark_embedding_key(
                    session,
                    "dedupe:source",
                    _payload_embedding_key(payload.meta if isinstance(payload.meta, dict) else None),
                )

                candidates = connector.extract(payload)
                for candidate in candidates:
                    if _is_duplicate_candidate(session, candidate):
                        continue

                    raw_metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
                    total_score, breakdown = scoring.score_candidate(
                        candidate.channel,
                        raw_metadata,
                    )
                    candidate_metadata: dict[str, Any] = dict(raw_metadata)
                    candidate_metadata["score_breakdown"] = breakdown

                    session.add(
                        CandidateModel(
                            source_id=source.id,
                            name=candidate.name,
                            channel=candidate.channel,
                            evidence=candidate.evidence,
                            metadata=candidate_metadata,
                            score=total_score,
                        )
                    )
                    _mark_embedding_key(
                        session,
                        "dedupe:candidate",
                        _payload_embedding_key(candidate_metadata),
                    )
                    item_total += 1

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

    redis.delete(lock_key)

