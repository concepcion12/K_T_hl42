"""Startup scheduler to enqueue due connector jobs."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from croniter import croniter
from redis import Redis
from rq import Queue

from sqlalchemy.orm import Session

from connectors.base import registry
from models import Schedule, SessionLocal


logger = logging.getLogger(__name__)


def calculate_next_due(cron: str, last_run: datetime | None, now: datetime) -> datetime:
    """Return the next datetime matching the cron schedule."""

    base = last_run or now
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    iterator = croniter(cron, base)
    next_occurrence = iterator.get_next(datetime)
    if next_occurrence.tzinfo is None:
        next_occurrence = next_occurrence.replace(tzinfo=timezone.utc)
    return next_occurrence


def _ensure_schedule_rows(session: Session) -> None:
    for connector in registry.all():
        schedule = session.get(Schedule, connector.name)
        if schedule is None:
            schedule = Schedule(
                connector=connector.name,
                cadence_cron=connector.default_cadence,
                enabled=True,
            )
            session.add(schedule)


def enqueue_due_jobs(now: datetime | None = None) -> None:
    """Enqueue any connectors that are due to run."""

    current_time = now or datetime.now(timezone.utc)
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis = Redis.from_url(redis_url)
    queue = Queue("connectors", connection=redis)

    with SessionLocal() as session:
        _ensure_schedule_rows(session)
        session.flush()

        schedules = (
            session.query(Schedule)
            .filter(Schedule.enabled.is_(True))
            .all()
        )

        for schedule in schedules:
            try:
                connector = registry.get(schedule.connector)
            except KeyError:
                logger.warning("Connector %s not registered", schedule.connector)
                continue

            due_at = schedule.next_due_at or schedule.last_run_at
            if due_at is None or due_at <= current_time:
                logger.info("Enqueuing connector run for %s", connector.name)
                queue.enqueue("workers.tasks.run_connector", connector.name, job_timeout="30m")
                schedule.last_run_at = current_time
                schedule.next_due_at = calculate_next_due(
                    schedule.cadence_cron,
                    current_time,
                    current_time,
                )

        session.commit()

