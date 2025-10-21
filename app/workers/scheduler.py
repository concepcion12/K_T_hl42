"""Startup scheduler to enqueue due connector jobs."""

from __future__ import annotations

from datetime import datetime, timezone

from redis import Redis
from rq import Queue

from connectors.base import registry
from models import Schedule, SessionLocal


def calculate_next_due(cron: str, last_run: datetime | None, now: datetime) -> datetime:
    # Placeholder implementation; integrate croniter later.
    return now


def enqueue_due_jobs() -> None:
    now = datetime.now(timezone.utc)
    redis = Redis.from_url("redis://localhost:6379/0")
    queue = Queue("connectors", connection=redis)

    with SessionLocal() as session:
        schedules = session.query(Schedule).filter(Schedule.enabled.is_(True)).all()
        for schedule in schedules:
            connector = registry.get(schedule.connector)
            next_due = schedule.next_due_at or schedule.last_run_at
            if not next_due or next_due <= now:
                queue.enqueue("workers.tasks.run_connector", connector.name, job_timeout="30m")
                schedule.last_run_at = now
                schedule.next_due_at = calculate_next_due(schedule.cadence_cron, now, now)
        session.commit()

