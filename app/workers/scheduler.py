"""Startup scheduler to enqueue due connector jobs."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Iterable

from croniter import croniter
from redis import Redis
from rq import Queue

from sqlalchemy.orm import Session

from connectors.base import Connector, registry
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


def _parse_priority_waves() -> list[list[str]]:
    env = os.getenv("CONNECTOR_PRIORITY_WAVES", "").strip()
    if not env:
        return []
    waves: list[list[str]] = []
    for group in env.split(";"):
        connectors = [name.strip() for name in group.split(",") if name.strip()]
        if connectors:
            waves.append(connectors)
    return waves


def _order_due_connectors(names: Iterable[str]) -> list[str]:
    pending = list(dict.fromkeys(names))
    if not pending:
        return []
    pending_set = set(pending)
    ordered: list[str] = []
    for wave in _parse_priority_waves():
        for name in wave:
            if name in pending_set:
                ordered.append(name)
                pending_set.remove(name)
    for name in pending:
        if name in pending_set:
            ordered.append(name)
            pending_set.remove(name)
    return ordered


def _compute_due_at(schedule: Schedule, now: datetime, connector_default: str) -> datetime:
    if schedule.next_due_at:
        return schedule.next_due_at
    if schedule.last_run_at:
        return calculate_next_due(schedule.cadence_cron or connector_default, schedule.last_run_at, now)
    return now


def enqueue_due_jobs(now: datetime | None = None) -> None:
    """Enqueue any connectors that are due to run."""

    current_time = now or datetime.now(timezone.utc)
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    lock_ttl = int(os.getenv("CONNECTOR_LOCK_TTL", "1800"))
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

        due_map: dict[str, tuple[Schedule, Connector]] = {}

        for schedule in schedules:
            try:
                connector = registry.get(schedule.connector)
            except KeyError:
                logger.warning("Connector %s not registered", schedule.connector)
                continue

            due_at = _compute_due_at(schedule, current_time, connector.default_cadence)
            if due_at <= current_time:
                due_map[schedule.connector] = (schedule, connector)

        for connector_name in _order_due_connectors(due_map.keys()):
            schedule, connector = due_map[connector_name]
            lock_key = f"connector:{connector_name}:lock"
            if not redis.set(lock_key, str(current_time.timestamp()), nx=True, ex=lock_ttl):
                logger.debug("Connector %s already locked", connector_name)
                continue

            logger.info("Enqueuing connector run for %s", connector.name)
            queue.enqueue("workers.tasks.run_connector", connector.name, job_timeout="30m")

        session.commit()

