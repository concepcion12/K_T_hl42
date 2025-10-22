"""Schedule endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.orm import Session

from api.deps.auth import require_user
from api.deps.db import get_db
from models import Schedule


router = APIRouter(
    prefix="/api/schedules",
    tags=["schedules"],
    dependencies=[Depends(require_user)],
)


def _serialize_schedule(schedule: Schedule) -> dict[str, Any]:
    return {
        "connector": schedule.connector,
        "cadence_cron": schedule.cadence_cron,
        "last_run_at": schedule.last_run_at,
        "next_due_at": schedule.next_due_at,
        "enabled": schedule.enabled,
    }


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid datetime format") from exc


@router.get("/")
def list_schedules(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    enabled: bool | None = None,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    query = db.query(Schedule)
    if enabled is not None:
        query = query.filter(Schedule.enabled.is_(enabled))

    total = query.count()
    schedules = (
        query.order_by(Schedule.connector.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return {
        "items": [_serialize_schedule(schedule) for schedule in schedules],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.post("/", status_code=status.HTTP_201_CREATED)
def create_schedule(payload: dict[str, Any], db: Session = Depends(get_db)) -> dict[str, Any]:
    connector = payload.get("connector")
    cadence_cron = payload.get("cadence_cron")
    if not connector or not cadence_cron:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="connector and cadence_cron are required",
        )

    if db.get(Schedule, connector) is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Schedule already exists")

    schedule = Schedule(
        connector=connector,
        cadence_cron=cadence_cron,
        last_run_at=_coerce_datetime(payload.get("last_run_at")),
        next_due_at=_coerce_datetime(payload.get("next_due_at")),
        enabled=payload.get("enabled", True),
    )

    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    return _serialize_schedule(schedule)


@router.get("/{connector}")
def get_schedule(connector: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    schedule = db.get(Schedule, connector)
    if schedule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schedule not found")
    return _serialize_schedule(schedule)


@router.put("/{connector}")
def update_schedule(connector: str, payload: dict[str, Any], db: Session = Depends(get_db)) -> dict[str, Any]:
    schedule = db.get(Schedule, connector)
    if schedule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schedule not found")

    for field in ("cadence_cron", "last_run_at", "next_due_at", "enabled"):
        if field in payload:
            value = payload[field]
            if field in {"last_run_at", "next_due_at"}:
                value = _coerce_datetime(value)
            setattr(schedule, field, value)

    db.add(schedule)
    db.commit()
    db.refresh(schedule)
    return _serialize_schedule(schedule)


@router.delete("/{connector}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
def delete_schedule(connector: str, db: Session = Depends(get_db)) -> Response:
    schedule = db.get(Schedule, connector)
    if schedule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schedule not found")

    db.delete(schedule)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
