"""Ingestion log endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.orm import Session

from api.deps.auth import require_user
from api.deps.db import get_db
from models import Source


router = APIRouter(
    prefix="/api/logs",
    tags=["logs"],
    dependencies=[Depends(require_user)],
)


def _serialize_source(source: Source) -> dict[str, Any]:
    return {
        "id": source.id,
        "channel": source.channel,
        "url": source.url,
        "kind": source.kind,
        "fetched_at": source.fetched_at,
        "content_hash": source.content_hash,
        "raw_blob_ptr": source.raw_blob_ptr,
        "meta": source.meta or {},
    }


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid datetime format") from exc


@router.get("/")
def list_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    channel: str | None = None,
    kind: str | None = None,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    query = db.query(Source)

    if channel:
        query = query.filter(Source.channel == channel)
    if kind:
        query = query.filter(Source.kind == kind)

    total = query.count()
    logs = (
        query.order_by(Source.fetched_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return {
        "items": [_serialize_source(source) for source in logs],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.post("/", status_code=status.HTTP_201_CREATED)
def create_log(payload: dict[str, Any], db: Session = Depends(get_db)) -> dict[str, Any]:
    channel = payload.get("channel")
    fetched_at = payload.get("fetched_at")
    if not channel or not fetched_at:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="channel and fetched_at are required",
        )

    source = Source(
        channel=channel,
        url=payload.get("url"),
        kind=payload.get("kind"),
        fetched_at=_coerce_datetime(fetched_at),
        content_hash=payload.get("content_hash"),
        raw_blob_ptr=payload.get("raw_blob_ptr"),
        meta=payload.get("meta") or {},
    )

    db.add(source)
    db.commit()
    db.refresh(source)
    return _serialize_source(source)


@router.get("/{log_id}")
def get_log(log_id: int, db: Session = Depends(get_db)) -> dict[str, Any]:
    source = db.get(Source, log_id)
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log not found")
    return _serialize_source(source)


@router.put("/{log_id}")
def update_log(log_id: int, payload: dict[str, Any], db: Session = Depends(get_db)) -> dict[str, Any]:
    source = db.get(Source, log_id)
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log not found")

    for field in ("channel", "url", "kind", "fetched_at", "content_hash", "raw_blob_ptr", "meta"):
        if field in payload:
            value = payload[field]
            if field == "fetched_at":
                value = _coerce_datetime(value)
            setattr(source, field, value)

    db.add(source)
    db.commit()
    db.refresh(source)
    return _serialize_source(source)


@router.delete("/{log_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
def delete_log(log_id: int, db: Session = Depends(get_db)) -> Response:
    source = db.get(Source, log_id)
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log not found")

    db.delete(source)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
