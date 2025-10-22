"""Candidate endpoints."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import func
from sqlalchemy.orm import Session

from api.deps.auth import require_user
from api.deps.db import get_db
from models import Candidate, Source


router = APIRouter(
    prefix="/api/candidates",
    tags=["candidates"],
    dependencies=[Depends(require_user)],
)

_ALLOWED_STATUSES = {"pending", "approved", "watch", "dismissed"}


def _serialize_candidate(candidate: Candidate) -> dict[str, Any]:
    return {
        "id": candidate.id,
        "source_id": candidate.source_id,
        "name": candidate.name,
        "channel": candidate.channel,
        "evidence": candidate.evidence,
        "metadata": candidate.metadata_json or {},
        "status": candidate.status,
        "score": float(candidate.score) if isinstance(candidate.score, Decimal) else candidate.score,
        "created_at": candidate.created_at,
        "updated_at": candidate.updated_at,
    }


def _json_filter(column, key: str, value: str, dialect_name: str):
    if dialect_name == "sqlite":
        return func.json_extract(column, f"$.{key}") == value
    return column[key].astext == value  # type: ignore[index]


@router.get("/")
def list_candidates(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    discipline: str | None = None,
    affiliation: str | None = None,
    min_score: float | None = None,
    max_score: float | None = None,
    status_filter: str | None = Query(None, alias="status"),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    query = db.query(Candidate)
    dialect_name = db.bind.dialect.name  # type: ignore[union-attr]

    if discipline:
        query = query.filter(
            _json_filter(Candidate.metadata_json, "discipline", discipline, dialect_name)
        )
    if affiliation:
        query = query.filter(
            _json_filter(Candidate.metadata_json, "affiliation", affiliation, dialect_name)
        )
    if min_score is not None:
        query = query.filter(Candidate.score >= min_score)
    if max_score is not None:
        query = query.filter(Candidate.score <= max_score)
    if status_filter:
        query = query.filter(Candidate.status == status_filter)

    total = query.count()
    candidates = (
        query.order_by(Candidate.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return {
        "items": [_serialize_candidate(candidate) for candidate in candidates],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.post("/", status_code=status.HTTP_201_CREATED)
def create_candidate(payload: dict[str, Any], db: Session = Depends(get_db)) -> dict[str, Any]:
    source_id = payload.get("source_id")
    if source_id is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source_id is required")

    if db.get(Source, source_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source not found")

    candidate = Candidate(
        source_id=source_id,
        name=payload.get("name"),
        channel=payload.get("channel"),
        evidence=payload.get("evidence"),
        metadata_json=payload.get("metadata") or {},
        score=payload.get("score"),
        status=payload.get("status", "pending"),
    )

    if candidate.name is None or candidate.channel is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="name and channel are required")

    if candidate.status not in _ALLOWED_STATUSES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid status value")

    db.add(candidate)
    db.commit()
    db.refresh(candidate)
    return _serialize_candidate(candidate)


@router.get("/{candidate_id}")
def get_candidate(candidate_id: int, db: Session = Depends(get_db)) -> dict[str, Any]:
    candidate = db.get(Candidate, candidate_id)
    if candidate is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Candidate not found")
    return _serialize_candidate(candidate)


@router.put("/{candidate_id}")
def update_candidate(candidate_id: int, payload: dict[str, Any], db: Session = Depends(get_db)) -> dict[str, Any]:
    candidate = db.get(Candidate, candidate_id)
    if candidate is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Candidate not found")

    for field in ("name", "channel", "evidence", "metadata", "score", "status"):
        if field in payload:
            if field == "metadata":
                setattr(candidate, "metadata_json", payload[field] or {})
            else:
                setattr(candidate, field, payload[field])

    if candidate.status not in _ALLOWED_STATUSES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid status value")

    db.add(candidate)
    db.commit()
    db.refresh(candidate)
    return _serialize_candidate(candidate)


@router.delete("/{candidate_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
def delete_candidate(candidate_id: int, db: Session = Depends(get_db)) -> Response:
    candidate = db.get(Candidate, candidate_id)
    if candidate is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Candidate not found")

    db.delete(candidate)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _transition_candidate(candidate_id: int, status_value: str, db: Session) -> dict[str, Any]:
    candidate = db.get(Candidate, candidate_id)
    if candidate is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Candidate not found")

    candidate.status = status_value
    db.add(candidate)
    db.commit()
    db.refresh(candidate)
    return _serialize_candidate(candidate)


@router.post("/{candidate_id}/approve")
def approve_candidate(candidate_id: int, db: Session = Depends(get_db)) -> dict[str, Any]:
    return _transition_candidate(candidate_id, "approved", db)


@router.post("/{candidate_id}/watch")
def watch_candidate(candidate_id: int, db: Session = Depends(get_db)) -> dict[str, Any]:
    return _transition_candidate(candidate_id, "watch", db)


@router.post("/{candidate_id}/dismiss")
def dismiss_candidate(candidate_id: int, db: Session = Depends(get_db)) -> dict[str, Any]:
    return _transition_candidate(candidate_id, "dismissed", db)
