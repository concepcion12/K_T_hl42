"""Talent endpoints."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.orm import Session

from api.deps.auth import require_user
from api.deps.db import get_db
from models import Org, Talent, TalentOrg


router = APIRouter(
    prefix="/api/talent",
    tags=["talent"],
    dependencies=[Depends(require_user)],
)


def _serialize_talent(talent: Talent) -> dict[str, Any]:
    return {
        "id": talent.id,
        "name": talent.name,
        "discipline": talent.discipline,
        "subdiscipline": talent.subdiscipline,
        "primary_handle_url": talent.primary_handle_url,
        "other_links": talent.other_links or [],
        "contact_public": talent.contact_public,
        "contact_email": talent.contact_email,
        "phone": talent.phone,
        "location_tags": talent.location_tags or [],
        "themes": talent.themes or [],
        "notes": talent.notes,
        "score": float(talent.score) if isinstance(talent.score, Decimal) else talent.score,
        "score_json": talent.score_json or {},
        "created_at": talent.created_at,
        "updated_at": talent.updated_at,
    }


@router.get("/")
def list_talent(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    discipline: str | None = None,
    affiliation: str | None = None,
    min_score: float | None = None,
    max_score: float | None = None,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    query = db.query(Talent)

    if discipline:
        query = query.filter(Talent.discipline == discipline)
    if min_score is not None:
        query = query.filter(Talent.score >= min_score)
    if max_score is not None:
        query = query.filter(Talent.score <= max_score)
    if affiliation:
        query = (
            query.join(TalentOrg).join(Org).filter(Org.name.ilike(f"%{affiliation}%")).distinct()
        )

    total = query.count()
    talents = (
        query.order_by(Talent.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return {
        "items": [_serialize_talent(talent) for talent in talents],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.post("/", status_code=status.HTTP_201_CREATED)
def create_talent(payload: dict[str, Any], db: Session = Depends(get_db)) -> dict[str, Any]:
    name = payload.get("name")
    if not name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="name is required")

    talent = Talent(
        name=name,
        discipline=payload.get("discipline"),
        subdiscipline=payload.get("subdiscipline"),
        primary_handle_url=payload.get("primary_handle_url"),
        other_links=payload.get("other_links") or [],
        contact_public=payload.get("contact_public", False),
        contact_email=payload.get("contact_email"),
        phone=payload.get("phone"),
        location_tags=payload.get("location_tags"),
        themes=payload.get("themes"),
        notes=payload.get("notes"),
        score=payload.get("score"),
        score_json=payload.get("score_json") or {},
    )

    db.add(talent)
    db.commit()
    db.refresh(talent)
    return _serialize_talent(talent)


@router.get("/{talent_id}")
def get_talent(talent_id: int, db: Session = Depends(get_db)) -> dict[str, Any]:
    talent = db.get(Talent, talent_id)
    if talent is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Talent not found")
    return _serialize_talent(talent)


@router.put("/{talent_id}")
def update_talent(talent_id: int, payload: dict[str, Any], db: Session = Depends(get_db)) -> dict[str, Any]:
    talent = db.get(Talent, talent_id)
    if talent is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Talent not found")

    for field in (
        "name",
        "discipline",
        "subdiscipline",
        "primary_handle_url",
        "other_links",
        "contact_public",
        "contact_email",
        "phone",
        "location_tags",
        "themes",
        "notes",
        "score",
        "score_json",
    ):
        if field in payload:
            value = payload[field]
            if field in {"other_links", "score_json"} and value is None:
                value = [] if field == "other_links" else {}
            setattr(talent, field, value)

    db.add(talent)
    db.commit()
    db.refresh(talent)
    return _serialize_talent(talent)


@router.delete("/{talent_id}", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
def delete_talent(talent_id: int, db: Session = Depends(get_db)) -> Response:
    talent = db.get(Talent, talent_id)
    if talent is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Talent not found")

    db.delete(talent)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
