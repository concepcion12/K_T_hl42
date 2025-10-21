"""Runs endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from api.deps.db import get_db
from models import Run


router = APIRouter(prefix="/api/runs", tags=["runs"])


@router.get("/")
def list_runs(db: Session = Depends(get_db)) -> list[dict]:
    runs = db.query(Run).order_by(Run.started_at.desc()).limit(50).all()
    return [
        {
            "id": run.id,
            "connector": run.connector,
            "status": run.status,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "item_count": run.item_count,
        }
        for run in runs
    ]

