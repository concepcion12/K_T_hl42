from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import health, runs
from connectors import caha_pdf, events, instagram_stub, reddit, tiktok_stub  # noqa: F401
from workers.scheduler import enqueue_due_jobs


def create_app() -> FastAPI:
    """Application factory for the FastAPI backend."""
    logger = logging.getLogger(__name__)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            enqueue_due_jobs()
        except Exception:  # pragma: no cover - safety
            logger.exception("Failed to enqueue due jobs on startup")
        yield

    app = FastAPI(title="Guam Talent Scouting Console", version="0.1.0", lifespan=lifespan)

    app.include_router(health.router)
    app.include_router(runs.router)

    return app


app = create_app()

