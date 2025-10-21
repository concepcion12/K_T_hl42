from fastapi import FastAPI

from api.routes import health, runs


def create_app() -> FastAPI:
    """Application factory for the FastAPI backend."""
    app = FastAPI(title="Guam Talent Scouting Console", version="0.1.0")

    app.include_router(health.router)
    app.include_router(runs.router)

    return app


app = create_app()

