"""Integration tests for API routers."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="session")
def api_client() -> TestClient:
    os.environ["DATABASE_URL"] = "sqlite+pysqlite:///:memory:"

    from models import Base, session as session_module  # type: ignore[attr-defined]

    test_engine = session_module._build_engine(os.environ["DATABASE_URL"])
    session_module.engine = test_engine
    session_module.SessionLocal.configure(bind=test_engine)

    # Keep the public exports in sync with the updated engine.
    import models as models_module

    models_module.engine = test_engine
    models_module.SessionLocal = session_module.SessionLocal

    Base.metadata.create_all(bind=test_engine)

    from api.main import create_app
    from workers import scheduler

    original_enqueue = scheduler.enqueue_due_jobs
    scheduler.enqueue_due_jobs = lambda: None
    try:
        app = create_app()
        with TestClient(app) as client:
            yield client
    finally:
        scheduler.enqueue_due_jobs = original_enqueue
        Base.metadata.drop_all(bind=test_engine)


@pytest.fixture()
def db_session():
    from models import SessionLocal

    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture(autouse=True)
def clean_database():
    from models import Base, engine

    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield


def _make_source(db_session):
    from models import Source

    source = Source(
        channel="instagram",
        url="https://example.com/post",
        kind="social",
        fetched_at=datetime.now(timezone.utc),
        meta={"note": "seed"},
    )
    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)
    return source


def test_candidate_filters_and_mutations(api_client: TestClient, db_session):
    from models import Candidate

    source = _make_source(db_session)
    candidate_music = Candidate(
        source_id=source.id,
        name="Artist One",
        channel="instagram",
        metadata_json={"discipline": "music", "affiliation": "Guam Music"},
        score=0.82,
    )
    candidate_dance = Candidate(
        source_id=source.id,
        name="Performer Two",
        channel="tiktok",
        metadata_json={"discipline": "dance", "affiliation": "Guam Dance"},
        score=0.65,
    )
    db_session.add_all([candidate_music, candidate_dance])
    db_session.commit()

    resp = api_client.get("/api/candidates", params={"discipline": "music"})
    payload = resp.json()
    assert resp.status_code == 200
    assert payload["total"] == 1
    assert payload["items"][0]["name"] == "Artist One"

    resp = api_client.get("/api/candidates", params={"affiliation": "Guam Dance"})
    assert resp.status_code == 200
    assert resp.json()["items"][0]["name"] == "Performer Two"

    resp = api_client.get("/api/candidates", params={"min_score": 0.8})
    assert resp.status_code == 200
    assert resp.json()["items"][0]["score"] == 0.82

    approve_resp = api_client.post(f"/api/candidates/{candidate_music.id}/approve")
    assert approve_resp.status_code == 200
    assert approve_resp.json()["status"] == "approved"

    watch_resp = api_client.post(f"/api/candidates/{candidate_dance.id}/watch")
    assert watch_resp.status_code == 200
    assert watch_resp.json()["status"] == "watch"

    dismiss_resp = api_client.post(f"/api/candidates/{candidate_dance.id}/dismiss")
    assert dismiss_resp.status_code == 200
    assert dismiss_resp.json()["status"] == "dismissed"


def test_candidate_crud(api_client: TestClient, db_session):
    source = _make_source(db_session)

    create_resp = api_client.post(
        "/api/candidates",
        json={
            "source_id": source.id,
            "name": "New Candidate",
            "channel": "instagram",
            "metadata": {"discipline": "music"},
        },
    )
    assert create_resp.status_code == 201
    candidate_id = create_resp.json()["id"]

    get_resp = api_client.get(f"/api/candidates/{candidate_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["name"] == "New Candidate"

    update_resp = api_client.put(
        f"/api/candidates/{candidate_id}",
        json={"name": "Updated Candidate", "score": 0.9},
    )
    assert update_resp.status_code == 200
    assert update_resp.json()["score"] == 0.9

    delete_resp = api_client.delete(f"/api/candidates/{candidate_id}")
    assert delete_resp.status_code == 204

    missing_resp = api_client.get(f"/api/candidates/{candidate_id}")
    assert missing_resp.status_code == 404


def test_talent_filters_and_crud(api_client: TestClient, db_session):
    from models import Org, Talent, TalentOrg

    org_music = Org(name="Guam Music Collective")
    org_dance = Org(name="Dance Assoc")
    talent_music = Talent(
        name="Perla",
        discipline="music",
        score=0.91,
    )
    talent_dance = Talent(name="Inarajan Crew", discipline="dance", score=0.7)
    db_session.add_all([org_music, org_dance, talent_music, talent_dance])
    db_session.commit()

    db_session.add_all(
        [
            TalentOrg(talent_id=talent_music.id, org_id=org_music.id),
            TalentOrg(talent_id=talent_dance.id, org_id=org_dance.id),
        ]
    )
    db_session.commit()

    resp = api_client.get("/api/talent", params={"discipline": "music"})
    assert resp.status_code == 200
    assert resp.json()["items"][0]["name"] == "Perla"

    resp = api_client.get("/api/talent", params={"affiliation": "Dance"})
    assert resp.status_code == 200
    assert resp.json()["items"][0]["name"] == "Inarajan Crew"

    resp = api_client.get("/api/talent", params={"min_score": 0.9})
    assert resp.status_code == 200
    assert resp.json()["items"][0]["score"] == 0.91

    create_resp = api_client.post(
        "/api/talent",
        json={"name": "New Talent", "discipline": "music", "score": 0.4},
    )
    assert create_resp.status_code == 201
    talent_id = create_resp.json()["id"]

    get_resp = api_client.get(f"/api/talent/{talent_id}")
    assert get_resp.status_code == 200

    update_resp = api_client.put(
        f"/api/talent/{talent_id}",
        json={"notes": "Keep watch", "score": 0.5},
    )
    assert update_resp.status_code == 200
    assert update_resp.json()["score"] == 0.5

    delete_resp = api_client.delete(f"/api/talent/{talent_id}")
    assert delete_resp.status_code == 204

    missing_resp = api_client.get(f"/api/talent/{talent_id}")
    assert missing_resp.status_code == 404


def test_schedule_crud(api_client: TestClient):
    create_resp = api_client.post(
        "/api/schedules",
        json={
            "connector": "reddit",
            "cadence_cron": "0 * * * *",
        },
    )
    assert create_resp.status_code == 201

    list_resp = api_client.get("/api/schedules")
    assert list_resp.status_code == 200
    assert list_resp.json()["total"] == 1

    update_resp = api_client.put(
        "/api/schedules/reddit",
        json={"enabled": False},
    )
    assert update_resp.status_code == 200
    assert update_resp.json()["enabled"] is False

    delete_resp = api_client.delete("/api/schedules/reddit")
    assert delete_resp.status_code == 204

    missing_resp = api_client.get("/api/schedules/reddit")
    assert missing_resp.status_code == 404


def test_logs_crud(api_client: TestClient):
    timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
    create_resp = api_client.post(
        "/api/logs",
        json={
            "channel": "instagram",
            "kind": "fetch",
            "fetched_at": timestamp.isoformat(),
            "meta": {"items": 3},
        },
    )
    assert create_resp.status_code == 201
    log_id = create_resp.json()["id"]

    list_resp = api_client.get("/api/logs", params={"channel": "instagram"})
    assert list_resp.status_code == 200
    assert list_resp.json()["total"] == 1

    update_resp = api_client.put(
        f"/api/logs/{log_id}",
        json={"raw_blob_ptr": "s3://bucket/object"},
    )
    assert update_resp.status_code == 200
    assert update_resp.json()["raw_blob_ptr"] == "s3://bucket/object"

    delete_resp = api_client.delete(f"/api/logs/{log_id}")
    assert delete_resp.status_code == 204

    missing_resp = api_client.get(f"/api/logs/{log_id}")
    assert missing_resp.status_code == 404
