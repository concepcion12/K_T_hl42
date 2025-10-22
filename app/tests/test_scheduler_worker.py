"""Tests for scheduler computations and worker behavior."""

from __future__ import annotations

import types
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from connectors.base import Candidate as CandidatePayload
from connectors.base import SourcePayload
from models.tables import Base, Candidate as CandidateModel, Embedding, Run, Schedule, Source
from workers import scheduler, tasks


@pytest.fixture
def session_factory(monkeypatch):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(
        engine,
        tables=[Source.__table__, CandidateModel.__table__, Run.__table__, Schedule.__table__, Embedding.__table__],
    )
    SessionTesting = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    monkeypatch.setattr(scheduler, "SessionLocal", SessionTesting)
    monkeypatch.setattr(tasks, "SessionLocal", SessionTesting)

    yield SessionTesting

    Base.metadata.drop_all(engine)


@pytest.fixture
def stub_registry(monkeypatch):
    from connectors.base import ConnectorRegistry

    registry = ConnectorRegistry()
    monkeypatch.setattr(scheduler, "registry", registry)
    monkeypatch.setattr(tasks, "registry", registry)
    return registry


@pytest.fixture
def fake_redis(monkeypatch):
    class FakeRedisClient:
        def __init__(self) -> None:
            self.store: dict[str, str] = {}

        def set(self, key: str, value: str, nx: bool = False, ex: int | None = None) -> bool:  # noqa: ARG002
            if nx and key in self.store:
                return False
            self.store[key] = value
            return True

        def delete(self, key: str) -> None:
            self.store.pop(key, None)

    client = FakeRedisClient()
    redis_ns = types.SimpleNamespace(from_url=lambda url: client)  # noqa: ARG005
    monkeypatch.setattr(scheduler, "Redis", redis_ns)
    monkeypatch.setattr(tasks, "Redis", redis_ns)
    return client


@pytest.fixture
def fake_queue(monkeypatch):
    class FakeQueue:
        def __init__(self) -> None:
            self.jobs: list[tuple[str, tuple, dict]] = []

        def enqueue(self, func: str, *args, **kwargs) -> None:
            self.jobs.append((func, args, kwargs))

    queue = FakeQueue()

    def queue_factory(name: str, connection):  # noqa: ARG001
        return queue

    monkeypatch.setattr(scheduler, "Queue", queue_factory)
    return queue


def test_calculate_next_due_rounds_forward():
    base = datetime(2024, 1, 1, 12, 30, tzinfo=timezone.utc)
    result = scheduler.calculate_next_due("0 * * * *", base, base)
    assert result > base
    assert result.minute == 0
    assert result.tzinfo is not None


def test_enqueue_due_jobs_priority_and_locking(
    session_factory,
    stub_registry,
    fake_redis,
    fake_queue,
    monkeypatch,
):
    class DummyConnector:
        def __init__(self, name: str, cadence: str) -> None:
            self.name = name
            self.default_cadence = cadence

        def fetch(self, since):  # noqa: ARG002
            return []

        def extract(self, source):  # noqa: ARG002
            return []

    alpha = DummyConnector("alpha", "*/5 * * * *")
    beta = DummyConnector("beta", "*/5 * * * *")
    stub_registry.register(alpha)
    stub_registry.register(beta)

    monkeypatch.setenv("CONNECTOR_PRIORITY_WAVES", "beta,alpha")
    monkeypatch.setenv("REDIS_URL", "redis://test")

    with session_factory() as session:
        session.add_all(
            [
                Schedule(connector="alpha", cadence_cron="*/5 * * * *", enabled=True),
                Schedule(connector="beta", cadence_cron="*/5 * * * *", enabled=True),
            ]
        )
        session.commit()

    now = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    scheduler.enqueue_due_jobs(now=now)

    assert [args[0] for _, args, _ in fake_queue.jobs] == ["beta", "alpha"]
    assert set(fake_redis.store.keys()) == {
        "connector:beta:lock",
        "connector:alpha:lock",
    }

    # Schedules should not advance until the run finishes.
    with session_factory() as session:
        refreshed = {s.connector: s for s in session.query(Schedule).all()}
    assert refreshed["alpha"].last_run_at is None
    assert refreshed["beta"].last_run_at is None

    # A second pass while locks are present should not enqueue more work.
    scheduler.enqueue_due_jobs(now=now)
    assert len(fake_queue.jobs) == 2


def test_run_connector_since_dedupe_and_scoring(
    session_factory,
    stub_registry,
    fake_redis,
    monkeypatch,
):
    class StubConnector:
        name = "stub"
        default_cadence = "* * * * *"

        def __init__(self) -> None:
            self.seen_since: datetime | None = None

        def fetch(self, since):
            self.seen_since = since
            now = datetime.now(timezone.utc)
            return [
                SourcePayload(
                    channel=self.name,
                    url="https://duplicate.example",
                    kind="html",
                    fetched_at=now,
                    content_hash="dup",
                    meta={"embedding_key": "dup-source"},
                ),
                SourcePayload(
                    channel=self.name,
                    url="https://unique.example",
                    kind="html",
                    fetched_at=now + timedelta(seconds=1),
                    meta={"title": "Unique", "embedding_key": "unique-source"},
                ),
            ]

        def extract(self, source):  # noqa: ARG002
            return [
                CandidatePayload(
                    name="Existing Talent",
                    evidence="profile",
                    channel=self.name,
                    metadata={"embedding_key": "dup-candidate"},
                ),
                CandidatePayload(
                    name="New Talent",
                    evidence="profile",
                    channel=self.name,
                    metadata={"community_signal": True, "embedding_key": "new-candidate"},
                ),
            ]

    connector = StubConnector()
    stub_registry.register(connector)
    monkeypatch.setenv("REDIS_URL", "redis://test")

    previous = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with session_factory() as session:
        schedule = Schedule(
            connector="stub",
            cadence_cron="* * * * *",
            last_run_at=previous,
            enabled=True,
        )
        duplicate_source = Source(
            channel="stub",
            url="https://duplicate.example",
            kind="html",
            fetched_at=previous,
            content_hash="dup",
            meta={"embedding_key": "dup-source"},
        )
        session.add_all([schedule, duplicate_source])
        session.flush()
        session.add(
            CandidateModel(
                source_id=duplicate_source.id,
                name="Existing Talent",
                channel="stub",
                evidence="historic",
                metadata={"embedding_key": "dup-candidate"},
            )
        )
        session.commit()

    fake_redis.store["connector:stub:lock"] = "locked"
    tasks.run_connector("stub")

    with session_factory() as session:
        schedule = session.get(Schedule, "stub")
        assert schedule is not None
        assert schedule.last_run_at is not None

        def _normalize(dt: datetime) -> datetime:
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        assert _normalize(schedule.last_run_at) > previous
        assert schedule.next_due_at is not None

        run = session.query(Run).filter(Run.connector == "stub").one()
        assert run.status == "success"
        assert run.item_count == 1

        sources = session.query(Source).filter(Source.channel == "stub").all()
        assert len(sources) == 2  # duplicate skipped, unique stored

        candidates = session.query(CandidateModel).filter(CandidateModel.channel == "stub").all()
        assert len(candidates) == 2
        new_candidate = next(candidate for candidate in candidates if candidate.name == "New Talent")
        assert "score_breakdown" in new_candidate.metadata_json
        assert new_candidate.score == pytest.approx(40.0)
        assert new_candidate.metadata_json["score_breakdown"]["community"] == pytest.approx(30.0)

        embeddings = session.query(Embedding).all()
        assert len(embeddings) == 2

    assert connector.seen_since is not None
    assert _normalize(connector.seen_since) == previous
    assert "connector:stub:lock" not in fake_redis.store
