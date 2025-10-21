"""Database table models reflecting the scouting console schema."""

from __future__ import annotations

from datetime import datetime, date
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY
from pgvector.sqlalchemy import Vector
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class Source(Base):
    __tablename__ = "source"

    id: Mapped[int] = mapped_column(primary_key=True)
    channel: Mapped[str] = mapped_column(String, nullable=False)
    url: Mapped[str | None] = mapped_column(String, nullable=True)
    kind: Mapped[str | None] = mapped_column(String, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    content_hash: Mapped[str | None] = mapped_column(String, unique=True)
    raw_blob_ptr: Mapped[str | None] = mapped_column(String)
    meta: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    mentions: Mapped[list["Mention"]] = relationship(back_populates="source", cascade="all, delete-orphan")
    candidates: Mapped[list["Candidate"]] = relationship("Candidate", back_populates="source", cascade="all, delete-orphan")


class Candidate(Base):
    __tablename__ = "candidate"

    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("source.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String, nullable=False)
    channel: Mapped[str] = mapped_column(String, nullable=False)
    evidence: Mapped[str | None] = mapped_column(Text)
    metadata: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String, default="pending")
    score: Mapped[float | None] = mapped_column(Numeric)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    source: Mapped["Source"] = relationship("Source", back_populates="candidates")


class Talent(Base):
    __tablename__ = "talent"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    discipline: Mapped[str | None] = mapped_column(String)
    subdiscipline: Mapped[str | None] = mapped_column(String)
    primary_handle_url: Mapped[str | None] = mapped_column(String)
    other_links: Mapped[list[str]] = mapped_column(JSON, default=list)
    contact_public: Mapped[bool] = mapped_column(Boolean, default=False)
    contact_email: Mapped[str | None] = mapped_column(String)
    phone: Mapped[str | None] = mapped_column(String)
    location_tags: Mapped[list[str] | None] = mapped_column(ARRAY(String))
    themes: Mapped[list[str] | None] = mapped_column(ARRAY(String))
    notes: Mapped[str | None] = mapped_column(Text)
    score: Mapped[float | None] = mapped_column(Numeric)
    score_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    mentions: Mapped[list["Mention"]] = relationship(back_populates="talent", cascade="all, delete-orphan")
    orgs: Mapped[list["TalentOrg"]] = relationship(back_populates="talent", cascade="all, delete-orphan")
    events: Mapped[list["TalentEvent"]] = relationship(back_populates="talent", cascade="all, delete-orphan")
    sources: Mapped[list["Source"]] = relationship("Source", secondary="mention", viewonly=True)


class Org(Base):
    __tablename__ = "org"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str | None] = mapped_column(String)
    url: Mapped[str | None] = mapped_column(String)

    talents: Mapped[list["TalentOrg"]] = relationship(back_populates="org", cascade="all, delete-orphan")


class Event(Base):
    __tablename__ = "event"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)
    venue: Mapped[str | None] = mapped_column(String)
    url: Mapped[str | None] = mapped_column(String)
    source_id: Mapped[int | None] = mapped_column(ForeignKey("source.id", ondelete="SET NULL"))

    source: Mapped[Source | None] = relationship("Source")
    talents: Mapped[list["TalentEvent"]] = relationship(back_populates="event", cascade="all, delete-orphan")


class Mention(Base):
    __tablename__ = "mention"

    id: Mapped[int] = mapped_column(primary_key=True)
    talent_id: Mapped[int | None] = mapped_column(ForeignKey("talent.id", ondelete="CASCADE"))
    source_id: Mapped[int | None] = mapped_column(ForeignKey("source.id", ondelete="CASCADE"))
    context: Mapped[str | None] = mapped_column(Text)
    endorsement_score: Mapped[int | None] = mapped_column(Integer)

    talent: Mapped[Talent | None] = relationship("Talent", back_populates="mentions")
    source: Mapped[Source | None] = relationship("Source", back_populates="mentions")


class TalentOrg(Base):
    __tablename__ = "talent_org"

    talent_id: Mapped[int] = mapped_column(ForeignKey("talent.id", ondelete="CASCADE"), primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("org.id", ondelete="CASCADE"), primary_key=True)
    role: Mapped[str | None] = mapped_column(String)
    first_seen_source: Mapped[int | None] = mapped_column(ForeignKey("source.id"))
    priority: Mapped[int | None] = mapped_column(Integer)

    talent: Mapped[Talent] = relationship("Talent", back_populates="orgs")
    org: Mapped[Org] = relationship("Org", back_populates="talents")


class TalentEvent(Base):
    __tablename__ = "talent_event"

    talent_id: Mapped[int] = mapped_column(ForeignKey("talent.id", ondelete="CASCADE"), primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("event.id", ondelete="CASCADE"), primary_key=True)
    role: Mapped[str | None] = mapped_column(String)

    talent: Mapped[Talent] = relationship("Talent", back_populates="events")
    event: Mapped[Event] = relationship("Event", back_populates="talents")


class Embedding(Base):
    __tablename__ = "embeddings"

    object_type: Mapped[str] = mapped_column(String, primary_key=True)
    object_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    vector: Mapped[list[float] | None] = mapped_column(Vector(1536))


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    connector: Mapped[str] = mapped_column(String, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String, nullable=False)
    item_count: Mapped[int] = mapped_column(Integer, default=0)
    error_log: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("status IN ('queued','running','success','error')", name="runs_status_check"),
    )


class Schedule(Base):
    __tablename__ = "schedules"

    connector: Mapped[str] = mapped_column(String, primary_key=True)
    cadence_cron: Mapped[str] = mapped_column(String, nullable=False)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)

