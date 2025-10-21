"""Initial schema for Guam talent scouting console.

Revision ID: 0001_initial_schema
Revises: None
Create Date: 2025-10-21
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from pgvector.sqlalchemy import Vector


revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "source",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("url", sa.Text()),
        sa.Column("kind", sa.Text()),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content_hash", sa.Text(), unique=True),
        sa.Column("raw_blob_ptr", sa.Text()),
        sa.Column("meta", sa.JSON(), server_default=sa.text("'{}'::json")),
    )

    op.create_table(
        "candidate",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("source_id", sa.BigInteger(), sa.ForeignKey("source.id", ondelete="CASCADE")),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("evidence", sa.Text()),
        sa.Column("metadata", sa.JSON(), server_default=sa.text("'{}'::json")),
        sa.Column("status", sa.Text(), server_default="pending"),
        sa.Column("score", sa.Numeric()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
    )

    op.create_table(
        "talent",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("discipline", sa.Text()),
        sa.Column("subdiscipline", sa.Text()),
        sa.Column("primary_handle_url", sa.Text()),
        sa.Column("other_links", sa.JSON(), server_default=sa.text("'[]'::json")),
        sa.Column("contact_public", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("contact_email", sa.Text()),
        sa.Column("phone", sa.Text()),
        sa.Column("location_tags", sa.ARRAY(sa.Text())),
        sa.Column("themes", sa.ARRAY(sa.Text())),
        sa.Column("notes", sa.Text()),
        sa.Column("score", sa.Numeric()),
        sa.Column("score_json", sa.JSON(), server_default=sa.text("'{}'::json")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
    )

    op.create_table(
        "org",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("type", sa.Text()),
        sa.Column("url", sa.Text()),
    )

    op.create_table(
        "event",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("start_date", sa.DATE()),
        sa.Column("end_date", sa.DATE()),
        sa.Column("venue", sa.Text()),
        sa.Column("url", sa.Text()),
        sa.Column("source_id", sa.BigInteger(), sa.ForeignKey("source.id", ondelete="SET NULL")),
    )

    op.create_table(
        "mention",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("talent_id", sa.BigInteger(), sa.ForeignKey("talent.id", ondelete="CASCADE")),
        sa.Column("source_id", sa.BigInteger(), sa.ForeignKey("source.id", ondelete="CASCADE")),
        sa.Column("context", sa.Text()),
        sa.Column("endorsement_score", sa.Integer()),
    )

    op.create_table(
        "talent_org",
        sa.Column("talent_id", sa.BigInteger(), sa.ForeignKey("talent.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("org_id", sa.BigInteger(), sa.ForeignKey("org.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("role", sa.Text()),
        sa.Column("first_seen_source", sa.BigInteger(), sa.ForeignKey("source.id")),
        sa.Column("priority", sa.Integer()),
    )

    op.create_table(
        "talent_event",
        sa.Column("talent_id", sa.BigInteger(), sa.ForeignKey("talent.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("event_id", sa.BigInteger(), sa.ForeignKey("event.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("role", sa.Text()),
    )

    op.create_table(
        "embeddings",
        sa.Column("object_type", sa.Text(), primary_key=True),
        sa.Column("object_id", sa.BigInteger(), primary_key=True),
        sa.Column("vector", Vector(dim=1536)),
    )

    op.create_table(
        "runs",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("connector", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("item_count", sa.Integer(), server_default="0"),
        sa.Column("error_log", sa.Text()),
        sa.CheckConstraint("status IN ('queued','running','success','error')", name="runs_status_check"),
    )

    op.create_table(
        "schedules",
        sa.Column("connector", sa.Text(), primary_key=True),
        sa.Column("cadence_cron", sa.Text(), nullable=False),
        sa.Column("last_run_at", sa.DateTime(timezone=True)),
        sa.Column("next_due_at", sa.DateTime(timezone=True)),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true")),
    )

