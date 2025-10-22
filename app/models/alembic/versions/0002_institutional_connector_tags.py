"""Track institutional connectors.

Revision ID: 0002_institutional_connector_tags
Revises: 0001_initial_schema
Create Date: 2025-10-22
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_institutional_connector_tags"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connector_profile",
        sa.Column("connector", sa.Text(), primary_key=True),
        sa.Column("institutional_anchor", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("notes", sa.Text()),
    )

    op.execute(
        sa.text(
            """
            INSERT INTO connector_profile (connector, institutional_anchor, notes)
            VALUES
                ('caha_pdf', true, 'CAHA artist directory import'),
                ('guma', true, 'GUMA member roster'),
                ('artspace', true, 'Artspace residency cohort'),
                ('uog', true, 'University of Guam fine arts directory')
            ON CONFLICT (connector) DO UPDATE
            SET institutional_anchor = EXCLUDED.institutional_anchor,
                notes = EXCLUDED.notes
            """
        )
    )


def downgrade() -> None:
    op.drop_table("connector_profile")
