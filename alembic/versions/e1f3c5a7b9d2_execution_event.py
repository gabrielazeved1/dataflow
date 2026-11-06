"""execution_event: rastreio passo a passo de cada ingestao

Revision ID: e1f3c5a7b9d2
Revises: d4b8e2f71a36
Create Date: 2026-11-07 00:00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "e1f3c5a7b9d2"
down_revision: Union[str, Sequence[str], None] = "d4b8e2f71a36"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "execution_event",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("trace_id", sa.UUID(), nullable=False),
        sa.Column("execution_id", sa.UUID(), nullable=True),
        sa.Column("service", sa.String(), nullable=False),
        sa.Column("step", sa.String(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["trace_id"], ["execution.id"]),
        sa.ForeignKeyConstraint(["execution_id"], ["execution.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_execution_event_trace", "execution_event", ["trace_id", "created_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_execution_event_trace", table_name="execution_event")
    op.drop_table("execution_event")
