"""execution: indice unico parcial de filha ativa

garante no banco a idempotencia do pipeline: no maximo uma execucao
RUNNING/SUCCESS por (input_execution_id, layer)

Revision ID: c7e2a91d4f10
Revises: bec933f66ff0
Create Date: 2026-11-06 00:00:00

"""

from typing import Sequence, Union

from alembic import op

revision: str = "c7e2a91d4f10"
down_revision: Union[str, Sequence[str], None] = "bec933f66ff0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # IF NOT EXISTS: bancos antigos criados por create_all() ja tem o indice
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_child_active
        ON execution (input_execution_id, layer)
        WHERE status IN ('RUNNING', 'SUCCESS')
        """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_execution_child_active")
