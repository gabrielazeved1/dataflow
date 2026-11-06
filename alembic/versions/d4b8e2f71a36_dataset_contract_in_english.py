"""dataset: contrato em ingles (colunas e chaves do schema jsonb)

nome -> name, formato -> format
schema jsonb: campos -> fields; em cada campo nome -> name, tipo -> type,
obrigatorio -> required

Revision ID: d4b8e2f71a36
Revises: c7e2a91d4f10
Create Date: 2026-11-06 00:00:00

"""

from typing import Sequence, Union

from alembic import op

revision: str = "d4b8e2f71a36"
down_revision: Union[str, Sequence[str], None] = "c7e2a91d4f10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _rename_schema_keys(
    old_list: str, new_list: str, keys: dict[str, str], required_key: str
) -> str:
    # reescreve cada campo trocando as chaves e mantendo o resto (min, max)
    removed = " - ".join(f"'{old}'" for old in keys)
    renamed = ", ".join(
        (
            f"'{new}', f->'{old}'"
            if old != required_key
            # chave ausente: o default do pydantic é true
            else f"'{new}', COALESCE(f->'{old}', 'true'::jsonb)"
        )
        for old, new in keys.items()
    )
    return f"""
        UPDATE dataset
        SET schema = (schema - '{old_list}') || jsonb_build_object(
            '{new_list}',
            COALESCE(
                (
                    SELECT jsonb_agg(
                        (f - {removed}) || jsonb_build_object({renamed})
                        ORDER BY ord
                    )
                    FROM jsonb_array_elements(schema->'{old_list}')
                        WITH ORDINALITY AS t(f, ord)
                ),
                '[]'::jsonb
            )
        )
        WHERE schema ? '{old_list}'
    """


def upgrade() -> None:
    op.alter_column("dataset", "nome", new_column_name="name")
    op.alter_column("dataset", "formato", new_column_name="format")
    op.execute(
        "ALTER TABLE dataset RENAME CONSTRAINT dataset_nome_key TO dataset_name_key"
    )
    op.execute(
        _rename_schema_keys(
            "campos",
            "fields",
            {"nome": "name", "tipo": "type", "obrigatorio": "required"},
            required_key="obrigatorio",
        )
    )


def downgrade() -> None:
    op.execute(
        _rename_schema_keys(
            "fields",
            "campos",
            {"name": "nome", "type": "tipo", "required": "obrigatorio"},
            required_key="required",
        )
    )
    op.execute(
        "ALTER TABLE dataset RENAME CONSTRAINT dataset_name_key TO dataset_nome_key"
    )
    op.alter_column("dataset", "format", new_column_name="formato")
    op.alter_column("dataset", "name", new_column_name="nome")
