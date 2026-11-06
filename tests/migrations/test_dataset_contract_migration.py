import json
import os
from pathlib import Path

import pytest
from alembic.config import Config
from sqlalchemy import create_engine, text

from alembic import command

_CFG = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
_BEFORE = "c7e2a91d4f10"

OLD_SCHEMA = {
    "campos": [
        {"nome": "data", "tipo": "date", "obrigatorio": True},
        {"nome": "valor", "tipo": "float", "obrigatorio": False, "min": 0.01},
        # dado antigo sem a chave: deve virar required=true
        {"nome": "produto", "tipo": "string"},
    ]
}


@pytest.fixture
def engine():
    eng = create_engine(os.environ["POSTGRES_DSN"])
    with eng.begin() as conn:
        conn.execute(text("TRUNCATE TABLE execution, dataset CASCADE"))
    yield eng
    # outros testes dependem do schema em head
    command.upgrade(_CFG, "head")
    with eng.begin() as conn:
        conn.execute(text("TRUNCATE TABLE execution, dataset CASCADE"))
    eng.dispose()


def test_upgrade_renames_columns_and_jsonb_keys(engine):
    command.downgrade(_CFG, _BEFORE)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dataset (id, nome, formato, schema, created_at) "
                "VALUES (gen_random_uuid(), 'vendas', 'csv', CAST(:s AS jsonb), now())"
            ),
            {"s": json.dumps(OLD_SCHEMA)},
        )

    command.upgrade(_CFG, "head")

    with engine.connect() as conn:
        row = conn.execute(text("SELECT name, format, schema FROM dataset")).one()
    assert row.name == "vendas"
    assert row.format == "csv"
    assert row.schema == {
        "fields": [
            {"name": "data", "type": "date", "required": True},
            {"name": "valor", "type": "float", "required": False, "min": 0.01},
            {"name": "produto", "type": "string", "required": True},
        ]
    }


def test_downgrade_restores_old_contract(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dataset (id, name, format, schema, created_at) "
                "VALUES (gen_random_uuid(), 'vendas', 'csv', CAST(:s AS jsonb), now())"
            ),
            {
                "s": json.dumps(
                    {"fields": [{"name": "x", "type": "string", "required": True}]}
                )
            },
        )

    command.downgrade(_CFG, _BEFORE)

    with engine.connect() as conn:
        row = conn.execute(text("SELECT nome, formato, schema FROM dataset")).one()
    assert row.nome == "vendas"
    assert row.schema == {
        "campos": [{"nome": "x", "tipo": "string", "obrigatorio": True}]
    }
