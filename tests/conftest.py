import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

# banco proprio dos testes, separado do banco do compose
# definido antes dos imports: os Settings leem POSTGRES_DSN no import
_PG = "postgresql+psycopg://dataflow:changeme@localhost:5432"
TEST_DB_NAME = "dataflow_test"
os.environ["POSTGRES_DSN"] = f"{_PG}/{TEST_DB_NAME}"

_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session", autouse=True)
def test_database():
    # schema via alembic, nao create_all: migration quebrada quebra a suite
    from alembic.config import Config

    from alembic import command

    # CREATE DATABASE nao roda dentro de transacao
    admin = create_engine(f"{_PG}/postgres", isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": TEST_DB_NAME},
        ).scalar()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{TEST_DB_NAME}"'))
    admin.dispose()

    engine = create_engine(os.environ["POSTGRES_DSN"])
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    engine.dispose()

    command.upgrade(Config(str(_ROOT / "alembic.ini")), "head")
    yield
