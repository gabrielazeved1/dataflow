import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

# Banco próprio dos testes. Antes os testes usavam o mesmo banco do docker
# compose: rodar pytest apagava os dados da demo, e o schema vinha do
# create_all() em vez das migrations (ver dividas.md #8).
#
# Precisa ser definido antes de qualquer import da api/processing: os
# Settings leem POSTGRES_DSN no import. "localhost" porque os testes rodam
# fora do Docker, com a porta 5432 mapeada.
_PG = "postgresql+psycopg://dataflow:changeme@localhost:5432"
TEST_DB_NAME = "dataflow_test"
os.environ["POSTGRES_DSN"] = f"{_PG}/{TEST_DB_NAME}"

_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session", autouse=True)
def test_database():
    """Cria o banco de teste se preciso, zera o schema e aplica as migrations.

    O schema vem do `alembic upgrade head`, não do create_all(): assim uma
    migration quebrada ou divergente do model quebra a suíte.
    """
    from alembic.config import Config

    from alembic import command

    # CREATE DATABASE não roda dentro de transação: AUTOCOMMIT no banco admin
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
