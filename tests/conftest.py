import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

# banco proprio dos testes, separado do banco do compose
# definido antes dos imports: os Settings leem POSTGRES_DSN no import
_PG = "postgresql+psycopg://dataflow:changeme@localhost:5432"
TEST_DB_NAME = "dataflow_test"
os.environ["POSTGRES_DSN"] = f"{_PG}/{TEST_DB_NAME}"
# bucket proprio tambem: os testes nao deixam arquivos no bucket da demo
TEST_BUCKET = "dataflow-test"
os.environ["MINIO_BUCKET"] = TEST_BUCKET

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
    _reset_test_bucket()
    yield


def _reset_test_bucket() -> None:
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    if TEST_BUCKET not in existing:
        s3.create_bucket(Bucket=TEST_BUCKET)
        return
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=TEST_BUCKET):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=TEST_BUCKET, Delete={"Objects": keys})
