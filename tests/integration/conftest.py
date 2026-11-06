import os
from unittest.mock import patch

import boto3
import pytest
from api.config import settings
from api.database import get_db
from api.main import app
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

settings.minio_endpoint = "localhost:9000"

# definido pelo tests/conftest.py (banco dataflow_test)
TEST_DATABASE_URL = os.environ["POSTGRES_DSN"]

engine = create_engine(TEST_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(autouse=True)
def clean_tables():
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE execution, dataset CASCADE"))
    yield


@pytest.fixture(autouse=True)
def no_real_queue():
    # Sem isso, todo teste de ingestão manda uma task de verdade pro Redis,
    # e o worker do docker compose a consome contra o banco de dev.
    # Testes que verificam o enfileiramento fazem o próprio patch por cima.
    with patch("api.services.ingest.celery_client.send_task"):
        yield


@pytest.fixture
def db():
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client(db):
    def override_get_db():
        yield db

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def minio_client():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
