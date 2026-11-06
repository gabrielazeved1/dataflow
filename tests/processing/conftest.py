import os

os.environ.setdefault("MINIO_ENDPOINT", "localhost:9000")
os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin")
os.environ.setdefault("MINIO_BUCKET", "dataflow")

import boto3
import pytest
from dataflow_common.models import Dataset
from dataflow_common.repository import create_execution, finish_execution
from dataflow_common.storage import build_bronze_path, upload
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# definido pelo tests/conftest.py (banco dataflow_test)
_DB_URL = os.environ["POSTGRES_DSN"]
_engine = create_engine(_DB_URL)
_Session = sessionmaker(bind=_engine)

_SCHEMA = {
    "fields": [
        {"name": "produto", "type": "string", "required": True},
        {"name": "quantidade", "type": "integer", "required": True, "min": 1},
        {"name": "valor", "type": "float", "required": False},
    ]
}


@pytest.fixture(autouse=True)
def clean_tables():
    with _engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE execution, dataset CASCADE"))
    yield


@pytest.fixture
def db():
    with _Session() as session:
        yield session


@pytest.fixture
def minio_client():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )


def _make_bronze(db, csv_content: bytes):
    dataset = Dataset(name="vendas_test", format="csv", schema=_SCHEMA)
    db.add(dataset)
    db.commit()
    db.refresh(dataset)

    execution = create_execution(db, dataset_id=dataset.id, layer="BRONZE")
    storage_path = build_bronze_path(dataset.name, execution.id, "vendas.csv")
    upload(
        bucket="dataflow",
        key=storage_path,
        body=csv_content,
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    finish_execution(db, execution, "SUCCESS", storage_path=storage_path)
    return execution


@pytest.fixture
def bronze_execution(db):
    return _make_bronze(db, b"produto,quantidade,valor\ncaneta,2,1.50\nregua,5,3.00")


@pytest.fixture
def bronze_execution_with_nulls(db):
    # segunda linha: produto null — vai pro rejected
    return _make_bronze(db, b"produto,quantidade,valor\ncaneta,2,1.50\n,5,3.00")


@pytest.fixture
def silver_execution(db, bronze_execution_with_nulls):
    import sys
    from unittest.mock import patch

    from dataflow_common.models import Execution
    from processing.bronze_to_silver import main as bronze_main

    with patch.object(sys, "argv", ["processing", str(bronze_execution_with_nulls.id)]):
        bronze_main()

    db.expire_all()
    return db.query(Execution).filter(Execution.layer == "SILVER").first()
