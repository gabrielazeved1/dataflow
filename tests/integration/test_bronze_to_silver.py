import io
import uuid

import pandas as pd
import pytest

from dataflow_common import storage as st
from dataflow_common.models import Dataset, Execution
from processing import bronze_to_silver


VENDAS_SCHEMA = {
    "campos": [
        {"nome": "data", "tipo": "date", "obrigatorio": True},
        {"nome": "produto", "tipo": "string", "obrigatorio": True},
        {"nome": "quantidade", "tipo": "integer", "obrigatorio": True, "min": 1},
        {"nome": "valor_unitario", "tipo": "float", "obrigatorio": True, "min": 0.01},
    ]
}

MINIO_KW = {
    "bucket": "dataflow",
    "endpoint": "localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
}


@pytest.fixture
def dataset(db):
    d = Dataset(nome="vendas_bts", formato="csv", schema=VENDAS_SCHEMA)
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


def _upload_bronze(dataset_name: str, content: bytes) -> str:
    path = st.build_bronze_path(dataset_name, uuid.uuid4(), "vendas.csv")
    st.upload(key=path, body=content, **MINIO_KW)
    return path


def test_bronze_to_silver_happy_path(db, dataset, minio_client):
    csv = (
        b"data,produto,quantidade,valor_unitario\n"
        b"2026-10-15,caneta,2,2.50\n"
        b"2026-10-15,caderno,1,10.00\n"
    )
    bronze_path = _upload_bronze(dataset.nome, csv)

    bronze = Execution(
        dataset_id=dataset.id,
        layer="BRONZE",
        status="SUCCESS",
        storage_path=bronze_path,
        file_name="vendas.csv",
    )
    db.add(bronze)
    db.commit()
    db.refresh(bronze)

    bronze_to_silver.run(bronze.id)

    db.expire_all()
    silver = (
        db.query(Execution)
        .filter(Execution.layer == "SILVER", Execution.input_execution_id == bronze.id)
        .first()
    )
    assert silver is not None
    assert silver.status == "SUCCESS"
    assert silver.storage_path is not None
    assert silver.storage_path.startswith(f"silver/{dataset.nome}/")

    obj = minio_client.get_object(Bucket="dataflow", Key=silver.storage_path)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    assert len(df) == 2
    assert set(df["produto"]) == {"caneta", "caderno"}


def test_bronze_to_silver_fails_when_execution_not_found(db, dataset):
    with pytest.raises(ValueError, match="não encontrada"):
        bronze_to_silver.run(uuid.uuid4())
