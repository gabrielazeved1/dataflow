import io
import uuid

import pandas as pd
import pytest

from dataflow_common import storage as st
from dataflow_common.models import Dataset, Execution
from processing import silver_to_gold


SCHEMA = {
    "campos": [
        {"nome": "data", "tipo": "date"},
        {"nome": "quantidade", "tipo": "integer"},
        {"nome": "valor_unitario", "tipo": "float"},
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
    d = Dataset(nome="vendas_stg", formato="csv", schema=SCHEMA)
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


def _upload_silver(dataset_name: str, df: pd.DataFrame) -> str:
    path = st.build_silver_path(dataset_name, "2026-10-18", uuid.uuid4())
    st.upload(key=path, body=df.to_parquet(index=False), **MINIO_KW)
    return path


def test_silver_to_gold_happy_path(db, dataset, minio_client):
    df = pd.DataFrame(
        [
            {"data": "2026-10-15", "produto": "caneta", "quantidade": 2, "valor_unitario": 2.50},
            {"data": "2026-10-15", "produto": "caderno", "quantidade": 1, "valor_unitario": 10.00},
            {"data": "2026-10-16", "produto": "caneta", "quantidade": 5, "valor_unitario": 2.50},
        ]
    )
    silver_path = _upload_silver(dataset.nome, df)

    silver = Execution(
        dataset_id=dataset.id,
        layer="SILVER",
        status="SUCCESS",
        storage_path=silver_path,
    )
    db.add(silver)
    db.commit()
    db.refresh(silver)

    silver_to_gold.run(silver.id)

    db.expire_all()
    gold = (
        db.query(Execution)
        .filter(Execution.layer == "GOLD", Execution.input_execution_id == silver.id)
        .first()
    )
    assert gold is not None
    assert gold.status == "SUCCESS"
    assert gold.storage_path is not None

    obj = minio_client.get_object(Bucket="dataflow", Key=gold.storage_path)
    agg = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    assert len(agg) == 2  # dois dias distintos
    row15 = agg[agg["data"] == "2026-10-15"].iloc[0]
    assert row15["total_quantidade"] == 3
    assert row15["total_valor"] == 15.00


def test_silver_to_gold_fails_when_execution_not_found():
    with pytest.raises(ValueError, match="não encontrada"):
        silver_to_gold.run(uuid.uuid4())
