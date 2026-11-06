from datetime import datetime, timezone
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dataflow_common.models import Dataset, Execution
from dataflow_common.storage import build_silver_rejected_path

SCHEMA = {"campos": [{"nome": "produto", "tipo": "string", "obrigatorio": True}]}


@pytest.fixture
def dataset(db):
    d = Dataset(nome="vendas", formato="csv", schema=SCHEMA)
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


def _make_execution(db, dataset_id, layer, status, **extra):
    now = datetime(2026, 11, 1, tzinfo=timezone.utc)
    execution = Execution(
        dataset_id=dataset_id,
        layer=layer,
        status=status,
        created_at=now,
        started_at=now,
        finished_at=now,
        **extra,
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)
    return execution


def _upload_rejected_parquet(minio_client, dataset_name, execution_id, rows):
    table = pa.Table.from_pylist(rows)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    key = build_silver_rejected_path(dataset_name, execution_id)
    minio_client.put_object(Bucket="dataflow", Key=key, Body=buf.getvalue())


def test_rejected_unknown_execution(client):
    response = client.get("/executions/00000000-0000-0000-0000-000000000000/rejected")
    assert response.status_code == 404


def test_rejected_non_silver_execution(client, db, dataset):
    bronze = _make_execution(
        db, dataset.id, "BRONZE", "SUCCESS", storage_path="bronze/x"
    )
    response = client.get(f"/executions/{bronze.id}/rejected")
    assert response.status_code == 400
    assert "SILVER" in response.json()["detail"]


def test_rejected_silver_failed(client, db, dataset):
    silver = _make_execution(
        db, dataset.id, "SILVER", "FAILED", storage_path="silver/x"
    )
    response = client.get(f"/executions/{silver.id}/rejected")
    assert response.status_code == 400
    assert "SUCCESS" in response.json()["detail"]


def test_rejected_silver_without_storage_path(client, db, dataset):
    silver = _make_execution(db, dataset.id, "SILVER", "SUCCESS")
    response = client.get(f"/executions/{silver.id}/rejected")
    assert response.status_code == 400
    assert "storage_path" in response.json()["detail"]


def test_rejected_happy_path(client, db, dataset, minio_client):
    silver = _make_execution(
        db,
        dataset.id,
        "SILVER",
        "SUCCESS",
        storage_path="silver/vendas/dt=2026-11-01/x.parquet",
    )
    _upload_rejected_parquet(
        minio_client,
        dataset_name="vendas",
        execution_id=silver.id,
        rows=[
            {"produto": "a", "motivo": "min_violated"},
            {"produto": "b", "motivo": "corrupt"},
            {"produto": "c", "motivo": "null"},
        ],
    )

    response = client.get(f"/executions/{silver.id}/rejected?limit=2")

    assert response.status_code == 200
    body = response.json()
    assert len(body["rows"]) == 2
    assert body["rows"][0] == {"produto": "a", "motivo": "min_violated"}
