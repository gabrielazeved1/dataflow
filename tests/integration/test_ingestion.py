from unittest.mock import patch

import pytest

from api.storage import StorageUnavailableError


@pytest.fixture
def dataset_csv(client):
    response = client.post("/datasets/", json={"nome": "vendas", "formato": "csv"})
    return response.json()["id"]


@pytest.fixture
def dataset_json(client):
    response = client.post("/datasets/", json={"nome": "eventos", "formato": "json"})
    return response.json()["id"]


def test_ingest_csv_success(client, dataset_csv):
    csv_content = b"data,produto,valor\n2026-10-15,caneta,2.50"

    response = client.post(
        f"/datasets/{dataset_csv}/ingest",
        files={"file": ("vendas.csv", csv_content, "text/csv")},
    )

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert data["checksum"] is not None
    assert data["file_size"] == len(csv_content)
    assert data["error_message"] is None
    assert data["storage_path"] is not None
    assert data["storage_path"].startswith("bronze/vendas/")
    assert "vendas.csv" in data["storage_path"]


def test_ingest_file_exists_in_minio(client, dataset_csv, minio_client):
    csv_content = b"data,produto,valor\n2026-10-15,caneta,2.50"

    response = client.post(
        f"/datasets/{dataset_csv}/ingest",
        files={"file": ("vendas.csv", csv_content, "text/csv")},
    )

    storage_path = response.json()["storage_path"]
    obj = minio_client.get_object(Bucket="dataflow", Key=storage_path)
    assert obj["Body"].read() == csv_content


def test_ingest_invalid_json_fails(client, dataset_json):
    response = client.post(
        f"/datasets/{dataset_json}/ingest",
        files={"file": ("dados.json", b"isso nao e json { {", "application/json")},
    )

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "FAILED"
    assert data["error_message"] is not None
    assert data["checksum"] is None
    assert data["storage_path"] is None


def test_ingest_minio_unavailable_returns_503(client, dataset_csv):
    with patch("api.routers.ingestion.upload_bronze", side_effect=StorageUnavailableError("timeout")):
        response = client.post(
            f"/datasets/{dataset_csv}/ingest",
            files={"file": ("vendas.csv", b"data,valor\n1,100", "text/csv")},
        )

    assert response.status_code == 503


def test_ingest_minio_unavailable_registers_failed(client, dataset_csv, db):
    from api.models import Execution

    with patch("api.routers.ingestion.upload_bronze", side_effect=StorageUnavailableError("timeout")):
        client.post(
            f"/datasets/{dataset_csv}/ingest",
            files={"file": ("vendas.csv", b"data,valor\n1,100", "text/csv")},
        )

    execution = db.query(Execution).first()
    assert execution.status == "FAILED"
    assert execution.storage_path is None


def test_ingest_dataset_not_found(client):
    response = client.post(
        "/datasets/00000000-0000-0000-0000-000000000000/ingest",
        files={"file": ("vendas.csv", b"data,valor", "text/csv")},
    )

    assert response.status_code == 404
