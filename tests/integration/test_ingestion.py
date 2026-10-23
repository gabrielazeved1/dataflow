from unittest.mock import patch

from dataflow_common.storage import StorageUnavailableError


VENDAS_SCHEMA = {
    "campos": [
        {"nome": "data", "tipo": "date", "obrigatorio": True},
        {"nome": "produto", "tipo": "string", "obrigatorio": True},
        {"nome": "quantidade", "tipo": "integer", "obrigatorio": True, "min": 1},
        {"nome": "valor_unitario", "tipo": "float", "obrigatorio": True, "min": 0.01},
    ]
}


def _create_dataset(client, nome: str, formato: str = "csv"):
    response = client.post(
        "/datasets/", json={"nome": nome, "formato": formato, "schema": VENDAS_SCHEMA}
    )
    return response.json()["id"]


def test_ingest_csv_success(client):
    dataset_id = _create_dataset(client, "vendas")
    csv_content = b"data,produto,quantidade,valor_unitario\n2026-10-15,caneta,2,2.50"

    response = client.post(
        f"/datasets/{dataset_id}/ingest",
        files={"file": ("vendas.csv", csv_content, "text/csv")},
    )

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert data["layer"] == "BRONZE"
    assert data["checksum"] is not None
    assert data["file_size"] == len(csv_content)
    assert data["error_message"] is None
    assert data["storage_path"] is not None
    assert data["storage_path"].startswith("bronze/vendas/")
    assert "vendas.csv" in data["storage_path"]


def test_ingest_file_exists_in_minio(client, minio_client):
    dataset_id = _create_dataset(client, "vendas")
    csv_content = b"data,produto,quantidade,valor_unitario\n2026-10-15,caneta,2,2.50"

    response = client.post(
        f"/datasets/{dataset_id}/ingest",
        files={"file": ("vendas.csv", csv_content, "text/csv")},
    )

    storage_path = response.json()["storage_path"]
    obj = minio_client.get_object(Bucket="dataflow", Key=storage_path)
    assert obj["Body"].read() == csv_content


def test_ingest_invalid_json_fails(client):
    dataset_id = _create_dataset(client, "eventos", formato="json")
    response = client.post(
        f"/datasets/{dataset_id}/ingest",
        files={"file": ("dados.json", b"isso nao e json { {", "application/json")},
    )

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "FAILED"
    assert data["error_message"] is not None
    assert data["checksum"] is None
    assert data["storage_path"] is None


def test_ingest_minio_unavailable_returns_503(client):
    dataset_id = _create_dataset(client, "vendas")
    with patch("api.routers.ingestion.upload_bronze", side_effect=StorageUnavailableError("timeout")):
        response = client.post(
            f"/datasets/{dataset_id}/ingest",
            files={"file": ("vendas.csv", b"data,valor\n1,100", "text/csv")},
        )

    assert response.status_code == 503


def test_ingest_minio_unavailable_registers_failed(client, db):
    from dataflow_common.models import Execution

    dataset_id = _create_dataset(client, "vendas")
    with patch("api.routers.ingestion.upload_bronze", side_effect=StorageUnavailableError("timeout")):
        client.post(
            f"/datasets/{dataset_id}/ingest",
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
