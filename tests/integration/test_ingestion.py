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


def test_create_dataset_json_rejected(client):
    response = client.post(
        "/datasets/", json={"nome": "eventos", "formato": "json", "schema": VENDAS_SCHEMA}
    )

    assert response.status_code == 422


def test_ingest_invalid_utf8_fails(client):
    dataset_id = _create_dataset(client, "vendas")
    response = client.post(
        f"/datasets/{dataset_id}/ingest",
        files={"file": ("vendas.csv", b"\xff\xfe\x00binario", "text/csv")},
    )

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "FAILED"
    assert data["error_message"] is not None
    assert data["checksum"] is None
    assert data["storage_path"] is None


def test_ingest_legacy_json_dataset_fails(client, db):
    from dataflow_common.models import Dataset

    legacy = Dataset(nome="legado", formato="json", schema=VENDAS_SCHEMA)
    db.add(legacy)
    db.commit()

    with patch("api.services.ingest.celery_client.send_task") as send_task:
        response = client.post(
            f"/datasets/{legacy.id}/ingest",
            files={"file": ("dados.json", b'[{"a": 1}]', "application/json")},
        )

    data = response.json()
    assert data["status"] == "FAILED"
    assert "não suportado" in data["error_message"]
    send_task.assert_not_called()


def test_ingest_minio_unavailable_returns_503(client):
    dataset_id = _create_dataset(client, "vendas")
    with patch("api.services.ingest.upload_bronze", side_effect=StorageUnavailableError("timeout")):
        response = client.post(
            f"/datasets/{dataset_id}/ingest",
            files={"file": ("vendas.csv", b"data,valor\n1,100", "text/csv")},
        )

    assert response.status_code == 503


def test_ingest_minio_unavailable_registers_failed(client, db):
    from dataflow_common.models import Execution

    dataset_id = _create_dataset(client, "vendas")
    with patch("api.services.ingest.upload_bronze", side_effect=StorageUnavailableError("timeout")):
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


#  modo sync / async + Celery 

VALID_CSV = b"data,produto,quantidade,valor_unitario\n2026-10-15,caneta,2,2.50"


def _make_worker_mock(db, status: str = "SUCCESS"):
    """Simula o worker: ao receber send_task, cria Silver + Gold com o status pedido."""
    from datetime import datetime, timezone

    from dataflow_common.models import Execution, ExecutionMetrics

    def fake_send_task(name, args, **kwargs):
        bronze_id = args[0]
        bronze = db.query(Execution).filter(Execution.id == bronze_id).first()
        now = datetime.now(timezone.utc)
        silver = Execution(
            dataset_id=bronze.dataset_id,
            input_execution_id=bronze.id,
            layer="SILVER",
            status=status,
            started_at=now,
            finished_at=now,
            storage_path=f"silver/fake/{bronze.id}.parquet" if status == "SUCCESS" else None,
        )
        db.add(silver)
        db.commit()
        db.refresh(silver)

        gold = Execution(
            dataset_id=bronze.dataset_id,
            input_execution_id=silver.id,
            layer="GOLD",
            status=status,
            started_at=now,
            finished_at=now,
            storage_path=f"gold/fake/{silver.id}.parquet" if status == "SUCCESS" else None,
        )
        db.add(gold)
        db.commit()
        db.refresh(gold)

        if status == "SUCCESS":
            db.add(ExecutionMetrics(
                gold_execution_id=gold.id,
                valid_count=1, rejected_count=0, rejection_rate=0.0,
                duration_seconds=0.1, corrupt_count=0,
            ))
            db.commit()

    return fake_send_task


def test_ingest_default_eh_async(client):
    dataset_id = _create_dataset(client, "vendas")
    with patch("api.services.ingest.celery_client.send_task") as send_task:
        response = client.post(
            f"/datasets/{dataset_id}/ingest",
            files={"file": ("vendas.csv", VALID_CSV, "text/csv")},
        )

    assert response.status_code == 202
    data = response.json()
    assert data["layer"] == "BRONZE"
    assert data["status"] == "SUCCESS"
    send_task.assert_called_once()
    name, kwargs = send_task.call_args.args[0], send_task.call_args.kwargs
    assert name == "processing.ingest"
    assert kwargs["args"] == [data["id"]]


def test_ingest_async_explicito(client):
    dataset_id = _create_dataset(client, "vendas")
    with patch("api.services.ingest.celery_client.send_task") as send_task:
        response = client.post(
            f"/datasets/{dataset_id}/ingest?mode=async",
            files={"file": ("vendas.csv", VALID_CSV, "text/csv")},
        )

    assert response.status_code == 202
    assert response.json()["layer"] == "BRONZE"
    send_task.assert_called_once()


def test_ingest_sync_sucesso(client, db):
    dataset_id = _create_dataset(client, "vendas")
    worker = _make_worker_mock(db, status="SUCCESS")
    with patch("api.services.ingest.celery_client.send_task", side_effect=worker):
        response = client.post(
            f"/datasets/{dataset_id}/ingest?mode=sync",
            files={"file": ("vendas.csv", VALID_CSV, "text/csv")},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["layer"] == "GOLD"
    assert data["status"] == "SUCCESS"


def test_ingest_sync_falha(client, db):
    dataset_id = _create_dataset(client, "vendas")
    worker = _make_worker_mock(db, status="FAILED")
    with patch("api.services.ingest.celery_client.send_task", side_effect=worker):
        response = client.post(
            f"/datasets/{dataset_id}/ingest?mode=sync",
            files={"file": ("vendas.csv", VALID_CSV, "text/csv")},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["layer"] == "GOLD"
    assert data["status"] == "FAILED"


def test_ingest_sync_timeout(client):
    dataset_id = _create_dataset(client, "vendas")
    with patch("api.services.ingest.celery_client.send_task"), \
         patch("api.routers.ingestion._SYNC_TIMEOUT_SECONDS", 1):
        response = client.post(
            f"/datasets/{dataset_id}/ingest?mode=sync",
            files={"file": ("vendas.csv", VALID_CSV, "text/csv")},
        )

    assert response.status_code == 504


def test_mode_invalido_retorna_422(client):
    dataset_id = _create_dataset(client, "vendas")
    response = client.post(
        f"/datasets/{dataset_id}/ingest?mode=batata",
        files={"file": ("vendas.csv", VALID_CSV, "text/csv")},
    )

    assert response.status_code == 422
