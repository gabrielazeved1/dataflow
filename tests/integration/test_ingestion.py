from unittest.mock import patch

from dataflow_common.storage import StorageUnavailableError

SALES_SCHEMA = {
    "fields": [
        {"name": "data", "type": "date", "required": True},
        {"name": "produto", "type": "string", "required": True},
        {"name": "quantidade", "type": "integer", "required": True, "min": 1},
        {"name": "valor_unitario", "type": "float", "required": True, "min": 0.01},
    ]
}


def _create_dataset(client, name: str, format: str = "csv"):
    response = client.post(
        "/datasets/", json={"name": name, "format": format, "schema": SALES_SCHEMA}
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
        "/datasets/",
        json={"name": "eventos", "format": "json", "schema": SALES_SCHEMA},
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

    legacy = Dataset(name="legado", format="json", schema=SALES_SCHEMA)
    db.add(legacy)
    db.commit()

    with patch("api.services.ingest.celery_client.send_task") as send_task:
        response = client.post(
            f"/datasets/{legacy.id}/ingest",
            files={"file": ("dados.json", b'[{"a": 1}]', "application/json")},
        )

    data = response.json()
    assert data["status"] == "FAILED"
    assert "Unsupported format" in data["error_message"]
    send_task.assert_not_called()


def test_ingest_minio_unavailable_returns_503(client):
    dataset_id = _create_dataset(client, "vendas")
    with patch(
        "api.services.ingest.upload_bronze",
        side_effect=StorageUnavailableError("timeout"),
    ):
        response = client.post(
            f"/datasets/{dataset_id}/ingest",
            files={"file": ("vendas.csv", b"data,valor\n1,100", "text/csv")},
        )

    assert response.status_code == 503


def test_ingest_minio_unavailable_registers_failed(client, db):
    from dataflow_common.models import Execution

    dataset_id = _create_dataset(client, "vendas")
    with patch(
        "api.services.ingest.upload_bronze",
        side_effect=StorageUnavailableError("timeout"),
    ):
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


def _add_child(db, parent, layer: str, status: str, created_at=None):
    from datetime import datetime, timezone

    from dataflow_common.models import Execution

    now = datetime.now(timezone.utc)
    child = Execution(
        dataset_id=parent.dataset_id,
        input_execution_id=parent.id,
        layer=layer,
        status=status,
        created_at=created_at or now,
        started_at=now,
        finished_at=now,
        storage_path=(
            f"{layer.lower()}/fake/{parent.id}.parquet" if status == "SUCCESS" else None
        ),
    )
    db.add(child)
    db.commit()
    db.refresh(child)
    return child


def _make_worker_mock(
    db, fail_at: str | None = None, failed_silver_first: bool = False
):
    """Simula o worker: ao receber send_task, cria Silver (+ Gold) no banco.

    fail_at=None     -> Silver SUCCESS, Gold SUCCESS (+ métricas)
    fail_at="SILVER" -> Silver FAILED, sem Gold (como no pipeline real)
    fail_at="GOLD"   -> Silver SUCCESS, Gold FAILED
    failed_silver_first=True simula um retry: antes da Silver boa, uma
    Silver FAILED mais antiga da primeira tentativa.
    """
    from datetime import datetime, timedelta, timezone

    from dataflow_common.models import Execution, ExecutionMetrics

    def fake_send_task(name, args, **kwargs):
        bronze = db.query(Execution).filter(Execution.id == args[0]).first()
        if failed_silver_first:
            earlier = datetime.now(timezone.utc) - timedelta(seconds=30)
            _add_child(db, bronze, "SILVER", "FAILED", created_at=earlier)

        if fail_at == "SILVER":
            _add_child(db, bronze, "SILVER", "FAILED")
            return
        silver = _add_child(db, bronze, "SILVER", "SUCCESS")

        if fail_at == "GOLD":
            _add_child(db, silver, "GOLD", "FAILED")
            return
        gold = _add_child(db, silver, "GOLD", "SUCCESS")
        db.add(
            ExecutionMetrics(
                gold_execution_id=gold.id,
                valid_count=1,
                rejected_count=0,
                rejection_rate=0.0,
                duration_seconds=0.1,
                corrupt_count=0,
            )
        )
        db.commit()

    return fake_send_task


def _post_sync(client, dataset_id, worker, grace=0, timeout=None):
    patches = [
        patch("api.services.ingest.celery_client.send_task", side_effect=worker),
        patch("api.routers.ingestion._SYNC_FAILED_GRACE_SECONDS", grace),
    ]
    if timeout is not None:
        patches.append(patch("api.routers.ingestion._SYNC_TIMEOUT_SECONDS", timeout))
    for p in patches:
        p.start()
    try:
        return client.post(
            f"/datasets/{dataset_id}/ingest?mode=sync",
            files={"file": ("vendas.csv", VALID_CSV, "text/csv")},
        )
    finally:
        for p in patches:
            p.stop()


def test_ingest_defaults_to_async(client):
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


def test_ingest_explicit_async(client):
    dataset_id = _create_dataset(client, "vendas")
    with patch("api.services.ingest.celery_client.send_task") as send_task:
        response = client.post(
            f"/datasets/{dataset_id}/ingest?mode=async",
            files={"file": ("vendas.csv", VALID_CSV, "text/csv")},
        )

    assert response.status_code == 202
    assert response.json()["layer"] == "BRONZE"
    send_task.assert_called_once()


def test_ingest_sync_success(client, db):
    dataset_id = _create_dataset(client, "vendas")
    response = _post_sync(client, dataset_id, _make_worker_mock(db))

    assert response.status_code == 200
    data = response.json()
    assert data["layer"] == "GOLD"
    assert data["status"] == "SUCCESS"


def test_ingest_sync_gold_failure(client, db):
    dataset_id = _create_dataset(client, "vendas")
    response = _post_sync(client, dataset_id, _make_worker_mock(db, fail_at="GOLD"))

    assert response.status_code == 200
    data = response.json()
    assert data["layer"] == "GOLD"
    assert data["status"] == "FAILED"


def test_ingest_sync_silver_failure_returns_silver(client, db):
    # antes: esperava uma Gold que nunca vinha e devolvia 504 após 300s
    dataset_id = _create_dataset(client, "vendas")
    response = _post_sync(
        client, dataset_id, _make_worker_mock(db, fail_at="SILVER"), timeout=5
    )

    assert response.status_code == 200
    data = response.json()
    assert data["layer"] == "SILVER"
    assert data["status"] == "FAILED"


def test_ingest_sync_ignores_failed_silver_from_previous_attempt(client, db):
    # antes: .first() sem ordenação pegava a Silver FAILED do 1º attempt
    dataset_id = _create_dataset(client, "vendas")
    worker = _make_worker_mock(db, failed_silver_first=True)
    response = _post_sync(client, dataset_id, worker, timeout=5)

    assert response.status_code == 200
    data = response.json()
    assert data["layer"] == "GOLD"
    assert data["status"] == "SUCCESS"


def test_ingest_sync_waits_retry_window_before_accepting_failed(client, db):
    # Silver acabou de falhar: pode haver retry vindo. Dentro da janela,
    # o sync continua esperando (aqui, até estourar o timeout curto).
    dataset_id = _create_dataset(client, "vendas")
    response = _post_sync(
        client, dataset_id, _make_worker_mock(db, fail_at="SILVER"), grace=90, timeout=1
    )

    assert response.status_code == 504


def test_ingest_sync_timeout(client):
    dataset_id = _create_dataset(client, "vendas")
    with (
        patch("api.services.ingest.celery_client.send_task"),
        patch("api.routers.ingestion._SYNC_TIMEOUT_SECONDS", 1),
    ):
        response = client.post(
            f"/datasets/{dataset_id}/ingest?mode=sync",
            files={"file": ("vendas.csv", VALID_CSV, "text/csv")},
        )

    assert response.status_code == 504


def test_invalid_mode_returns_422(client):
    dataset_id = _create_dataset(client, "vendas")
    response = client.post(
        f"/datasets/{dataset_id}/ingest?mode=batata",
        files={"file": ("vendas.csv", VALID_CSV, "text/csv")},
    )

    assert response.status_code == 422
