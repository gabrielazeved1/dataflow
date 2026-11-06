import sys
from unittest.mock import patch

import pytest
from dataflow_common.models import Execution
from dataflow_common.storage import build_silver_rejected_path
from processing.bronze_to_silver import main


def _run(execution_id):
    with patch.object(sys, "argv", ["processing", str(execution_id)]):
        main()


def test_success_status(db, bronze_execution):
    _run(bronze_execution.id)

    db.expire_all()
    silver = db.query(Execution).filter(Execution.layer == "SILVER").first()
    assert silver.status == "SUCCESS"
    assert silver.storage_path.startswith("silver/vendas_test/")


def test_parquet_exists_in_minio(db, bronze_execution, minio_client):
    _run(bronze_execution.id)

    db.expire_all()
    silver = db.query(Execution).filter(Execution.layer == "SILVER").first()
    minio_client.head_object(Bucket="dataflow", Key=silver.storage_path)


def test_rejected_parquet_in_minio(db, bronze_execution_with_nulls, minio_client):
    _run(bronze_execution_with_nulls.id)

    db.expire_all()
    silver = db.query(Execution).filter(Execution.layer == "SILVER").first()
    rejected_key = build_silver_rejected_path("vendas_test", silver.id)
    minio_client.head_object(Bucket="dataflow", Key=rejected_key)


def test_unknown_execution_id_exits():
    with pytest.raises(SystemExit):
        _run("00000000-0000-0000-0000-000000000000")


def test_idempotent_does_not_duplicate_silver(db, bronze_execution):
    _run(bronze_execution.id)
    _run(bronze_execution.id)

    db.expire_all()
    silvers = db.query(Execution).filter(Execution.layer == "SILVER").all()
    assert len(silvers) == 1
    assert silvers[0].status == "SUCCESS"


def test_run_unknown_execution_raises_precondition_error():
    # run() é usado pela task Celery: não pode levantar SystemExit
    from uuid import UUID

    from processing.bronze_to_silver import run
    from processing.errors import PreconditionError

    with pytest.raises(PreconditionError):
        run(UUID("00000000-0000-0000-0000-000000000000"))


def test_download_failure_marks_silver_failed_and_allows_retry(db, bronze_execution):
    # regressão: o download rodava fora do try e deixava a Silver RUNNING;
    # com o índice de idempotência, o retry desistia em vez de reprocessar
    from dataflow_common.storage import StorageUnavailableError
    from processing.bronze_to_silver import run

    with patch(
        "processing.bronze_to_silver.download",
        side_effect=StorageUnavailableError("minio fora"),
    ):
        with pytest.raises(StorageUnavailableError):
            run(bronze_execution.id)

    db.expire_all()
    silvers = db.query(Execution).filter(Execution.layer == "SILVER").all()
    assert [s.status for s in silvers] == ["FAILED"]

    run(bronze_execution.id)  # retry: MinIO de volta

    db.expire_all()
    statuses = sorted(
        s.status for s in db.query(Execution).filter(Execution.layer == "SILVER")
    )
    assert statuses == ["FAILED", "SUCCESS"]
