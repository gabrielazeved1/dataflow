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
