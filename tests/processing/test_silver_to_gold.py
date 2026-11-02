import io
import sys
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from dataflow_common.models import Execution, ExecutionMetrics
from dataflow_common.storage import build_gold_path

from processing.silver_to_gold import main


def _run(execution_id):
    with patch.object(sys, "argv", ["processing", str(execution_id)]):
        main()


def test_success_status(db, silver_execution):
    _run(silver_execution.id)

    db.expire_all()
    gold = db.query(Execution).filter(Execution.layer == "GOLD").first()
    assert gold.status == "SUCCESS"
    assert gold.storage_path.startswith("gold/vendas_test/")


def test_parquet_exists_in_minio(db, silver_execution, minio_client):
    _run(silver_execution.id)

    db.expire_all()
    gold = db.query(Execution).filter(Execution.layer == "GOLD").first()
    minio_client.head_object(Bucket="dataflow", Key=gold.storage_path)


def test_metrics_sao_corretas(db, silver_execution, minio_client):
    _run(silver_execution.id)

    db.expire_all()
    gold = db.query(Execution).filter(Execution.layer == "GOLD").first()
    obj = minio_client.get_object(Bucket="dataflow", Key=gold.storage_path)
    rows = pq.read_table(io.BytesIO(obj["Body"].read())).to_pylist()

    assert len(rows) == 1
    row = rows[0]
    assert row["valid_count"] == 1
    assert row["rejected_count"] == 1
    assert row["rejection_rate"] == 0.5
    assert row["corrupt_count"] == 0
    assert row["duration_seconds"] > 0


def test_metrics_persistidas_em_execution_metrics(db, silver_execution):
    _run(silver_execution.id)

    db.expire_all()
    gold = db.query(Execution).filter(Execution.layer == "GOLD").first()
    metrics = (
        db.query(ExecutionMetrics)
        .filter(ExecutionMetrics.gold_execution_id == gold.id)
        .one()
    )
    assert metrics.valid_count == 1
    assert metrics.rejected_count == 1
    assert metrics.rejection_rate == 0.5
    assert metrics.corrupt_count == 0
    assert metrics.duration_seconds > 0


def test_silver_not_success_exits(db, bronze_execution_with_nulls):
    from dataflow_common.repository import create_execution

    silver = create_execution(
        db,
        dataset_id=bronze_execution_with_nulls.dataset_id,
        layer="SILVER",
        input_execution_id=bronze_execution_with_nulls.id,
    )
    # silver fica em RUNNING, sem storage_path
    with pytest.raises(SystemExit):
        _run(silver.id)


def test_unknown_execution_id_exits():
    with pytest.raises(SystemExit):
        _run("00000000-0000-0000-0000-000000000000")
