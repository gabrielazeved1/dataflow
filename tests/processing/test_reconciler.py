from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from dataflow_common.models import Dataset, Execution
from processing import tasks
from processing.tasks import find_bronzes_to_reconcile

_SCHEMA = {"fields": [{"name": "x", "type": "string", "required": True}]}
NOW = datetime.now(timezone.utc)


@pytest.fixture
def dataset(db):
    d = Dataset(name="reconcile", format="csv", schema=_SCHEMA)
    db.add(d)
    db.commit()
    return d


def _bronze(db, dataset, minutes_ago=30, status="SUCCESS"):
    finished = NOW - timedelta(minutes=minutes_ago)
    b = Execution(
        dataset_id=dataset.id,
        layer="BRONZE",
        status=status,
        started_at=finished,
        finished_at=finished,
        storage_path="bronze/x.csv",
    )
    db.add(b)
    db.commit()
    return b


def _silver(db, bronze, status, error_message=None):
    s = Execution(
        dataset_id=bronze.dataset_id,
        input_execution_id=bronze.id,
        layer="SILVER",
        status=status,
        error_message=error_message,
    )
    db.add(s)
    db.commit()
    return s


def _found(db):
    return set(find_bronzes_to_reconcile(db, NOW))


def test_bronze_without_silver_is_reconciled(db, dataset):
    b = _bronze(db, dataset)
    assert _found(db) == {b.id}


def test_recent_bronze_waits(db, dataset):
    # pode estar so na fila
    _bronze(db, dataset, minutes_ago=2)
    assert _found(db) == set()


def test_too_old_bronze_is_ignored(db, dataset):
    _bronze(db, dataset, minutes_ago=60 * 48)
    assert _found(db) == set()


def test_failed_bronze_is_ignored(db, dataset):
    _bronze(db, dataset, status="FAILED")
    assert _found(db) == set()


def test_running_or_success_silver_blocks(db, dataset):
    b1 = _bronze(db, dataset)
    _silver(db, b1, "RUNNING")
    b2 = _bronze(db, dataset)
    _silver(db, b2, "SUCCESS")
    assert _found(db) == set()


def test_silver_failed_by_timeout_is_reconciled(db, dataset):
    # worker morreu e o reaper marcou: vale tentar de novo
    b = _bronze(db, dataset)
    _silver(db, b, "FAILED", error_message="timeout")
    assert _found(db) == {b.id}


def test_silver_failed_by_data_error_is_not_reconciled(db, dataset):
    b = _bronze(db, dataset)
    _silver(db, b, "FAILED", error_message="cannot parse")
    assert _found(db) == set()


def test_attempt_limit(db, dataset):
    b = _bronze(db, dataset)
    for _ in range(3):
        _silver(db, b, "FAILED", error_message="timeout")
    assert _found(db) == set()


def test_task_enqueues_ingest_for_each_bronze(db, dataset):
    b1 = _bronze(db, dataset)
    b2 = _bronze(db, dataset)
    with patch.object(tasks.ingest, "delay") as delay:
        count = tasks.reconcile_bronzes()

    assert count == 2
    assert {c.args[0] for c in delay.call_args_list} == {str(b1.id), str(b2.id)}
