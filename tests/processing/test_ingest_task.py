from unittest.mock import patch
from uuid import uuid4

import pytest
from processing import tasks
from processing.celery_app import celery_app
from sqlalchemy.exc import OperationalError


@pytest.fixture
def celery_eager_fast(monkeypatch):
    # task no mesmo processo e sem espera entre retries
    monkeypatch.setattr(celery_app.conf, "task_always_eager", True)
    monkeypatch.setattr(celery_app.conf, "task_eager_propagates", False)
    monkeypatch.setattr(tasks.ingest, "default_retry_delay", 0)
    monkeypatch.setattr(tasks.ingest, "retry_backoff", False)
    monkeypatch.setattr(tasks.ingest, "retry_jitter", False)
    yield


def test_ingest_retries_operational_error_until_success(
    celery_eager_fast,
):
    bronze_id = uuid4()
    silver_id = uuid4()
    gold_id = uuid4()

    bronze_side_effects = [
        OperationalError("conn lost", None, Exception("boom")),
        OperationalError("conn lost", None, Exception("boom")),
        silver_id,  # terceira tentativa passa
    ]

    with (
        patch.object(
            tasks, "_run_bronze_to_silver", side_effect=bronze_side_effects
        ) as mock_bronze,
        patch.object(tasks, "_run_silver_to_gold", return_value=gold_id) as mock_gold,
    ):
        result = tasks.ingest.apply(args=[str(bronze_id)])

    assert result.successful()
    assert result.result == str(gold_id)
    assert mock_bronze.call_count == 3
    assert mock_gold.call_count == 1


def test_ingest_gives_up_after_max_retries(celery_eager_fast):
    bronze_id = uuid4()

    with patch.object(
        tasks,
        "_run_bronze_to_silver",
        side_effect=OperationalError("sempre falha", None, Exception("boom")),
    ) as mock_bronze:
        result = tasks.ingest.apply(args=[str(bronze_id)])

    assert result.failed()
    # 3 retries = 4 chamadas
    assert mock_bronze.call_count == 4


def test_ingest_does_not_retry_permanent_error(celery_eager_fast):
    bronze_id = uuid4()

    # ValueError fora do autoretry_for: sem retry
    with patch.object(
        tasks,
        "_run_bronze_to_silver",
        side_effect=ValueError("csv malformado"),
    ) as mock_bronze:
        result = tasks.ingest.apply(args=[str(bronze_id)])

    assert result.failed()
    assert mock_bronze.call_count == 1


def test_duplicate_ingest_gives_up_without_failing(celery_eager_fast):
    from processing.errors import ConcurrentExecutionError

    with (
        patch.object(
            tasks, "_run_bronze_to_silver", side_effect=ConcurrentExecutionError("dup")
        ) as mock_bronze,
        patch.object(tasks, "_run_silver_to_gold") as mock_gold,
    ):
        result = tasks.ingest.apply(args=[str(uuid4())])

    assert result.successful()
    assert result.result is None
    assert mock_bronze.call_count == 1  # sem retry
    mock_gold.assert_not_called()
