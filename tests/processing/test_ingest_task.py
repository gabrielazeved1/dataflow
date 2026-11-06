from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy.exc import OperationalError

from processing import tasks
from processing.celery_app import celery_app


@pytest.fixture
def celery_eager_fast(monkeypatch):
    # roda a task no mesmo processo + zera delays de retry pra teste
    # nao ficar dormindo 2s + 4s + 8s entre tentativas.
    monkeypatch.setattr(celery_app.conf, "task_always_eager", True)
    monkeypatch.setattr(celery_app.conf, "task_eager_propagates", False)
    monkeypatch.setattr(tasks.ingest, "default_retry_delay", 0)
    monkeypatch.setattr(tasks.ingest, "retry_backoff", False)
    monkeypatch.setattr(tasks.ingest, "retry_jitter", False)
    yield


def test_ingest_retenta_em_operational_error_e_eventualmente_succeeds(celery_eager_fast):
    bronze_id = uuid4()
    silver_id = uuid4()
    gold_id = uuid4()

    bronze_side_effects = [
        OperationalError("conn lost", None, Exception("boom")),
        OperationalError("conn lost", None, Exception("boom")),
        silver_id,  # 3a tentativa succeed
    ]

    with (
        patch.object(tasks, "_run_bronze_to_silver", side_effect=bronze_side_effects) as mock_bronze,
        patch.object(tasks, "_run_silver_to_gold", return_value=gold_id) as mock_gold,
    ):
        result = tasks.ingest.apply(args=[str(bronze_id)])

    assert result.successful()
    assert result.result == str(gold_id)
    assert mock_bronze.call_count == 3
    assert mock_gold.call_count == 1


def test_ingest_desiste_apos_max_retries(celery_eager_fast):
    bronze_id = uuid4()

    with patch.object(
        tasks,
        "_run_bronze_to_silver",
        side_effect=OperationalError("sempre falha", None, Exception("boom")),
    ) as mock_bronze:
        result = tasks.ingest.apply(args=[str(bronze_id)])

    assert result.failed()
    # max_retries=3 → 4 execucoes totais (1 original + 3 retries)
    assert mock_bronze.call_count == 4


def test_ingest_nao_retenta_em_erro_permanente(celery_eager_fast):
    bronze_id = uuid4()

    # ValueError nao esta em autoretry_for → nao retenta
    with patch.object(
        tasks,
        "_run_bronze_to_silver",
        side_effect=ValueError("csv malformado"),
    ) as mock_bronze:
        result = tasks.ingest.apply(args=[str(bronze_id)])

    assert result.failed()
    assert mock_bronze.call_count == 1


def test_ingest_duplicado_desiste_sem_falhar(celery_eager_fast):
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
