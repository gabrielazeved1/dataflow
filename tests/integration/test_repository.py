import uuid

import pytest
from dataflow_common import repository
from dataflow_common.models import Dataset, Execution


@pytest.fixture
def sample_dataset(db):
    dataset = Dataset(
        name="vendas_test",
        format="csv",
        schema={"fields": [{"name": "data", "type": "date"}]},
    )
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    return dataset


def test_get_dataset_returns_dataset_when_found(db, sample_dataset):
    result = repository.get_dataset(db, sample_dataset.id)
    assert result is not None
    assert result.id == sample_dataset.id
    assert result.name == "vendas_test"


def test_get_dataset_returns_none_when_missing(db):
    assert repository.get_dataset(db, uuid.uuid4()) is None


def test_get_execution_returns_execution_when_found(db, sample_dataset):
    execution = Execution(
        dataset_id=sample_dataset.id, layer="BRONZE", status="SUCCESS"
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)
    result = repository.get_execution(db, execution.id)
    assert result is not None
    assert result.id == execution.id


def test_get_execution_returns_none_when_missing(db):
    assert repository.get_execution(db, uuid.uuid4()) is None


def test_create_execution_starts_running(db, sample_dataset):
    execution = repository.create_execution(
        db, dataset_id=sample_dataset.id, layer="SILVER"
    )
    assert execution.status == "RUNNING"
    assert execution.layer == "SILVER"
    assert execution.started_at is not None
    assert execution.input_execution_id is None


def test_create_execution_with_input_execution_id(db, sample_dataset):
    bronze = repository.create_execution(
        db, dataset_id=sample_dataset.id, layer="BRONZE"
    )
    silver = repository.create_execution(
        db,
        dataset_id=sample_dataset.id,
        layer="SILVER",
        input_execution_id=bronze.id,
    )
    assert silver.input_execution_id == bronze.id


def test_finish_execution_success(db, sample_dataset):
    execution = repository.create_execution(
        db, dataset_id=sample_dataset.id, layer="BRONZE"
    )
    repository.finish_execution(
        db, execution, status="SUCCESS", storage_path="bronze/foo/bar.csv"
    )
    assert execution.status == "SUCCESS"
    assert execution.finished_at is not None
    assert execution.storage_path == "bronze/foo/bar.csv"
    assert execution.error_message is None


def test_finish_execution_failed(db, sample_dataset):
    execution = repository.create_execution(
        db, dataset_id=sample_dataset.id, layer="BRONZE"
    )
    repository.finish_execution(db, execution, status="FAILED", error_message="boom")
    assert execution.status == "FAILED"
    assert execution.error_message == "boom"
    assert execution.storage_path is None


def test_finish_execution_does_not_overwrite_reaper_failed(db, sample_dataset):
    execution = repository.create_execution(
        db, dataset_id=sample_dataset.id, layer="SILVER"
    )
    # simula o reaper marcando timeout enquanto o job ainda roda
    db.query(Execution).filter(Execution.id == execution.id).update(
        {"status": "FAILED", "error_message": "timeout"}
    )
    db.commit()

    with pytest.raises(repository.ExecutionAlreadyFinishedError):
        repository.finish_execution(
            db, execution, status="SUCCESS", storage_path="silver/x.parquet"
        )

    db.expire_all()
    db.refresh(execution)
    assert execution.status == "FAILED"
    assert execution.error_message == "timeout"
    assert execution.storage_path is None
