import pytest
from dataflow_common.models import Dataset, Execution
from processing.errors import ConcurrentExecutionError
from processing.idempotency import start_child_execution
from sqlalchemy.exc import IntegrityError

_SCHEMA = {"campos": [{"nome": "x", "tipo": "string", "obrigatorio": True}]}


@pytest.fixture
def bronze(db):
    dataset = Dataset(nome="idem", formato="csv", schema=_SCHEMA)
    db.add(dataset)
    db.commit()
    execution = Execution(dataset_id=dataset.id, layer="BRONZE", status="SUCCESS")
    db.add(execution)
    db.commit()
    db.refresh(execution)
    return execution


def _child(db, bronze, status):
    child = Execution(
        dataset_id=bronze.dataset_id,
        input_execution_id=bronze.id,
        layer="SILVER",
        status=status,
    )
    db.add(child)
    db.commit()
    return child


def test_index_blocks_two_active_children(db, bronze):
    _child(db, bronze, "RUNNING")
    with pytest.raises(IntegrityError):
        _child(db, bronze, "RUNNING")
    db.rollback()


def test_index_allows_new_attempt_after_failed(db, bronze):
    _child(db, bronze, "FAILED")
    _child(db, bronze, "FAILED")
    _child(db, bronze, "RUNNING")  # retry: não colide com as FAILED


def test_start_child_creates_when_no_child(db, bronze):
    child, created = start_child_execution(
        db, dataset_id=bronze.dataset_id, parent_id=bronze.id, layer="SILVER"
    )
    assert created
    assert child.status == "RUNNING"


def test_start_child_returns_existing_success(db, bronze):
    existing = _child(db, bronze, "SUCCESS")
    child, created = start_child_execution(
        db, dataset_id=bronze.dataset_id, parent_id=bronze.id, layer="SILVER"
    )
    assert not created
    assert child.id == existing.id


def test_start_child_with_running_child_raises_concurrency(db, bronze):
    # outra task já está processando esta Bronze
    _child(db, bronze, "RUNNING")
    with pytest.raises(ConcurrentExecutionError):
        start_child_execution(
            db, dataset_id=bronze.dataset_id, parent_id=bronze.id, layer="SILVER"
        )
    running = db.query(Execution).filter(Execution.layer == "SILVER").all()
    assert len(running) == 1
