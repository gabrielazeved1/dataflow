import pytest
from sqlalchemy.exc import IntegrityError

from dataflow_common.models import Dataset, Execution
from processing.errors import ConcurrentExecutionError
from processing.idempotency import start_child_execution

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


def test_indice_impede_duas_filhas_ativas(db, bronze):
    _child(db, bronze, "RUNNING")
    with pytest.raises(IntegrityError):
        _child(db, bronze, "RUNNING")
    db.rollback()


def test_indice_permite_nova_tentativa_apos_failed(db, bronze):
    _child(db, bronze, "FAILED")
    _child(db, bronze, "FAILED")
    _child(db, bronze, "RUNNING")  # retry: não colide com as FAILED


def test_start_child_cria_quando_nao_ha_filha(db, bronze):
    child, created = start_child_execution(
        db, dataset_id=bronze.dataset_id, parent_id=bronze.id, layer="SILVER"
    )
    assert created
    assert child.status == "RUNNING"


def test_start_child_devolve_success_existente(db, bronze):
    existing = _child(db, bronze, "SUCCESS")
    child, created = start_child_execution(
        db, dataset_id=bronze.dataset_id, parent_id=bronze.id, layer="SILVER"
    )
    assert not created
    assert child.id == existing.id


def test_start_child_com_filha_running_levanta_concorrencia(db, bronze):
    # outra task já está processando esta Bronze
    _child(db, bronze, "RUNNING")
    with pytest.raises(ConcurrentExecutionError):
        start_child_execution(
            db, dataset_id=bronze.dataset_id, parent_id=bronze.id, layer="SILVER"
        )
    running = db.query(Execution).filter(Execution.layer == "SILVER").all()
    assert len(running) == 1
