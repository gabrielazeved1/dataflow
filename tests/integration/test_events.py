import uuid

from dataflow_common.events import trace_event
from dataflow_common.models import Dataset, Execution, ExecutionEvent


def _bronze(db):
    dataset = Dataset(name="eventos", format="csv", schema={"fields": []})
    db.add(dataset)
    db.commit()
    bronze = Execution(dataset_id=dataset.id, layer="BRONZE", status="RUNNING")
    db.add(bronze)
    db.commit()
    return bronze


def test_trace_event_records_event(db):
    bronze = _bronze(db)

    trace_event(
        db.get_bind(), bronze.id, "api", "received", "csv recebido", size=10, path=None
    )

    event = db.query(ExecutionEvent).one()
    assert event.trace_id == bronze.id
    assert (event.service, event.step) == ("api", "received")
    assert event.details == {"size": 10, "path": None}


def test_trace_event_survives_step_rollback(db):
    # evento de falha gravado no meio de uma transacao que depois é desfeita
    bronze = _bronze(db)
    db.add(Dataset(name="vai_sumir", format="csv", schema={"fields": []}))

    trace_event(db.get_bind(), bronze.id, "worker", "failed", "spark quebrou")
    db.rollback()

    assert db.query(Dataset).filter(Dataset.name == "vai_sumir").count() == 0
    assert db.query(ExecutionEvent).count() == 1


def test_trace_event_never_raises(db):
    # trace_id inexistente viola a fk: o evento se perde, o pipeline segue
    trace_event(db.get_bind(), uuid.uuid4(), "api", "received", "sem bronze")

    assert db.query(ExecutionEvent).count() == 0
