from datetime import datetime, timedelta, timezone

from dataflow_common.models import Dataset, Execution
from processing.tasks import reap_stuck_executions

_SCHEMA = {"fields": [{"name": "x", "type": "string", "required": True}]}


def _make_running_execution(db, started_at: datetime) -> Execution:
    dataset = Dataset(name="reaper_test", format="csv", schema=_SCHEMA)
    db.add(dataset)
    db.commit()
    db.refresh(dataset)

    execution = Execution(
        dataset_id=dataset.id,
        layer="BRONZE",
        status="RUNNING",
        started_at=started_at,
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)
    return execution


def test_reaper_marks_old_execution_failed(db):
    old_started = datetime.now(timezone.utc) - timedelta(hours=2)
    execution = _make_running_execution(db, started_at=old_started)

    updated = reap_stuck_executions()

    assert updated == 1
    db.expire_all()
    db.refresh(execution)
    assert execution.status == "FAILED"
    assert execution.error_message == "timeout"
    assert execution.finished_at is not None


def test_reaper_ignores_recent_execution(db):
    recent_started = datetime.now(timezone.utc) - timedelta(seconds=10)
    execution = _make_running_execution(db, started_at=recent_started)

    updated = reap_stuck_executions()

    assert updated == 0
    db.expire_all()
    db.refresh(execution)
    assert execution.status == "RUNNING"
    assert execution.error_message is None


def test_reaper_ignores_finished_execution(db):
    old_started = datetime.now(timezone.utc) - timedelta(hours=2)
    dataset = Dataset(name="reaper_test", format="csv", schema=_SCHEMA)
    db.add(dataset)
    db.commit()
    db.refresh(dataset)

    execution = Execution(
        dataset_id=dataset.id,
        layer="BRONZE",
        status="SUCCESS",
        started_at=old_started,
        finished_at=old_started + timedelta(seconds=5),
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)

    updated = reap_stuck_executions()

    assert updated == 0
    db.expire_all()
    db.refresh(execution)
    assert execution.status == "SUCCESS"
