from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.orm import Session

from dataflow_common.models import Dataset, Execution, ExecutionMetrics


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_execution(session: Session, execution_id: UUID) -> Execution | None:
    return session.query(Execution).filter(Execution.id == execution_id).first()


def get_dataset(session: Session, dataset_id: UUID) -> Dataset | None:
    return session.query(Dataset).filter(Dataset.id == dataset_id).first()


def create_execution(
    session: Session,
    dataset_id: UUID,
    layer: str,
    input_execution_id: UUID | None = None,
) -> Execution:
    execution = Execution(
        dataset_id=dataset_id,
        layer=layer,
        input_execution_id=input_execution_id,
        status="RUNNING",
        started_at=_utcnow(),
    )
    session.add(execution)
    session.commit()
    session.refresh(execution)
    return execution


def get_trace_root(session: Session, execution: Execution) -> UUID:
    # sobe gold -> silver -> bronze; a bronze é o trace_id
    current = execution
    while current.input_execution_id is not None:
        parent = get_execution(session, current.input_execution_id)
        if parent is None:
            break
        current = parent
    return current.id


# a Execution saiu de RUNNING por outro caminho (ex.: reaper)
class ExecutionAlreadyFinishedError(Exception):
    pass


def finish_execution(
    session: Session,
    execution: Execution,
    status: str,
    storage_path: str | None = None,
    error_message: str | None = None,
) -> None:
    # so sai de RUNNING: se o reaper ja marcou FAILED, ele vence
    values: dict = {"status": status, "finished_at": _utcnow()}
    if storage_path:
        values["storage_path"] = storage_path
    if error_message:
        values["error_message"] = error_message

    updated_id = session.execute(
        update(Execution)
        .where(Execution.id == execution.id, Execution.status == "RUNNING")
        .values(**values)
        .returning(Execution.id)
        .execution_options(synchronize_session=False)
    ).scalar_one_or_none()
    if updated_id is None:
        # descarta pendentes da transacao (ex.: metricas da Gold)
        session.rollback()
        raise ExecutionAlreadyFinishedError(
            f"Execution {execution.id} nao esta mais em RUNNING"
        )
    session.commit()
    session.refresh(execution)


def create_execution_metrics(
    session: Session,
    gold_execution: Execution,
    valid_count: int,
    rejected_count: int,
    rejection_rate: float,
    duration_seconds: float,
    corrupt_count: int,
) -> ExecutionMetrics:
    metrics = ExecutionMetrics(
        gold_execution_id=gold_execution.id,
        valid_count=valid_count,
        rejected_count=rejected_count,
        rejection_rate=rejection_rate,
        duration_seconds=duration_seconds,
        corrupt_count=corrupt_count,
    )
    session.add(metrics)
    session.flush()
    return metrics
