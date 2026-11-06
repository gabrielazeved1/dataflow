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


class ExecutionAlreadyFinishedError(Exception):
    """A Execution saiu de RUNNING por outro caminho (ex.: reaper)."""


def finish_execution(
    session: Session,
    execution: Execution,
    status: str,
    storage_path: str | None = None,
    error_message: str | None = None,
) -> None:
    # UPDATE condicional: só sai de RUNNING. Se o reaper já marcou FAILED
    # por timeout, estado terminal não volta atrás: o reaper vence.
    values: dict = {"status": status, "finished_at": _utcnow()}
    if storage_path:
        values["storage_path"] = storage_path
    if error_message:
        values["error_message"] = error_message

    result = session.execute(
        update(Execution)
        .where(Execution.id == execution.id, Execution.status == "RUNNING")
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    if result.rowcount == 0:
        # descarta o que estava pendente na transação (ex.: métricas da Gold)
        session.rollback()
        raise ExecutionAlreadyFinishedError(
            f"Execution {execution.id} não está mais em RUNNING"
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
