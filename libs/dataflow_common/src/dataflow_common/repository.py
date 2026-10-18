from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy.orm import Session

from dataflow_common.models import Dataset, Execution


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


def finish_execution(
    session: Session,
    execution: Execution,
    status: str,
    storage_path: str | None = None,
    error_message: str | None = None,
) -> None:
    execution.status = status
    execution.finished_at = _utcnow()
    if storage_path:
        execution.storage_path = storage_path
    if error_message:
        execution.error_message = error_message
    session.commit()
