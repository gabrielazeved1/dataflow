from uuid import UUID

from dataflow_common.models import Execution
from dataflow_common.repository import create_execution
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from processing.errors import ConcurrentExecutionError


def _find_success_child(
    session: Session, parent_id: UUID, layer: str
) -> Execution | None:
    return (
        session.query(Execution)
        .filter(
            Execution.input_execution_id == parent_id,
            Execution.layer == layer,
            Execution.status == "SUCCESS",
        )
        .first()
    )


def start_child_execution(
    session: Session, *, dataset_id: UUID, parent_id: UUID, layer: str
) -> tuple[Execution, bool]:
    # created=False: ja existe filha SUCCESS, pule
    # o SELECT é atalho->> quem garante é o indice uq_execution_child_active
    existing = _find_success_child(session, parent_id, layer)
    if existing is not None:
        return existing, False

    try:
        child = create_execution(
            session, dataset_id=dataset_id, layer=layer, input_execution_id=parent_id
        )
    except IntegrityError:
        session.rollback()
        # a concorrente pode ter terminado entre o SELECT e o INSERT
        existing = _find_success_child(session, parent_id, layer)
        if existing is not None:
            return existing, False
        raise ConcurrentExecutionError(
            f"{layer} de {parent_id} ja esta RUNNING em outra task"
        ) from None
    return child, True
