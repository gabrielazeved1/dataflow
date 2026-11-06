import time
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from dataflow_common.models import Dataset, Execution
from fastapi import APIRouter, Depends, File, HTTPException, Query, Response, UploadFile
from sqlalchemy.orm import Session

from api.database import get_db
from api.schemas import ExecutionResponse
from api.services.ingest import ingest_bytes
from api.storage import StorageUnavailableError

router = APIRouter(prefix="/datasets", tags=["ingestion"])

_SYNC_TIMEOUT_SECONDS = 300
_SYNC_POLL_INTERVAL_SECONDS = 1
# Uma execução FAILED só é tratada como resultado final depois dessa janela
# sem tentativa nova. Precisa ser maior que o backoff máximo do retry da
# task (retry_backoff_max=60s), senão o sync devolve FAILED no meio de um
# retry que ainda pode dar certo.
_SYNC_FAILED_GRACE_SECONDS = 90


def _latest_child(db: Session, parent_id: UUID, layer: str) -> Execution | None:
    # a mais recente: com retry, a mesma Bronze pode ter várias Silvers
    # (as tentativas FAILED ficam no histórico)
    return (
        db.query(Execution)
        .filter(Execution.input_execution_id == parent_id, Execution.layer == layer)
        .order_by(Execution.created_at.desc())
        .first()
    )


def _is_terminal(execution: Execution) -> bool:
    if execution.status == "SUCCESS":
        return True
    if execution.status != "FAILED":
        return False
    failed_at = execution.finished_at or execution.created_at
    age = (datetime.now(timezone.utc) - failed_at).total_seconds()
    return age >= _SYNC_FAILED_GRACE_SECONDS


def _wait_for_result(db: Session, bronze_id: UUID, timeout: int) -> Execution:
    """Polling no Postgres até o pipeline terminar.

    Devolve a Gold (SUCCESS ou FAILED) ou, se o pipeline parou na Silver,
    a Silver FAILED. Levanta TimeoutError se nada terminar em `timeout`s.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        silver = _latest_child(db, bronze_id, "SILVER")
        if silver is not None and _is_terminal(silver):
            if silver.status == "FAILED":
                return silver
            gold = _latest_child(db, silver.id, "GOLD")
            if gold is not None and _is_terminal(gold):
                return gold
        # close() devolve a conexão ao pool durante o sleep; a Session
        # reabre outra na próxima query. Sem isso, cada chamada sync
        # segura uma conexão por até `timeout` segundos.
        db.close()
        time.sleep(_SYNC_POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"Pipeline did not finish within {timeout}s")


@router.post("/{dataset_id}/ingest", response_model=ExecutionResponse, status_code=202)
def ingest(
    dataset_id: UUID,
    response: Response,
    file: UploadFile = File(...),
    mode: Literal["sync", "async"] = Query("async"),
    db: Session = Depends(get_db),
):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not file.filename:
        raise HTTPException(status_code=400, detail="File has no name")

    content = file.file.read()
    try:
        execution = ingest_bytes(db, dataset, file.filename, content)
    except StorageUnavailableError:
        raise HTTPException(status_code=503, detail="Storage unavailable, try again")

    if mode == "async" or execution.status == "FAILED":
        return execution

    try:
        result = _wait_for_result(db, execution.id, _SYNC_TIMEOUT_SECONDS)
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc))
    response.status_code = 200
    return result
