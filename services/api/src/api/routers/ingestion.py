import time
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response, UploadFile, File
from sqlalchemy.orm import Session

from api.database import get_db
from api.schemas import ExecutionResponse
from api.services.ingest import ingest_bytes
from api.storage import StorageUnavailableError
from dataflow_common.models import Dataset, Execution

router = APIRouter(prefix="/datasets", tags=["ingestion"])

_SYNC_TIMEOUT_SECONDS = 300
_SYNC_POLL_INTERVAL_SECONDS = 1


def _wait_for_gold(db: Session, bronze_id: UUID, timeout: int) -> Execution:
    """faz polling no postgres até a gold filha chegar em SUCCESS ou FAILED.
    levanta timeouterror se não terminar dentro de `timeout` segundos."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        silver = (
            db.query(Execution)
            .filter(
                Execution.input_execution_id == bronze_id,
                Execution.layer == "SILVER",
            )
            .first()
        )
        if silver is not None:
            gold = (
                db.query(Execution)
                .filter(
                    Execution.input_execution_id == silver.id,
                    Execution.layer == "GOLD",
                    Execution.status.in_(["SUCCESS", "FAILED"]),
                )
                .first()
            )
            if gold is not None:
                return gold
        db.expire_all()
        time.sleep(_SYNC_POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"Pipeline não terminou em {timeout}s")


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
        raise HTTPException(status_code=404, detail="Dataset não encontrado")

    if not file.filename:
        raise HTTPException(status_code=400, detail="Arquivo sem nome")

    content = file.file.read()
    try:
        execution = ingest_bytes(db, dataset, file.filename, content)
    except StorageUnavailableError:
        raise HTTPException(status_code=503, detail="Storage indisponível, tente novamente")

    if mode == "async" or execution.status == "FAILED":
        return execution

    try:
        gold = _wait_for_gold(db, execution.id, _SYNC_TIMEOUT_SECONDS)
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc))
    response.status_code = 200
    return gold
