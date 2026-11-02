import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response, UploadFile, File
from sqlalchemy.orm import Session

from api.celery_client import celery_client
from api.config import settings
from api.database import get_db
from dataflow_common.models import Dataset, Execution
from api.schemas import ExecutionResponse
from api.storage import StorageUnavailableError, upload_bronze

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

    execution = Execution(
        dataset_id=dataset.id,
        layer="BRONZE",
        status="RUNNING",
        started_at=datetime.now(timezone.utc),
        file_name=file.filename,
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)

    try:
        content = file.file.read()

        if len(content) > settings.max_file_size_mb * 1024 * 1024:
            raise ValueError(f"Arquivo excede {settings.max_file_size_mb}MB")

        if dataset.formato == "csv":
            content.decode("utf-8")
        elif dataset.formato == "json":
            json.loads(content)

        checksum = hashlib.sha256(content).hexdigest()

        storage_path = upload_bronze(content, dataset.nome, execution.id, file.filename)

        execution.status = "SUCCESS"
        execution.finished_at = datetime.now(timezone.utc)
        execution.file_size = len(content)
        execution.checksum = checksum
        execution.storage_path = storage_path
        db.commit()
        db.refresh(execution)

    except StorageUnavailableError as e:
        execution.status = "FAILED"
        execution.finished_at = datetime.now(timezone.utc)
        execution.error_message = str(e)
        db.commit()
        raise HTTPException(status_code=503, detail="Storage indisponível, tente novamente")

    except Exception as e:
        execution.status = "FAILED"
        execution.finished_at = datetime.now(timezone.utc)
        execution.error_message = str(e)
        db.commit()
        db.refresh(execution)
        return execution

    celery_client.send_task("processing.ingest", args=[str(execution.id)])

    if mode == "async":
        return execution

    try:
        gold = _wait_for_gold(db, execution.id, _SYNC_TIMEOUT_SECONDS)
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e))
    response.status_code = 200
    return gold
