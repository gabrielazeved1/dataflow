import hashlib
import json
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session

from api.config import settings
from api.database import get_db
from dataflow_common.models import Dataset, Execution
from api.schemas import ExecutionResponse
from api.storage import StorageUnavailableError, upload_bronze

router = APIRouter(prefix="/datasets", tags=["ingestion"])


@router.post("/{dataset_id}/ingest", response_model=ExecutionResponse, status_code=202)
def ingest(dataset_id: UUID, file: UploadFile = File(...), db: Session = Depends(get_db)):
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
