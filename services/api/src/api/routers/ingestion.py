import hashlib
import json
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session

from api.config import settings
from api.database import get_db
from api.models import Dataset, Execution
from api.schemas import ExecutionResponse

router = APIRouter(prefix="/datasets", tags=["ingestion"])


# POST /datasets/{dataset_id}/ingest
# UploadFile = arquivo enviado via multipart/form-data
@router.post("/{dataset_id}/ingest", response_model=ExecutionResponse, status_code=202)
def ingest(dataset_id: UUID, file: UploadFile = File(...), db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset não encontrado")

    # regra de negócio: criar o registro ANTES de processar
    # se o processo morrer no meio, o registro fica em RUNNING — detectável
    execution = Execution(
        dataset_id=dataset.id,
        status="RUNNING",
        started_at=datetime.now(timezone.utc),
        file_name=file.filename,
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)

    try:
        content = file.file.read()

        # validacao de tamanho
        if len(content) > settings.max_file_size_mb * 1024 * 1024:
            raise ValueError(f"Arquivo excede {settings.max_file_size_mb}MB")

        # validacao estrutural: Bronze só verifica se o arquivo abre — não o conteudo
        if dataset.formato == "csv":
            content.decode("utf-8")
        elif dataset.formato == "json":
            json.loads(content)

        execution.status = "SUCCESS"
        execution.finished_at = datetime.now(timezone.utc)
        execution.file_size = len(content)
        execution.checksum = hashlib.sha256(content).hexdigest()

    except Exception as e:
        # qualquer falha: registra motivo e marca FAILED — nunca silencia o erro
        execution.status = "FAILED"
        execution.finished_at = datetime.now(timezone.utc)
        execution.error_message = str(e)

    db.commit()
    db.refresh(execution)
    return execution
