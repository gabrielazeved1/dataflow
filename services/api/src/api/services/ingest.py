import hashlib
from datetime import datetime, timezone

from dataflow_common.models import Dataset, Execution
from sqlalchemy.orm import Session

from api.celery_client import celery_client
from api.config import settings
from api.storage import StorageUnavailableError, upload_bronze


def ingest_bytes(
    db: Session,
    dataset: Dataset,
    filename: str,
    content: bytes,
) -> Execution:
    """Cria uma Execution BRONZE pros bytes recebidos: valida tamanho/formato,
    faz upload no MinIO, enfileira a task Celery de processamento.

    Retorna a Execution com status SUCCESS ou FAILED. Em caso de falha de
    storage (MinIO fora), repropaga StorageUnavailableError — cabe ao caller
    decidir o HTTP status. Qualquer outra falha (validação, formato) grava
    FAILED na Execution e retorna normalmente.
    """
    execution = Execution(
        dataset_id=dataset.id,
        layer="BRONZE",
        status="RUNNING",
        started_at=datetime.now(timezone.utc),
        file_name=filename,
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)

    try:
        if len(content) > settings.max_file_size_mb * 1024 * 1024:
            raise ValueError(f"Arquivo excede {settings.max_file_size_mb}MB")

        # dataset legado com formato != csv: falha explícita em vez de
        # gravar uma Bronze que o Spark não sabe ler.
        if dataset.formato != "csv":
            raise ValueError(f"formato '{dataset.formato}' não suportado")
        content.decode("utf-8")

        checksum = hashlib.sha256(content).hexdigest()
        storage_path = upload_bronze(content, dataset.nome, execution.id, filename)

        execution.status = "SUCCESS"
        execution.finished_at = datetime.now(timezone.utc)
        execution.file_size = len(content)
        execution.checksum = checksum
        execution.storage_path = storage_path
        db.commit()
        db.refresh(execution)

    except StorageUnavailableError as exc:
        execution.status = "FAILED"
        execution.finished_at = datetime.now(timezone.utc)
        execution.error_message = str(exc)
        db.commit()
        raise

    except Exception as exc:
        execution.status = "FAILED"
        execution.finished_at = datetime.now(timezone.utc)
        execution.error_message = str(exc)
        db.commit()
        db.refresh(execution)
        return execution

    celery_client.send_task("processing.ingest", args=[str(execution.id)])
    return execution
