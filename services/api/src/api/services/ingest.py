import hashlib
from datetime import datetime, timezone

from dataflow_common.events import trace_event
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
    # grava a bronze e enfileira o processamento
    # minio fora repropaga StorageUnavailableError pro caller decidir o status http
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
    bind = db.get_bind()

    def trace(step: str, message: str, **details) -> None:
        trace_event(bind, execution.id, "api", step, message, execution.id, **details)

    trace("received", f"{filename} recebido", size=len(content))

    try:
        if len(content) > settings.max_file_size_mb * 1024 * 1024:
            raise ValueError(f"File exceeds {settings.max_file_size_mb}MB")

        # dataset legado com format != csv
        if dataset.format != "csv":
            raise ValueError(f"Unsupported format '{dataset.format}'")
        content.decode("utf-8")

        checksum = hashlib.sha256(content).hexdigest()
        storage_path = upload_bronze(content, dataset.name, execution.id, filename)

        execution.status = "SUCCESS"
        execution.finished_at = datetime.now(timezone.utc)
        execution.file_size = len(content)
        execution.checksum = checksum
        execution.storage_path = storage_path
        db.commit()
        db.refresh(execution)
        trace("bronze_stored", "bronze gravada no minio", path=storage_path)

    except StorageUnavailableError as exc:
        execution.status = "FAILED"
        execution.finished_at = datetime.now(timezone.utc)
        execution.error_message = str(exc)
        db.commit()
        trace("failed", "minio indisponivel", error=str(exc))
        raise

    except Exception as exc:
        execution.status = "FAILED"
        execution.finished_at = datetime.now(timezone.utc)
        execution.error_message = str(exc)
        db.commit()
        db.refresh(execution)
        trace("failed", "bronze rejeitada", error=str(exc))
        return execution

    try:
        celery_client.send_task("processing.ingest", args=[str(execution.id)])
    except Exception as exc:
        # bronze ja esta salva: o reconciler reenfileira, nao é erro pro cliente
        trace(
            "enqueue_failed",
            "redis indisponivel, reconciler reenfileira",
            error=str(exc),
        )
        return execution
    trace("enqueued", "task enfileirada no redis")
    return execution
