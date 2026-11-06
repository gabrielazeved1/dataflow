from datetime import datetime, timedelta, timezone
from uuid import UUID

from dataflow_common.models import Execution
from dataflow_common.storage import StorageUnavailableError
from sqlalchemy.exc import OperationalError

from processing.bronze_to_silver import run as _run_bronze_to_silver
from processing.celery_app import celery_app
from processing.config import settings
from processing.errors import ConcurrentExecutionError
from processing.database import SessionLocal
from processing.silver_to_gold import run as _run_silver_to_gold


@celery_app.task(
    name="processing.ingest",
    autoretry_for=(OperationalError, StorageUnavailableError),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    default_retry_delay=2,
    max_retries=3,  # 3 retries = 4 execucoes totais
)
def ingest(bronze_execution_id: str) -> str | None:
    bronze_id = UUID(bronze_execution_id)
    try:
        silver_id = _run_bronze_to_silver(bronze_id)
        gold_id = _run_silver_to_gold(silver_id)
    except ConcurrentExecutionError as exc:
        # mensagem duplicada pra mesma Bronze: a outra task faz o trabalho
        print(f"[Ingest] desistindo: {exc}")
        return None
    return str(gold_id)


@celery_app.task(name="processing.reap_stuck_executions")
def reap_stuck_executions() -> int:
    # marca como failed toda execution  mais antiga que
    # execution_timeout_minutes. agendada pelo celery beat.
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=settings.execution_timeout_minutes)
    with SessionLocal() as session:
        updated = (
            session.query(Execution)
            .filter(
                Execution.status == "RUNNING",
                Execution.started_at < cutoff,
            )
            .update(
                {
                    Execution.status: "FAILED",
                    Execution.finished_at: now,
                    Execution.error_message: "timeout",
                },
                synchronize_session=False,
            )
        )
        session.commit()
        if updated:
            print(f"[Reaper] {updated} execution(s) em RUNNING timeout -> FAILED")
        return updated
