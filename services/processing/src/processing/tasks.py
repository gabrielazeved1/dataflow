from datetime import datetime, timedelta, timezone
from uuid import UUID

from dataflow_common.models import Execution
from dataflow_common.storage import StorageUnavailableError
from sqlalchemy import and_, exists, func, or_, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import aliased

from processing.bronze_to_silver import run as _run_bronze_to_silver
from processing.celery_app import celery_app
from processing.config import settings
from processing.database import SessionLocal
from processing.errors import ConcurrentExecutionError
from processing.silver_to_gold import run as _run_silver_to_gold


@celery_app.task(
    name="processing.ingest",
    autoretry_for=(OperationalError, StorageUnavailableError),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    default_retry_delay=2,
    max_retries=3,  # 3 retries = 4 tentativas no total
)
def ingest(bronze_execution_id: str) -> str | None:
    bronze_id = UUID(bronze_execution_id)
    try:
        silver_id = _run_bronze_to_silver(bronze_id)
        gold_id = _run_silver_to_gold(silver_id)
    except ConcurrentExecutionError as exc:
        # mensagem duplicada: a outra task faz o trabalho
        print(f"[Ingest] desistindo: {exc}")
        return None
    return str(gold_id)


@celery_app.task(name="processing.reap_stuck_executions")
def reap_stuck_executions() -> int:
    # marca FAILED o que passou do timeout em RUNNING
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


def find_bronzes_to_reconcile(session, now: datetime) -> list[UUID]:
    # Bronze sem Silver: send_task perdido (redis fora ou api caiu)
    # Bronze so com Silver FAILED por timeout: worker morreu no meio
    # erro de dado nao entra, repetir nao resolve
    blocking = aliased(Execution)
    attempt = aliased(Execution)

    # Silver ativa ou FAILED por outro motivo bloqueia
    has_blocking_silver = exists().where(
        blocking.input_execution_id == Execution.id,
        blocking.layer == "SILVER",
        or_(
            blocking.status.in_(["RUNNING", "SUCCESS"]),
            and_(
                blocking.status == "FAILED",
                or_(
                    blocking.error_message.is_(None),
                    blocking.error_message != "timeout",
                ),
            ),
        ),
    )
    attempts = (
        select(func.count())
        .where(attempt.input_execution_id == Execution.id, attempt.layer == "SILVER")
        .scalar_subquery()
    )

    rows = (
        session.query(Execution.id)
        .filter(
            Execution.layer == "BRONZE",
            Execution.status == "SUCCESS",
            Execution.finished_at
            < now - timedelta(minutes=settings.reconcile_after_minutes),
            Execution.finished_at
            > now - timedelta(hours=settings.reconcile_max_age_hours),
            ~has_blocking_silver,
            attempts < settings.reconcile_max_attempts,
        )
        .all()
    )
    return [row.id for row in rows]


@celery_app.task(name="processing.reconcile_bronzes")
def reconcile_bronzes() -> int:
    # a tabela execution faz papel de outbox
    # reenfileirar é seguro por causa do indice de idempotencia
    with SessionLocal() as session:
        bronze_ids = find_bronzes_to_reconcile(session, datetime.now(timezone.utc))
    for bronze_id in bronze_ids:
        ingest.delay(str(bronze_id))
    if bronze_ids:
        print(f"[Reconciler] {len(bronze_ids)} Bronze(s) reenfileirada(s)")
    return len(bronze_ids)
