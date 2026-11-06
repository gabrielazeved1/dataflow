import logging
from typing import Any
from uuid import UUID

from sqlalchemy import Connection, Engine
from sqlalchemy.orm import Session

from dataflow_common.models import ExecutionEvent

logger = logging.getLogger("dataflow.trace")


def trace_event(
    bind: Engine | Connection,
    trace_id: UUID,
    service: str,
    step: str,
    message: str,
    execution_id: UUID | None = None,
    **details: Any,
) -> None:
    # mesma linha no log e na tabela, nunca divergem
    logger.info("trace=%s %s.%s %s", trace_id, service, step, message)
    clean = {k: str(v) if isinstance(v, UUID) else v for k, v in details.items()}
    # sessao propria: o evento de falha sobrevive ao rollback da etapa
    try:
        with Session(bind) as session:
            session.add(
                ExecutionEvent(
                    trace_id=trace_id,
                    execution_id=execution_id,
                    service=service,
                    step=step,
                    message=message,
                    details=clean or None,
                )
            )
            session.commit()
    except Exception:
        # rastreio nunca derruba o pipeline
        logger.warning("trace=%s falha ao gravar evento %s", trace_id, step)
