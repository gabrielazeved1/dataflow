import base64
import binascii
import json
from datetime import date, datetime, time, timedelta, timezone
from io import BytesIO
from uuid import UUID

import pyarrow.parquet as pq
from dataflow_common.models import Dataset, Execution, ExecutionMetrics
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import tuple_
from sqlalchemy.orm import Session

from api.database import get_db
from api.schemas import (
    ExecutionDetailResponse,
    ExecutionListItem,
    ExecutionListResponse,
    ExecutionMetricsResponse,
    RejectedRowsResponse,
)
from api.storage import StorageUnavailableError, download_silver_rejected

router = APIRouter(prefix="/executions", tags=["executions"])


def _encode_cursor(created_at: datetime, execution_id: UUID) -> str:
    payload = json.dumps(
        {"created_at": created_at.isoformat(), "id": str(execution_id)}
    )
    return base64.urlsafe_b64encode(payload.encode()).decode()


def _decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        return datetime.fromisoformat(payload["created_at"]), UUID(payload["id"])
    except (
        binascii.Error,
        ValueError,
        KeyError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        raise HTTPException(status_code=400, detail="cursor inválido")


# GET /executions
@router.get("/", response_model=ExecutionListResponse)
def list_executions(
    dataset: str | None = Query(None, description="Nome do dataset"),
    status: str | None = Query(None, description="RUNNING | SUCCESS | FAILED"),
    layer: str | None = Query(None, description="BRONZE | SILVER | GOLD"),
    from_: date | None = Query(
        None, alias="from", description="Data inicial (YYYY-MM-DD, inclusivo)"
    ),
    to: date | None = Query(None, description="Data final (YYYY-MM-DD, inclusivo)"),
    limit: int = Query(50, ge=1, le=200),
    after: str | None = Query(None, description="Cursor opaco pra próxima página"),
    db: Session = Depends(get_db),
):
    query = db.query(Execution)

    if dataset is not None:
        query = query.join(Dataset).filter(Dataset.nome == dataset)
    if status is not None:
        query = query.filter(Execution.status == status)
    if layer is not None:
        query = query.filter(Execution.layer == layer)
    if from_ is not None:
        from_dt = datetime.combine(from_, time.min, tzinfo=timezone.utc)
        query = query.filter(Execution.created_at >= from_dt)
    if to is not None:
        to_dt = datetime.combine(to + timedelta(days=1), time.min, tzinfo=timezone.utc)
        query = query.filter(Execution.created_at < to_dt)

    if after is not None:
        cursor_created_at, cursor_id = _decode_cursor(after)
        query = query.filter(
            tuple_(Execution.created_at, Execution.id)
            < tuple_(cursor_created_at, cursor_id)
        )

    rows = (
        query.order_by(Execution.created_at.desc(), Execution.id.desc())
        .limit(limit + 1)
        .all()
    )

    has_next = len(rows) > limit
    items = rows[:limit]
    next_cursor = (
        _encode_cursor(items[-1].created_at, items[-1].id) if has_next else None
    )
    return ExecutionListResponse(
        items=[ExecutionListItem.model_validate(e) for e in items],
        next_cursor=next_cursor,
    )


# GET /executions/{silver_id}/rejected
@router.get("/{silver_id}/rejected", response_model=RejectedRowsResponse)
def get_rejected_preview(
    silver_id: UUID,
    limit: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db),
):
    execution = db.query(Execution).filter(Execution.id == silver_id).first()
    if execution is None:
        raise HTTPException(status_code=404, detail="Execution não encontrada")
    if execution.layer != "SILVER":
        raise HTTPException(status_code=400, detail="Execution precisa ser SILVER")
    if execution.status != "SUCCESS":
        raise HTTPException(
            status_code=400, detail="Execution SILVER precisa estar SUCCESS"
        )
    if execution.storage_path is None:
        raise HTTPException(status_code=400, detail="Execution sem storage_path")

    dataset = db.query(Dataset).filter(Dataset.id == execution.dataset_id).first()
    assert dataset is not None  # FK garante existência
    try:
        parquet_bytes = download_silver_rejected(dataset.nome, silver_id)
    except StorageUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    table = pq.read_table(BytesIO(parquet_bytes))
    # slice antes de converter: evita materializar todas as linhas em dicts
    return {"rows": table.slice(0, limit).to_pylist()}


# GET /executions/{execution_id}
@router.get("/{execution_id}", response_model=ExecutionDetailResponse)
def get_execution_detail(execution_id: UUID, db: Session = Depends(get_db)):
    execution = db.query(Execution).filter(Execution.id == execution_id).first()
    if execution is None:
        raise HTTPException(status_code=404, detail="Execution não encontrada")

    response = ExecutionDetailResponse.model_validate(execution)
    if execution.layer == "GOLD":
        metrics = (
            db.query(ExecutionMetrics)
            .filter(ExecutionMetrics.gold_execution_id == execution.id)
            .first()
        )
        if metrics is not None:
            response.metrics = ExecutionMetricsResponse.model_validate(metrics)
    return response
