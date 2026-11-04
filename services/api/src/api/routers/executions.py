from datetime import date, datetime, time, timedelta, timezone
from io import BytesIO
from uuid import UUID

import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from api.database import get_db
from api.schemas import (
    ExecutionDetailResponse,
    ExecutionListItem,
    ExecutionMetricsResponse,
    RejectedRowsResponse,
)
from api.storage import StorageUnavailableError, download_silver_rejected
from dataflow_common.models import Dataset, Execution, ExecutionMetrics

router = APIRouter(prefix="/executions", tags=["executions"])


# GET /executions
@router.get("/", response_model=list[ExecutionListItem])
def list_executions(
    dataset: str | None = Query(None, description="Nome do dataset"),
    status: str | None = Query(None, description="RUNNING | SUCCESS | FAILED"),
    layer: str | None = Query(None, description="BRONZE | SILVER | GOLD"),
    from_: date | None = Query(None, alias="from", description="Data inicial (YYYY-MM-DD, inclusivo)"),
    to: date | None = Query(None, description="Data final (YYYY-MM-DD, inclusivo)"),
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

    return query.order_by(Execution.created_at.desc()).all()


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
        raise HTTPException(status_code=400, detail="Execution SILVER precisa estar SUCCESS")
    if execution.storage_path is None:
        raise HTTPException(status_code=400, detail="Execution sem storage_path")

    dataset = db.query(Dataset).filter(Dataset.id == execution.dataset_id).first()
    assert dataset is not None  # FK garante existência
    try:
        parquet_bytes = download_silver_rejected(dataset.nome, silver_id)
    except StorageUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    table = pq.read_table(BytesIO(parquet_bytes))
    return {"rows": table.to_pylist()[:limit]}


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
