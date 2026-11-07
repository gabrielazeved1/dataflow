from uuid import UUID

from dataflow_common.models import Dataset, Execution, ExecutionEvent
from dataflow_common.repository import get_trace_root
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.database import get_db
from api.schemas import TraceResponse

router = APIRouter(prefix="/traces", tags=["traces"])


@router.get("/{execution_id}", response_model=TraceResponse)
def get_trace(execution_id: UUID, db: Session = Depends(get_db)):
    # aceita id de qualquer camada e sobe ate a bronze
    execution = db.query(Execution).filter(Execution.id == execution_id).first()
    if execution is None:
        raise HTTPException(status_code=404, detail="Execution not found")
    bronze_id = get_trace_root(db, execution)

    bronze = db.query(Execution).filter(Execution.id == bronze_id).one()
    silvers = (
        db.query(Execution).filter(Execution.input_execution_id == bronze_id).all()
    )
    golds = (
        db.query(Execution)
        .filter(Execution.input_execution_id.in_([s.id for s in silvers]))
        .all()
        if silvers
        else []
    )
    dataset = db.query(Dataset).filter(Dataset.id == bronze.dataset_id).one()
    events = (
        db.query(ExecutionEvent)
        .filter(ExecutionEvent.trace_id == bronze_id)
        .order_by(ExecutionEvent.created_at)
        .all()
    )
    chain = sorted([bronze, *silvers, *golds], key=lambda e: e.created_at)
    return TraceResponse(
        trace_id=bronze_id,
        dataset_name=dataset.name,
        executions=chain,
        events=events,
    )
