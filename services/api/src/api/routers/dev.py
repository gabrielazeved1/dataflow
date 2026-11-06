from uuid import UUID

from dataflow_common.models import Dataset
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from api.database import get_db
from api.dev_utils import generate_csv
from api.services.ingest import ingest_bytes
from api.storage import StorageUnavailableError

router = APIRouter(prefix="/dev", tags=["dev"])


@router.post("/csv/sample")
def post_csv_sample(
    dataset_id: UUID,
    rows: int = Query(100, ge=1, le=100_000),
    invalid_rate: float = Query(0.0, ge=0.0, le=1.0),
    db: Session = Depends(get_db),
) -> Response:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    csv_text = generate_csv(dataset.schema, rows, invalid_rate)
    filename = f"sample_{dataset.name}_{rows}rows.csv"
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/bulk-ingest", status_code=202)
def post_bulk_ingest(
    dataset_id: UUID,
    count: int = Query(5, ge=1, le=50),
    rows: int = Query(1000, ge=1, le=100_000),
    invalid_rate: float = Query(0.1, ge=0.0, le=1.0),
    db: Session = Depends(get_db),
) -> dict:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    execution_ids: list[str] = []
    try:
        for i in range(count):
            csv_bytes = generate_csv(dataset.schema, rows, invalid_rate).encode("utf-8")
            execution = ingest_bytes(db, dataset, f"synthetic_{i}.csv", csv_bytes)
            execution_ids.append(str(execution.id))
    except StorageUnavailableError:
        raise HTTPException(status_code=503, detail="Storage unavailable, try again")

    return {"count": count, "rows_each": rows, "execution_ids": execution_ids}
