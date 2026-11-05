from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from api.database import get_db
from api.dev_utils import generate_csv
from dataflow_common.models import Dataset

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
        raise HTTPException(status_code=404, detail="Dataset não encontrado")
    csv_text = generate_csv(dataset.schema, rows, invalid_rate)
    filename = f"sample_{dataset.nome}_{rows}rows.csv"
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
