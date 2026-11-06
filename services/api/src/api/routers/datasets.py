from uuid import UUID

from dataflow_common.models import Dataset
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.database import get_db
from api.schemas import DatasetCreate, DatasetResponse

router = APIRouter(prefix="/datasets", tags=["datasets"])


@router.post("/", response_model=DatasetResponse, status_code=201)
def create_dataset(data: DatasetCreate, db: Session = Depends(get_db)):
    if db.query(Dataset).filter(Dataset.name == data.name).first():
        raise HTTPException(status_code=409, detail="Dataset already exists")

    dataset = Dataset(
        name=data.name,
        format=data.format,
        schema=data.dataset_schema.model_dump(exclude_none=True),
    )
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    return dataset


@router.get("/", response_model=list[DatasetResponse])
def list_datasets(db: Session = Depends(get_db)):
    return db.query(Dataset).order_by(Dataset.created_at.desc()).all()


@router.get("/{dataset_id}", response_model=DatasetResponse)
def get_dataset(dataset_id: UUID, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset
