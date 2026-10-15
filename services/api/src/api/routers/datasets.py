from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import Dataset
from api.schemas import DatasetCreate, DatasetResponse

router = APIRouter(prefix="/datasets", tags=["datasets"])


# POST /datasets
@router.post("/", response_model=DatasetResponse, status_code=201)
def create_dataset(data: DatasetCreate, db: Session = Depends(get_db)):
    if db.query(Dataset).filter(Dataset.nome == data.nome).first():
        raise HTTPException(status_code=409, detail="Dataset já cadastrado")

    dataset = Dataset(nome=data.nome, formato=data.formato)
    db.add(dataset)
    db.commit()
    db.refresh(dataset)  # atualiza o objeto com os valores gerados pelo banco (id, created_at)
    return dataset


# GET /datasets/{dataset_id}
@router.get("/{dataset_id}", response_model=DatasetResponse)
def get_dataset(dataset_id: UUID, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset não encontrado")
    return dataset
