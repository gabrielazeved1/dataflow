from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

class DatasetCreate(BaseModel):
    # input: corpo do POST /datasets
    nome: str
    formato: str  


class DatasetResponse(BaseModel):
    # output: o que a API devolve ao criar ou consultar um dataset
    id: UUID
    nome: str
    formato: str
    created_at: datetime
    model_config = {"from_attributes": True}



class ExecutionResponse(BaseModel):
    # output: devolvido após uma ingestão ou consulta de status
    id: UUID
    dataset_id: UUID
    status: str
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    file_name: str | None = None
    file_size: int | None = None
    checksum: str | None = None

    model_config = {"from_attributes": True}
