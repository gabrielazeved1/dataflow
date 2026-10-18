from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CampoSchema(BaseModel):
    nome: str
    tipo: Literal["date", "string", "integer", "float"]
    obrigatorio: bool = True
    min: float | None = None
    max: float | None = None

    @model_validator(mode="after")
    def min_max_apenas_numerico(self) -> "CampoSchema":
        if self.min is not None or self.max is not None:
            if self.tipo not in ("integer", "float"):
                raise ValueError("min/max só são aplicáveis a campos integer ou float")
        return self


class DatasetSchema(BaseModel):
    campos: list[CampoSchema] = Field(min_length=1)


class DatasetCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    nome: str
    formato: Literal["csv", "json"]
    # "schema" é reservado em Pydantic — usamos validation_alias para ler o JSON
    dataset_schema: DatasetSchema = Field(validation_alias="schema")


class DatasetResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: UUID
    nome: str
    formato: str
    # lê do atributo SQLAlchemy "schema" via validation_alias, serializa como "schema"
    dataset_schema: dict[str, Any] = Field(
        validation_alias="schema", serialization_alias="schema"
    )
    created_at: datetime


class ExecutionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    dataset_id: UUID
    layer: str
    status: str
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    file_name: str | None = None
    file_size: int | None = None
    checksum: str | None = None
    storage_path: str | None = None
    input_execution_id: UUID | None = None
