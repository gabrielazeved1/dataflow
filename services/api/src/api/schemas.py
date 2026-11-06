from collections import Counter
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

    @model_validator(mode="after")
    def nomes_de_campos_unicos(self) -> "DatasetSchema":
        nomes = [campo.nome for campo in self.campos]
        duplicados = sorted(
            nome for nome, count in Counter(nomes).items() if count > 1
        )
        if duplicados:
            raise ValueError(f"nomes de campos duplicados: {duplicados}")
        return self


class DatasetCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    nome: str
    # só CSV por enquanto: o pipeline Spark lê apenas CSV. JSON Lines é V6
    # (ver v5e.md, achado A1).
    formato: Literal["csv"]
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


class ExecutionListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    dataset_id: UUID
    layer: str
    status: str
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    file_name: str | None = None
    storage_path: str | None = None
    input_execution_id: UUID | None = None


class ExecutionListResponse(BaseModel):
    items: list[ExecutionListItem]
    next_cursor: str | None = None


class ExecutionMetricsResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    valid_count: int
    rejected_count: int
    rejection_rate: float
    duration_seconds: float
    corrupt_count: int


class ExecutionDetailResponse(ExecutionResponse):
    metrics: ExecutionMetricsResponse | None = None


class RejectedRowsResponse(BaseModel):
    rows: list[dict[str, Any]]
