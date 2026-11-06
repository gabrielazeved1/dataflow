from collections import Counter
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class FieldSchema(BaseModel):
    name: str
    type: Literal["date", "string", "integer", "float"]
    required: bool = True
    min: float | None = None
    max: float | None = None

    @model_validator(mode="after")
    def check_min_max_numeric_only(self) -> "FieldSchema":
        if self.min is not None or self.max is not None:
            if self.type not in ("integer", "float"):
                raise ValueError("min/max only apply to integer or float fields")
        return self


class DatasetSchema(BaseModel):
    fields: list[FieldSchema] = Field(min_length=1)

    @model_validator(mode="after")
    def check_unique_field_names(self) -> "DatasetSchema":
        names = [field.name for field in self.fields]
        duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
        if duplicates:
            raise ValueError(f"duplicate field names: {duplicates}")
        return self


class DatasetCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    # so csv: o Spark nao le json
    format: Literal["csv"]
    # "schema" é reservado no pydantic, dai o alias
    dataset_schema: DatasetSchema = Field(validation_alias="schema")


class DatasetResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: UUID
    name: str
    format: str
    # le o atributo schema do model e devolve como "schema"
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
