import uuid
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Dataset(Base):
    __tablename__ = "dataset"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String, unique=True)
    format: Mapped[str] = mapped_column(String)
    schema: Mapped[dict] = mapped_column(JSONB)  # schema do usuario é dado, nao codigo
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow
    )


class Execution(Base):
    __tablename__ = "execution"
    # no maximo uma filha ativa por (pai, layer)
    # FAILED fica fora pro retry criar outra tentativa
    __table_args__ = (
        Index(
            "uq_execution_child_active",
            "input_execution_id",
            "layer",
            unique=True,
            postgresql_where=text("status IN ('RUNNING', 'SUCCESS')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    dataset_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("dataset.id")
    )
    input_execution_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("execution.id")
    )

    layer: Mapped[str] = mapped_column(String, default="BRONZE")
    status: Mapped[str] = mapped_column(String, default="PENDING")

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    error_message: Mapped[str | None] = mapped_column(Text)
    file_name: Mapped[str | None] = mapped_column(String)
    file_size: Mapped[int | None] = mapped_column(BigInteger)
    checksum: Mapped[str | None] = mapped_column(String)
    storage_path: Mapped[str | None] = mapped_column(String)


class ExecutionMetrics(Base):
    __tablename__ = "execution_metrics"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    gold_execution_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("execution.id"), unique=True
    )

    valid_count: Mapped[int] = mapped_column(BigInteger)
    rejected_count: Mapped[int] = mapped_column(BigInteger)
    rejection_rate: Mapped[float] = mapped_column(Float)
    duration_seconds: Mapped[float] = mapped_column(Float)
    corrupt_count: Mapped[int] = mapped_column(BigInteger)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow
    )


# passo a passo de uma ingestao, da api ate a gold
# trace_id é a bronze, raiz da cadeia: um grep por ele mostra o caminho todo
class ExecutionEvent(Base):
    __tablename__ = "execution_event"
    __table_args__ = (Index("ix_execution_event_trace", "trace_id", "created_at"),)

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    trace_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("execution.id")
    )
    execution_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("execution.id")
    )
    service: Mapped[str] = mapped_column(String)
    step: Mapped[str] = mapped_column(String)
    message: Mapped[str] = mapped_column(Text)
    details: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow
    )
