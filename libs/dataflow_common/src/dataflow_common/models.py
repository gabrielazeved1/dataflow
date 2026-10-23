import uuid
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Dataset(Base):
    __tablename__ = "dataset"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    nome: Mapped[str] = mapped_column(String, unique=True)
    formato: Mapped[str] = mapped_column(String)
    schema: Mapped[dict] = mapped_column(JSONB) # o postgres aceita um json , ou seja o schema mora como dado
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class Execution(Base):
    __tablename__ = "execution"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    dataset_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), ForeignKey("dataset.id"))
    input_execution_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), ForeignKey("execution.id"))

    layer: Mapped[str] = mapped_column(String, default="BRONZE")
    status: Mapped[str] = mapped_column(String, default="PENDING")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    error_message: Mapped[str | None] = mapped_column(Text)
    file_name: Mapped[str | None] = mapped_column(String)
    file_size: Mapped[int | None] = mapped_column(BigInteger)
    checksum: Mapped[str | None] = mapped_column(String)
    storage_path: Mapped[str | None] = mapped_column(String)
