import uuid
from datetime import datetime, timezone

from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Dataset(Base):
    __tablename__ = "dataset"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    nome = Column(String, nullable=False, unique=True)
    formato = Column(String, nullable=False)
    schema = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utcnow)


class Execution(Base):
    __tablename__ = "execution"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    dataset_id = Column(UUID(as_uuid=True), ForeignKey("dataset.id"), nullable=False)
    input_execution_id = Column(UUID(as_uuid=True), ForeignKey("execution.id"), nullable=True)

    layer = Column(String, nullable=False, default="BRONZE")
    status = Column(String, nullable=False, default="PENDING")

    created_at = Column(DateTime(timezone=True), nullable=False, default=_utcnow)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)

    error_message = Column(Text, nullable=True)
    file_name = Column(String, nullable=True)
    file_size = Column(BigInteger, nullable=True)
    checksum = Column(String, nullable=True)
    storage_path = Column(String, nullable=True)
