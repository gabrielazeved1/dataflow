from uuid import UUID

from api.config import settings
from dataflow_common.storage import (
    StorageUnavailableError,
    build_bronze_path,
    build_silver_rejected_path,
)
from dataflow_common.storage import download as _download
from dataflow_common.storage import upload as _upload

__all__ = ["StorageUnavailableError", "upload_bronze", "download_silver_rejected"]


def upload_bronze(content: bytes, dataset_name: str, execution_id: UUID, file_name: str) -> str:
    path = build_bronze_path(dataset_name, execution_id, file_name)
    _upload(
        bucket=settings.minio_bucket,
        key=path,
        body=content,
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )
    return path


def download_silver_rejected(dataset_name: str, silver_execution_id: UUID) -> bytes:
    path = build_silver_rejected_path(dataset_name, silver_execution_id)
    return _download(
        bucket=settings.minio_bucket,
        key=path,
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )
