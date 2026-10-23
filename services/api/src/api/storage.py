from uuid import UUID

from api.config import settings
from dataflow_common.storage import StorageUnavailableError, build_bronze_path
from dataflow_common.storage import upload as _upload

__all__ = ["StorageUnavailableError", "upload_bronze"]


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
