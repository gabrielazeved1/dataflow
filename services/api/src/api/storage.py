from datetime import datetime, timezone
from uuid import UUID

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from api.config import settings


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{settings.minio_endpoint}",
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
    )


def build_bronze_path(dataset_name: str, execution_id: UUID, file_name: str) -> str:
    now = datetime.now(timezone.utc)
    return (
        f"bronze/{dataset_name}"
        f"/ano={now.year}"
        f"/mes={now.month:02d}"
        f"/dia={now.day:02d}"
        f"/{execution_id}_{file_name}"
    )


def upload_bronze(content: bytes, dataset_name: str, execution_id: UUID, file_name: str) -> str:
    path = build_bronze_path(dataset_name, execution_id, file_name)
    try:
        client = _s3_client()
        client.put_object(
            Bucket=settings.minio_bucket,
            Key=path,
            Body=content,
        )
    except (BotoCoreError, ClientError) as exc:
        raise StorageUnavailableError(str(exc)) from exc
    return path


class StorageUnavailableError(Exception):
    pass
