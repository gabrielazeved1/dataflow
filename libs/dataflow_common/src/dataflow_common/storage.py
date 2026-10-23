from datetime import datetime, timezone
from io import BytesIO
from uuid import UUID

import boto3
from botocore.exceptions import BotoCoreError, ClientError


class StorageUnavailableError(Exception):
    pass


def _client(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
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


def build_silver_path(dataset_name: str, dt: str, execution_id: UUID) -> str:
    return f"silver/{dataset_name}/dt={dt}/{execution_id}.parquet"


def build_silver_rejected_path(dataset_name: str, execution_id: UUID) -> str:
    return f"silver/{dataset_name}/_rejected/{execution_id}.parquet"


def build_gold_path(dataset_name: str, dt: str, execution_id: UUID) -> str:
    return f"gold/{dataset_name}/dt={dt}/{execution_id}.parquet"


def upload(
    bucket: str,
    key: str,
    body: bytes,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> None:
    try:
        _client(endpoint, access_key, secret_key).put_object(
            Bucket=bucket, Key=key, Body=body
        )
    except (BotoCoreError, ClientError) as exc:
        raise StorageUnavailableError(str(exc)) from exc


def download(
    bucket: str,
    key: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> bytes:
    try:
        buf = BytesIO()
        _client(endpoint, access_key, secret_key).download_fileobj(bucket, key, buf)
        return buf.getvalue()
    except (BotoCoreError, ClientError) as exc:
        raise StorageUnavailableError(str(exc)) from exc
