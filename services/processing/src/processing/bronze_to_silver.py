import sys
from datetime import datetime, timezone
from functools import reduce
from operator import and_
from pathlib import Path
from uuid import UUID

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType

from dataflow_common.repository import (
    ExecutionAlreadyFinishedError,
    finish_execution,
    get_dataset,
    get_execution,
)
from dataflow_common.storage import (
    build_silver_path,
    build_silver_rejected_path,
    download,
    upload,
)

from processing.config import settings
from processing.errors import PreconditionError
from processing.idempotency import start_child_execution
from processing.database import SessionLocal
from processing.schema_mapper import build_struct_type
from processing.spark import build_spark


def _build_is_valid(dataset_schema: dict) -> Column:
    conditions: list[Column] = [col("_corrupt_record").isNull()]
    for campo in dataset_schema["campos"]:
        nome = campo["nome"]
        if campo["obrigatorio"]:
            conditions.append(col(nome).isNotNull())
        if campo.get("min") is not None:
            conditions.append(col(nome) >= campo["min"])
        if campo.get("max") is not None:
            conditions.append(col(nome) <= campo["max"])
    return reduce(and_, conditions)


def _split(df: DataFrame, dataset_schema: dict) -> tuple[DataFrame, DataFrame]:
    is_valid = _build_is_valid(dataset_schema)
    valid_df = df.filter(is_valid).drop("_corrupt_record")
    rejected_df = df.filter(~is_valid)
    return valid_df, rejected_df


def run(bronze_execution_id: UUID) -> UUID:
    with SessionLocal() as session:
        bronze_exec = get_execution(session, bronze_execution_id)
        if bronze_exec is None:
            raise PreconditionError(f"Execution {bronze_execution_id} nao encontrada")
        if bronze_exec.layer != "BRONZE":
            raise PreconditionError(
                f"Execution {bronze_execution_id} e {bronze_exec.layer}, esperava BRONZE"
            )
        if bronze_exec.storage_path is None:
            raise PreconditionError(f"Execution {bronze_execution_id} sem storage_path")

        dataset = get_dataset(session, bronze_exec.dataset_id)
        if dataset is None:
            raise PreconditionError(f"Dataset {bronze_exec.dataset_id} nao encontrado")

        print(f"[Bronze] execution={bronze_exec.id}")
        print(f"         storage_path={bronze_exec.storage_path}")
        print(f"         file_name={bronze_exec.file_name}")
        print(f"[Dataset] id={dataset.id} nome={dataset.nome} formato={dataset.formato}")
        print(f"          campos={[c['nome'] for c in dataset.schema['campos']]}")

        silver_exec, created = start_child_execution(
            session, dataset_id=dataset.id, parent_id=bronze_exec.id, layer="SILVER"
        )
        if not created:
            print(f"[Skip] Silver ja existe: {silver_exec.id} status=SUCCESS")
            return silver_exec.id

        print(f"[Silver] execution={silver_exec.id} status={silver_exec.status}")

        # tudo depois de criar a filha RUNNING fica dentro do try: se falhar
        # fora dele (download, boot da JVM), a filha fica RUNNING, o índice
        # de idempotência bloqueia o retry e a task desiste sem processar.
        spark = None
        try:
            bronze_bytes = download(
                bucket=settings.minio_bucket,
                key=bronze_exec.storage_path,
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
            )

            local_path = Path("/tmp") / f"{silver_exec.id}.csv"
            local_path.write_bytes(bronze_bytes)

            print(f"[Download] {len(bronze_bytes)} bytes -> {local_path}")

            base_struct = build_struct_type(dataset.schema)
            read_struct = StructType(
                base_struct.fields + [StructField("_corrupt_record", StringType(), True)]
            )
            spark = build_spark(f"bronze_to_silver:{silver_exec.id}")
            df = spark.read.csv(
                str(local_path),
                header=True,
                schema=read_struct,
                mode="PERMISSIVE",
                columnNameOfCorruptRecord="_corrupt_record",
            ).cache()

            total = df.count()

            valid_df, rejected_df = _split(df, dataset.schema)
            valid_count = valid_df.count()
            rejected_count = rejected_df.count()

            print(f"[Split] total={total} valid={valid_count} rejected={rejected_count}")
            if rejected_count > 0:
                print("[Rejected] amostras:")
                rejected_df.show(5, truncate=False)

            dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            valid_local = Path("/tmp") / f"{silver_exec.id}_valid"
            rejected_local = Path("/tmp") / f"{silver_exec.id}_rejected"

            valid_df.coalesce(1).write.parquet(str(valid_local))
            rejected_df.coalesce(1).write.parquet(str(rejected_local))

            valid_part = next(valid_local.glob("part-*.parquet"))
            rejected_part = next(rejected_local.glob("part-*.parquet"))

            silver_key = build_silver_path(dataset.nome, dt, silver_exec.id)
            rejected_key = build_silver_rejected_path(dataset.nome, silver_exec.id)

            upload(
                bucket=settings.minio_bucket,
                key=silver_key,
                body=valid_part.read_bytes(),
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
            )
            upload(
                bucket=settings.minio_bucket,
                key=rejected_key,
                body=rejected_part.read_bytes(),
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
            )

            print(f"[Upload] valid -> {silver_key}")
            print(f"[Upload] rejected -> {rejected_key}")

            finish_execution(session, silver_exec, "SUCCESS", storage_path=silver_key)
            print(f"[Done] status=SUCCESS")
            return silver_exec.id
        except ExecutionAlreadyFinishedError:
            # reaper já marcou FAILED; não há o que gravar
            raise
        except Exception as exc:
            finish_execution(session, silver_exec, "FAILED", error_message=str(exc))
            print(f"[Error] {exc}")
            raise
        finally:
            if spark is not None:
                spark.stop()


def main() -> None:
    bronze_execution_id = UUID(sys.argv[1])
    try:
        silver_id = run(bronze_execution_id)
    except PreconditionError as exc:
        raise SystemExit(str(exc)) from exc
    print(f"[Done] silver_id={silver_id}")


if __name__ == "__main__":
    main()
