import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from dataflow_common.events import trace_event
from dataflow_common.repository import (
    ExecutionAlreadyFinishedError,
    create_execution_metrics,
    finish_execution,
    get_dataset,
    get_execution,
)
from dataflow_common.storage import (
    build_gold_path,
    build_silver_rejected_path,
    download,
    upload,
)
from pyspark.sql import Row
from pyspark.sql.functions import col

from processing.config import settings
from processing.database import SessionLocal
from processing.errors import PreconditionError
from processing.idempotency import start_child_execution
from processing.spark import build_spark


def run(silver_execution_id: UUID) -> UUID:
    with SessionLocal() as session:
        silver_exec = get_execution(session, silver_execution_id)
        if silver_exec is None:
            raise PreconditionError(f"Execution {silver_execution_id} nao encontrada")
        if silver_exec.layer != "SILVER":
            raise PreconditionError(
                f"Execution {silver_execution_id} e {silver_exec.layer}, "
                "esperava SILVER"
            )
        if silver_exec.status != "SUCCESS":
            raise PreconditionError(
                f"Execution {silver_execution_id} status={silver_exec.status}, "
                "esperava SUCCESS"
            )
        if silver_exec.storage_path is None:
            raise PreconditionError(f"Execution {silver_execution_id} sem storage_path")
        if silver_exec.started_at is None or silver_exec.finished_at is None:
            raise PreconditionError(
                f"Execution {silver_execution_id} SUCCESS sem started_at/finished_at"
            )

        dataset = get_dataset(session, silver_exec.dataset_id)
        if dataset is None:
            raise PreconditionError(f"Dataset {silver_exec.dataset_id} nao encontrado")

        bind = session.get_bind()
        trace_id = silver_exec.input_execution_id or silver_exec.id

        gold_exec, created = start_child_execution(
            session, dataset_id=dataset.id, parent_id=silver_exec.id, layer="GOLD"
        )
        if not created:
            trace_event(
                bind, trace_id, "worker", "skipped", "gold ja existe", gold_exec.id
            )
            return gold_exec.id

        def trace(step: str, message: str, **details) -> None:
            trace_event(
                bind, trace_id, "worker", step, message, gold_exec.id, **details
            )

        trace("gold_started", "calculando metricas da silver")

        # tudo apos criar a filha fica no try: filha presa em RUNNING bloqueia o retry
        spark = None
        try:
            valid_key = silver_exec.storage_path
            rejected_key = build_silver_rejected_path(dataset.name, silver_exec.id)

            valid_bytes = download(
                bucket=settings.minio_bucket,
                key=valid_key,
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
            )
            rejected_bytes = download(
                bucket=settings.minio_bucket,
                key=rejected_key,
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
            )

            valid_local = Path("/tmp") / f"{gold_exec.id}_valid.parquet"
            rejected_local = Path("/tmp") / f"{gold_exec.id}_rejected.parquet"
            valid_local.write_bytes(valid_bytes)
            rejected_local.write_bytes(rejected_bytes)

            spark = build_spark(f"silver_to_gold:{gold_exec.id}")
            valid_df = spark.read.parquet(str(valid_local))
            rejected_df = spark.read.parquet(str(rejected_local)).cache()

            valid_count = valid_df.count()
            rejected_count = rejected_df.count()
            total = valid_count + rejected_count
            rejection_rate = rejected_count / total if total > 0 else 0.0
            corrupt_count = rejected_df.filter(
                col("_corrupt_record").isNotNull()
            ).count()
            duration_seconds = (
                silver_exec.finished_at - silver_exec.started_at
            ).total_seconds()

            dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            gold_df = spark.createDataFrame(
                [
                    Row(
                        execution_id=str(gold_exec.id),
                        silver_execution_id=str(silver_exec.id),
                        dataset_name=dataset.name,
                        dt=dt,
                        valid_count=valid_count,
                        rejected_count=rejected_count,
                        rejection_rate=rejection_rate,
                        corrupt_count=corrupt_count,
                        duration_seconds=duration_seconds,
                    )
                ]
            )

            gold_local = Path("/tmp") / f"{gold_exec.id}_gold"
            gold_df.coalesce(1).write.parquet(str(gold_local))

            gold_part = next(gold_local.glob("part-*.parquet"))
            gold_key = build_gold_path(dataset.name, dt, gold_exec.id)

            upload(
                bucket=settings.minio_bucket,
                key=gold_key,
                body=gold_part.read_bytes(),
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
            )

            create_execution_metrics(
                session,
                gold_execution=gold_exec,
                valid_count=valid_count,
                rejected_count=rejected_count,
                rejection_rate=rejection_rate,
                duration_seconds=duration_seconds,
                corrupt_count=corrupt_count,
            )
            finish_execution(session, gold_exec, "SUCCESS", storage_path=gold_key)
            trace(
                "gold_stored",
                f"gold e metricas gravadas: {rejection_rate:.1%} rejeitadas",
                path=gold_key,
                rejection_rate=rejection_rate,
                corrupt=corrupt_count,
            )
            return gold_exec.id
        except ExecutionAlreadyFinishedError:
            # o reaper ja marcou FAILED
            trace("failed", "reaper ja tinha marcado timeout")
            raise
        except Exception as exc:
            finish_execution(session, gold_exec, "FAILED", error_message=str(exc))
            trace("failed", "erro na gold", error=str(exc))
            raise
        finally:
            if spark is not None:
                spark.stop()


def main() -> None:
    silver_execution_id = UUID(sys.argv[1])
    try:
        gold_id = run(silver_execution_id)
    except PreconditionError as exc:
        raise SystemExit(str(exc)) from exc
    print(f"[Done] gold_id={gold_id}")


if __name__ == "__main__":
    main()
