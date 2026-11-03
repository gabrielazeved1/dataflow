import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from pyspark.sql import Row
from pyspark.sql.functions import col

from dataflow_common.models import Execution
from dataflow_common.repository import (
    create_execution,
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

from processing.config import settings
from processing.database import SessionLocal
from processing.spark import build_spark


def run(silver_execution_id: UUID) -> UUID:
    with SessionLocal() as session:
        silver_exec = get_execution(session, silver_execution_id)
        if silver_exec is None:
            raise SystemExit(f"Execution {silver_execution_id} nao encontrada")
        if silver_exec.layer != "SILVER":
            raise SystemExit(
                f"Execution {silver_execution_id} e {silver_exec.layer}, esperava SILVER"
            )
        if silver_exec.status != "SUCCESS":
            raise SystemExit(
                f"Execution {silver_execution_id} status={silver_exec.status}, esperava SUCCESS"
            )
        if silver_exec.storage_path is None:
            raise SystemExit(f"Execution {silver_execution_id} sem storage_path")
        if silver_exec.started_at is None or silver_exec.finished_at is None:
            raise SystemExit(
                f"Execution {silver_execution_id} SUCCESS sem started_at/finished_at"
            )

        dataset = get_dataset(session, silver_exec.dataset_id)
        if dataset is None:
            raise SystemExit(f"Dataset {silver_exec.dataset_id} nao encontrado")

        print(f"[Silver] execution={silver_exec.id}")
        print(f"         storage_path={silver_exec.storage_path}")
        print(f"         started_at={silver_exec.started_at}")
        print(f"         finished_at={silver_exec.finished_at}")
        print(f"[Dataset] id={dataset.id} nome={dataset.nome}")

        existing = (
            session.query(Execution)
            .filter(
                Execution.input_execution_id == silver_exec.id,
                Execution.layer == "GOLD",
                Execution.status == "SUCCESS",
            )
            .first()
        )
        if existing is not None:
            print(f"[Skip] Gold ja existe: {existing.id} status=SUCCESS")
            return existing.id

        gold_exec = create_execution(
            session,
            dataset_id=dataset.id,
            layer="GOLD",
            input_execution_id=silver_exec.id,
        )

        print(f"[Gold] execution={gold_exec.id} status={gold_exec.status}")

        valid_key = silver_exec.storage_path
        rejected_key = build_silver_rejected_path(dataset.nome, silver_exec.id)

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

        print(f"[Download] valid    {len(valid_bytes)} bytes -> {valid_local}")
        print(f"[Download] rejected {len(rejected_bytes)} bytes -> {rejected_local}")

        spark = build_spark(f"silver_to_gold:{gold_exec.id}")
        try:
            valid_df = spark.read.parquet(str(valid_local))
            rejected_df = spark.read.parquet(str(rejected_local)).cache()

            valid_count = valid_df.count()
            rejected_count = rejected_df.count()
            total = valid_count + rejected_count
            rejection_rate = rejected_count / total if total > 0 else 0.0
            corrupt_count = rejected_df.filter(col("_corrupt_record").isNotNull()).count()
            duration_seconds = (
                silver_exec.finished_at - silver_exec.started_at
            ).total_seconds()

            print(
                f"[Metrics] valid={valid_count} rejected={rejected_count} "
                f"total={total} rejection_rate={rejection_rate:.4f} "
                f"corrupt={corrupt_count} duration={duration_seconds:.2f}s"
            )

            dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            gold_df = spark.createDataFrame([
                Row(
                    execution_id=str(gold_exec.id),
                    silver_execution_id=str(silver_exec.id),
                    dataset_nome=dataset.nome,
                    dt=dt,
                    valid_count=valid_count,
                    rejected_count=rejected_count,
                    rejection_rate=rejection_rate,
                    corrupt_count=corrupt_count,
                    duration_seconds=duration_seconds,
                )
            ])

            gold_local = Path("/tmp") / f"{gold_exec.id}_gold"
            gold_df.coalesce(1).write.parquet(str(gold_local))

            gold_part = next(gold_local.glob("part-*.parquet"))
            gold_key = build_gold_path(dataset.nome, dt, gold_exec.id)

            upload(
                bucket=settings.minio_bucket,
                key=gold_key,
                body=gold_part.read_bytes(),
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
            )

            print(f"[Upload] gold -> {gold_key}")

            create_execution_metrics(
                session,
                gold_execution=gold_exec,
                valid_count=valid_count,
                rejected_count=rejected_count,
                rejection_rate=rejection_rate,
                duration_seconds=duration_seconds,
                corrupt_count=corrupt_count,
            )
            print(f"[Metrics] persisted in execution_metrics")

            finish_execution(session, gold_exec, "SUCCESS", storage_path=gold_key)
            print(f"[Done] status=SUCCESS")
            return gold_exec.id
        except Exception as exc:
            finish_execution(session, gold_exec, "FAILED", error_message=str(exc))
            print(f"[Error] {exc}")
            raise
        finally:
            spark.stop()


def main() -> None:
    silver_execution_id = UUID(sys.argv[1])
    gold_id = run(silver_execution_id)
    print(f"[Done] gold_id={gold_id}")


if __name__ == "__main__":
    main()
