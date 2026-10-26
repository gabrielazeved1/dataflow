import sys
from pathlib import Path
from uuid import UUID

from dataflow_common.repository import create_execution, get_dataset, get_execution
from dataflow_common.storage import download

from processing.config import settings
from processing.database import SessionLocal
from processing.schema_mapper import build_struct_type
from processing.spark import build_spark


def main() -> None:
    bronze_execution_id = UUID(sys.argv[1])

    with SessionLocal() as session:
        bronze_exec = get_execution(session, bronze_execution_id)
        if bronze_exec is None:
            raise SystemExit(f"Execution {bronze_execution_id} nao encontrada")
        if bronze_exec.layer != "BRONZE":
            raise SystemExit(
                f"Execution {bronze_execution_id} e {bronze_exec.layer}, esperava BRONZE"
            )
        if bronze_exec.storage_path is None:
            raise SystemExit(f"Execution {bronze_execution_id} sem storage_path")

        dataset = get_dataset(session, bronze_exec.dataset_id)
        if dataset is None:
            raise SystemExit(f"Dataset {bronze_exec.dataset_id} nao encontrado")

        print(f"[Bronze] execution={bronze_exec.id}")
        print(f"         storage_path={bronze_exec.storage_path}")
        print(f"         file_name={bronze_exec.file_name}")
        print(f"[Dataset] id={dataset.id} nome={dataset.nome} formato={dataset.formato}")
        print(f"          campos={[c['nome'] for c in dataset.schema['campos']]}")

        silver_exec = create_execution(
            session,
            dataset_id=dataset.id,
            layer="SILVER",
            input_execution_id=bronze_exec.id,
        )

        print(f"[Silver] execution={silver_exec.id} status={silver_exec.status}")

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

        struct_type = build_struct_type(dataset.schema)
        spark = build_spark(f"bronze_to_silver:{silver_exec.id}")
        try:
            df = spark.read.csv(str(local_path), header=True, schema=struct_type)
            print("[Spark] schema aplicado:")
            df.printSchema()
            print(f"[Spark] count={df.count()} (primeiras 5 linhas):")
            df.show(5, truncate=False)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
