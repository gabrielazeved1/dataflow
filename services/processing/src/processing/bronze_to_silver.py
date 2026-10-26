import sys
from functools import reduce
from operator import and_
from pathlib import Path
from uuid import UUID

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType

from dataflow_common.repository import create_execution, get_dataset, get_execution
from dataflow_common.storage import download

from processing.config import settings
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

        base_struct = build_struct_type(dataset.schema)
        read_struct = StructType(
            base_struct.fields + [StructField("_corrupt_record", StringType(), True)]
        )
        spark = build_spark(f"bronze_to_silver:{silver_exec.id}")
        try:
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
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
