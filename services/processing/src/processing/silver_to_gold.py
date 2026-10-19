import io
import sys
from datetime import datetime, timezone
from uuid import UUID

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dataflow_common import repository
from dataflow_common import storage as st
from processing.config import settings


def run(silver_execution_id: UUID) -> None:
    engine = create_engine(settings.postgres_dsn)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        silver_exec = repository.get_execution(session, silver_execution_id)
        if not silver_exec:
            raise ValueError(f"Execução {silver_execution_id} não encontrada")
        if silver_exec.layer != "SILVER":
            raise ValueError(f"Execução {silver_execution_id} não é da camada SILVER")
        if silver_exec.status != "SUCCESS":
            raise ValueError(f"Execução {silver_execution_id} está com status {silver_exec.status}, esperado SUCCESS")
        if not silver_exec.storage_path:
            raise ValueError(f"Execução {silver_execution_id} não tem storage_path")

        dataset = repository.get_dataset(session, silver_exec.dataset_id)
        if not dataset:
            raise ValueError(f"Dataset {silver_exec.dataset_id} não encontrado")

        gold_exec = repository.create_execution(
            session,
            dataset_id=dataset.id,
            layer="GOLD",
            input_execution_id=silver_execution_id,
        )

        try:
            content = st.download(
                bucket=settings.minio_bucket,
                key=silver_exec.storage_path,
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
            )

            df = pd.read_parquet(io.BytesIO(content))

            df["quantidade"] = pd.to_numeric(df["quantidade"])
            df["valor_unitario"] = pd.to_numeric(df["valor_unitario"])

            gold_df = (
                df.groupby("data")
                .agg(
                    total_quantidade=("quantidade", "sum"),
                    total_valor=("valor_unitario", lambda s: (s * df.loc[s.index, "quantidade"]).sum()),
                )
                .reset_index()
            )

            dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            gold_path = st.build_gold_path(dataset.nome, dt, gold_exec.id)

            st.upload(
                bucket=settings.minio_bucket,
                key=gold_path,
                body=gold_df.to_parquet(index=False),
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
            )

            repository.finish_execution(
                session,
                gold_exec,
                status="SUCCESS",
                storage_path=gold_path,
            )

            print(f"Gold concluído: {len(gold_df)} linha(s) — {gold_path}")

        except Exception as exc:
            repository.finish_execution(session, gold_exec, status="FAILED", error_message=str(exc))
            raise


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python -m processing.silver_to_gold <execution_id>")
        sys.exit(1)
    run(UUID(sys.argv[1]))
