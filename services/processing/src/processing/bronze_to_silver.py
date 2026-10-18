"""
Transforma uma execução Bronze em Silver.

Uso:
    python -m processing.bronze_to_silver <execution_id>

Lê o arquivo Bronze do MinIO, valida cada registro contra o schema do
dataset, escreve os registros válidos como Parquet em Silver e os
rejeitados em silver/<dataset>/_rejected/.
"""
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


def _validate_row(row: dict, campos: list[dict]) -> list[str]:
    errors = []
    for campo in campos:
        nome = campo["nome"]
        tipo = campo["tipo"]
        obrigatorio = campo.get("obrigatorio", True)
        min_val = campo.get("min")
        max_val = campo.get("max")

        value = row.get(nome)
        is_blank = value is None or str(value).strip() == ""

        if obrigatorio and is_blank:
            errors.append(f"campo obrigatorio ausente: {nome}")
            continue

        if is_blank:
            continue

        if tipo == "date":
            try:
                datetime.strptime(str(value).strip(), "%Y-%m-%d")
            except ValueError:
                errors.append(f"{nome}: data invalida, esperado YYYY-MM-DD, recebido '{value}'")

        elif tipo == "integer":
            try:
                v = int(value)
                if min_val is not None and v < min_val:
                    errors.append(f"{nome}: {v} abaixo do minimo {min_val}")
                if max_val is not None and v > max_val:
                    errors.append(f"{nome}: {v} acima do maximo {max_val}")
            except (ValueError, TypeError):
                errors.append(f"{nome}: esperado integer, recebido '{value}'")

        elif tipo == "float":
            try:
                v = float(value)
                if min_val is not None and v < min_val:
                    errors.append(f"{nome}: {v} abaixo do minimo {min_val}")
                if max_val is not None and v > max_val:
                    errors.append(f"{nome}: {v} acima do maximo {max_val}")
            except (ValueError, TypeError):
                errors.append(f"{nome}: esperado float, recebido '{value}'")

        elif tipo == "string":
            if str(value).strip() == "":
                errors.append(f"{nome}: string nao pode ser vazia")

    return errors


def run(bronze_execution_id: UUID) -> None:
    engine = create_engine(settings.postgres_dsn)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        bronze_exec = repository.get_execution(session, bronze_execution_id)
        if not bronze_exec:
            raise ValueError(f"Execução {bronze_execution_id} não encontrada")
        if bronze_exec.layer != "BRONZE":
            raise ValueError(f"Execução {bronze_execution_id} não é da camada BRONZE")
        if bronze_exec.status != "SUCCESS":
            raise ValueError(f"Execução {bronze_execution_id} está com status {bronze_exec.status}, esperado SUCCESS")

        dataset = repository.get_dataset(session, bronze_exec.dataset_id)
        campos = dataset.schema["campos"]

        silver_exec = repository.create_execution(
            session,
            dataset_id=dataset.id,
            layer="SILVER",
            input_execution_id=bronze_execution_id,
        )

        try:
            content = st.download(
                bucket=settings.minio_bucket,
                key=bronze_exec.storage_path,
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
            )

            df = pd.read_csv(io.BytesIO(content))

            valid_rows, rejected_rows = [], []
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                errors = _validate_row(row_dict, campos)
                if errors:
                    row_dict["_rejection_reason"] = "; ".join(errors)
                    rejected_rows.append(row_dict)
                else:
                    valid_rows.append(row_dict)

            dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            silver_path = None

            if valid_rows:
                silver_path = st.build_silver_path(dataset.nome, dt, silver_exec.id)
                st.upload(
                    bucket=settings.minio_bucket,
                    key=silver_path,
                    body=pd.DataFrame(valid_rows).to_parquet(index=False),
                    endpoint=settings.minio_endpoint,
                    access_key=settings.minio_access_key,
                    secret_key=settings.minio_secret_key,
                )

            if rejected_rows:
                rejected_path = st.build_silver_rejected_path(dataset.nome, silver_exec.id)
                st.upload(
                    bucket=settings.minio_bucket,
                    key=rejected_path,
                    body=pd.DataFrame(rejected_rows).to_parquet(index=False),
                    endpoint=settings.minio_endpoint,
                    access_key=settings.minio_access_key,
                    secret_key=settings.minio_secret_key,
                )

            n_ok, n_rej = len(valid_rows), len(rejected_rows)
            status = "SUCCESS" if n_ok > 0 else "FAILED"
            error_msg = f"{n_rej} registro(s) rejeitado(s)" if n_rej > 0 and n_ok == 0 else None

            repository.finish_execution(
                session,
                silver_exec,
                status=status,
                storage_path=silver_path,
                error_message=error_msg,
            )

            print(f"Silver concluído: {n_ok} válidos, {n_rej} rejeitados — status={status}")

        except Exception as exc:
            repository.finish_execution(session, silver_exec, status="FAILED", error_message=str(exc))
            raise


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python -m processing.bronze_to_silver <execution_id>")
        sys.exit(1)
    run(UUID(sys.argv[1]))
