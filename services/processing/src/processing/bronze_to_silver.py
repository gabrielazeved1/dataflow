import sys
from uuid import UUID

from dataflow_common.repository import create_execution, get_dataset, get_execution

from processing.database import SessionLocal


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


if __name__ == "__main__":
    main()
