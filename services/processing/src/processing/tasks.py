from uuid import UUID

from processing.bronze_to_silver import run as _run_bronze_to_silver
from processing.celery_app import celery_app
from processing.silver_to_gold import run as _run_silver_to_gold


@celery_app.task(name="processing.ingest")
def ingest(bronze_execution_id: str) -> str:
    # executa bronze → silver → gold encadeados.
   
    bronze_id = UUID(bronze_execution_id)
    silver_id = _run_bronze_to_silver(bronze_id)
    gold_id = _run_silver_to_gold(silver_id)
    return str(gold_id)
