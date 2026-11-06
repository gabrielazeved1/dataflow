import csv
import io
from datetime import date
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from api.config import settings
from api.database import get_db
from api.main import create_app
from dataflow_common.models import Dataset, Execution


SCHEMA = {
    "campos": [
        {"nome": "data", "tipo": "date", "obrigatorio": True},
        {"nome": "produto", "tipo": "string", "obrigatorio": True},
        {"nome": "qtd", "tipo": "integer", "obrigatorio": True, "min": 1, "max": 100},
        {"nome": "preco", "tipo": "float", "obrigatorio": True, "min": 0, "max": 1000},
    ]
}


@pytest.fixture
def dataset(db):
    d = Dataset(nome="demo", formato="csv", schema=SCHEMA)
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


def _row_valid(row: dict, schema: dict) -> bool:
    for campo in schema["campos"]:
        val = row[campo["nome"]]
        if campo["obrigatorio"] and not val:
            return False
        if not val:
            continue
        tipo = campo["tipo"]
        if tipo == "date":
            try:
                date.fromisoformat(val)
            except ValueError:
                return False
        elif tipo in ("integer", "float"):
            try:
                n = int(val) if tipo == "integer" else float(val)
            except ValueError:
                return False
            if campo.get("min") is not None and n < campo["min"]:
                return False
            if campo.get("max") is not None and n > campo["max"]:
                return False
    return True


# ----- gate dev-only -----

def test_dev_router_only_in_development(db, monkeypatch):
    monkeypatch.setattr(settings, "env", "production")
    prod_app = create_app()

    def override_get_db():
        yield db

    prod_app.dependency_overrides[get_db] = override_get_db
    with TestClient(prod_app) as c:
        response = c.post(
            "/dev/csv/sample?dataset_id=00000000-0000-0000-0000-000000000000"
        )
    assert response.status_code == 404


# ----- /dev/csv/sample -----

def test_csv_sample_gera_csv_todo_valido(client, dataset):
    response = client.post(
        f"/dev/csv/sample?dataset_id={dataset.id}&rows=100&invalid_rate=0.0"
    )

    assert response.status_code == 200
    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert len(rows) == 100
    assert all(_row_valid(r, SCHEMA) for r in rows)


def test_csv_sample_respeita_invalid_rate(client, dataset):
    response = client.post(
        f"/dev/csv/sample?dataset_id={dataset.id}&rows=200&invalid_rate=0.5"
    )

    assert response.status_code == 200
    rows = list(csv.DictReader(io.StringIO(response.text)))
    invalid = sum(1 for r in rows if not _row_valid(r, SCHEMA))
    # tolerância estatística: 50% ± 15% em 200 linhas (~85-115)
    assert 70 <= invalid <= 130


def test_csv_sample_dataset_inexistente(client):
    response = client.post(
        "/dev/csv/sample?dataset_id=00000000-0000-0000-0000-000000000000"
    )
    assert response.status_code == 404


# ----- /dev/bulk-ingest -----

def test_bulk_ingest_cria_n_execucoes(client, db, dataset):
    with patch("api.services.ingest.celery_client.send_task"):
        response = client.post(
            f"/dev/bulk-ingest?dataset_id={dataset.id}&count=10&rows=50&invalid_rate=0.0"
        )

    assert response.status_code == 202
    body = response.json()
    assert body["count"] == 10
    assert body["rows_each"] == 50
    assert len(body["execution_ids"]) == 10

    bronzes = db.query(Execution).filter(Execution.layer == "BRONZE").all()
    assert len(bronzes) == 10
    assert all(e.status == "SUCCESS" for e in bronzes)
    assert all(e.storage_path is not None for e in bronzes)
    assert {e.file_name for e in bronzes} == {f"synthetic_{i}.csv" for i in range(10)}


def test_bulk_ingest_respeita_limite_count(client, dataset):
    response = client.post(
        f"/dev/bulk-ingest?dataset_id={dataset.id}&count=51&rows=100&invalid_rate=0.0"
    )
    assert response.status_code == 422


def test_bulk_ingest_dataset_inexistente(client):
    response = client.post(
        "/dev/bulk-ingest?dataset_id=00000000-0000-0000-0000-000000000000"
    )
    assert response.status_code == 404
