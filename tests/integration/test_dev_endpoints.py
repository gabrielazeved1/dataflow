import csv
import io
from datetime import date
from unittest.mock import patch

import pytest
from api.config import settings
from api.database import get_db
from api.main import create_app
from dataflow_common.models import Dataset, Execution
from fastapi.testclient import TestClient

SCHEMA = {
    "fields": [
        {"name": "data", "type": "date", "required": True},
        {"name": "produto", "type": "string", "required": True},
        {"name": "qtd", "type": "integer", "required": True, "min": 1, "max": 100},
        {"name": "preco", "type": "float", "required": True, "min": 0, "max": 1000},
    ]
}


@pytest.fixture
def dataset(db):
    d = Dataset(name="demo", format="csv", schema=SCHEMA)
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


def _row_valid(row: dict, schema: dict) -> bool:
    for field in schema["fields"]:
        val = row[field["name"]]
        if field["required"] and not val:
            return False
        if not val:
            continue
        field_type = field["type"]
        if field_type == "date":
            try:
                date.fromisoformat(val)
            except ValueError:
                return False
        elif field_type in ("integer", "float"):
            try:
                n = int(val) if field_type == "integer" else float(val)
            except ValueError:
                return False
            if field.get("min") is not None and n < field["min"]:
                return False
            if field.get("max") is not None and n > field["max"]:
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


def test_csv_sample_generates_all_valid_rows(client, dataset):
    response = client.post(
        f"/dev/csv/sample?dataset_id={dataset.id}&rows=100&invalid_rate=0.0"
    )

    assert response.status_code == 200
    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert len(rows) == 100
    assert all(_row_valid(r, SCHEMA) for r in rows)


def test_csv_sample_respects_invalid_rate(client, dataset):
    response = client.post(
        f"/dev/csv/sample?dataset_id={dataset.id}&rows=200&invalid_rate=0.5"
    )

    assert response.status_code == 200
    rows = list(csv.DictReader(io.StringIO(response.text)))
    invalid = sum(1 for r in rows if not _row_valid(r, SCHEMA))
    # tolerancia: 50% +- 15% em 200 linhas
    assert 70 <= invalid <= 130


def test_csv_sample_unknown_dataset(client):
    response = client.post(
        "/dev/csv/sample?dataset_id=00000000-0000-0000-0000-000000000000"
    )
    assert response.status_code == 404


# ----- /dev/bulk-ingest -----


def test_bulk_ingest_creates_n_executions(client, db, dataset):
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


def test_bulk_ingest_enforces_count_limit(client, dataset):
    response = client.post(
        f"/dev/bulk-ingest?dataset_id={dataset.id}&count=51&rows=100&invalid_rate=0.0"
    )
    assert response.status_code == 422


def test_bulk_ingest_unknown_dataset(client):
    response = client.post(
        "/dev/bulk-ingest?dataset_id=00000000-0000-0000-0000-000000000000"
    )
    assert response.status_code == 404
