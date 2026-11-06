from datetime import datetime, timezone

import pytest

from dataflow_common.models import Dataset, Execution, ExecutionMetrics


SCHEMA = {"campos": [{"nome": "produto", "tipo": "string", "obrigatorio": True}]}


@pytest.fixture
def datasets(db):
    vendas = Dataset(nome="vendas", formato="csv", schema=SCHEMA)
    estoque = Dataset(nome="estoque", formato="json", schema=SCHEMA)
    db.add_all([vendas, estoque])
    db.commit()
    db.refresh(vendas)
    db.refresh(estoque)
    return vendas, estoque


def _make_execution(db, dataset_id, layer, status, created_at, **extra):
    execution = Execution(
        dataset_id=dataset_id,
        layer=layer,
        status=status,
        created_at=created_at,
        started_at=created_at,
        finished_at=created_at,
        **extra,
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)
    return execution


# ----- GET /executions -----

def test_list_executions_vazio(client):
    response = client.get("/executions/")
    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}


def test_list_executions_sem_filtros_ordenacao_desc(client, db, datasets):
    vendas, _ = datasets
    _make_execution(db, vendas.id, "BRONZE", "SUCCESS", datetime(2026, 10, 15, tzinfo=timezone.utc))
    _make_execution(db, vendas.id, "SILVER", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc))
    _make_execution(db, vendas.id, "GOLD", "SUCCESS", datetime(2026, 11, 2, tzinfo=timezone.utc))

    response = client.get("/executions/")

    assert response.status_code == 200
    body = response.json()
    assert [r["layer"] for r in body["items"]] == ["GOLD", "SILVER", "BRONZE"]
    assert body["next_cursor"] is None


def test_filtro_dataset_por_nome(client, db, datasets):
    vendas, estoque = datasets
    _make_execution(db, vendas.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc))
    _make_execution(db, estoque.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc))

    response = client.get("/executions/?dataset=vendas")

    assert response.status_code == 200
    rows = response.json()["items"]
    assert len(rows) == 1
    assert rows[0]["dataset_id"] == str(vendas.id)


def test_filtro_status(client, db, datasets):
    vendas, _ = datasets
    _make_execution(db, vendas.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc))
    _make_execution(db, vendas.id, "BRONZE", "FAILED", datetime(2026, 11, 2, tzinfo=timezone.utc))

    response = client.get("/executions/?status=FAILED")

    assert response.status_code == 200
    rows = response.json()["items"]
    assert len(rows) == 1
    assert rows[0]["status"] == "FAILED"


def test_filtro_layer(client, db, datasets):
    vendas, _ = datasets
    _make_execution(db, vendas.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc))
    _make_execution(db, vendas.id, "SILVER", "SUCCESS", datetime(2026, 11, 2, tzinfo=timezone.utc))

    response = client.get("/executions/?layer=SILVER")

    assert response.status_code == 200
    rows = response.json()["items"]
    assert len(rows) == 1
    assert rows[0]["layer"] == "SILVER"


def test_filtros_combinados(client, db, datasets):
    vendas, estoque = datasets
    _make_execution(db, vendas.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc))
    _make_execution(db, vendas.id, "BRONZE", "FAILED", datetime(2026, 11, 1, tzinfo=timezone.utc))
    _make_execution(db, vendas.id, "SILVER", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc))
    _make_execution(db, estoque.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc))

    response = client.get("/executions/?dataset=vendas&status=SUCCESS&layer=BRONZE")

    assert response.status_code == 200
    rows = response.json()["items"]
    assert len(rows) == 1
    assert rows[0]["dataset_id"] == str(vendas.id)
    assert rows[0]["status"] == "SUCCESS"
    assert rows[0]["layer"] == "BRONZE"


def test_filtro_intervalo_datas_inclusivo(client, db, datasets):
    vendas, _ = datasets
    _make_execution(db, vendas.id, "BRONZE", "SUCCESS", datetime(2026, 10, 14, tzinfo=timezone.utc))
    _make_execution(db, vendas.id, "BRONZE", "SUCCESS", datetime(2026, 10, 15, tzinfo=timezone.utc))
    _make_execution(db, vendas.id, "BRONZE", "SUCCESS", datetime(2026, 10, 20, 23, 59, tzinfo=timezone.utc))
    _make_execution(db, vendas.id, "BRONZE", "SUCCESS", datetime(2026, 10, 21, tzinfo=timezone.utc))

    response = client.get("/executions/?from=2026-10-15&to=2026-10-20")

    assert response.status_code == 200
    rows = response.json()["items"]
    assert len(rows) == 2


def test_dataset_inexistente_retorna_vazio(client, db, datasets):
    vendas, _ = datasets
    _make_execution(db, vendas.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc))

    response = client.get("/executions/?dataset=inexistente")

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}


# ----- GET /executions/{id} -----

def test_detail_bronze_metrics_null(client, db, datasets):
    vendas, _ = datasets
    bronze = _make_execution(
        db, vendas.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )

    response = client.get(f"/executions/{bronze.id}")

    assert response.status_code == 200
    assert response.json()["metrics"] is None


def test_detail_silver_metrics_null(client, db, datasets):
    vendas, _ = datasets
    silver = _make_execution(
        db, vendas.id, "SILVER", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )

    response = client.get(f"/executions/{silver.id}")

    assert response.status_code == 200
    assert response.json()["metrics"] is None


def test_detail_gold_sem_metrics_persistidas(client, db, datasets):
    vendas, _ = datasets
    gold = _make_execution(
        db, vendas.id, "GOLD", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )

    response = client.get(f"/executions/{gold.id}")

    assert response.status_code == 200
    assert response.json()["metrics"] is None


def test_detail_gold_com_metrics(client, db, datasets):
    vendas, _ = datasets
    gold = _make_execution(
        db, vendas.id, "GOLD", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )
    db.add(ExecutionMetrics(
        gold_execution_id=gold.id,
        valid_count=10,
        rejected_count=2,
        rejection_rate=0.1667,
        duration_seconds=4.5,
        corrupt_count=1,
    ))
    db.commit()

    response = client.get(f"/executions/{gold.id}")

    assert response.status_code == 200
    metrics = response.json()["metrics"]
    assert metrics == {
        "valid_count": 10,
        "rejected_count": 2,
        "rejection_rate": 0.1667,
        "duration_seconds": 4.5,
        "corrupt_count": 1,
    }


def test_detail_not_found(client):
    response = client.get("/executions/00000000-0000-0000-0000-000000000000")

    assert response.status_code == 404
