from datetime import datetime, timezone

import pytest
from dataflow_common.models import Dataset, Execution, ExecutionMetrics

SCHEMA = {"fields": [{"name": "produto", "type": "string", "required": True}]}


@pytest.fixture
def datasets(db):
    sales = Dataset(name="vendas", format="csv", schema=SCHEMA)
    stock = Dataset(name="estoque", format="csv", schema=SCHEMA)
    db.add_all([sales, stock])
    db.commit()
    db.refresh(sales)
    db.refresh(stock)
    return sales, stock


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


def test_list_executions_empty(client):
    response = client.get("/executions/")
    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}


def test_list_executions_no_filters_sorted_desc(client, db, datasets):
    sales, _ = datasets
    _make_execution(
        db, sales.id, "BRONZE", "SUCCESS", datetime(2026, 10, 15, tzinfo=timezone.utc)
    )
    _make_execution(
        db, sales.id, "SILVER", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )
    _make_execution(
        db, sales.id, "GOLD", "SUCCESS", datetime(2026, 11, 2, tzinfo=timezone.utc)
    )

    response = client.get("/executions/")

    assert response.status_code == 200
    body = response.json()
    assert [r["layer"] for r in body["items"]] == ["GOLD", "SILVER", "BRONZE"]
    assert body["next_cursor"] is None


def test_filter_by_dataset_name(client, db, datasets):
    sales, stock = datasets
    _make_execution(
        db, sales.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )
    _make_execution(
        db, stock.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )

    response = client.get("/executions/?dataset=vendas")

    assert response.status_code == 200
    rows = response.json()["items"]
    assert len(rows) == 1
    assert rows[0]["dataset_id"] == str(sales.id)


def test_filter_by_status(client, db, datasets):
    sales, _ = datasets
    _make_execution(
        db, sales.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )
    _make_execution(
        db, sales.id, "BRONZE", "FAILED", datetime(2026, 11, 2, tzinfo=timezone.utc)
    )

    response = client.get("/executions/?status=FAILED")

    assert response.status_code == 200
    rows = response.json()["items"]
    assert len(rows) == 1
    assert rows[0]["status"] == "FAILED"


def test_filter_by_layer(client, db, datasets):
    sales, _ = datasets
    _make_execution(
        db, sales.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )
    _make_execution(
        db, sales.id, "SILVER", "SUCCESS", datetime(2026, 11, 2, tzinfo=timezone.utc)
    )

    response = client.get("/executions/?layer=SILVER")

    assert response.status_code == 200
    rows = response.json()["items"]
    assert len(rows) == 1
    assert rows[0]["layer"] == "SILVER"


def test_combined_filters(client, db, datasets):
    sales, stock = datasets
    _make_execution(
        db, sales.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )
    _make_execution(
        db, sales.id, "BRONZE", "FAILED", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )
    _make_execution(
        db, sales.id, "SILVER", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )
    _make_execution(
        db, stock.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )

    response = client.get("/executions/?dataset=vendas&status=SUCCESS&layer=BRONZE")

    assert response.status_code == 200
    rows = response.json()["items"]
    assert len(rows) == 1
    assert rows[0]["dataset_id"] == str(sales.id)
    assert rows[0]["status"] == "SUCCESS"
    assert rows[0]["layer"] == "BRONZE"


def test_filter_date_range_inclusive(client, db, datasets):
    sales, _ = datasets
    _make_execution(
        db, sales.id, "BRONZE", "SUCCESS", datetime(2026, 10, 14, tzinfo=timezone.utc)
    )
    _make_execution(
        db, sales.id, "BRONZE", "SUCCESS", datetime(2026, 10, 15, tzinfo=timezone.utc)
    )
    _make_execution(
        db,
        sales.id,
        "BRONZE",
        "SUCCESS",
        datetime(2026, 10, 20, 23, 59, tzinfo=timezone.utc),
    )
    _make_execution(
        db, sales.id, "BRONZE", "SUCCESS", datetime(2026, 10, 21, tzinfo=timezone.utc)
    )

    response = client.get("/executions/?from=2026-10-15&to=2026-10-20")

    assert response.status_code == 200
    rows = response.json()["items"]
    assert len(rows) == 2


def test_unknown_dataset_returns_empty(client, db, datasets):
    sales, _ = datasets
    _make_execution(
        db, sales.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )

    response = client.get("/executions/?dataset=inexistente")

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}


# ----- GET /executions/{id} -----


def test_detail_bronze_metrics_null(client, db, datasets):
    sales, _ = datasets
    bronze = _make_execution(
        db, sales.id, "BRONZE", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )

    response = client.get(f"/executions/{bronze.id}")

    assert response.status_code == 200
    assert response.json()["metrics"] is None


def test_detail_silver_metrics_null(client, db, datasets):
    sales, _ = datasets
    silver = _make_execution(
        db, sales.id, "SILVER", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )

    response = client.get(f"/executions/{silver.id}")

    assert response.status_code == 200
    assert response.json()["metrics"] is None


def test_detail_gold_without_persisted_metrics(client, db, datasets):
    sales, _ = datasets
    gold = _make_execution(
        db, sales.id, "GOLD", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )

    response = client.get(f"/executions/{gold.id}")

    assert response.status_code == 200
    assert response.json()["metrics"] is None


def test_detail_gold_with_metrics(client, db, datasets):
    sales, _ = datasets
    gold = _make_execution(
        db, sales.id, "GOLD", "SUCCESS", datetime(2026, 11, 1, tzinfo=timezone.utc)
    )
    db.add(
        ExecutionMetrics(
            gold_execution_id=gold.id,
            valid_count=10,
            rejected_count=2,
            rejection_rate=0.1667,
            duration_seconds=4.5,
            corrupt_count=1,
        )
    )
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
