from datetime import datetime, timedelta, timezone

from dataflow_common.events import trace_event
from dataflow_common.models import Dataset, Execution

T0 = datetime(2026, 11, 7, tzinfo=timezone.utc)


def _chain(db):
    dataset = Dataset(name="vendas", format="csv", schema={"fields": []})
    db.add(dataset)
    db.commit()
    bronze = Execution(
        dataset_id=dataset.id, layer="BRONZE", status="SUCCESS", created_at=T0
    )
    db.add(bronze)
    db.commit()
    silver = Execution(
        dataset_id=dataset.id,
        layer="SILVER",
        status="SUCCESS",
        input_execution_id=bronze.id,
        created_at=T0 + timedelta(seconds=1),
    )
    db.add(silver)
    db.commit()
    gold = Execution(
        dataset_id=dataset.id,
        layer="GOLD",
        status="SUCCESS",
        input_execution_id=silver.id,
        created_at=T0 + timedelta(seconds=2),
    )
    db.add(gold)
    db.commit()
    bind = db.get_bind()
    trace_event(bind, bronze.id, "api", "received", "csv recebido", bronze.id)
    trace_event(bind, bronze.id, "worker", "gold_stored", "gold gravada", gold.id)
    return bronze, silver, gold


def test_trace_from_bronze_returns_full_chain_and_events(client, db):
    bronze, silver, gold = _chain(db)

    response = client.get(f"/traces/{bronze.id}")

    assert response.status_code == 200
    body = response.json()
    assert body["trace_id"] == str(bronze.id)
    assert body["dataset_name"] == "vendas"
    assert [e["layer"] for e in body["executions"]] == ["BRONZE", "SILVER", "GOLD"]
    assert [e["step"] for e in body["events"]] == ["received", "gold_stored"]


def test_trace_from_gold_walks_up_to_bronze(client, db):
    bronze, _, gold = _chain(db)

    response = client.get(f"/traces/{gold.id}")

    assert response.json()["trace_id"] == str(bronze.id)


def test_trace_unknown_execution_returns_404(client):
    response = client.get("/traces/00000000-0000-0000-0000-000000000000")

    assert response.status_code == 404
