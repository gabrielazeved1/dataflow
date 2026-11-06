from datetime import datetime, timedelta, timezone

import pytest
from dataflow_common.models import Dataset, Execution

SCHEMA = {"campos": [{"nome": "produto", "tipo": "string", "obrigatorio": True}]}


@pytest.fixture
def dataset(db):
    d = Dataset(nome="vendas", formato="csv", schema=SCHEMA)
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


def _seed(db, dataset_id, n):
    base = datetime(2026, 11, 1, tzinfo=timezone.utc)
    for i in range(n):
        db.add(
            Execution(
                dataset_id=dataset_id,
                layer="BRONZE",
                status="SUCCESS",
                created_at=base + timedelta(seconds=i),
            )
        )
    db.commit()


def test_paginacao_percorre_todas_paginas_sem_duplicata(client, db, dataset):
    _seed(db, dataset.id, 150)

    ids_vistos: list[str] = []
    cursor: str | None = None
    paginas = 0
    while True:
        url = "/executions/?limit=50"
        if cursor:
            url += f"&after={cursor}"
        response = client.get(url)
        assert response.status_code == 200
        body = response.json()
        ids_vistos.extend(e["id"] for e in body["items"])
        paginas += 1
        cursor = body["next_cursor"]
        if cursor is None:
            break

    assert paginas == 3
    assert len(ids_vistos) == 150
    assert len(set(ids_vistos)) == 150


def test_cursor_invalido_retorna_400(client, dataset):
    response = client.get("/executions/?after=nao-eh-base64-valido!!!")
    assert response.status_code == 400


def test_limit_acima_do_maximo_retorna_422(client):
    response = client.get("/executions/?limit=201")
    assert response.status_code == 422


def test_limit_abaixo_do_minimo_retorna_422(client):
    response = client.get("/executions/?limit=0")
    assert response.status_code == 422


def test_pagina_final_sem_next_cursor(client, db, dataset):
    _seed(db, dataset.id, 30)

    response = client.get("/executions/?limit=50")

    assert response.status_code == 200
    body = response.json()
    assert len(body["items"]) == 30
    assert body["next_cursor"] is None
