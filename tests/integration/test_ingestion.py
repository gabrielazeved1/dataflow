import pytest


@pytest.fixture
def dataset_csv(client):
    response = client.post("/datasets/", json={"nome": "vendas", "formato": "csv"})
    return response.json()["id"]


@pytest.fixture
def dataset_json(client):
    response = client.post("/datasets/", json={"nome": "eventos", "formato": "json"})
    return response.json()["id"]


def test_ingest_csv_success(client, dataset_csv):
    csv_content = b"data,produto,valor\n2026-10-15,caneta,2.50"

    response = client.post(
        f"/datasets/{dataset_csv}/ingest",
        files={"file": ("vendas.csv", csv_content, "text/csv")},
    )

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert data["checksum"] is not None
    assert data["file_size"] == len(csv_content)
    assert data["error_message"] is None


def test_ingest_invalid_json_fails(client, dataset_json):
    response = client.post(
        f"/datasets/{dataset_json}/ingest",
        files={"file": ("dados.json", b"isso nao e json { {", "application/json")},
    )

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "FAILED"
    assert data["error_message"] is not None
    assert data["checksum"] is None


def test_ingest_dataset_not_found(client):
    response = client.post(
        "/datasets/00000000-0000-0000-0000-000000000000/ingest",
        files={"file": ("vendas.csv", b"data,valor", "text/csv")},
    )

    assert response.status_code == 404
