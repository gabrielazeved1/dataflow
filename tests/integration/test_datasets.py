def test_create_dataset(client):
    response = client.post("/datasets/", json={"nome": "vendas_diarias", "formato": "csv"})

    assert response.status_code == 201
    data = response.json()
    assert data["nome"] == "vendas_diarias"
    assert data["formato"] == "csv"
    assert "id" in data
    assert "created_at" in data


def test_create_dataset_duplicate(client):
    client.post("/datasets/", json={"nome": "vendas_diarias", "formato": "csv"})
    response = client.post("/datasets/", json={"nome": "vendas_diarias", "formato": "csv"})

    assert response.status_code == 409


def test_get_dataset(client):
    create = client.post("/datasets/", json={"nome": "vendas_diarias", "formato": "csv"})
    dataset_id = create.json()["id"]

    response = client.get(f"/datasets/{dataset_id}")

    assert response.status_code == 200
    assert response.json()["id"] == dataset_id


def test_get_dataset_not_found(client):
    response = client.get("/datasets/00000000-0000-0000-0000-000000000000")

    assert response.status_code == 404
