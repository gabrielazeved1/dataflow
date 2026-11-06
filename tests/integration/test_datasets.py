SALES_SCHEMA = {
    "fields": [
        {"name": "data", "type": "date", "required": True},
        {"name": "produto", "type": "string", "required": True},
        {"name": "quantidade", "type": "integer", "required": True, "min": 1},
        {"name": "valor_unitario", "type": "float", "required": True, "min": 0.01},
    ]
}


def test_create_dataset(client):
    response = client.post(
        "/datasets/",
        json={"name": "vendas_diarias", "format": "csv", "schema": SALES_SCHEMA},
    )

    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "vendas_diarias"
    assert data["format"] == "csv"
    assert data["schema"] == SALES_SCHEMA
    assert "id" in data
    assert "created_at" in data


def test_create_dataset_duplicate(client):
    client.post(
        "/datasets/",
        json={"name": "vendas_diarias", "format": "csv", "schema": SALES_SCHEMA},
    )
    response = client.post(
        "/datasets/",
        json={"name": "vendas_diarias", "format": "csv", "schema": SALES_SCHEMA},
    )

    assert response.status_code == 409


def test_create_dataset_rejects_duplicate_fields(client):
    invalid_schema = {
        "fields": [
            {"name": "id", "type": "integer", "required": True},
            {"name": "valor", "type": "float", "required": True},
            {"name": "id", "type": "string", "required": True},
        ]
    }

    response = client.post(
        "/datasets/",
        json={"name": "dataset_invalido", "format": "csv", "schema": invalid_schema},
    )

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert any("id" in err["msg"] and "duplicate" in err["msg"] for err in detail)


def test_get_dataset(client):
    create = client.post(
        "/datasets/",
        json={"name": "vendas_diarias", "format": "csv", "schema": SALES_SCHEMA},
    )
    dataset_id = create.json()["id"]

    response = client.get(f"/datasets/{dataset_id}")

    assert response.status_code == 200
    assert response.json()["id"] == dataset_id


def test_get_dataset_not_found(client):
    response = client.get("/datasets/00000000-0000-0000-0000-000000000000")

    assert response.status_code == 404


def test_list_datasets_empty(client):
    response = client.get("/datasets/")

    assert response.status_code == 200
    assert response.json() == []


def test_list_datasets_newest_first(client):
    client.post(
        "/datasets/",
        json={"name": "primeiro", "format": "csv", "schema": SALES_SCHEMA},
    )
    client.post(
        "/datasets/",
        json={"name": "segundo", "format": "csv", "schema": SALES_SCHEMA},
    )

    response = client.get("/datasets/")

    assert response.status_code == 200
    names = [d["name"] for d in response.json()]
    assert names == ["segundo", "primeiro"]
