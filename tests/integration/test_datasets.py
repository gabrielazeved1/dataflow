VENDAS_SCHEMA = {
    "campos": [
        {"nome": "data", "tipo": "date", "obrigatorio": True},
        {"nome": "produto", "tipo": "string", "obrigatorio": True},
        {"nome": "quantidade", "tipo": "integer", "obrigatorio": True, "min": 1},
        {"nome": "valor_unitario", "tipo": "float", "obrigatorio": True, "min": 0.01},
    ]
}


def test_create_dataset(client):
    response = client.post(
        "/datasets/",
        json={"nome": "vendas_diarias", "formato": "csv", "schema": VENDAS_SCHEMA},
    )

    assert response.status_code == 201
    data = response.json()
    assert data["nome"] == "vendas_diarias"
    assert data["formato"] == "csv"
    assert data["schema"] == VENDAS_SCHEMA
    assert "id" in data
    assert "created_at" in data


def test_create_dataset_duplicate(client):
    client.post(
        "/datasets/",
        json={"nome": "vendas_diarias", "formato": "csv", "schema": VENDAS_SCHEMA},
    )
    response = client.post(
        "/datasets/",
        json={"nome": "vendas_diarias", "formato": "csv", "schema": VENDAS_SCHEMA},
    )

    assert response.status_code == 409


def test_create_dataset_rejects_duplicate_fields(client):
    schema_invalido = {
        "campos": [
            {"nome": "id", "tipo": "integer", "obrigatorio": True},
            {"nome": "valor", "tipo": "float", "obrigatorio": True},
            {"nome": "id", "tipo": "string", "obrigatorio": True},
        ]
    }

    response = client.post(
        "/datasets/",
        json={"nome": "dataset_invalido", "formato": "csv", "schema": schema_invalido},
    )

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert any("id" in err["msg"] and "duplicad" in err["msg"] for err in detail)


def test_get_dataset(client):
    create = client.post(
        "/datasets/",
        json={"nome": "vendas_diarias", "formato": "csv", "schema": VENDAS_SCHEMA},
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
        json={"nome": "primeiro", "formato": "csv", "schema": VENDAS_SCHEMA},
    )
    client.post(
        "/datasets/",
        json={"nome": "segundo", "formato": "csv", "schema": VENDAS_SCHEMA},
    )

    response = client.get("/datasets/")

    assert response.status_code == 200
    names = [d["nome"] for d in response.json()]
    assert names == ["segundo", "primeiro"]
