# DATAFLOW

Plataforma de dados de estudo e portfólio com arquitetura Medalhão (Bronze/Silver/Gold).

```
Cliente → FastAPI (ingestão) → MinIO/Bronze → PySpark → Silver → Gold
                                                              ↓
                                                   PostgreSQL (metadados/status)
```

Documentação completa em [`dataflow-docs/docs/`](dataflow-docs/docs/README.md).

---

## Pré-requisitos

- Docker e Docker Compose
- Python 3.11+
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

---

## Subindo o ambiente

```bash
cp .env.example .env
docker compose up --build
```

Serviços disponíveis:

| Serviço | URL |
|---|---|
| API | http://localhost:8000 |
| API docs (Swagger) | http://localhost:8000/docs |
| MinIO console | http://localhost:9001 |
| PostgreSQL | localhost:5432 |

Credenciais padrão de desenvolvimento estão no `.env.example`.

---

## Desenvolvimento local

Instalar dependências de desenvolvimento na raiz (pytest, black, ruff):

```bash
uv sync
```

Instalar dependências da API:

```bash
cd services/api
uv sync
```

---

## Testes

```bash
# da raiz do projeto
uv run pytest
```

O `pyproject.toml` raiz configura o `pythonpath` para `services/api/src` e `libs/dataflow_common/src`, então os imports funcionam sem instalação adicional.

---

## Lint e formatação

```bash
uv run ruff check .
uv run black --check .
```

---

## Estrutura

```
dataflow/
├── docker-compose.yml
├── pyproject.toml              # dev tools + pytest
├── dataflow-docs/docs/         # arquitetura, regras de negócio, roadmap, ADRs
├── services/
│   └── api/                    # FastAPI (ingestão + consulta de status)
├── libs/
│   └── dataflow_common/        # código compartilhado (schemas, clients)
└── tests/
    ├── unit/
    └── integration/
```

---

## Roadmap

O projeto evolui em versões incrementais — cada uma com objetivo e critério de conclusão definidos em [`dataflow-docs/docs/roadmap.md`](dataflow-docs/docs/roadmap.md).

| Versão | Foco | Status |
|---|---|---|
| V0 | FastAPI + PostgreSQL + testes | em andamento |
| V1 | MinIO (Bronze) | — |
| V2 | Medallion formalizado + Parquet | — |
| V3 | PySpark (Bronze→Silver→Gold) | — |
| V4 | Celery + Redis (assíncrono) | — |
| V5 | Confiabilidade | — |
| V6 | CI/CD | — |
| V7 | Deploy | — |
