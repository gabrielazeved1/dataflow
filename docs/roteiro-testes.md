# Roteiro de testes

> Como verificar a plataforma inteira, inclusive logs e recuperação de
> falhas. Os 4 cenários foram rodados ponta a ponta na `v1.0.0`.

## 0. Preparar

```bash
docker compose up -d --build          # db-migrate aplica a migration e1f3c5a7b9d2
curl localhost:8000/health            # {"status":"ok"}

export JAVA_HOME=$(/usr/libexec/java_home -v 21)
uv run pytest -q                      # 129 passed (usa dataflow_test, nao apaga a demo)
uv run ruff check . && uv run black --check .
```

Dataset de teste (guarda o id em `$ID`):

```bash
ID=$(curl -s -X POST localhost:8000/datasets/ -H 'content-type: application/json' -d '{
  "name": "vendas_teste", "format": "csv",
  "schema": {"fields": [
    {"name": "data", "type": "date", "required": true},
    {"name": "produto", "type": "string", "required": true},
    {"name": "qtd", "type": "integer", "required": true, "min": 1},
    {"name": "valor", "type": "float", "required": true, "min": 0.01}
  ]}}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])'); echo $ID
```

## 1. Abrir as 4 janelas

| Janela | O que mostra | Comando |
|---|---|---|
| T1 · logs | cada passo de cada ingestao, api e worker juntos | `docker compose logs -f api worker 2>&1 \| grep --line-buffered "trace="` |
| T2 · minio | cada arquivo gravado/lido no data lake | `docker compose run --rm --entrypoint sh minio-init -c 'mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && mc watch local/dataflow --recursive'` |
| T3 · postgres | ultimos eventos na tabela, a cada 1s | ver abaixo |
| navegador | grafo + timeline | http://localhost:8501 → **trace** (seguir a mais recente ligado) |

T3:

```bash
while true; do clear; docker compose exec -T postgres psql -U dataflow -d dataflow -c "select to_char(created_at,'HH24:MI:SS.MS') hora, service, step, message from execution_event order by created_at desc limit 15;"; sleep 1; done
```

Pra seguir uma ingestao so nos logs: `docker compose logs -f api worker | grep <trace_id>`
(o trace_id aparece na pagina trace e na resposta do ingest, é o id da bronze).

## 2. Cenário A — ingestão normal

```bash
curl -s -X POST "localhost:8000/dev/bulk-ingest?dataset_id=$ID&count=1&rows=1000&invalid_rate=0.1"
```

Esperado (~15-30s, mais na primeira por causa do boot da JVM):
- T1: `api.received` → `api.bronze_stored` → `api.enqueued` → `worker.silver_started` → `worker.bronze_downloaded` → `worker.spark_validated 1000 linhas: ~900 validas, ~100 rejeitadas` → `worker.silver_stored` → `worker.gold_started` → `worker.gold_stored`
- T2: `Put bronze/...csv` → `Get bronze` → `Put silver/...parquet` + `Put silver/.../_rejected/...` → `Put gold/...`
- navegador: setas ficando verdes da esquerda pra direita

## 3. Cenário B — carga

```bash
curl -s -X POST "localhost:8000/dev/bulk-ingest?dataset_id=$ID&count=20&rows=1000&invalid_rate=0.1"
```

Esperado: 20 bronzes na hora, silvers chegando de 2 em 2 (`WORKER_CONCURRENCY=2`), ~1 min no total. Taxa de rejeição media ~10%, dos quais ~10% `corrupt` (tipo errado).
Ver tambem: pagina **overview** (KPIs) e **execution** → silver → "ver linhas rejeitadas".

## 4. Cenário C — requisição perdida (redis fora)

```bash
docker compose -f docker-compose.yml -f docker-compose.demo.yml up -d worker beat   # timeouts curtos
docker compose stop redis
curl -s -w "\nhttp=%{http_code}\n" -X POST "localhost:8000/dev/bulk-ingest?dataset_id=$ID&count=1&rows=200"
docker compose start redis
```

Esperado:
- ingest responde **202** (nao 500): a bronze ja esta salva
- T1: `api.enqueue_failed redis indisponivel, reconciler reenfileira`
- navegador: seta api → redis **vermelha tracejada "PERDIDA"**, status "requisição perdida"
- 1-2 min depois: `reconciler.requeued`, seta **laranja**, resto fica verde

## 5. Cenário D — worker morre no meio do spark

Com o `docker-compose.demo.yml` ainda ativo:

```bash
curl -s -X POST "localhost:8000/dev/bulk-ingest?dataset_id=$ID&count=1&rows=3000"
# quando aparecer worker.silver_started no T1:
docker compose kill worker && docker compose -f docker-compose.yml -f docker-compose.demo.yml start worker
```

Esperado (~2-3 min):
- a silver fica parada em RUNNING (o `except` nunca rodou: o processo morreu)
- `reaper.timeout silver presa em RUNNING, marcada FAILED` → seta reaper → postgres vermelha
- `reconciler.requeued` → nova silver → gold
- cadeia final: `BRONZE:SUCCESS, SILVER:FAILED, SILVER:SUCCESS, GOLD:SUCCESS`

Voltar ao normal: `docker compose up -d worker beat`

## 6. Erros de entrada (API)

```bash
curl -s -X POST localhost:8000/datasets/ -H 'content-type: application/json' \
  -d '{"name":"x","format":"json","schema":{"fields":[{"name":"a","type":"string"}]}}'   # 422
curl -s "localhost:8000/executions/?after=$(printf '[1]' | base64)"                       # 400 Invalid cursor
curl -s localhost:8000/datasets/00000000-0000-0000-0000-000000000000                     # 404
```

## Perguntas pra demo

- Por que as bronzes aparecem na hora e as silvers de 2 em 2?
- Por que o redis fora nao derruba a ingestao?
- Por que reaper **e** reconciler? (um corrige o estado, o outro recupera o trabalho)
- O que impede o reconciler de processar a mesma bronze duas vezes? (indice unico parcial)
