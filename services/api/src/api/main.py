from fastapi import FastAPI

from api.routers import datasets, executions, ingestion

app = FastAPI(title="DATAFLOW API", version="0.1.0")

app.include_router(datasets.router)
app.include_router(executions.router)
app.include_router(ingestion.router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
