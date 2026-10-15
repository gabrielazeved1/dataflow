from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.database import Base, engine
from api.routers import datasets, ingestion


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield  


app = FastAPI(title="DATAFLOW API", version="0.1.0", lifespan=lifespan)

app.include_router(datasets.router)
app.include_router(ingestion.router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
