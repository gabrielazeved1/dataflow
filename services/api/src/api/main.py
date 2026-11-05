from fastapi import FastAPI

from api.config import settings
from api.routers import datasets, executions, ingestion


def create_app() -> FastAPI:
    app = FastAPI(title="DATAFLOW API", version="0.1.0")

    app.include_router(datasets.router)
    app.include_router(executions.router)
    app.include_router(ingestion.router)

    if settings.env == "development":
        from api.routers import dev

        app.include_router(dev.router)

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    return app


app = create_app()
