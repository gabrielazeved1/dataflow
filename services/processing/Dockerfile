FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY libs/dataflow_common ./libs/dataflow_common
COPY services/api ./services/api
COPY services/processing ./services/processing

RUN uv sync --package dataflow-processing --frozen --no-dev

CMD ["uv", "run", "--package", "dataflow-processing", "celery", "-A", "processing.celery_app", "worker", "--loglevel=info"]
