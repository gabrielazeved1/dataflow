FROM python:3.11-slim

RUN pip install uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY libs/dataflow_common ./libs/dataflow_common
COPY services/api ./services/api
COPY services/processing ./services/processing

RUN uv sync --package dataflow-api --frozen --no-dev

CMD ["uv", "run", "--package", "dataflow-api", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
