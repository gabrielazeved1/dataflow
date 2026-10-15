FROM python:3.11-slim

RUN pip install uv

WORKDIR /app

COPY pyproject.toml ./
RUN uv sync --no-dev #instalar dependencias

COPY src/ ./src/

CMD ["uv", "run", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
