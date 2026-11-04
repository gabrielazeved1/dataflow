FROM python:3.11-slim

RUN pip install uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY libs/dataflow_common ./libs/dataflow_common
COPY services/api ./services/api
COPY services/processing ./services/processing
COPY services/dashboard ./services/dashboard
COPY services/dashboard/.streamlit ./.streamlit

RUN uv sync --package dataflow-dashboard --frozen --no-dev

CMD ["uv", "run", "--package", "dataflow-dashboard", "streamlit", "run", \
     "services/dashboard/src/dashboard/app.py", \
     "--server.port=8501", "--server.address=0.0.0.0"]
