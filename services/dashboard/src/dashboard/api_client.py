import os
from typing import Any

import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")
TIMEOUT = 10


def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    response = requests.get(f"{API_URL}{path}", params=params, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


@st.cache_data(ttl=60)
def list_datasets() -> list[dict]:
    return _get("/datasets/")


@st.cache_data(ttl=5)
def list_executions(
    dataset: str | None = None,
    status: str | None = None,
    layer: str | None = None,
    from_: str | None = None,
    to: str | None = None,
) -> list[dict]:
    params = {
        "dataset": dataset,
        "status": status,
        "layer": layer,
        "from": from_,
        "to": to,
    }
    params = {k: v for k, v in params.items() if v is not None}
    return _get("/executions/", params=params)["items"]


@st.cache_data(ttl=5)
def get_execution(execution_id: str) -> dict:
    return _get(f"/executions/{execution_id}")


@st.cache_data(ttl=30)
def get_rejected_preview(silver_id: str, limit: int = 20) -> dict:
    return _get(f"/executions/{silver_id}/rejected", params={"limit": limit})
