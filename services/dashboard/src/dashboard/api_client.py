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


_PAGE_SIZE = 200
_MAX_PAGES = 50  # teto de 10 mil execucoes por consulta


@st.cache_data(ttl=5)
def list_executions(
    dataset: str | None = None,
    status: str | None = None,
    layer: str | None = None,
    from_: str | None = None,
    to: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    # a api pagina por cursor: sem percorrer as paginas os kpis travavam em 50
    params = {
        "dataset": dataset,
        "status": status,
        "layer": layer,
        "from": from_,
        "to": to,
    }
    params = {k: v for k, v in params.items() if v is not None}
    page_size = min(limit, _PAGE_SIZE) if limit else _PAGE_SIZE
    items: list[dict] = []
    cursor = None
    for _ in range(_MAX_PAGES):
        page = _get(
            "/executions/",
            params={
                **params,
                "limit": page_size,
                **({"after": cursor} if cursor else {}),
            },
        )
        items.extend(page["items"])
        cursor = page["next_cursor"]
        if cursor is None or (limit and len(items) >= limit):
            break
    return items[:limit] if limit else items


@st.cache_data(ttl=5)
def get_execution(execution_id: str) -> dict:
    return _get(f"/executions/{execution_id}")


@st.cache_data(ttl=30)
def get_rejected_preview(silver_id: str, limit: int = 20) -> dict:
    return _get(f"/executions/{silver_id}/rejected", params={"limit": limit})


@st.cache_data(ttl=1)
def list_recent_bronzes(limit: int = 50) -> list[dict]:
    return _get("/executions/", params={"layer": "BRONZE", "limit": limit})["items"]


@st.cache_data(ttl=1)
def get_trace(execution_id: str) -> dict:
    return _get(f"/traces/{execution_id}")
