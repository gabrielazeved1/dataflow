import pandas as pd
import streamlit as st

from dashboard.api_client import (
    get_execution,
    get_rejected_preview,
    list_datasets,
    list_executions,
)

st.set_page_config(page_title="Detalhe — DataFlow", layout="wide")

st.title("Detalhe da execução")

execution_id = st.query_params.get("execution_id")

if not execution_id:
    st.info("Passe `?execution_id=<uuid>` na URL ou escolha abaixo.")
    execucoes = list_executions()[:50]
    if not execucoes:
        st.stop()
    opcoes = {
        f"{e['created_at']} · {e['layer']} · {e['status']} · {e['id']}": e["id"]
        for e in execucoes
    }
    escolha = st.selectbox("Últimas 50 execuções", list(opcoes.keys()))
    execution_id = opcoes[escolha]

execution = get_execution(execution_id)

datasets = list_datasets()
dataset_nome = next(
    (d["nome"] for d in datasets if d["id"] == execution["dataset_id"]),
    "—",
)

st.caption(f"ID: `{execution['id']}`")

col1, col2 = st.columns(2)
with col1:
    st.markdown(f"**Dataset:** {dataset_nome}")
    st.markdown(f"**Layer:** {execution['layer']}")
    st.markdown(f"**Status:** {execution['status']}")
    st.markdown(f"**Created at:** {execution['created_at']}")
    st.markdown(f"**Started at:** {execution.get('started_at') or '—'}")
    st.markdown(f"**Finished at:** {execution.get('finished_at') or '—'}")
with col2:
    st.markdown(f"**File name:** {execution.get('file_name') or '—'}")
    st.markdown(f"**File size:** {execution.get('file_size') or '—'}")
    st.markdown(f"**Checksum:** `{execution.get('checksum') or '—'}`")
    st.markdown(f"**Storage path:** `{execution.get('storage_path') or '—'}`")
    st.markdown(f"**Input execution:** `{execution.get('input_execution_id') or '—'}`")

if execution.get("error_message"):
    st.error(execution["error_message"])

st.divider()

layer = execution["layer"]
status = execution["status"]

if layer == "GOLD":
    st.subheader("Métricas")
    metrics = execution.get("metrics")
    if metrics is None:
        st.warning("Sem métricas registradas para esta execução.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Valid", metrics["valid_count"])
        c2.metric("Rejected", metrics["rejected_count"])
        c3.metric("Rejection rate", f"{metrics['rejection_rate'] * 100:.2f}%")
        c4.metric("Duração (s)", f"{metrics['duration_seconds']:.1f}")

elif layer == "SILVER":
    st.subheader("Linhas rejeitadas")
    if status != "SUCCESS":
        st.info("Preview de rejeitadas disponível apenas em Silver SUCCESS.")
    elif st.button("Ver linhas rejeitadas (até 20)"):
        preview = get_rejected_preview(execution_id, limit=20)
        rows = preview.get("rows", [])
        if not rows:
            st.success("Nenhuma linha rejeitada nessa execução.")
        else:
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
