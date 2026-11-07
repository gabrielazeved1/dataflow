import pandas as pd
import streamlit as st
from dashboard.api_client import (
    get_execution,
    get_rejected_preview,
    list_datasets,
    list_executions,
)
from dashboard.styles import apply_theme

st.set_page_config(page_title="Detalhe — DataFlow", layout="wide")
apply_theme()

st.title("Detalhe da execução")

execution_id = st.query_params.get("execution_id")

if not execution_id:
    executions = list_executions(limit=50)
    if not executions:
        st.info("Nenhuma execução registrada ainda.")
        st.stop()
    options = {
        f"{e['created_at']} · {e['layer']} · {e['status']} · {e['id'][:8]}": e["id"]
        for e in executions
    }
    choice = st.selectbox("Escolha uma execução", list(options.keys()))
    execution_id = options[choice]

execution = get_execution(execution_id)

datasets = list_datasets()
dataset_name = next(
    (d["name"] for d in datasets if d["id"] == execution["dataset_id"]),
    "—",
)

st.caption(f"ID: `{execution['id']}`")

col1, col2 = st.columns(2)
with col1:
    st.markdown(f"**Dataset:** {dataset_name}")
    st.markdown(f"**Layer:** {execution['layer']}")
    st.markdown(f"**Status:** {execution['status']}")
    st.markdown(f"**Created at:** {execution['created_at']}")
with col2:
    st.markdown(f"**Started at:** {execution.get('started_at') or '—'}")
    st.markdown(f"**Finished at:** {execution.get('finished_at') or '—'}")
    if execution.get("input_execution_id"):
        st.markdown(f"**Input execution:** `{execution['input_execution_id']}`")
    if execution.get("storage_path"):
        st.markdown(f"**Storage path:** `{execution['storage_path']}`")

if execution.get("error_message"):
    st.error(execution["error_message"])

layer = execution["layer"]
status = execution["status"]

if layer == "BRONZE":
    st.divider()
    st.subheader("Dados do arquivo")
    if execution.get("file_name") is None:
        st.info("Esta execução não tem metadados de arquivo (criada sem upload real).")
    else:
        fc1, fc2, fc3 = st.columns(3)
        fc1.metric("File name", execution["file_name"])
        fc2.metric("File size (bytes)", execution.get("file_size") or 0)
        checksum = execution.get("checksum") or ""
        fc3.metric("Checksum (sha256)", f"{checksum[:12]}…" if checksum else "—")
        if checksum:
            st.caption(f"Full checksum: `{checksum}`")

if layer == "GOLD":
    st.divider()
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
    st.divider()
    st.subheader("Linhas rejeitadas")
    if status != "SUCCESS":
        st.info("Preview de rejeitadas disponível apenas em Silver SUCCESS.")
    elif st.button("Ver linhas rejeitadas (até 20)"):
        preview = get_rejected_preview(execution_id, limit=20)
        rows = preview.get("rows", [])
        if not rows:
            st.success("Nenhuma linha rejeitada nessa execução.")
        else:
            st.dataframe(pd.DataFrame(rows), width="stretch")
