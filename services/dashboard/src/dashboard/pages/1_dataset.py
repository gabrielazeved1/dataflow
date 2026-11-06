import pandas as pd
import streamlit as st
from dashboard.api_client import get_execution, list_datasets, list_executions
from dashboard.styles import apply_theme

st.set_page_config(page_title="Por dataset — DataFlow", layout="wide")
apply_theme()

st.title("Por dataset")

datasets = list_datasets()

if not datasets:
    st.info("Nenhum dataset cadastrado.")
    st.stop()

datasets_by_nome = {d["nome"]: d for d in datasets}
nome = st.selectbox("Dataset", list(datasets_by_nome.keys()))
dataset = datasets_by_nome[nome]

with st.expander("Schema (JSONB)"):
    st.json(dataset["schema"])

st.divider()

st.subheader("Taxa de rejeição (últimas 20 Golds)")

golds = list_executions(dataset=nome, layer="GOLD", status="SUCCESS")[:20]

if not golds:
    st.info("Nenhuma execução GOLD SUCCESS para este dataset.")
else:
    pontos = []
    for g in golds:
        detalhe = get_execution(g["id"])
        metrics = detalhe.get("metrics")
        if metrics is None:
            continue
        pontos.append(
            {
                "created_at": detalhe["created_at"],
                "rejection_rate": metrics["rejection_rate"],
            }
        )

    if not pontos:
        st.info("Golds sem métricas registradas.")
    else:
        df = pd.DataFrame(pontos)
        df["created_at"] = pd.to_datetime(df["created_at"])
        df = df.sort_values("created_at").set_index("created_at")
        st.line_chart(df)

st.divider()

st.subheader("Execuções do dataset")

execucoes = list_executions(dataset=nome)

if not execucoes:
    st.info("Sem execuções para este dataset.")
else:
    rows = [
        {
            "created_at": e["created_at"],
            "layer": e["layer"],
            "status": e["status"],
            "id": e["id"],
        }
        for e in execucoes
    ]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
