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

datasets_by_name = {d["name"]: d for d in datasets}
name = st.selectbox("Dataset", list(datasets_by_name.keys()))
dataset = datasets_by_name[name]

with st.expander("Schema (JSONB)"):
    st.json(dataset["schema"])

st.divider()

st.subheader("Taxa de rejeição (últimas 20 Golds)")

golds = list_executions(dataset=name, layer="GOLD", status="SUCCESS", limit=20)

if not golds:
    st.info("Nenhuma execução GOLD SUCCESS para este dataset.")
else:
    points = []
    for g in golds:
        detail = get_execution(g["id"])
        metrics = detail.get("metrics")
        if metrics is None:
            continue
        points.append(
            {
                "created_at": detail["created_at"],
                "rejection_rate": metrics["rejection_rate"],
            }
        )

    if not points:
        st.info("Golds sem métricas registradas.")
    else:
        df = pd.DataFrame(points)
        df["created_at"] = pd.to_datetime(df["created_at"])
        df = df.sort_values("created_at").set_index("created_at")
        st.line_chart(df)

st.divider()

st.subheader("Execuções do dataset")

executions = list_executions(dataset=name)

if not executions:
    st.info("Sem execuções para este dataset.")
else:
    rows = [
        {
            "created_at": e["created_at"],
            "layer": e["layer"],
            "status": e["status"],
            "id": e["id"],
        }
        for e in executions
    ]
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
