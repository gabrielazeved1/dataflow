from datetime import date, timedelta

import pandas as pd
import streamlit as st
from dashboard.api_client import list_datasets, list_executions
from dashboard.styles import apply_theme
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Overview — DataFlow", layout="wide")
apply_theme()

st_autorefresh(interval=10_000, key="overview_refresh")

st.title("Overview")

today = date.today()
seven_days_ago = today - timedelta(days=6)

datasets = list_datasets()
executions_7d = list_executions(from_=seven_days_ago.isoformat())
executions_running = list_executions(status="RUNNING")

failed_7d = [e for e in executions_7d if e["status"] == "FAILED"]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Datasets cadastrados", len(datasets))
col2.metric("Execuções (7 dias)", len(executions_7d))
col3.metric("Falhas (7 dias)", len(failed_7d))
col4.metric("RUNNING agora", len(executions_running))

st.divider()

st.subheader("Execuções por dia (últimos 7 dias)")

if executions_7d:
    df = pd.DataFrame(executions_7d)
    df["dia"] = pd.to_datetime(df["created_at"]).dt.date
    chart_data = df.groupby(["dia", "status"]).size().unstack(fill_value=0)
    st.bar_chart(chart_data)
else:
    st.info("Nenhuma execução nos últimos 7 dias.")

st.divider()

st.subheader("Últimas 10 execuções")

if executions_7d:
    dataset_by_id = {d["id"]: d["name"] for d in datasets}
    rows = []
    for e in executions_7d[:10]:
        rows.append(
            {
                "created_at": e["created_at"],
                "dataset": dataset_by_id.get(e["dataset_id"], "—"),
                "layer": e["layer"],
                "status": e["status"],
                "id": e["id"],
            }
        )
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
else:
    st.info("Sem execuções para listar.")
