import pandas as pd
import streamlit as st
from dashboard.api_client import get_trace, list_recent_bronzes
from dashboard.styles import apply_theme
from dashboard.trace_graph import build_dot, parse_ts, summarize, timeline_rows
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Rastreio — DataFlow", layout="wide")
apply_theme()

st.title("Rastreio da ingestão")
st.caption(
    "O caminho de um CSV do cliente até a Gold, passando por API, MinIO, Redis, "
    "worker e Postgres. Atualiza a cada 2 segundos."
)

follow = st.toggle("Seguir a ingestão mais recente", value=True)

bronzes = list_recent_bronzes()
if not bronzes:
    st.info("Nenhuma ingestão ainda. Envie um CSV ou use /dev/bulk-ingest.")
    st.stop()

if follow:
    bronze_id = bronzes[0]["id"]
else:
    options = {
        f"{b['created_at'][11:19]} · {b['file_name']} · {b['id'][:8]}": b["id"]
        for b in bronzes
    }
    bronze_id = options[st.selectbox("Ingestão", list(options.keys()))]

trace = get_trace(bronze_id)
status = summarize(trace)
bronze = trace["executions"][0]

labels = {
    "success": "concluída",
    "running": "em andamento",
    "failed": "falhou",
    "lost": "requisição perdida, aguardando o reconciler",
}
events = trace["events"]
elapsed = (
    (
        parse_ts(events[-1]["created_at"]) - parse_ts(events[0]["created_at"])
    ).total_seconds()
    if len(events) > 1
    else 0.0
)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Dataset", trace["dataset_name"])
col2.metric("Arquivo", bronze.get("file_name") or "—")
col3.metric("Status", labels[status])
col4.metric("Tempo até agora", f"{elapsed:.1f}s")
st.caption(f"trace_id `{trace['trace_id']}` · busque nos logs com esse id")

st.graphviz_chart(build_dot(trace), width="stretch")
st.caption(
    "verde: feito · amarelo: em andamento · vermelho: perdido ou falhou · "
    "laranja: recuperado pelo reconciler"
)

st.subheader("Linha do tempo")
rows = timeline_rows(trace)
if rows:
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
else:
    st.info("Sem eventos ainda.")

# para de atualizar quando a ingestao fixa terminou
if follow or status in ("running", "lost"):
    st_autorefresh(interval=2000, key="trace_refresh")
