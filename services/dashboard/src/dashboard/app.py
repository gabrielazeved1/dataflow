import streamlit as st

from dashboard.styles import apply_theme

st.set_page_config(page_title="DataFlow Admin", layout="wide")
apply_theme()

st.title("DataFlow — Admin dashboard")

st.markdown("""
    Dashboard operacional da plataforma **DataFlow**. Use a barra lateral
    para navegar entre as páginas:

    - **Overview** — visão geral da plataforma (KPIs e execuções recentes).
    - **Por dataset** — histórico de ingestões por dataset.
    - **Detalhe** — tudo sobre uma execução, incluindo preview de linhas
      rejeitadas.
    """)
