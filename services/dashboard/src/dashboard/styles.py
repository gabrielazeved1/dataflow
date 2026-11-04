import streamlit as st

_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"], .stApp, .stMarkdown, .stText, button, input, textarea, select {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    font-feature-settings: 'cv11', 'ss01';
}

code, pre, .stCode, [data-testid="stMarkdownContainer"] code {
    font-family: 'JetBrains Mono', ui-monospace, monospace !important;
    font-size: 0.85rem;
    background: #141414 !important;
    color: #00e5ff !important;
    padding: 0.1rem 0.4rem;
    border-radius: 4px;
    border: 1px solid #1f1f1f;
}

.stApp { background: #0a0a0a; }

h1 {
    font-weight: 700;
    letter-spacing: -0.03em;
    color: #fafafa;
}

h2, h3 {
    font-weight: 600;
    letter-spacing: -0.015em;
    color: #ededed;
}

h1::after {
    content: '';
    display: block;
    width: 48px;
    height: 2px;
    background: #00e5ff;
    margin-top: 0.75rem;
    box-shadow: 0 0 12px rgba(0, 229, 255, 0.5);
}

[data-testid="stMetric"] {
    background: #111111;
    border: 1px solid #1f1f1f;
    border-radius: 6px;
    padding: 1.25rem 1.25rem;
    transition: border-color 0.15s ease;
}

[data-testid="stMetric"]:hover {
    border-color: #2a2a2a;
}

[data-testid="stMetricLabel"] {
    color: #737373;
    font-size: 0.72rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}

[data-testid="stMetricLabel"] p {
    color: #737373 !important;
    font-weight: 500 !important;
}

[data-testid="stMetricValue"] {
    color: #fafafa;
    font-weight: 600;
    font-size: 2rem;
    letter-spacing: -0.02em;
}

[data-testid="stCaptionContainer"], .stCaption, [data-testid="stCaptionContainer"] p {
    color: #525252 !important;
    font-size: 0.8rem;
}

hr, [data-testid="stDivider"] {
    border-color: #1a1a1a !important;
    background: #1a1a1a !important;
    margin: 1.5rem 0 !important;
}

[data-testid="stDataFrame"], [data-testid="stTable"] {
    border: 1px solid #1f1f1f;
    border-radius: 6px;
    overflow: hidden;
}

section[data-testid="stSidebar"] {
    background: #050505;
    border-right: 1px solid #1a1a1a;
}

section[data-testid="stSidebar"] [data-testid="stSidebarNav"] {
    padding-top: 2rem;
}

section[data-testid="stSidebar"] [data-testid="stSidebarNav"] ul li a {
    font-weight: 500;
    color: #a3a3a3;
    padding: 0.6rem 1rem;
    border-radius: 4px;
    transition: all 0.15s ease;
}

section[data-testid="stSidebar"] [data-testid="stSidebarNav"] ul li a:hover {
    background: #141414;
    color: #fafafa;
}

section[data-testid="stSidebar"] [data-testid="stSidebarNav"] ul li a[aria-current="page"] {
    background: #0f1f24;
    color: #00e5ff;
    border-left: 2px solid #00e5ff;
    padding-left: calc(1rem - 2px);
}

.stButton > button {
    border: 1px solid #262626;
    background: #111111;
    color: #ededed;
    font-weight: 500;
    border-radius: 6px;
    transition: all 0.15s ease;
}

.stButton > button:hover {
    border-color: #00e5ff;
    color: #00e5ff;
    box-shadow: 0 0 0 1px rgba(0, 229, 255, 0.15);
}

.stButton > button:active {
    background: #0f1f24;
}

[data-testid="stExpander"] {
    border: 1px solid #1f1f1f;
    border-radius: 6px;
    background: #0d0d0d;
}

[data-testid="stExpander"] summary {
    color: #d4d4d4;
    font-weight: 500;
}

[data-testid="stSelectbox"] label, [data-testid="stTextInput"] label {
    color: #a3a3a3;
    font-size: 0.85rem;
    font-weight: 500;
}

[data-testid="stAlert"] {
    border-radius: 6px;
    border-width: 1px;
}

[data-baseweb="select"] > div {
    background: #111111;
    border: 1px solid #262626;
}

::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-track { background: #0a0a0a; }
::-webkit-scrollbar-thumb { background: #262626; border-radius: 5px; }
::-webkit-scrollbar-thumb:hover { background: #404040; }
</style>
"""


def apply_theme() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
