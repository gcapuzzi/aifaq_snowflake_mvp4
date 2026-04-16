import streamlit as st
import snowflake.connector
import re
import pandas as pd


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SnowMind · COVID Data Intelligence",
    page_icon="❄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:ital,wght@0,300;0,400;0,500;1,300&family=Bebas+Neue&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

html, body, [data-testid="stAppViewContainer"] {
    background: #080c10;
    color: #c8d8e8;
    font-family: 'IBM Plex Mono', monospace;
}

#MainMenu, footer, header { visibility: hidden; }
[data-testid="stDecoration"] { display: none; }

[data-testid="stSidebar"] {
    background: #0a0f14 !important;
    border-right: 1px solid #1a2535;
}

.main .block-container {
    padding: 1.5rem 2rem 4rem;
    max-width: 960px;
    margin: 0 auto;
}

/* ── Header ── */
.app-header {
    border-bottom: 1px solid #1a2535;
    padding-bottom: 1.2rem;
    margin-bottom: 1.5rem;
}
.app-title {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 2.8rem;
    letter-spacing: 0.08em;
    color: #e8f4ff;
    line-height: 1;
}
.app-title span { color: #2a9fd6; }
.app-subtitle {
    font-size: 0.65rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: #3a5570;
    margin-top: 0.3rem;
}

/* ── Status ── */
.status-row {
    display: flex;
    gap: 0.6rem;
    align-items: center;
    margin-bottom: 1rem;
}
.badge {
    font-size: 0.62rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 0.25rem 0.6rem;
    border-radius: 1px;
    border: 1px solid;
}
.badge-ok { border-color: #1a4a2a; color: #3a9a5a; background: #0a1a0f; }
.badge-err { border-color: #4a1a1a; color: #9a3a3a; background: #1a0a0a; }
.badge-info { border-color: #1a3a5a; color: #3a7aaa; background: #0a1a2a; }

/* ── Schema panel ── */
.schema-item {
    font-size: 0.68rem;
    color: #4a7a9a;
    padding: 0.35rem 0.6rem;
    background: #0d1520;
    border: 1px solid #1a2535;
    border-left: 2px solid #2a9fd6;
    margin-bottom: 0.25rem;
    border-radius: 1px;
}
.schema-item strong { color: #7ac8e8; font-weight: 500; }

/* ── Chat ── */
.chat-area { margin-bottom: 1.5rem; }

.msg {
    padding: 0.9rem 1.1rem;
    margin-bottom: 0.8rem;
    border-radius: 2px;
    font-size: 0.82rem;
    line-height: 1.7;
}
.msg-user {
    background: #0d1825;
    border: 1px solid #1e3450;
    border-left: 3px solid #2a9fd6;
    margin-left: 2rem;
}
.msg-assistant {
    background: #0a1218;
    border: 1px solid #1a2530;
    border-left: 3px solid #2ad68a;
    margin-right: 2rem;
}
.msg-role {
    font-size: 0.58rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    opacity: 0.45;
    margin-bottom: 0.45rem;
}
.msg-user .msg-role { color: #2a9fd6; }
.msg-assistant .msg-role { color: #2ad68a; }

/* ── SQL box ── */
.sql-box {
    margin-top: 0.7rem;
    padding: 0.7rem 0.9rem;
    background: #060a0e;
    border: 1px solid #1a2535;
    border-top: 2px solid #f0a500;
    border-radius: 1px;
    font-size: 0.7rem;
    color: #f0a500;
    white-space: pre-wrap;
    word-break: break-word;
    font-family: 'IBM Plex Mono', monospace;
}
.sql-label {
    font-size: 0.58rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: #7a5a00;
    margin-bottom: 0.4rem;
}

/* ── Empty state ── */
.empty-state {
    text-align: center;
    padding: 4rem 2rem;
    color: #1e3550;
}
.empty-icon {
    font-size: 3rem;
    margin-bottom: 0.8rem;
    display: block;
}
.empty-text {
    font-size: 0.75rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
}

/* ── Example queries ── */
.example-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.5rem;
    margin-top: 1.5rem;
}
.example-q {
    padding: 0.6rem 0.8rem;
    background: #0a1520;
    border: 1px solid #1a2535;
    border-radius: 1px;
    font-size: 0.68rem;
    color: #4a7a9a;
    cursor: pointer;
    transition: all 0.15s;
}
.example-q:hover { border-color: #2a9fd6; color: #7ac8e8; background: #0d1f30; }

/* ── Buttons ── */
.stButton > button {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.72rem !important;
    letter-spacing: 0.08em !important;
    background: #0d1520 !important;
    color: #c8d8e8 !important;
    border: 1px solid #1e3550 !important;
    border-radius: 1px !important;
    padding: 0.45rem 1rem !important;
    transition: all 0.15s !important;
}
.stButton > button:hover {
    background: #142030 !important;
    border-color: #2a9fd6 !important;
    color: #7ac8e8 !important;
}

/* ── Chat input ── */
[data-testid="stChatInput"] {
    background: #0d1520 !important;
    border: 1px solid #1e3550 !important;
    border-radius: 2px !important;
    color: #c8d8e8 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.82rem !important;
}
[data-testid="stChatInput"]:focus-within {
    border-color: #2a9fd6 !important;
    box-shadow: 0 0 0 1px #2a9fd620 !important;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border: 1px solid #1a2535 !important;
    border-radius: 2px !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: #080c10; }
::-webkit-scrollbar-thumb { background: #1e3550; border-radius: 2px; }
::-webkit-scrollbar-thumb:hover { background: #2a9fd6; }

/* ── Spinner ── */
.stSpinner > div { border-top-color: #2a9fd6 !important; }

/* ── Sidebar labels ── */
.sidebar-label {
    font-size: 0.6rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: #2a4a6a;
    margin: 1.2rem 0 0.5rem;
    border-bottom: 1px solid #1a2535;
    padding-bottom: 0.3rem;
}
</style>
""", unsafe_allow_html=True)

# ── Connection ────────────────────────────────────────────────────────────────
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def get_connection():
    try:
        private_key_str = st.secrets["snowflake"]["private_key"]
        private_key = serialization.load_pem_private_key(
            private_key_str.encode(),
            password=None,
            backend=default_backend()
        )
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return snowflake.connector.connect(
            account=st.secrets["snowflake"]["account"],
            user=st.secrets["snowflake"]["user"],
            private_key=private_key_bytes,
            warehouse=st.secrets["snowflake"]["warehouse"],
            database="COVID19_EPIDEMIOLOGICAL_DATA",
            schema="PUBLIC",
            role=st.secrets["snowflake"].get("role", "ACCOUNTADMIN"),
        )
    except Exception as e:
        st.session_state["conn_error"] = str(e)
        return None

conn = get_connection()

# ── Snowflake connection ──────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_connection():
    try:
        return snowflake.connector.connect(
            account=st.secrets["snowflake"]["account"],
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database="COVID19_EPIDEMIOLOGICAL_DATA",
            schema="PUBLIC",
            role=st.secrets["snowflake"].get("role", ""),
        )
    except Exception as e:
        return None

def run_sql(conn, sql: str):
    """Execute SQL and return a DataFrame."""
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

# ── Schema context ────────────────────────────────────────────────────────────
SCHEMA_CONTEXT = """
You have access to the Snowflake database COVID19_EPIDEMIOLOGICAL_DATA (schema PUBLIC).
Key tables:

1. JHU_COVID_19
   - COUNTRY_REGION (VARCHAR): country name
   - PROVINCE_STATE (VARCHAR): province or state (nullable)
   - DATE (DATE): date of record
   - CONFIRMED (NUMBER): cumulative confirmed cases
   - DEATHS (NUMBER): cumulative deaths
   - RECOVERED (NUMBER): cumulative recovered

2. PCM_DPS_COVID19 — Italy only
   - DATA (DATE): date
   - STATO (VARCHAR): always 'ITA'
   - DENOMINAZIONE_REGIONE (VARCHAR): region name
   - TOTALE_CASI (NUMBER): total confirmed cases
   - DECEDUTI (NUMBER): deaths
   - DIMESSI_GUARITI (NUMBER): recovered
   - TERAPIA_INTENSIVA (NUMBER): ICU patients
   - RICOVERATI_CON_SINTOMI (NUMBER): hospitalised

3. OWID_VACCINATIONS
   - LOCATION (VARCHAR): country name
   - DATE (DATE): date
   - TOTAL_VACCINATIONS (NUMBER): total doses administered
   - PEOPLE_VACCINATED (NUMBER): people with at least one dose
   - PEOPLE_FULLY_VACCINATED (NUMBER): fully vaccinated
   - DAILY_VACCINATIONS (NUMBER): daily doses

Always use DATE >= '2020-01-01' filters when querying time ranges.
Always use LIMIT 100 unless the user asks for aggregated totals.
Always alias columns with clear names in SELECT.
For Italy questions prefer PCM_DPS_COVID19.
For global questions use JHU_COVID_19.
For vaccination questions use OWID_VACCINATIONS.
"""

# ── Text-to-SQL prompt ────────────────────────────────────────────────────────
def build_prompt(question: str, history: list) -> str:
    history_str = ""
    if history:
        last = history[-4:]  # last 2 turns
        for m in last:
            role = "User" if m["role"] == "user" else "Assistant"
            history_str += f"{role}: {m['content'][:300]}\n"

    return f"""You are a Snowflake SQL expert assistant for COVID-19 data analysis.

{SCHEMA_CONTEXT}

Recent conversation:
{history_str}

User question: {question}

Instructions:
1. Generate a valid Snowflake SQL query that answers the question.
2. After the SQL, write a brief natural language explanation of what the query does and what to expect.
3. Format your response EXACTLY as:
SQL:
```sql
<your sql here>
```
EXPLANATION:
<your explanation here>

If the question is not about data (e.g. greetings, general questions), respond with:
SQL:
NONE
EXPLANATION:
<answer the question directly>
"""

# ── Call Cortex LLM ───────────────────────────────────────────────────────────
def call_cortex(conn, prompt: str, model: str = "mistral-large2") -> str:
    safe = prompt.replace("'", "\\'").replace("\\", "\\\\")
    cur = conn.cursor()
    cur.execute(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            '{safe}'
        )
    """)
    row = cur.fetchone()
    return row[0] if row else ""

# ── Parse LLM response ────────────────────────────────────────────────────────
def parse_response(text: str):
    """Returns (sql, explanation). sql is None if no query."""
    sql = None
    explanation = text.strip()

    sql_match = re.search(r'```sql\s*(.*?)\s*```', text, re.DOTALL | re.IGNORECASE)
    if sql_match:
        candidate = sql_match.group(1).strip()
        if candidate.upper() != "NONE" and len(candidate) > 10:
            sql = candidate

    exp_match = re.search(r'EXPLANATION:\s*(.*)', text, re.DOTALL | re.IGNORECASE)
    if exp_match:
        explanation = exp_match.group(1).strip()

    return sql, explanation

# ── Session state ─────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []
if "model" not in st.session_state:
    st.session_state.model = "mistral-large2"

# ── Connection ────────────────────────────────────────────────────────────────
conn = get_connection()

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="font-family:'Bebas Neue',sans-serif;font-size:1.6rem;
                letter-spacing:0.1em;color:#e8f4ff;margin-bottom:0.2rem;">
        ❄ SNOWMIND
    </div>
    <div style="font-size:0.6rem;letter-spacing:0.18em;text-transform:uppercase;
                color:#2a4a6a;margin-bottom:1rem;">
        COVID · Data Intelligence
    </div>
    """, unsafe_allow_html=True)

    if conn:
        st.markdown('<div class="badge badge-ok">● Snowflake connected</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="badge badge-err">● Connection error</div>', unsafe_allow_html=True)
        st.error("Check secrets.toml")

    st.markdown('<div class="sidebar-label">Model</div>', unsafe_allow_html=True)
    st.session_state.model = st.selectbox(
        "Cortex model",
        ["mistral-large2", "mixtral-8x7b", "llama3-70b", "snowflake-arctic"],
        index=0,
        label_visibility="collapsed",
    )

    st.markdown('<div class="sidebar-label">Available Tables</div>', unsafe_allow_html=True)
    tables = [
        ("JHU_COVID_19", "Global cases · 9.7M rows"),
        ("PCM_DPS_COVID19", "Italy regions · 116K rows"),
        ("OWID_VACCINATIONS", "Global vaccines · 169K rows"),
        ("ECDC_GLOBAL", "ECDC global · 61K rows"),
        ("JHU_VACCINES", "JHU vaccines · 40K rows"),
    ]
    for name, desc in tables:
        st.markdown(f"""
        <div class="schema-item">
            <strong>{name}</strong><br>{desc}
        </div>
        """, unsafe_allow_html=True)

    st.markdown('<div class="sidebar-label">Session</div>', unsafe_allow_html=True)
    if st.button("Clear conversation", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# ── MAIN ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="app-header">
    <div class="app-title">SNOW<span>MIND</span></div>
    <div class="app-subtitle">Natural language · Snowflake SQL · COVID-19 Data</div>
</div>
""", unsafe_allow_html=True)

# Render chat history
if not st.session_state.messages:
    st.markdown("""
    <div class="empty-state">
        <span class="empty-icon">❄</span>
        <div class="empty-text">Ask anything about COVID-19 data</div>
        <div style="font-size:0.62rem;color:#1e3550;margin-top:0.5rem;letter-spacing:0.08em;">
            Powered by Snowflake Cortex · Text-to-SQL
        </div>
    </div>

    <div class="example-grid">
        <div class="example-q">How many total COVID cases in Italy?</div>
        <div class="example-q">Which country had the most deaths in 2020?</div>
        <div class="example-q">Show Italy ICU patients by month in 2021</div>
        <div class="example-q">Top 10 countries by total vaccinations</div>
        <div class="example-q">Daily cases in Italy during March 2020</div>
        <div class="example-q">Compare deaths in Italy vs Germany</div>
    </div>
    """, unsafe_allow_html=True)
else:
    for msg in st.session_state.messages:
        role_label = "you" if msg["role"] == "user" else "snowmind"
        css_class = "msg-user" if msg["role"] == "user" else "msg-assistant"

        sql_html = ""
        if msg.get("sql"):
            sql_html = f"""
            <div class="sql-box">
                <div class="sql-label">Generated SQL</div>
                {msg["sql"]}
            </div>"""

        st.markdown(f"""
        <div class="msg {css_class}">
            <div class="msg-role">{role_label}</div>
            {msg["content"]}
            {sql_html}
        </div>
        """, unsafe_allow_html=True)

        if msg.get("dataframe") is not None:
            st.dataframe(
                msg["dataframe"],
                use_container_width=True,
                hide_index=True,
            )

# ── Chat input ────────────────────────────────────────────────────────────────
if question := st.chat_input("Ask about COVID data… (e.g. 'Italy cases in 2020')"):
    active_conn = get_connection()
    if not active_conn:
        err = st.session_state.get("conn_error", "unknown")
        st.error(f"Connection failed: {err}")
        st.stop()

    st.session_state.messages.append({"role": "user", "content": question})

    with st.spinner("Generating SQL and querying Snowflake…"):
        prompt = build_prompt(question, st.session_state.messages[:-1])
        raw = call_cortex(active_conn, prompt, st.session_state.model)
        sql, explanation = parse_response(raw)

        df = None
        error_msg = None

        if sql:
            try:
                df = run_sql(active_conn, sql)
                if df.empty:
                    explanation += "\n\n*(No rows returned — try a different time range or country name.)*"
            except Exception as e:
                error_msg = str(e)
                explanation += f"\n\n⚠ SQL error: `{error_msg}`"
                sql = None

    st.session_state.messages.append({
        "role": "assistant",
        "content": explanation,
        "sql": sql,
        "dataframe": df,
    })

    st.rerun()
