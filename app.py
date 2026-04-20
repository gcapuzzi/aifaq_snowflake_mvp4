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
   - CASE_TYPE (VARCHAR): one of 'Confirmed', 'Deaths', 'Recovered', 'Active'
   - CASES (NUMBER): number of cases for that CASE_TYPE
   - LONG (FLOAT): longitude
   - LAT (FLOAT): latitude
   - DIFFERENCE (NUMBER): daily change
   - ISO3166_1 (VARCHAR): country code
   - ISO3166_2 (VARCHAR): region code
   - LAST_UPDATED_DATE (TIMESTAMP): last update
   - LAST_REPORTED_FLAG (BOOLEAN): whether this is the last reported value
   IMPORTANT: always filter by CASE_TYPE when querying this table
   IMPORTANT: to get confirmed cases use WHERE CASE_TYPE = 'Confirmed'
   IMPORTANT: to get deaths use WHERE CASE_TYPE = 'Deaths'
   IMPORTANT: JHU_COVID_19 has both country-level and province-level rows.
    Some countries have ONLY province-level data: 'United States', 'Australia', 
    'Canada', 'China', 'United Kingdom'.
    For these countries you MUST aggregate using SUM of MAX per province:
    SELECT COUNTRY_REGION, SUM(max_cases) AS total
    FROM (
        SELECT COUNTRY_REGION, PROVINCE_STATE, MAX(CASES) AS max_cases
        FROM JHU_COVID_19
        WHERE CASE_TYPE = 'Confirmed'
        GROUP BY COUNTRY_REGION, PROVINCE_STATE
    )
    GROUP BY COUNTRY_REGION
    For all other countries filter WHERE PROVINCE_STATE IS NULL to avoid double counting.
    For global rankings that include all countries, use the subquery approach for everyone.

2. PCM_DPS_COVID19 — Italy data
   - COUNTRY_REGION (VARCHAR): always 'Italy'
   - PROVINCE_STATE (VARCHAR): region name (nullable)
   - COUNTY (VARCHAR): county name (nullable)
   - FIPS (VARCHAR): FIPS code (nullable)
   - DATE (DATE): date of record
   - CASE_TYPE (VARCHAR): one of 'Confirmed', 'Deaths', 'Recovered', 'Active'
   - CASES (NUMBER): number of cases for that CASE_TYPE
   - LONG (FLOAT): longitude
   - LAT (FLOAT): latitude
   - ISO3166_1 (VARCHAR): country code
   - ISO3166_2 (VARCHAR): region code
   - DIFFERENCE (NUMBER): daily change
   - LAST_UPDATED_DATE (TIMESTAMP): last update
   - LAST_REPORTED_FLAG (BOOLEAN): whether this is the last reported value
   IMPORTANT: always filter by CASE_TYPE when querying this table
   IMPORTANT: to get total confirmed cases use WHERE CASE_TYPE = 'Confirmed'
   IMPORTANT: to get deaths use WHERE CASE_TYPE = 'Deaths'

3. OWID_VACCINATIONS
   - COUNTRY_REGION (VARCHAR): country name  ← not LOCATION!
   - DATE (DATE): date
   - ISO3166_1 (VARCHAR): country code
   - TOTAL_VACCINATIONS (NUMBER): total doses administered
   - PEOPLE_VACCINATED (NUMBER): people with at least one dose
   - PEOPLE_FULLY_VACCINATED (NUMBER): fully vaccinated
   - DAILY_VACCINATIONS (NUMBER): daily doses
   - DAILY_VACCINATIONS_RAW (NUMBER): raw daily doses
   - TOTAL_VACCINATIONS_PER_HUNDRED (FLOAT): doses per 100 people
   - PEOPLE_VACCINATED_PER_HUNDRED (FLOAT): vaccinated per 100 people
   - PEOPLE_FULLY_VACCINATED_PER_HUNDRED (FLOAT): fully vaccinated per 100 people
   - DAILY_VACCINATIONS_PER_MILLION (FLOAT): daily doses per million
   - VACCINES (VARCHAR): vaccine types used
   - SOURCE_NAME (VARCHAR): data source
   - LAST_UPDATE_DATE (TIMESTAMP): last update
   - LAST_REPORTED_FLAG (BOOLEAN): whether this is the last reported value
   IMPORTANT: use COUNTRY_REGION not LOCATION for country filtering

Always use LIMIT 100 unless the user asks for aggregated totals.
Always alias columns with clear names in SELECT.
For Italy questions use PCM_DPS_COVID19 with appropriate CASE_TYPE filter.
For global questions use JHU_COVID_19 with appropriate CASE_TYPE filter.
For vaccination questions use OWID_VACCINATIONS.

CRITICAL — CASE_TYPE values differ by table:
- JHU_COVID_19: 'Confirmed', 'Deaths', 'Recovered', 'Active'
- PCM_DPS_COVID19: 'Confirmed', 'Deceased', 'Recovered', 'Active'
NEVER use 'Deaths' when querying PCM_DPS_COVID19, always use 'Deceased'.

CRITICAL — CUMULATIVE DATA:
CASES, TOTAL_VACCINATIONS, PEOPLE_VACCINATED, PEOPLE_FULLY_VACCINATED are cumulative.
NEVER use SUM() on these columns — it multiplies values incorrectly.
To get the total for a country or region, always use MAX(CASES) or filter by the latest DATE.
To get daily changes use the DIFFERENCE column instead.
CRITICAL — DATE FILTER:
NEVER add a DATE filter unless the user explicitly mentions a specific 
time period, year, or date range in their question.
If the user asks for totals or rankings without mentioning dates, 
query ALL available data with no date restrictions.
"""

# ── Text-to-SQL prompt ────────────────────────────────────────────────────────
def build_prompt(question: str, history: list) -> str:
    history_str = ""
    if history:
        last = history[-4:]  # last 2 turns
        for m in last:
            role = "User" if m["role"] == "user" else "Assistant"
            history_str += f"{role}: {m['content'][:300]}\n"

    return f"""You are a data analyst assistant with access to COVID-19 data in Snowflake.

{SCHEMA_CONTEXT}

Recent conversation:
{history_str}

User question: {question}

Instructions:
1. Generate a valid Snowflake SQL query to answer the question.
2. Execute it mentally and write a clear, conversational answer in plain English.
3. Include the key numbers in your explanation naturally (e.g. "Italy had 26 million confirmed cases...").
4. Format your response EXACTLY as:
SQL:
```sql
<your sql here>
```
EXPLANATION:
<write a friendly, conversational answer that includes the data findings. Do not just say "the query returns X" — actually interpret the results and give a meaningful answer.>

If the question is not about data, respond with:
SQL:
NONE
EXPLANATION:
<answer directly>
"""

# ── Call Cortex LLM ───────────────────────────────────────────────────────────
def call_cortex(conn, prompt: str, model: str = "mistral-large2") -> str:
    cur = conn.cursor()
    cur.execute(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
        (model, prompt)
    )
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

def remove_implicit_date_filter(sql: str, question: str) -> str:
    """Rimuove filtri DATE aggiunti dal LLM senza che l'utente li abbia richiesti."""
    if not sql:
        return sql

    date_keywords = [
        "2020", "2021", "2022", "2023", "2024",
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
        "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
        "last year", "this year", "last month", "yesterday",
        "gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno",
        "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre",
    ]
    question_lower = question.lower()
    user_mentioned_date = any(kw in question_lower for kw in date_keywords)

    if not user_mentioned_date:
        # Rimuove AND DATE BETWEEN '...' AND '...'
        sql = re.sub(
            r'\s+AND\s+DATE\s+BETWEEN\s+\'\d{4}-\d{2}-\d{2}\'\s+AND\s+\'\d{4}-\d{2}-\d{2}\'',
            '',
            sql,
            flags=re.IGNORECASE
        )
        # Rimuove AND DATE >= / <= / = '...'
        sql = re.sub(
            r'\s+AND\s+DATE\s*[><=!]+\s*\'\d{4}-\d{2}-\d{2}\'',
            '',
            sql,
            flags=re.IGNORECASE
        )
        # Rimuove WHERE DATE BETWEEN '...' AND '...' (quando è l'unica condizione)
        sql = re.sub(
            r'\s+WHERE\s+DATE\s+BETWEEN\s+\'\d{4}-\d{2}-\d{2}\'\s+AND\s+\'\d{4}-\d{2}-\d{2}\'',
            '',
            sql,
            flags=re.IGNORECASE
        )

    return sql

def summarize_dataframe(df: pd.DataFrame, max_rows: int = 20) -> str:
    """Prepara un riassunto del dataframe da passare al LLM."""
    if df.empty:
        return "The query returned no results."
    
    total_rows = len(df)
    
    if total_rows <= max_rows:
        # Passa tutto
        return f"Query returned {total_rows} rows:\n{df.to_string(index=False)}"
    
    # Ibrida: prime 5 righe + statistiche
    sample = df.head(5).to_string(index=False)
    
    stats_lines = [f"Query returned {total_rows} rows. Showing first 5 rows + summary statistics:"]
    stats_lines.append(f"\nFirst 5 rows:\n{sample}")
    stats_lines.append("\nSummary statistics:")
    
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            stats_lines.append(
                f"- {col}: min={df[col].min():,.0f}, max={df[col].max():,.0f}, "
                f"avg={df[col].mean():,.0f}, total={df[col].sum():,.0f}"
            )
        else:
            unique_vals = df[col].nunique()
            stats_lines.append(f"- {col}: {unique_vals} unique values")
    
    return "\n".join(stats_lines)

def build_conversation_prompt(question: str, sql: str, data_summary: str, history: list) -> str:
    history_str = ""
    if history:
        last = history[-4:]
        for m in last:
            role = "User" if m["role"] == "user" else "Assistant"
            history_str += f"{role}: {m['content'][:300]}\n"

    return f"""You are a friendly data analyst assistant specializing in COVID-19 data.
You have just executed a SQL query and received the results below.
Your job is to answer the user's question in a clear, conversational way using the actual data.

Recent conversation:
{history_str}

User question: {question}

SQL executed:
{sql}

Query results:
{data_summary}

Instructions:
- Answer in a friendly, conversational tone
- Include the actual numbers from the results naturally in your answer
- Highlight the most interesting findings
- If the data shows a trend, describe it
- Keep the answer concise but informative
- Do NOT say "the query returns" or "the results show" — just answer directly
- Answer in the same language the user used
"""



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

        st.markdown(f"""
        <div class="msg {css_class}">
            <div class="msg-role">{role_label}</div>
            {msg["content"]}
            {sql_html}
        </div>
        """, unsafe_allow_html=True)

        if msg.get("sql"):
            st.markdown("**Generated SQL**")
            st.code(msg["sql"], language="sql")

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

    with st.spinner("Querying Snowflake…"):
        # Step 1 — genera SQL
        prompt = build_prompt(question, st.session_state.messages[:-1])
        raw = call_cortex(active_conn, prompt, st.session_state.model)
        sql, _ = parse_response(raw)
        sql = remove_implicit_date_filter(sql, question)

        df = None
        explanation = ""
        error_msg = None

        if sql:
            try:
                df = run_sql(active_conn, sql)
                
                # Step 2 — risposta conversazionale con i dati reali
                data_summary = summarize_dataframe(df)
                conv_prompt = build_conversation_prompt(
                    question, sql, data_summary, st.session_state.messages[:-1]
                )
                explanation = call_cortex(active_conn, conv_prompt, st.session_state.model)
                
                if df.empty:
                    explanation += "\n\n*(No data found — try a different time range or country name.)*"
                    
            except Exception as e:
                error_msg = str(e)
                explanation = f"I encountered an error while querying the data: `{error_msg}`"
                sql = None
        else:
            # Nessun SQL necessario — risposta diretta
            exp_match = re.search(r'EXPLANATION:\s*(.*)', raw, re.DOTALL | re.IGNORECASE)
            explanation = exp_match.group(1).strip() if exp_match else raw.strip()

    st.session_state.messages.append({
        "role": "assistant",
        "content": explanation,
        "sql": sql,
        "dataframe": df,
    })

    st.rerun()
