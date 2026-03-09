import streamlit as st
import os
import duckdb
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv

# --- Config and Setup ---
st.set_page_config(page_title="Gas & Power Dashboard", layout="wide")
load_dotenv(".env")

output_path = os.getenv("output_path", ".")
model_name = os.getenv("model_name", "default")
db_path = os.path.join(output_path, "solution_views.ddb")
MAX_ROWS = 3000

# --- Load DuckDB ---
if not os.path.exists(db_path):
    from prepare_duckdb import prepare_duckdb
    prepare_duckdb(model_name)
con = duckdb.connect(db_path, read_only=True)

# --- Verify required views exist ---
required_views = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
if "fullkeyinfo" not in required_views:
    st.error("The 'fullkeyinfo' view is missing. Please verify the simulation output or re-run prepare_duckdb.")
    st.stop()

row_count = con.execute("SELECT COUNT(*) FROM fullkeyinfo").fetchone()[0]
if row_count == 0:
    st.warning("The 'fullkeyinfo' view exists but contains no data.")

# --- Filters ---
phases_df = con.execute("SELECT DISTINCT PhaseName FROM fullkeyinfo WHERE PhaseName IS NOT NULL").fetchdf()
phases = sorted(phases_df["PhaseName"].dropna().unique().tolist())
period_types_df = con.execute("SELECT DISTINCT PeriodTypeName FROM fullkeyinfo WHERE PeriodTypeName IS NOT NULL").fetchdf()
period_types = sorted(period_types_df["PeriodTypeName"].dropna().unique().tolist())

st.sidebar.header("Global Filters")
phase = st.sidebar.selectbox("Phase", phases)
period_type = st.sidebar.selectbox("Period Type", period_types)
max_rows = st.sidebar.slider("Max Rows", 1000, 10000, MAX_ROWS)

@st.cache_data(show_spinner=False)
def load_data(child_class, keywords, phase, period_type, max_rows):
    if not keywords:
        return pd.DataFrame()

    keyword_clause = " OR ".join([f"LOWER(fki.PropertyName) LIKE '%{kw.lower()}%'" for kw in keywords])
    query = f'''
        SELECT Period.StartDate AS Timestamp,
               fki.ChildObjectName AS Object,
               fki.PropertyName AS Property,
               data.Value
        FROM fullkeyinfo fki
        JOIN data ON fki.SeriesId = data.SeriesId
        JOIN Period ON data.PeriodId = Period.PeriodId
        WHERE fki.PhaseName ILIKE '{phase}'
          AND fki.PeriodTypeName ILIKE '{period_type}'
          AND fki.ChildClassName = '{child_class}'
          AND ({keyword_clause})
        LIMIT {max_rows}
    '''
    df = con.execute(query).fetchdf()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

    # Assign default Unit if missing
    if "Unit" not in df.columns:
        df["Unit"] = ""

    cost_keywords = " ".join(keywords).lower()
    if any(k in cost_keywords for k in ["cost", "price", "srmc", "lcoe", "marginal"]):
        if df["Unit"].eq("TJ").any() or df["Unit"].eq("").any():
            if "price" in cost_keywords or "srmc" in cost_keywords:
                df["Unit"] = "$ / MWh"
            else:
                total = df["Value"].sum()
                df["Unit"] = "$ millions" if total > 1e6 else "$"
                df["Value"] = df["Value"] / 1e6 if total > 1e6 else df["Value"]
    else:
        df["Unit"] = "TJ"

    return df.dropna()

def render_chart(df, y_label, tab_suffix="", chart_type="bar"):
    if df.empty:
        st.warning("No data found.")
        return

    unit = df["Unit"].dropna().unique()
    unit_label = unit[0] if len(unit) == 1 else "various"
    st.markdown(f"**Insight:** Showing trends for `{y_label}` in `{unit_label}`")
    st.markdown("Each object is a colored line or bar. Time is on the horizontal axis. Watch for peaks, drops, or seasonality.")

    chart_title = f"{y_label} Over Time"
    y_title = f"{y_label} ({unit_label})"

    color_sequence = px.colors.qualitative.Plotly

    if chart_type == "bar":
        fig = px.bar(df, x="Timestamp", y="Value", color="Object", title=chart_title,
                     labels={"Value": y_title}, template="plotly_dark", color_discrete_sequence=color_sequence)
    else:
        fig = px.line(df, x="Timestamp", y="Value", color="Object", title=chart_title,
                      labels={"Value": y_title}, template="plotly_dark", color_discrete_sequence=color_sequence)
        fig.update_traces(connectgaps=False)

    fig.update_layout(barmode="group", margin=dict(l=20, r=20, t=40, b=20), height=420)
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("View Data Table"):
        st.dataframe(df.head(100))
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", csv, f"{y_label}_{tab_suffix}.csv")

# --- Tabs ---
tabs = st.tabs([
    "Overview", "Gas Storage", "Gas Fields", "Gas Plants", "Gas Pipelines", "Gas Contracts",
    "Gas Nodes", "Power2X", "Electric Generators", "Gas Demand", "Region Metrics", "Comparison"
])

# --- Overview Tab ---
with tabs[0]:
    st.title("Gas & Power Dashboard")
    col1, col2 = st.columns(2)

    with col1:
        df_prod = load_data("Gas Plant", ["production"], phase, period_type, max_rows)
        st.metric("Production (TJ) - Total", f"{int(df_prod['Value'].sum()):,}")
        st.metric("Production (TJ) - Peak", f"{int(df_prod['Value'].max()):,}")
    with col2:
        df_demand = load_data("Gas Demand", ["demand"], phase, period_type, max_rows)
        if df_demand.empty:
            df_demand = load_data("Gas Node", ["demand"], phase, period_type, max_rows)
        st.metric("Demand (TJ) - Total", f"{int(df_demand['Value'].sum()):,}")
        st.metric("Demand (TJ) - Peak", f"{int(df_demand['Value'].max()):,}")

    st.subheader("Production and Demand Over Time")
    c1, c2 = st.columns(2)
    with c1:
        render_chart(df_prod, "Production", "overview_prod")
    with c2:
        render_chart(df_demand, "Gas Demand", "overview_demand")

    st.subheader("Regional Pricing and Costs")
    col3, col4 = st.columns(2)
    with col3:
        df_price = load_data("Region", ["price"], phase, period_type, max_rows)
        st.metric("Avg Region Price ($/MWh)", f"{df_price['Value'].mean():,.2f}")
    with col4:
        df_srmc = load_data("Region", ["srmc"], phase, period_type, max_rows)
        st.metric("Avg SRMC ($/MWh)", f"{df_srmc['Value'].mean():,.2f}")

# --- Other Sections ---
sections = [
    (1, "Gas Storage", "Gas Storage", ["initial", "end", "withdrawal", "injection"]),
    (2, "Gas Fields", "Gas Field", ["production"]),
    (3, "Gas Plants", "Gas Plant", ["production"]),
    (4, "Pipelines", "Gas Pipeline", ["flow"]),
    (5, "Gas Contracts", "Gas Contract", ["volume", "flow"]),
    (6, "Gas Nodes", "Gas Node", ["balance", "demand"]),
    (7, "Power2X", "Power2X", ["production", "input", "output"]),
    (8, "Electric Generators", "Generator", ["production", "output"]),
    (9, "Gas Demand", "Gas Demand", ["demand"]),
    (10, "Region Metrics", "Region", ["price", "srmc", "generation cost"])
]

for idx, tab_title, class_name, default_keywords in sections:
    with tabs[idx]:
        st.header(tab_title)
        prop_df = con.execute(f"""
            SELECT DISTINCT fki.PropertyName FROM fullkeyinfo fki
            WHERE fki.ChildClassName = '{class_name}'
              AND fki.PhaseName ILIKE '{phase}'
              AND fki.PeriodTypeName ILIKE '{period_type}'
        """).fetchdf()
        all_props = sorted(prop_df["PropertyName"].dropna().tolist())
        default_selection = [p for p in all_props if any(k in p.lower() for k in default_keywords)]
        selected_properties = st.multiselect("Choose Properties", options=all_props, default=default_selection or all_props[:1])
        chart_type = st.radio("Chart Type", ["Bar", "Line"], horizontal=True, key=f"chart_{idx}")
        for prop in selected_properties:
            df = load_data(class_name, [prop], phase, period_type, max_rows)
            render_chart(df, prop, tab_title.replace(" ", "_"), "bar" if chart_type == "Bar" else "line")

# --- Comparison Tab ---
with tabs[-1]:
    st.header("Compare Two Properties")
    class1 = st.selectbox("Class A", ["Gas Plant", "Gas Node", "Region", "Generator"])
    prop1 = st.text_input("Property A", "production")
    class2 = st.selectbox("Class B", ["Gas Plant", "Gas Node", "Region", "Generator"])
    prop2 = st.text_input("Property B", "demand")
    chart_mode = st.radio("Chart Type", ["Line", "Bar"], horizontal=True)

    if st.button("Compare"):
        df1 = load_data(class1, [prop1], phase, period_type, max_rows)
        df2 = load_data(class2, [prop2], phase, period_type, max_rows)
        if df1.empty or df2.empty:
            st.warning("One or both datasets are empty.")
        else:
            df1["Series"] = prop1
            df2["Series"] = prop2
            df_all = pd.concat([df1[["Timestamp", "Value", "Series"]], df2[["Timestamp", "Value", "Series"]]])
            fig = px.line(df_all, x="Timestamp", y="Value", color="Series", template="plotly_dark") if chart_mode == "Line" \
                else px.bar(df_all, x="Timestamp", y="Value", color="Series", template="plotly_dark")
            fig.update_traces(connectgaps=False)
            st.plotly_chart(fig, use_container_width=True)
