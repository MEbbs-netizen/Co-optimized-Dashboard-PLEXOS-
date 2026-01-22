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

# --- Check if fullkeyinfo is empty ---
row_count = con.execute("SELECT COUNT(*) FROM fullkeyinfo").fetchone()[0]
if row_count == 0:
    st.warning("The 'fullkeyinfo' view exists but contains no data. Dashboard may be empty or incomplete.")

# --- Dynamic Filters ---
phases_df = con.execute("SELECT DISTINCT PhaseName FROM fullkeyinfo WHERE PhaseName IS NOT NULL").fetchdf()
phases = sorted(phases_df["PhaseName"].dropna().unique().tolist())
period_types_df = con.execute("SELECT DISTINCT PeriodTypeName FROM fullkeyinfo WHERE PeriodTypeName IS NOT NULL").fetchdf()
period_types = sorted(period_types_df["PeriodTypeName"].dropna().unique().tolist())

st.sidebar.header("Global Filters")
phase = st.sidebar.selectbox("Phase", phases, key="phase_filter")
period_type = st.sidebar.selectbox("Period Type", period_types, key="period_type_filter")
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

    # Enforce correct units for relevant keywords like cost, production cost, price, etc.
    if child_class == "Region":
        cost_keywords = str(keywords).lower()
        df["Unit"] = df.get("Unit", pd.Series([""] * len(df)))

        if df["Unit"].isnull().all() or df["Unit"].eq("TJ").all():
            if "cost" in cost_keywords or "price" in cost_keywords or "srmc" in cost_keywords:
                if any(k in cost_keywords for k in ["price", "srmc", "marginal cost", "cost per", "levelized cost", "lcoe"]):
                    df["Unit"] = "$ / MWh"
                else:
                    df["Unit"] = "$"
                    total = df["Value"].sum()
                    if total > 1e6:
                        df["Unit"] = "$ millions"
                        df["Value"] = df["Value"] / 1e6
            else:
                df["Unit"] = "TJ"
        else:
            # Convert from TJ to $/MWh or $ millions if needed
            if "cost" in cost_keywords or "price" in cost_keywords or "srmc" in cost_keywords:
                if df["Unit"].eq("TJ").any():
                    df["Unit"] = "$ / MWh"
                    df["Value"] = df["Value"] * 1e-6  # Assuming cost/price is in $/MWh
                else:
                    df["Unit"] = "$"
                    total = df["Value"].sum()
                    if total > 1e6:
                        df["Unit"] = "$ millions"
                        df["Value"] = df["Value"] / 1e6
            else:
                df["Unit"] = "TJ"
    else:
        df["Unit"] = "TJ"

    return df.dropna()

def render_chart(df, y_label, tab_suffix="", chart_type="bar"):
    if df.empty:
        st.warning("No data found.")
        return

    units = df["Unit"].dropna().unique()
    unit_label = units[0] if len(units) == 1 else "various"
    st.markdown(f"**Insight:** Showing latest trends for {y_label} in {unit_label}")

    chart_title = f"{y_label} Over Time"
    y_title = f"{y_label} ({unit_label})"

    color_palettes = [
        px.colors.qualitative.Plotly,
        px.colors.qualitative.D3,
        px.colors.qualitative.Pastel,
        px.colors.qualitative.Prism,
        px.colors.qualitative.Set1,
        px.colors.qualitative.Set3,
        px.colors.qualitative.Bold,
        px.colors.qualitative.Safe
    ]
    chart_index = abs(hash(f"{y_label}_{tab_suffix}")) % len(color_palettes)
    color_sequence = color_palettes[chart_index]

    if chart_type == "bar":
        fig = px.bar(
            df,
            x="Timestamp",
            y="Value",
            color="Object",
            title=chart_title,
            labels={"Value": y_title},
            template="plotly_dark",
            color_discrete_sequence=color_sequence,
            opacity=0.85
        )
    else:
        fig = px.line(
            df,
            x="Timestamp",
            y="Value",
            color="Object",
            title=chart_title,
            labels={"Value": y_title},
            template="plotly_dark",
            color_discrete_sequence=color_sequence
        )

    fig.update_layout(barmode="group", margin=dict(l=20, r=20, t=40, b=20), height=420)
    chart_key = f"chart_{y_label}_{tab_suffix}".replace(" ", "_").lower()
    st.plotly_chart(fig, use_container_width=True, key=chart_key)

    with st.expander("Show table"):
        st.dataframe(df.head(100))
        csv = df.to_csv(index=False).encode('utf-8')
        unique_key = f"download_{y_label}_{tab_suffix}".replace(" ", "_").lower()
        st.download_button("Download CSV", data=csv, file_name=f"{unique_key}.csv", key=unique_key)

# --- TABS --- 
tabs = st.tabs([
    "Overview", "Gas Storage", "Gas Fields", "Gas Plants", "Gas Pipelines", "Gas Contracts",
    "Gas Nodes", "Power2X", "Electric Generators", "Gas Demand", "Region Metrics", "Comparison"
])

# Overview tab
with tabs[0]:
    st.title("Gas & Power Dashboard")
    col1, col2 = st.columns(2)
    with col1:
        df_prod = load_data("Gas Plant", ["production"], phase, period_type, max_rows)
        total_prod = df_prod["Value"].sum() if not df_prod.empty else 0
        peak_prod = df_prod["Value"].max() if not df_prod.empty else 0
        st.metric("Production (TJ) - Total", f"{int(total_prod):,}")
        st.metric("Production (TJ) - Peak", f"{int(peak_prod):,}")
    with col2:
        df_demand = load_data("Gas Demand", ["hydrogen demand", "h2 demand", "offtake", "demand"], phase, period_type, max_rows)
        if df_demand.empty:
            df_demand = load_data("Gas Node", ["demand"], phase, period_type, max_rows)
        total_demand = df_demand["Value"].sum() if not df_demand.empty else 0
        peak_demand = df_demand["Value"].max() if not df_demand.empty else 0
        st.metric("Demand (TJ) - Total", f"{int(total_demand):,}")
        st.metric("Demand (TJ) - Peak", f"{int(peak_demand):,}")

    st.subheader("Production and Demand Trends")
    c1, c2 = st.columns(2)
    with c1:
        render_chart(df_prod, "Production", tab_suffix="overview_prod")
    with c2:
        render_chart(df_demand, "Gas Demand", tab_suffix="overview_demand")

    st.subheader("Cost and Price Metrics")
    col3, col4 = st.columns(2)
    with col3:
        df_price = load_data("Region", ["price"], phase, period_type, max_rows)
        avg_price = df_price["Value"].mean() if not df_price.empty else 0
        st.metric("Avg Region Price ($/MWh)", f"{avg_price:,.2f}")
    with col4:
        df_srmc = load_data("Region", ["srmc"], phase, period_type, max_rows)
        if df_srmc.empty:
            df_srmc = load_data("Generator", ["srmc"], phase, period_type, max_rows)
        avg_srmc = df_srmc["Value"].mean() if not df_srmc.empty else 0

        df_cost = load_data("Region", ["total generation cost", "generation cost"], phase, period_type, max_rows)
        if df_cost.empty:
            df_cost = load_data("Generator", ["total generation cost", "generation cost"], phase, period_type, max_rows)
        total_cost = df_cost["Value"].sum() if not df_cost.empty else 0

        st.metric("Avg SRMC ($/MWh)", f"{avg_srmc:,.2f}")
        st.metric("Total Gen Cost ($)", f"{total_cost:,.0f}")

    st.subheader("Price and Cost Trends")
    col5, col6, col7 = st.columns(3)
    with col5:
        render_chart(df_price, "Region Price", tab_suffix="overview_price")
    with col6:
        render_chart(df_srmc, "SRMC", tab_suffix="overview_srmc")
    with col7:
        render_chart(df_cost, "Generation Cost", tab_suffix="overview_cost")

# Generic tabs
sections = [
    (1, "Storage", "Gas Storage", ["initial", "end", "withdrawal", "injection", "build cost"]),
    (2, "Gas Fields", "Gas Field", ["production"]),
    (3, "Gas Plants", "Gas Plant", ["production"]),
    (4, "Pipelines", "Gas Pipeline", ["flow"]),
    (5, "Gas Contracts", "Gas Contract", ["volume", "flow"]),
    (6, "Gas Nodes", "Gas Node", ["balance", "demand"]),
    (7, "Power2X", "Power2X", ["production", "input", "output"]),
    (8, "Generators", "Generator", ["production", "output", "mw"]),
    (9, "Gas Demand", "Gas Demand", ["hydrogen demand", "offtake", "demand"]),
    (10, "Region Metrics", "Region", ["price", "srmc", "generation cost"])
]

for tab_index, tab_title, class_name, default_keywords in sections:
    with tabs[tab_index]:
        st.header(tab_title)
        prop_query = f"""
            SELECT DISTINCT fki.PropertyName
            FROM fullkeyinfo fki
            WHERE fki.ChildClassName = '{class_name}'
              AND fki.PhaseName ILIKE '{phase}'
              AND fki.PeriodTypeName ILIKE '{period_type}'
        """
        prop_df = con.execute(prop_query).fetchdf()
        all_properties = sorted(prop_df["PropertyName"].dropna().tolist())
        default_selection = [p for p in all_properties if any(k in p.lower() for k in default_keywords)]
        default_selection = default_selection or all_properties[:1]
        selected_properties = st.multiselect(
            "Select Properties to Visualize",
            options=all_properties,
            default=default_selection,
            key=f"prop_selector_{tab_title.lower().replace(' ', '_')}"
        )
        chart_type = st.radio("Chart Type", ["Bar", "Line"], horizontal=True, key=f"chart_type_{tab_index}")
        chart_mode = "bar" if chart_type == "Bar" else "line"
        if not selected_properties:
            st.info("Select at least one property to show charts.")
        else:
            for prop in selected_properties:
                df = load_data(class_name, [prop], phase, period_type, max_rows)
                render_chart(df, prop, tab_suffix=tab_title.lower().replace(" ", "_"), chart_type=chart_mode)

# --- Comparison Tab ---
with tabs[-1]:
    st.header("Comparison")
    class1 = st.selectbox("Class A", ["Gas Plant", "Gas Node", "Region", "Generator"], key="cmp_class1")
    prop1 = st.text_input("Property A", "production", key="cmp_prop1")
    class2 = st.selectbox("Class B", ["Gas Plant", "Gas Node", "Region", "Generator"], key="cmp_class2")
    prop2 = st.text_input("Property B", "demand", key="cmp_prop2")
    chart_type = st.radio("Chart Type", ["Line", "Bar"], horizontal=True, key="cmp_chart_type")
    chart_mode = "line" if chart_type == "Line" else "bar"

    if st.button("Compare"):
        df1 = load_data(class1, [prop1], phase, period_type, max_rows)
        df2 = load_data(class2, [prop2], phase, period_type, max_rows)
        if df1.empty or df2.empty:
            st.warning("One or both datasets returned no data.")
        else:
            df1["Series"] = prop1
            df2["Series"] = prop2
            df_all = pd.concat([df1[["Timestamp", "Value", "Series"]], df2[["Timestamp", "Value", "Series"]]])
            if chart_mode == "line":
                fig = px.line(df_all, x="Timestamp", y="Value", color="Series", template="plotly_dark", title="Comparison Over Time")
            else:
                fig = px.bar(df_all, x="Timestamp", y="Value", color="Series", template="plotly_dark", title="Comparison Over Time")
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Show comparison data"):
                st.dataframe(df_all.head(100))
                csv = df_all.to_csv(index=False).encode("utf-8")
                st.download_button("Download CSV", data=csv, file_name="comparison_data.csv", key="cmp_download")