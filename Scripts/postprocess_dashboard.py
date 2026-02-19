import streamlit as st
import os
import duckdb
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from pathlib import Path
import urllib.request

# -----------------------------
# App setup
# -----------------------------
st.set_page_config(page_title="Co-optimized Gas, & Hydrogen Dashboard", layout="wide")
load_dotenv(".env")

output_path = os.getenv("output_path", ".")
model_name = os.getenv("model_name", "default")
db_path = os.path.join(output_path, "solution_views.ddb")
MAX_ROWS = 3000

DB_URL = os.getenv("DB_URL", "").strip()

# UI defaults + "nuclear" tabs visibility fix (theme-proof)
st.markdown(
    """
    <style>
      .block-container {padding-top: 1.2rem; padding-bottom: 2.0rem;}
      h1, h2, h3 {letter-spacing: 0.2px;}
      div[data-testid="stMetricValue"] {font-size: 2.0rem;}
      div[data-testid="stMetricLabel"] {font-size: 1.0rem;}

      /* ===== Tabs: force labels visible across Streamlit versions/themes ===== */
      div[data-testid="stTabs"] * { filter:none !important; opacity:1 !important; }

      div[data-testid="stTabs"] [role="tablist"] {
        border-bottom: 1px solid rgba(255,255,255,0.20) !important;
        margin-bottom: 0.75rem !important;
        gap: 0.35rem !important;
      }

      div[data-testid="stTabs"] button[role="tab"] {
        font-size: 16px !important;
        font-weight: 800 !important;
        padding: 10px 14px !important;
        border-radius: 10px 10px 0 0 !important;
        background: rgba(255,255,255,0.06) !important;
        border: 1px solid rgba(255,255,255,0.18) !important;
      }

      div[data-testid="stTabs"] button[role="tab"] p,
      div[data-testid="stTabs"] button[role="tab"] span,
      div[data-testid="stTabs"] button[role="tab"] div {
        color: rgba(255,255,255,0.92) !important;
        -webkit-text-fill-color: rgba(255,255,255,0.92) !important;
        visibility: visible !important;
        display: inline !important;
      }

      div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
        background: rgba(255,255,255,0.12) !important;
        border-bottom: 3px solid #ff4b4b !important;
      }

      div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] p,
      div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] span,
      div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] div {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Load DuckDB
# -----------------------------
if not os.path.exists(db_path):
    if DB_URL:
        try:
            Path(output_path).mkdir(parents=True, exist_ok=True)
            with st.status("Loading model database...", expanded=False):
                urllib.request.urlretrieve(DB_URL, db_path)
        except Exception as e:
            st.error(f"Failed to download DuckDB file from DB_URL. Error: {e}")
            st.stop()
    else:
        from prepare_duckdb import prepare_duckdb
        prepare_duckdb(model_name)

con = duckdb.connect(db_path, read_only=True)

tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
if "fullkeyinfo" not in tables or "data" not in tables or "Period" not in tables:
    st.error("Required tables are missing (expected: fullkeyinfo, data, Period). Rebuild the DuckDB file.")
    with st.expander("Detected tables"):
        st.write(sorted(tables))
    st.stop()

row_count = con.execute("SELECT COUNT(*) FROM fullkeyinfo").fetchone()[0]
if row_count == 0:
    st.warning("The model index (fullkeyinfo) is empty. Charts will be blank.")

# -----------------------------
# Global filters
# -----------------------------
phases_df = con.execute("SELECT DISTINCT PhaseName FROM fullkeyinfo WHERE PhaseName IS NOT NULL").fetchdf()
phases = sorted(phases_df["PhaseName"].dropna().unique().tolist())

period_types_df = con.execute("SELECT DISTINCT PeriodTypeName FROM fullkeyinfo WHERE PeriodTypeName IS NOT NULL").fetchdf()
period_types = sorted(period_types_df["PeriodTypeName"].dropna().unique().tolist())

st.sidebar.header("Global Filters")
phase = st.sidebar.selectbox("Phase", phases, key="phase_filter")
period_type = st.sidebar.selectbox("Period Type", period_types, key="period_type_filter")
max_rows = st.sidebar.slider("Max Rows", 1000, 10000, MAX_ROWS)

st.sidebar.header("Chart Settings")
top_n = st.sidebar.slider("Show top contributors", 3, 25, 10, help="Groups the rest as 'Other' to keep charts readable.")
show_table = st.sidebar.checkbox("Show data tables under charts", value=False)
chart_height = st.sidebar.slider("Chart height", 420, 900, 650)

# -----------------------------
# Data loading (parameterised)
# -----------------------------
@st.cache_data(show_spinner=False)
def load_data(child_class, keywords, phase, period_type, max_rows):
    if not keywords:
        return pd.DataFrame()

    kw_patterns = [f"%{str(k).lower()}%" for k in keywords if str(k).strip()]
    if not kw_patterns:
        return pd.DataFrame()

    keyword_clause = " OR ".join(["LOWER(fki.PropertyName) LIKE ?"] * len(kw_patterns))

    query = f"""
        SELECT
            Period.StartDate AS Timestamp,
            fki.ChildObjectName AS Object,
            fki.PropertyName AS Property,
            data.Value
        FROM fullkeyinfo fki
        JOIN data ON fki.SeriesId = data.SeriesId
        JOIN Period ON data.PeriodId = Period.PeriodId
        WHERE fki.PhaseName ILIKE ?
          AND fki.PeriodTypeName ILIKE ?
          AND fki.ChildClassName = ?
          AND ({keyword_clause})
        LIMIT ?
    """

    params = [phase, period_type, child_class] + kw_patterns + [int(max_rows)]
    df = con.execute(query, params).fetchdf()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df = df.dropna(subset=["Timestamp", "Value", "Object"])

    df["Unit"] = "TJ"
    if child_class == "Region":
        k = " ".join([str(x).lower() for x in keywords])
        if any(term in k for term in ["price", "srmc", "cost"]):
            df["Unit"] = "$ / MWh"
    return df

# -----------------------------
# Insight helpers
# -----------------------------
def _fmt(x):
    try:
        return f"{x:,.2f}"
    except Exception:
        return str(x)

def _trend_label(series: pd.Series):
    s = series.dropna()
    if len(s) < 4:
        return "Not enough history to assess"
    tail = s.tail(6)
    delta = tail.iloc[-1] - tail.iloc[0]
    pct = (delta / (abs(tail.iloc[0]) + 1e-9)) * 100
    if abs(pct) < 1:
        return "Stable"
    return "Rising" if pct > 0 else "Falling"

def build_insights(df: pd.DataFrame):
    if df.empty:
        return {}
    d = df.dropna(subset=["Timestamp", "Value"]).sort_values("Timestamp")
    if d.empty:
        return {}

    latest_ts = d["Timestamp"].max()
    latest_val = d.loc[d["Timestamp"] == latest_ts, "Value"].sum()
    peak_idx = d["Value"].idxmax()
    peak_val = d.loc[peak_idx, "Value"]
    peak_ts = d.loc[peak_idx, "Timestamp"]
    avg = d["Value"].mean()

    by_time = d.groupby("Timestamp")["Value"].sum().sort_index()
    trend = _trend_label(by_time)

    by_obj = d.groupby("Object")["Value"].sum().sort_values(ascending=False)
    top_obj = by_obj.index[0] if len(by_obj) else None
    top_share = (by_obj.iloc[0] / (by_obj.sum() + 1e-9)) * 100 if len(by_obj) else None

    return {
        "latest_ts": latest_ts,
        "latest_val": latest_val,
        "peak_ts": peak_ts,
        "peak_val": peak_val,
        "avg": avg,
        "trend": trend,
        "top_obj": top_obj,
        "top_share": top_share,
    }

def render_summary_panel(df: pd.DataFrame, unit: str):
    ins = build_insights(df)
    if not ins:
        return
    lines = [
        f"Latest value ({ins['latest_ts'].date()}): {_fmt(ins['latest_val'])} {unit}",
        f"Peak value ({ins['peak_ts'].date()}): {_fmt(ins['peak_val'])} {unit}",
        f"Recent direction: {ins['trend']}",
        f"Average over the period: {_fmt(ins['avg'])} {unit}",
    ]
    if ins.get("top_obj"):
        lines.append(f"Main driver: {ins['top_obj']} (~{ins['top_share']:.0f}% of total)")

    st.markdown("**Chart summary**")
    st.markdown("\n".join([f"- {x}" for x in lines]))

# -----------------------------
# Chart helpers (dynamic colors)
# -----------------------------
def top_n_other(d: pd.DataFrame, group_col: str, n: int):
    totals = d.groupby(group_col)["Value"].sum().sort_values(ascending=False)
    keep = set(totals.head(n).index.astype(str).tolist())
    out = d.copy()
    out[group_col] = out[group_col].astype(str)
    out.loc[~out[group_col].isin(keep), group_col] = "Other"
    return out

def _pick_color_sequence(key: str):
    palettes = [
        px.colors.qualitative.Bold,
        px.colors.qualitative.D3,
        px.colors.qualitative.G10,
        px.colors.qualitative.Set3,
        px.colors.qualitative.Dark24,
        px.colors.qualitative.Alphabet,
        px.colors.qualitative.Prism,
        px.colors.qualitative.Safe,
        px.colors.qualitative.Vivid,
    ]
    idx = abs(hash(key)) % len(palettes)
    return palettes[idx]

def _apply_other_color_map(df: pd.DataFrame, palette: list[str]):
    labels = [c for c in df["Object"].astype(str).unique().tolist() if c != "Other"]
    color_map = {}
    for i, lab in enumerate(labels):
        color_map[lab] = palette[i % len(palette)]
    if "Other" in df["Object"].astype(str).unique():
        color_map["Other"] = "#B0B0B0"
    return color_map

def render_chart(df: pd.DataFrame, y_label: str, tab_suffix: str = "", chart_type: str = "line", top_n_objects: int = 10):
    if df.empty:
        st.warning("No data found for this selection.")
        return

    unit = df["Unit"].dropna().unique()
    unit_label = unit[0] if len(unit) == 1 else "various"

    d = df.copy().dropna(subset=["Timestamp", "Value", "Object"]).sort_values("Timestamp")
    d = d.groupby(["Timestamp", "Object"], as_index=False)["Value"].sum()
    d = top_n_other(d, group_col="Object", n=top_n_objects)

    render_summary_panel(df, unit_label)

    title = f"{y_label}"
    y_title = f"{y_label} ({unit_label})"

    palette = _pick_color_sequence(f"{y_label}_{tab_suffix}")
    color_map = _apply_other_color_map(d, palette)

    if chart_type == "bar":
        fig = px.bar(
            d,
            x="Timestamp",
            y="Value",
            color="Object",
            title=title,
            labels={"Value": y_title},
            template="plotly_white",
            opacity=0.92,
            color_discrete_map=color_map,
        )
        fig.update_layout(barmode="stack")
    else:
        fig = px.area(
            d,
            x="Timestamp",
            y="Value",
            color="Object",
            title=title,
            labels={"Value": y_title},
            template="plotly_white",
            color_discrete_map=color_map,
        )

    fig.update_layout(
        height=chart_height,
        margin=dict(l=20, r=20, t=60, b=20),
        title_font=dict(size=22),
        legend_title_text="",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        font=dict(size=15),
    )

    chart_key = f"chart_{y_label}_{tab_suffix}".replace(" ", "_").lower()
    st.plotly_chart(fig, use_container_width=True, key=chart_key)

    if show_table:
        st.dataframe(d.head(300), use_container_width=True)
        csv = d.to_csv(index=False).encode("utf-8")
        unique_key = f"download_{y_label}_{tab_suffix}".replace(" ", "_").lower()
        st.download_button("Download CSV", data=csv, file_name=f"{unique_key}.csv", key=unique_key)

# -----------------------------
# Tabs
# -----------------------------
tabs = st.tabs([
    "Overview",
    "Gas Storage",
    "Gas Fields",
    "Gas Plants",
    "Gas Pipelines",
    "Gas Contracts",
    "Gas Nodes",
    "Power2X",
    "Electric Generators",
    "Gas Demand",
    "Region Metrics",
    "Comparison"
])

with tabs[0]:
    st.title("Co-optimized Gas, & Hydrogen Dashboard")

    st.info(
        "This dashboard summarises how the model meets demand by coordinating gas, power, and hydrogen.\n\n"
        "Use the filters on the left to switch Phase and Period Type, then review:\n"
        "- Demand and supply trends\n"
        "- Cost and price signals\n"
        "- Operational behaviour across assets"
    )

    df_prod = load_data("Gas Plant", ["production"], phase, period_type, max_rows)
    df_dem = load_data("Gas Demand", ["hydrogen demand", "h2 demand", "offtake", "demand"], phase, period_type, max_rows)
    if df_dem.empty:
        df_dem = load_data("Gas Node", ["demand"], phase, period_type, max_rows)

    df_price = load_data("Region", ["price"], phase, period_type, max_rows)
    df_cost = load_data("Region", ["total generation cost", "generation cost"], phase, period_type, max_rows)

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("Total Production", f"{int(df_prod['Value'].sum() if not df_prod.empty else 0):,}")
    with k2:
        st.metric("Total Demand", f"{int(df_dem['Value'].sum() if not df_dem.empty else 0):,}")
    with k3:
        st.metric("Average Region Price", f"{(df_price['Value'].mean() if not df_price.empty else 0):,.2f}")
    with k4:
        st.metric("Total Generation Cost", f"{(df_cost['Value'].sum() if not df_cost.empty else 0):,.0f}")

    st.subheader("Supply and Demand")
    c1, c2 = st.columns(2)
    with c1:
        render_chart(df_prod, "Production", tab_suffix="overview_prod", chart_type="line", top_n_objects=top_n)
    with c2:
        render_chart(df_dem, "Demand", tab_suffix="overview_dem", chart_type="line", top_n_objects=top_n)

    st.subheader("Economic Signals")
    c3, c4 = st.columns(2)
    with c3:
        render_chart(df_price, "Region Price", tab_suffix="overview_price", chart_type="line", top_n_objects=min(top_n, 8))
    with c4:
        render_chart(df_cost, "Generation Cost", tab_suffix="overview_cost", chart_type="bar", top_n_objects=min(top_n, 8))

sections = [
    (1, "Gas Storage", "Gas Storage", ["initial", "end", "withdrawal", "injection", "build cost"]),
    (2, "Gas Fields", "Gas Field", ["production"]),
    (3, "Gas Plants", "Gas Plant", ["production"]),
    (4, "Gas Pipelines", "Gas Pipeline", ["flow"]),
    (5, "Gas Contracts", "Gas Contract", ["volume", "flow"]),
    (6, "Gas Nodes", "Gas Node", ["balance", "demand"]),
    (7, "Power2X", "Power2X", ["production", "input", "output"]),
    (8, "Electric Generators", "Generator", ["production", "output", "mw"]),
    (9, "Gas Demand", "Gas Demand", ["hydrogen demand", "offtake", "demand"]),
    (10, "Region Metrics", "Region", ["price", "srmc", "generation cost"]),
]

for tab_index, tab_title, class_name, default_keywords in sections:
    with tabs[tab_index]:
        st.header(tab_title)
        st.caption("Select properties to display. Charts show the top contributors and group the rest as 'Other' for readability.")

        prop_query = """
            SELECT DISTINCT fki.PropertyName
            FROM fullkeyinfo fki
            WHERE fki.ChildClassName = ?
              AND fki.PhaseName ILIKE ?
              AND fki.PeriodTypeName ILIKE ?
        """
        prop_df = con.execute(prop_query, [class_name, phase, period_type]).fetchdf()
        all_properties = sorted(prop_df["PropertyName"].dropna().tolist())

        default_selection = [p for p in all_properties if any(k in p.lower() for k in default_keywords)]
        default_selection = default_selection or (all_properties[:1] if all_properties else [])

        selected_properties = st.multiselect(
            "Properties",
            options=all_properties,
            default=default_selection,
            key=f"prop_selector_{tab_title.lower().replace(' ', '_')}",
        )

        chart_type = st.radio("Visual style", ["Stacked (area)", "Stacked (bar)"], horizontal=True, key=f"chart_type_{tab_index}")
        chart_mode = "line" if chart_type == "Stacked (area)" else "bar"

        if not selected_properties:
            st.info("Select at least one property.")
        else:
            for prop in selected_properties:
                df = load_data(class_name, [prop], phase, period_type, max_rows)
                render_chart(df, prop, tab_suffix=f"{tab_title}_{prop}", chart_type=chart_mode, top_n_objects=top_n)

with tabs[-1]:
    st.header("Comparison")

    class1 = st.selectbox("Class A", ["Gas Plant", "Gas Node", "Region", "Generator", "Gas Demand", "Power2X"], key="cmp_class1")
    prop1 = st.text_input("Property keywords A", "production", key="cmp_prop1")

    class2 = st.selectbox("Class B", ["Gas Plant", "Gas Node", "Region", "Generator", "Gas Demand", "Power2X"], key="cmp_class2")
    prop2 = st.text_input("Property keywords B", "demand", key="cmp_prop2")

    chart_type = st.radio("Visual style", ["Stacked (area)", "Stacked (bar)"], horizontal=True, key="cmp_chart_type")
    chart_mode = "line" if chart_type == "Stacked (area)" else "bar"

    if st.button("Compare"):
        kw1 = [x.strip() for x in prop1.split(",") if x.strip()] or [prop1]
        kw2 = [x.strip() for x in prop2.split(",") if x.strip()] or [prop2]

        df1 = load_data(class1, kw1, phase, period_type, max_rows)
        df2 = load_data(class2, kw2, phase, period_type, max_rows)

        if df1.empty or df2.empty:
            st.warning("One or both selections returned no data.")
        else:
            df1 = df1.copy()
            df2 = df2.copy()
            df1["Object"] = f"{class1}: {', '.join(kw1)}"
            df2["Object"] = f"{class2}: {', '.join(kw2)}"
            df_all = pd.concat([df1[["Timestamp", "Object", "Value"]], df2[["Timestamp", "Object", "Value"]]])
            df_all["Unit"] = "unit"

            render_chart(df_all, "Comparison", tab_suffix="cmp", chart_type=chart_mode, top_n_objects=2)
