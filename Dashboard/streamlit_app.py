import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine

st.set_page_config(page_title="Air Quality Dashboard", layout="wide")

st.title("Air Quality Dashboard")

st.markdown(
    """
    **Data source:** DBT model output tables

    - `fct_daily_air_quality_summary`
    - `fct_air_quality_measurements`
    - `city_air_quality_rankings`
    - `forecast_accuracy_analysis`

    Configure `DB_URL` in environment variables (any SQLAlchemy database URL)."""
)

DB_URL = os.environ.get("DB_URL", "")

if not DB_URL:
    st.warning(
        "Set environment variable `DB_URL` first, for example: `postgresql://user:pass@host:5432/dbname` or BigQuery format via `gcsfs`/`pybigquery` driver.`"
    )

@st.cache_data(show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    if not DB_URL:
        raise ValueError("DB_URL is not configured")
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df

source_mode = st.radio("Select data source", ["Database", "Upload CSV"], horizontal=True)

df_summary = None

if source_mode == "Upload CSV":
    st.info("Upload CSVs exported from your DBT tables (or local sample files).")
    summary_file = st.file_uploader("fct_daily_air_quality_summary.csv", type=["csv"])
    measurements_file = st.file_uploader("fct_air_quality_measurements.csv", type=["csv"])
    rankings_file = st.file_uploader("city_air_quality_rankings.csv", type=["csv"])
    accuracy_file = st.file_uploader("forecast_accuracy_analysis.csv", type=["csv"])
    if summary_file is not None:
        df_summary = pd.read_csv(summary_file, parse_dates=["record_date"], infer_datetime_format=True)
    if measurements_file is not None:
        df_measurements = pd.read_csv(measurements_file, parse_dates=["record_date"], infer_datetime_format=True)
    else:
        df_measurements = None
    if rankings_file is not None:
        df_rankings = pd.read_csv(rankings_file)
    else:
        df_rankings = None
    if accuracy_file is not None:
        df_accuracy = pd.read_csv(accuracy_file, parse_dates=["record_date"], infer_datetime_format=True)
    else:
        df_accuracy = None

else:
    st.info("Running queries against DB. Please set DB_URL as environment variable.")
    try:
        df_summary = run_query("SELECT * FROM fct_daily_air_quality_summary ORDER BY record_date DESC LIMIT 10000")
        df_measurements = run_query("SELECT * FROM fct_air_quality_measurements ORDER BY record_date DESC LIMIT 10000")
        df_rankings = run_query("SELECT * FROM city_air_quality_rankings ORDER BY rank LIMIT 100")
        df_accuracy = run_query("SELECT * FROM forecast_accuracy_analysis ORDER BY record_date DESC LIMIT 10000")
    except Exception as e:
        st.error(f"Query failed: {e}")
        st.stop()

if df_summary is None:
    st.stop()

with st.expander("Data preview", expanded=False):
    st.write("Daily summary", df_summary.head(20))
    if df_measurements is not None:
        st.write("Measurements", df_measurements.head(20))
    if df_rankings is not None:
        st.write("Rankings", df_rankings.head(20))
    if df_accuracy is not None:
        st.write("Forecast accuracy", df_accuracy.head(20))

# KPIs
st.subheader("Key metrics")

latest = df_summary.sort_values("record_date").iloc[-1:]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest date", latest["record_date"].dt.date.iloc[0])
col2.metric("Avg PM2.5", f"{latest['avg_pm25'].iloc[0]:.2f}" if "avg_pm25" in latest else "NA")
col3.metric("Avg PM10", f"{latest['avg_pm10'].iloc[0]:.2f}" if "avg_pm10" in latest else "NA")
col4.metric("Avg AQI", f"{latest['avg_aqi'].iloc[0]:.2f}" if "avg_aqi" in latest else "NA")

st.subheader("Temporal trends")

city_filter = st.multiselect(
    "City (optional)",
    options=df_summary["city"].sort_values().unique() if "city" in df_summary else [],
    default=[],
)

df_trends = df_summary.copy()
if city_filter and "city" in df_trends:
    df_trends = df_trends[df_trends["city"].isin(city_filter)]

if "record_date" in df_trends:
    fig = px.line(
        df_trends,
        x="record_date",
        y="avg_pm25" if "avg_pm25" in df_trends else df_trends.columns[1],
        color="city" if "city" in df_trends else None,
        title="Daily PM2.5 Trend",
        markers=True,
    )
    st.plotly_chart(fig, use_container_width=True)

if df_rankings is not None and not df_rankings.empty:
    st.subheader("City ranking")
    top_n = st.slider("Top N cities", 5, 50, 10)
    if "city" in df_rankings and "aqi_value" in df_rankings:
        fig_rank = px.bar(
            df_rankings.sort_values("aqi_value", ascending=False).head(top_n),
            x="city",
            y="aqi_value",
            title="Top Polluted Cities (AQI)",
            labels={"aqi_value": "AQI", "city": "City"},
        )
        st.plotly_chart(fig_rank, use_container_width=True)

if df_accuracy is not None and not df_accuracy.empty:
    st.subheader("Forecast accuracy")
    if "record_date" in df_accuracy and "forecast_error" in df_accuracy:
        fig_acc = px.line(
            df_accuracy.sort_values("record_date"),
            x="record_date",
            y="forecast_error",
            title="Forecast Error Over Time",
        )
        st.plotly_chart(fig_acc, use_container_width=True)

st.markdown("---")
st.info("Use this app as a starting point; customize queries, plots, and join logic for your final UX.")
