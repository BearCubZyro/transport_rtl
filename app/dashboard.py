import os
import time
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.getenv("TRANSPORT_DB_PATH", os.path.join(BASE_DIR, "data_pipeline.db"))
TABLE_NAME = os.getenv("TRANSPORT_TABLE", "transport_traffic")
REFRESH_SECS = int(os.getenv("DASH_REFRESH_SECS", "60"))

st.set_page_config(page_title="Transport & Traffic Dashboard", layout="wide")
st.title("Transport & Traffic: Real-time Analytics")

@st.cache_data(ttl=REFRESH_SECS)
def load_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    with sqlite3.connect(DB_PATH) as conn:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
        except Exception:
            df = pd.DataFrame()
    # Parse timestamps
    for col in ["event_hour", "timestamp_public", "timestamp_traffic"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

# Auto-refresh info
st.caption(f"Auto-refresh every {REFRESH_SECS} seconds (cached)")

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    df = load_data()
    all_routes = sorted(df["route_id"].dropna().unique().tolist()) if not df.empty else []
    selected_routes = st.multiselect("Route(s)", options=all_routes, default=all_routes[:5])
    city_col = "city_public" if "city_public" in df.columns else None
    if city_col:
        all_cities = sorted(df[city_col].dropna().unique().tolist())
        selected_cities = st.multiselect("City", options=all_cities, default=all_cities)
    else:
        selected_cities = []

if df.empty:
    st.warning("No data available yet. Run the ETL pipeline to populate the database.")
    st.stop()

# Filter data
if selected_routes:
    df = df[df["route_id"].isin(selected_routes)]
if selected_cities and "city_public" in df.columns:
    df = df[df["city_public"].isin(selected_cities)]

# KPIs
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Rows", len(df))
with col2:
    st.metric("Routes", df["route_id"].nunique())
with col3:
    st.metric("Avg Ridership", round(df.get("ridership", pd.Series(dtype=float)).mean() or 0, 2))
with col4:
    st.metric("Avg Congestion", round(df.get("congestion_index", pd.Series(dtype=float)).mean() or 0, 2))

# Time series
if "event_hour" in df.columns:
    ts = df.groupby("event_hour").agg(
        avg_ridership=("ridership", "mean"),
        avg_congestion=("congestion_index", "mean"),
    ).reset_index()
    fig_ts = px.line(ts, x="event_hour", y=["avg_ridership", "avg_congestion"], title="Hourly Trends")
    st.plotly_chart(fig_ts, use_container_width=True)

# Ridership by route
if "ridership" in df.columns:
    by_route = df.groupby("route_id")["ridership"].mean().reset_index().sort_values("ridership", ascending=False)
    fig_route = px.bar(by_route, x="route_id", y="ridership", title="Avg Ridership by Route")
    st.plotly_chart(fig_route, use_container_width=True)

# Congestion vs speed
if set(["congestion_index", "avg_speed"]).issubset(df.columns):
    fig_scatter = px.scatter(df, x="avg_speed", y="congestion_index", color="route_id", title="Speed vs Congestion")
    st.plotly_chart(fig_scatter, use_container_width=True)

st.dataframe(df.head(200))
