import pandas as pd
import streamlit as st
from pages import deals, inventory, market_trends
from queries import MART_FRESHNESS

from db import run_duckdb_query

st.set_page_config(page_title="Cartracker Dashboard", layout="wide")

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("Cartracker")
if st.sidebar.button("Refresh Data"):
    st.rerun()

# Quicklinks
st.sidebar.markdown("---")
st.sidebar.markdown("**Quick Links**")
st.sidebar.markdown("[Project Info](https://cartracker.info/info)")
st.sidebar.markdown("[Search Config Admin](https://cartracker.info/admin)")
st.sidebar.markdown("[Airflow](https://cartracker.info/airflow)")
st.sidebar.markdown("[pgAdmin](https://cartracker.info/pgadmin)")
st.sidebar.markdown("[MinIO](https://cartracker.info/minio)")

# Data freshness
_freshness_df = run_duckdb_query(MART_FRESHNESS)
_freshness_val = _freshness_df["ts"].iloc[0]
if _freshness_val is not None:
    if pd.notna(_freshness_val):
        ts = pd.Timestamp(_freshness_val).strftime("%b %d %H:%M")
        st.sidebar.caption(f"Data as of: {ts} UTC")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs([
    "Inventory Overview", "Deal Finder", "Market Trends",
])

with tab1:
    inventory.render()
with tab2:
    deals.render()
with tab3:
    market_trends.render()
