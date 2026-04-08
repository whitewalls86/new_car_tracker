import pandas as pd
import streamlit as st
from pages import deals, inventory, market_trends, pipeline_health

from db import run_query

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
st.sidebar.markdown("[n8n Workflows](http://localhost:5678)")
st.sidebar.markdown("[Search Config Admin](http://localhost:8060/admin)")
st.sidebar.markdown("[pgAdmin](http://localhost:5050)")
st.sidebar.markdown("[minio](http://localhost:9001)")

# Data freshness
_freshness_df = run_query("""
    SELECT MAX(price_observed_at) AT TIME ZONE 'America/Chicago' AS ts
    FROM analytics.mart_vehicle_snapshot
""")
_freshness_val = _freshness_df["ts"].iloc[0]
if pd.notna(_freshness_val):
    st.sidebar.caption(f"Data as of: {_freshness_val.strftime('%b %d %H:%M')}")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "Pipeline Health", "Inventory Overview", "Deal Finder", "Market Trends",
])

with tab1:
    pipeline_health.render()
with tab2:
    inventory.render()
with tab3:
    deals.render()
with tab4:
    market_trends.render()
