import plotly.express as px
import streamlit as st
from queries import (
    INVENTORY_ACTIVE_COUNT,
    INVENTORY_BY_MAKE_MODEL,
    INVENTORY_NEW_7D,
    INVENTORY_NEW_24H,
    INVENTORY_NEW_30D,
    INVENTORY_NEW_OVER_TIME,
    INVENTORY_TOP_DEALERS,
    INVENTORY_UNLISTED_OVER_TIME,
)

from db import run_duckdb_query


def render():
    st.header("Inventory Overview")

    # -- Scalar cards --------------------------------------------------------
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        df = run_duckdb_query(INVENTORY_ACTIVE_COUNT)
        st.metric("Total Active Listings", f"{df['cnt'].iloc[0]:,}")
    with col2:
        df = run_duckdb_query(INVENTORY_NEW_24H)
        st.metric("New (24h)", f"{df['cnt'].iloc[0]:,}")
    with col3:
        df = run_duckdb_query(INVENTORY_NEW_7D)
        st.metric("New (7d)", f"{df['cnt'].iloc[0]:,}")
    with col4:
        df = run_duckdb_query(INVENTORY_NEW_30D)
        st.metric("New (30d)", f"{df['cnt'].iloc[0]:,}")

    # -- Active listings by make/model ---------------------------------------
    st.subheader("Active Listings by Make / Model")
    df = run_duckdb_query(INVENTORY_BY_MAKE_MODEL)
    if not df.empty:
        fig = px.bar(df, x="model", y="active_listings", color="make",
                     hover_data=["avg_price", "min_price"])
        fig.update_layout(xaxis_title=None, yaxis_title="Listings", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # -- New listings over time ----------------------------------------------
    st.subheader("New Listings Over Time (30 Days)")
    df = run_duckdb_query(INVENTORY_NEW_OVER_TIME)
    if not df.empty:
        fig = px.bar(df, x="day", y="new_listings", color="make", barmode="stack")
        fig.update_layout(xaxis_title=None, yaxis_title="New Listings", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)

    # -- Listings going unlisted ---------------------------------------------
    st.subheader("Listings Going Unlisted Over Time (30 Days)")
    df = run_duckdb_query(INVENTORY_UNLISTED_OVER_TIME)
    if not df.empty:
        fig = px.bar(df, x="day", y="vehicles_unlisted")
        fig.update_layout(xaxis_title=None, yaxis_title="Vehicles Unlisted", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)

    # -- Active by dealer ----------------------------------------------------
    st.subheader("Active Listings by Dealer")
    df = run_duckdb_query(INVENTORY_TOP_DEALERS)
    st.dataframe(df, use_container_width=True, hide_index=True)
