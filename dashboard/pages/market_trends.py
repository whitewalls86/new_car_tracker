import plotly.express as px
import streamlit as st
from queries import (
    MARKET_TRENDS_DAYS_ON_MARKET,
    MARKET_TRENDS_NATIONAL_SUPPLY,
    MARKET_TRENDS_PRICE_DISTRIBUTION,
)

from db import run_duckdb_query


def render():
    st.header("Market Trends")

    # -- Days on market by model ---------------------------------------------
    st.subheader("Days on Market by Model")
    df = run_duckdb_query(MARKET_TRENDS_DAYS_ON_MARKET)
    if not df.empty:
        fig = px.bar(df, x="model", y="median_days", color="make",
                     hover_data=["avg_days", "min_days", "max_days", "listings"])
        fig.update_layout(xaxis_title=None, yaxis_title="Median Days on Market", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # -- National supply by model --------------------------------------------
    st.subheader("National Supply by Model")
    df = run_duckdb_query(MARKET_TRENDS_NATIONAL_SUPPLY)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # -- Price distribution by model -----------------------------------------
    st.subheader("Price Distribution by Model")
    df = run_duckdb_query(MARKET_TRENDS_PRICE_DISTRIBUTION)
    if not df.empty:
        fig = px.bar(
            df, x="model", y="median", color="make",
            error_y=df["p75"] - df["median"],
            error_y_minus=df["median"] - df["p25"],
            hover_data=["p10", "p25", "p75", "p90", "listings"],
        )
        fig.update_layout(xaxis_title=None, yaxis_title="Median Price ($)", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)
