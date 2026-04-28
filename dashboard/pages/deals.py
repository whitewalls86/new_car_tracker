import plotly.express as px
import streamlit as st
from queries import (
    DEALS_DAYS_ON_MARKET,
    DEALS_MAKES,
    DEALS_PRICE_DROPS,
    DEALS_PRICE_VS_MSRP,
    DEALS_TABLE,
    DEALS_TIER_DISTRIBUTION,
)

from db import run_duckdb_query


def render():
    st.header("Deal Finder")

    # -- Filters -------------------------------------------------------------
    col1, col2 = st.columns(2)

    all_makes = run_duckdb_query(DEALS_MAKES)["make"].tolist()

    with col1:
        selected_makes = st.multiselect("Make", all_makes, default=[])
    with col2:
        selected_tiers = st.multiselect(
            "Deal Tier", ["excellent", "good", "fair", "weak"], default=[]
        )

    # Build parameterized filter fragment (always "AND ..." so SQL files use WHERE 1=1)
    filter_parts = []
    query_params: list = []
    if selected_makes:
        placeholders = ", ".join(["?"] * len(selected_makes))
        filter_parts.append(f"AND make IN ({placeholders})")
        query_params.extend(selected_makes)
    if selected_tiers:
        placeholders = ", ".join(["?"] * len(selected_tiers))
        filter_parts.append(f"AND deal_tier IN ({placeholders})")
        query_params.extend(selected_tiers)
    filter_clause = "\n".join(filter_parts)
    params = query_params or None

    # -- Main deals table ----------------------------------------------------
    st.subheader("All Active Deals")
    df = run_duckdb_query(DEALS_TABLE.format(filter_clause=filter_clause), params=params)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "canonical_detail_url": st.column_config.LinkColumn("Link", display_text="View"),
            "current_price": st.column_config.NumberColumn(format="$%d"),
            "national_median_price": st.column_config.NumberColumn(format="$%d"),
            "msrp": st.column_config.NumberColumn(format="$%d"),
        },
    )

    # -- Charts row ----------------------------------------------------------
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Deal Tier Distribution")
        df = run_duckdb_query(
            DEALS_TIER_DISTRIBUTION.format(filter_clause=filter_clause), params=params
        )
        if not df.empty:
            fig = px.bar(df, x="deal_tier", y="listings", color="deal_tier",
                         color_discrete_map={
                             "excellent": "#27ae60", "good": "#2ecc71",
                             "fair": "#f39c12", "weak": "#e74c3c",
                         })
            fig.update_layout(showlegend=False, xaxis_title=None, yaxis_title="Listings")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Days on Market Distribution")
        df = run_duckdb_query(
            DEALS_DAYS_ON_MARKET.format(filter_clause=filter_clause), params=params
        )
        if not df.empty:
            fig = px.bar(df, x="bucket", y="listings")
            fig.update_layout(xaxis_title=None, yaxis_title="Listings")
            st.plotly_chart(fig, use_container_width=True)

    # -- Price drops ---------------------------------------------------------
    st.subheader("Price Drop Events")
    df = run_duckdb_query(
        DEALS_PRICE_DROPS.format(filter_clause=filter_clause), params=params
    )
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "canonical_detail_url": st.column_config.LinkColumn("Link", display_text="View"),
            "current_price": st.column_config.NumberColumn(format="$%d"),
            "first_price": st.column_config.NumberColumn(format="$%d"),
            "price_change": st.column_config.NumberColumn(format="$%d"),
        },
    )

    # -- Price vs MSRP -------------------------------------------------------
    st.subheader("Price vs MSRP by Model")
    df = run_duckdb_query(
        DEALS_PRICE_VS_MSRP.format(filter_clause=filter_clause), params=params
    )
    if not df.empty:
        fig = px.bar(df, x="model", y=["avg_price", "avg_msrp"], barmode="group",
                     hover_data=["avg_msrp_off_pct", "listings"])
        fig.update_layout(xaxis_title=None, yaxis_title="Price ($)", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
