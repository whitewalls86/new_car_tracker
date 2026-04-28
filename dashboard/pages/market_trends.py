import plotly.express as px
import streamlit as st

from db import run_duckdb_query


def render():
    st.header("Market Trends")

    # -- Days on market by model ---------------------------------------------
    st.subheader("Days on Market by Model")
    df = run_duckdb_query("""
        SELECT
            make, model,
            ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY days_on_market)) AS median_days,
            ROUND(AVG(days_on_market)::DOUBLE, 1) AS avg_days,
            MIN(days_on_market) AS min_days,
            MAX(days_on_market) AS max_days,
            COUNT(*) AS listings
        FROM mart_deal_scores
        GROUP BY make, model ORDER BY median_days DESC
    """)
    if not df.empty:
        fig = px.bar(df, x="model", y="median_days", color="make",
                     hover_data=["avg_days", "min_days", "max_days", "listings"])
        fig.update_layout(xaxis_title=None, yaxis_title="Median Days on Market", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # -- National supply by model --------------------------------------------
    st.subheader("National Supply by Model")
    df = run_duckdb_query("""
        SELECT
            make, model,
            COUNT(*) AS tracked_listings,
            ROUND(AVG(national_listing_count)) AS avg_national_supply,
            ROUND(AVG(current_price)) AS avg_price,
            ROUND(AVG(msrp_discount_pct)::DOUBLE, 1) AS avg_msrp_off_pct
        FROM mart_deal_scores
        GROUP BY make, model ORDER BY tracked_listings DESC
    """)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # -- Price distribution by model -----------------------------------------
    st.subheader("Price Distribution by Model")
    df = run_duckdb_query("""
        SELECT
            make, model,
            ROUND(percentile_cont(0.10) WITHIN GROUP (ORDER BY current_price)) AS p10,
            ROUND(percentile_cont(0.25) WITHIN GROUP (ORDER BY current_price)) AS p25,
            ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY current_price)) AS median,
            ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY current_price)) AS p75,
            ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY current_price)) AS p90,
            COUNT(*) AS listings
        FROM mart_deal_scores
        GROUP BY make, model ORDER BY median DESC
    """)
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
