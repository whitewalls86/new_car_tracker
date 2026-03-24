import streamlit as st
import plotly.express as px

from db import run_query


def render():
    st.header("Market Trends")

    # -- Median price by model over time -------------------------------------
    st.subheader("Median Price by Model Over Time (Weekly, SRP Source)")
    df = run_query("""
        SELECT
            date_trunc('week', s.fetched_at AT TIME ZONE 'America/Chicago') AS week,
            s.make, s.model,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.price) AS median_price,
            COUNT(DISTINCT s.vin) AS listing_count
        FROM srp_observations s
        INNER JOIN analytics.int_scrape_targets t
            ON t.make = s.make AND t.model = s.model
        WHERE s.fetched_at > now() - interval '90 days'
          AND s.price > 0 AND s.vin IS NOT NULL
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
    """)
    if not df.empty:
        df["label"] = df["make"] + " " + df["model"]
        fig = px.line(df, x="week", y="median_price", color="label",
                      hover_data=["listing_count"])
        fig.update_layout(xaxis_title=None, yaxis_title="Median Price ($)", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)

    # -- Inventory levels over time ------------------------------------------
    st.subheader("Inventory Levels by Model (Daily)")
    df = run_query("""
        SELECT
            date_trunc('day', s.fetched_at AT TIME ZONE 'America/Chicago') AS day,
            s.make, s.model,
            COUNT(DISTINCT s.vin) AS listings_seen
        FROM srp_observations s
        INNER JOIN analytics.int_scrape_targets t
            ON t.make = s.make AND t.model = s.model
        WHERE s.fetched_at > now() - interval '30 days' AND s.vin IS NOT NULL
        GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC
    """)
    if not df.empty:
        df["label"] = df["make"] + " " + df["model"]
        fig = px.line(df, x="day", y="listings_seen", color="label")
        fig.update_layout(xaxis_title=None, yaxis_title="Distinct VINs Seen", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)

    # -- Days on market by model ---------------------------------------------
    st.subheader("Days on Market by Model")
    df = run_query("""
        SELECT
            make, model,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_on_market)) AS median_days,
            ROUND(AVG(days_on_market)::numeric, 1) AS avg_days,
            MIN(days_on_market) AS min_days,
            MAX(days_on_market) AS max_days,
            COUNT(*) AS listings
        FROM analytics.mart_deal_scores
        WHERE COALESCE(listing_state, 'active') != 'unlisted'
        GROUP BY make, model ORDER BY median_days DESC
    """)
    if not df.empty:
        fig = px.bar(df, x="model", y="median_days", color="make",
                     hover_data=["avg_days", "min_days", "max_days", "listings"])
        fig.update_layout(xaxis_title=None, yaxis_title="Median Days on Market", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # -- National supply vs local --------------------------------------------
    st.subheader("National Supply vs Local Availability")
    df = run_query("""
        SELECT
            make, model,
            COUNT(*) AS national_listings,
            COUNT(*) FILTER (WHERE is_local) AS local_listings,
            ROUND(AVG(national_listing_count)) AS avg_national_supply,
            ROUND(AVG(current_price)) AS avg_price,
            ROUND(AVG(msrp_discount_pct)::numeric, 1) AS avg_msrp_off_pct
        FROM analytics.mart_deal_scores
        WHERE COALESCE(listing_state, 'active') != 'unlisted'
        GROUP BY make, model ORDER BY national_listings DESC
    """)
    st.dataframe(df, use_container_width=True, hide_index=True)
