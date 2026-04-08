import plotly.express as px
import streamlit as st

from db import run_query


def render():
    st.header("Market Trends")

    # -- Median price by model over time -------------------------------------
    st.subheader("Median Price by Model Over Time (Weekly)")
    df = run_query("""
        SELECT
            date_trunc('week', ph.observed_at AT TIME ZONE 'America/Chicago') AS week,
            va.make, va.model,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mrt.price) AS median_price,
            COUNT(DISTINCT ph.vin) AS listing_count
        FROM analytics.mart_vehicle_snapshot mrt
        LEFT JOIN analytics.int_price_events ph ON mrt.vin = ph.vin
		LEFT JOIN analytics.int_vehicle_attributes va ON  mrt.vin = va.vin
        WHERE ph.observed_at > now() - interval '90 days'
          AND ph.price > 0 AND mrt.vin IS NOT NULL
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
            date_trunc('day', ph.observed_at AT TIME ZONE 'America/Chicago') AS day,
            va.make, va.model,
            COUNT(DISTINCT ph.vin) AS listings_seen
        FROM analytics.mart_vehicle_snapshot mrt
        LEFT JOIN analytics.int_price_events ph ON mrt.vin = ph.vin
		LEFT JOIN analytics.int_vehicle_attributes va ON  mrt.vin = va.vin
        WHERE ph.observed_at > now() - interval '30 days' AND mrt.vin IS NOT NULL
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
