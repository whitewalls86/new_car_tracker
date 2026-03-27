import streamlit as st
import plotly.express as px

from db import run_query


def render():
    st.header("Inventory Overview")

    # -- Scalar cards --------------------------------------------------------
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        df = run_query("""
            SELECT COUNT(*) AS cnt
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
        """)
        st.metric("Total Active Listings", f"{df['cnt'].iloc[0]:,}")

    with col2:
        df = run_query("""
            SELECT COUNT(*) AS cnt
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND first_seen_at > now() - interval '24 hours'
        """)
        st.metric("New (24h)", f"{df['cnt'].iloc[0]:,}")

    with col3:
        df = run_query("""
            SELECT COUNT(*) AS cnt
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND first_seen_at > now() - interval '7 days'
        """)
        st.metric("New (7d)", f"{df['cnt'].iloc[0]:,}")

    with col4:
        df = run_query("""
            SELECT COUNT(*) AS cnt
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND first_seen_at > now() - interval '30 days'
        """)
        st.metric("New (30d)", f"{df['cnt'].iloc[0]:,}")

    # -- Active listings by make/model ---------------------------------------
    st.subheader("Active Listings by Make / Model")
    df = run_query("""
        SELECT
            make, model,
            COUNT(*) AS active_listings,
            ROUND(AVG(current_price)) AS avg_price,
            MIN(current_price) AS min_price
        FROM analytics.mart_deal_scores
        WHERE COALESCE(listing_state, 'active') != 'unlisted'
        GROUP BY make, model
        ORDER BY active_listings DESC
    """)
    if not df.empty:
        fig = px.bar(df, x="model", y="active_listings", color="make",
                     hover_data=["avg_price", "min_price"])
        fig.update_layout(xaxis_title=None, yaxis_title="Listings", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # -- New listings over time ----------------------------------------------
    st.subheader("New Listings Over Time (30 Days)")
    df = run_query("""
        SELECT
            date_trunc('day', first_seen_at AT TIME ZONE 'America/Chicago') AS day,
            make, COUNT(*) AS new_listings
        FROM analytics.mart_deal_scores
        WHERE COALESCE(listing_state, 'active') != 'unlisted'
          AND first_seen_at > now() - interval '30 days'
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    if not df.empty:
        fig = px.bar(df, x="day", y="new_listings", color="make", barmode="stack")
        fig.update_layout(xaxis_title=None, yaxis_title="New Listings", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)

    # -- Listings going unlisted ---------------------------------------------
    st.subheader("Listings Going Unlisted Over Time (30 Days)")
    df = run_query("""
        WITH first_unlisted AS (
            SELECT upper(d.vin) AS vin, MIN(d.fetched_at) AS unlisted_at
            FROM analytics.stg_detail_observations d
            WHERE d.listing_state = 'unlisted'
              AND d.vin IS NOT NULL AND length(d.vin) = 17
              AND d.fetched_at > now() - interval '30 days'
            GROUP BY upper(d.vin)
        )
        SELECT
            date_trunc('day', unlisted_at AT TIME ZONE 'America/Chicago') AS day,
            COUNT(*) AS vehicles_unlisted
        FROM first_unlisted
        GROUP BY 1 ORDER BY 1
    """)
    if not df.empty:
        fig = px.bar(df, x="day", y="vehicles_unlisted")
        fig.update_layout(xaxis_title=None, yaxis_title="Vehicles Unlisted", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)

    # -- Active by dealer ----------------------------------------------------
    st.subheader("Active Listings by Dealer")
    df = run_query("""
        SELECT
            COALESCE(dealer_name, seller_customer_id) AS dealer,
            make, model,
            COUNT(*) AS active_listings,
            ROUND(AVG(current_price)) AS avg_price,
            MIN(current_price) AS min_price
        FROM analytics.mart_deal_scores
        WHERE COALESCE(listing_state, 'active') != 'unlisted'
          AND seller_customer_id IS NOT NULL
        GROUP BY COALESCE(dealer_name, seller_customer_id), make, model
        ORDER BY active_listings DESC
        LIMIT 50
    """)
    st.dataframe(df, use_container_width=True, hide_index=True)
