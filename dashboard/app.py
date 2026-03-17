import os
import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2

st.set_page_config(page_title="Cartracker Dashboard", layout="wide")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://cartracker@postgres:5432/cartracker",
)


@st.cache_resource
def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn)
    except Exception:
        # Connection may be dead — clear cache and reconnect
        try:
            conn.close()
        except Exception:
            pass
        get_connection.clear()
        conn = get_connection()
        return pd.read_sql(sql, conn)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("Cartracker")
if st.sidebar.button("Refresh Data"):
    st.rerun()

# Data freshness
_freshness_df = run_query("""
    SELECT MAX(price_observed_at) AT TIME ZONE 'America/Chicago' AS ts
    FROM analytics.mart_vehicle_snapshot
""")
_freshness_val = _freshness_df["ts"].iloc[0]
if pd.notna(_freshness_val):
    st.sidebar.caption(f"Data as of: {_freshness_val.strftime('%b %d %H:%M')}")

section = st.sidebar.radio(
    "Section",
    ["Pipeline Health", "Inventory Overview", "Deal Finder", "Market Trends"],
)

# ---------------------------------------------------------------------------
# Section 1: Pipeline Health
# ---------------------------------------------------------------------------
if section == "Pipeline Health":
    st.header("Pipeline Health")

    # -- Active run indicator
    active_runs_df = run_query("""
        SELECT trigger, started_at AT TIME ZONE 'America/Chicago' AS started_at,
               ROUND(EXTRACT(EPOCH FROM now() - started_at) / 60) AS elapsed_min
        FROM runs WHERE status = 'running' ORDER BY started_at
    """)
    if not active_runs_df.empty:
        for _, row in active_runs_df.iterrows():
            st.warning(f"Running: {row['trigger']} — {int(row['elapsed_min'])}m elapsed (started {row['started_at'].strftime('%H:%M')})")
    else:
        st.success("No active runs")

    # -- Row 1: Last scrape timestamps + counts
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        df = run_query("""
            SELECT MAX(started_at) AT TIME ZONE 'America/Chicago' AS ts
            FROM runs
            WHERE status = 'success' AND trigger = 'search scrape'
        """)
        val = df["ts"].iloc[0]
        st.metric("Last Search Scrape", val.strftime("%b %d %H:%M") if pd.notna(val) else "Never")

    with col2:
        df = run_query("""
            SELECT MAX(started_at) AT TIME ZONE 'America/Chicago' AS ts
            FROM runs
            WHERE status = 'success' AND trigger = 'detail scrape'
        """)
        val = df["ts"].iloc[0]
        st.metric("Last Detail Scrape", val.strftime("%b %d %H:%M") if pd.notna(val) else "Never")

    with col3:
        df = run_query("""
            WITH last_run AS (
                SELECT started_at FROM runs
                WHERE status = 'success' AND trigger = 'search scrape'
                ORDER BY started_at DESC LIMIT 1
            )
            SELECT COUNT(DISTINCT vin) AS cnt
            FROM analytics.int_listing_days_on_market
            WHERE first_seen_at >= (SELECT started_at FROM last_run)
        """)
        st.metric("New Vehicles Added", f"{df['cnt'].iloc[0]:,}")

    with col4:
        df = run_query("""
            WITH last_run AS (
                SELECT started_at FROM runs
                WHERE status = 'success' AND trigger = 'search scrape'
                ORDER BY started_at DESC LIMIT 1
            )
            SELECT COUNT(DISTINCT vin) AS cnt
            FROM srp_observations
            WHERE fetched_at >= (SELECT started_at FROM last_run)
              AND vin IS NOT NULL
        """)
        st.metric("Vehicles Observed", f"{df['cnt'].iloc[0]:,}")

    # -- Row 2: Price updates + stale backlog
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Price Updates Since Last Detail Scrape")
        df = run_query("""
            WITH last_run AS (
                SELECT started_at FROM runs
                WHERE status = 'success' AND trigger = 'detail scrape'
                ORDER BY started_at DESC LIMIT 1
            )
            SELECT 'Direct Detail Page' AS source, COUNT(*) AS updates
            FROM detail_observations
            WHERE fetched_at >= (SELECT started_at FROM last_run)
            UNION ALL
            SELECT 'Carousel Hint' AS source, COUNT(*) AS updates
            FROM detail_carousel_hints
            WHERE fetched_at >= (SELECT started_at FROM last_run)
        """)
        st.dataframe(df, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Stale Vehicle Backlog")
        df = run_query("""
            SELECT
                stale_reason,
                COUNT(*) AS vehicle_count,
                ROUND(AVG(tier1_age_hours)::numeric, 1) AS avg_tier1_age_hours,
                ROUND(AVG(price_age_hours)::numeric, 1) AS avg_price_age_hours
            FROM ops.ops_vehicle_staleness
            WHERE listing_state IS DISTINCT FROM 'unlisted'
            GROUP BY stale_reason
            ORDER BY vehicle_count DESC
        """)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # -- Price freshness distribution
    st.subheader("Price Freshness — Expiring in Next 24h")
    freshness_df = run_query("""
        SELECT
            CASE
                WHEN price_age_hours <= 3  THEN '0-3h'
                WHEN price_age_hours <= 6  THEN '3-6h'
                WHEN price_age_hours <= 9  THEN '6-9h'
                WHEN price_age_hours <= 12 THEN '9-12h'
                WHEN price_age_hours <= 15 THEN '12-15h'
                WHEN price_age_hours <= 18 THEN '15-18h'
                WHEN price_age_hours <= 21 THEN '18-21h'
                WHEN price_age_hours <= 24 THEN '21-24h'
                ELSE '24h+ (stale)'
            END AS age_bucket,
            COUNT(*) FILTER (WHERE price_tier = 1) AS tier1,
            COUNT(*) FILTER (WHERE price_tier = 2) AS tier2,
            COUNT(*) AS total
        FROM ops.ops_vehicle_staleness
        GROUP BY 1
        ORDER BY MIN(price_age_hours)
    """)
    if not freshness_df.empty:
        fig = px.bar(
            freshness_df, x="age_bucket", y=["tier1", "tier2"], barmode="stack",
            labels={"value": "VINs", "age_bucket": "Price Age"},
            color_discrete_map={"tier1": "#3498db", "tier2": "#95a5a6"},
        )
        fig.update_layout(xaxis_title=None, yaxis_title="Active VINs", legend_title="Price Tier")
        st.plotly_chart(fig, use_container_width=True)

    # -- Row 3: Detail scrape success rate
    st.subheader("Detail Scrape Success Rate (Last 30 Days)")
    df = run_query("""
        SELECT
            date_trunc('day', fetched_at AT TIME ZONE 'America/Chicago') AS day,
            CASE
                WHEN http_status = 200 THEN '200 OK'
                WHEN http_status = 403 THEN '403 Blocked'
                WHEN http_status IS NULL THEN 'Error/Timeout'
                ELSE http_status::text
            END AS result,
            COUNT(*) AS fetches
        FROM raw_artifacts
        WHERE artifact_type = 'detail_page'
          AND fetched_at > now() - interval '30 days'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    if not df.empty:
        fig = px.bar(df, x="day", y="fetches", color="result", barmode="stack",
                     color_discrete_map={"200 OK": "#2ecc71", "403 Blocked": "#e74c3c", "Error/Timeout": "#95a5a6"})
        fig.update_layout(xaxis_title=None, yaxis_title="Fetches", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No detail page artifacts in the last 30 days.")

    # -- Row 4: Runs over time
    st.subheader("Runs Over Time")
    df = run_query("""
        SELECT
            date_trunc('day', started_at AT TIME ZONE 'America/Chicago') AS day,
            trigger,
            COUNT(*) AS runs,
            COUNT(*) FILTER (WHERE status = 'success') AS successful,
            COUNT(*) FILTER (WHERE status = 'terminated') AS terminated,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed
        FROM runs
        WHERE started_at > now() - interval '30 days'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    if not df.empty:
        fig = px.bar(df, x="day", y="runs", color="trigger", barmode="group")
        fig.update_layout(xaxis_title=None, yaxis_title="Runs", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)

    # -- Row 5: Artifact processing backlog + pipeline errors + terminated runs
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Artifact Processing Backlog")
        df = run_query("""
            SELECT
                processor,
                status,
                COUNT(*) AS count,
                MIN(processed_at) AT TIME ZONE 'America/Chicago' AS oldest
            FROM artifact_processing
            WHERE status IN ('retry', 'processing')
            GROUP BY processor, status
            ORDER BY count DESC
        """)
        st.dataframe(df, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Terminated Runs (Last 7 Days)")
        df = run_query("""
            SELECT
                trigger,
                COUNT(*) AS terminated_count,
                MAX(started_at) AT TIME ZONE 'America/Chicago' AS most_recent
            FROM runs
            WHERE status = 'terminated'
              AND started_at > now() - interval '7 days'
            GROUP BY trigger
            ORDER BY terminated_count DESC
        """)
        if df.empty:
            st.success("No terminated runs in the last 7 days.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)

    st.subheader("Recent Pipeline Errors")
    df = run_query("""
        SELECT
            occurred_at AT TIME ZONE 'America/Chicago' AS occurred_at_ct,
            workflow_name,
            node_name,
            error_type,
            error_message
        FROM pipeline_errors
        ORDER BY occurred_at DESC
        LIMIT 50
    """)
    if df.empty:
        st.success("No pipeline errors recorded.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Section 2: Inventory Overview
# ---------------------------------------------------------------------------
elif section == "Inventory Overview":
    st.header("Inventory Overview")

    # -- Row 1: Scalar cards
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
            SELECT COUNT(DISTINCT vin) AS cnt
            FROM analytics.int_listing_days_on_market
            WHERE first_seen_at > now() - interval '24 hours'
        """)
        st.metric("New (24h)", f"{df['cnt'].iloc[0]:,}")

    with col3:
        df = run_query("""
            SELECT COUNT(DISTINCT vin) AS cnt
            FROM analytics.int_listing_days_on_market
            WHERE first_seen_at > now() - interval '7 days'
        """)
        st.metric("New (7d)", f"{df['cnt'].iloc[0]:,}")

    with col4:
        df = run_query("""
            SELECT COUNT(DISTINCT vin) AS cnt
            FROM analytics.int_listing_days_on_market
            WHERE first_seen_at > now() - interval '30 days'
        """)
        st.metric("New (30d)", f"{df['cnt'].iloc[0]:,}")

    # -- Row 2: Active listings by make/model
    st.subheader("Active Listings by Make / Model")
    df = run_query("""
        SELECT
            make,
            model,
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

    # -- Row 3: New listings over time
    st.subheader("New Listings Over Time (30 Days)")
    df = run_query("""
        SELECT
            date_trunc('day', dom.first_seen_at AT TIME ZONE 'America/Chicago') AS day,
            a.make,
            COUNT(DISTINCT dom.vin) AS new_listings
        FROM analytics.int_listing_days_on_market dom
        JOIN analytics.int_srp_vehicle_attributes a ON a.vin = dom.vin
        WHERE dom.first_seen_at > now() - interval '30 days'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    if not df.empty:
        fig = px.bar(df, x="day", y="new_listings", color="make", barmode="stack")
        fig.update_layout(xaxis_title=None, yaxis_title="New Listings", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)

    # -- Row 4: Listings going unlisted
    st.subheader("Listings Going Unlisted Over Time (30 Days)")
    df = run_query("""
        WITH first_unlisted AS (
            SELECT
                vin,
                MIN(fetched_at) AS unlisted_at
            FROM detail_observations
            WHERE listing_state = 'unlisted'
              AND vin IS NOT NULL
            GROUP BY vin
        )
        SELECT
            date_trunc('day', unlisted_at AT TIME ZONE 'America/Chicago') AS day,
            COUNT(*) AS vehicles_unlisted
        FROM first_unlisted
        WHERE unlisted_at > now() - interval '30 days'
        GROUP BY 1
        ORDER BY 1
    """)
    if not df.empty:
        fig = px.bar(df, x="day", y="vehicles_unlisted")
        fig.update_layout(xaxis_title=None, yaxis_title="Vehicles Unlisted", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)

    # -- Row 5: Active by dealer
    st.subheader("Active Listings by Dealer")
    df = run_query("""
        SELECT
            COALESCE(dealer_name, seller_customer_id) AS dealer,
            make,
            model,
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


# ---------------------------------------------------------------------------
# Section 3: Deal Finder
# ---------------------------------------------------------------------------
elif section == "Deal Finder":
    st.header("Deal Finder")

    # -- Filters
    col1, col2, col3 = st.columns(3)

    makes_df = run_query("""
        SELECT DISTINCT make FROM analytics.mart_deal_scores
        WHERE COALESCE(listing_state, 'active') != 'unlisted' ORDER BY make
    """)
    all_makes = makes_df["make"].tolist()

    with col1:
        selected_makes = st.multiselect("Make", all_makes, default=[])
    with col2:
        selected_tiers = st.multiselect(
            "Deal Tier", ["excellent", "good", "fair", "weak"], default=[]
        )
    with col3:
        scope_filter = st.selectbox("Scope", ["All", "Local", "National"])

    # Build WHERE clauses
    where = ["COALESCE(listing_state, 'active') != 'unlisted'"]
    if selected_makes:
        makes_str = ", ".join(f"'{m}'" for m in selected_makes)
        where.append(f"make IN ({makes_str})")
    if selected_tiers:
        tiers_str = ", ".join(f"'{t}'" for t in selected_tiers)
        where.append(f"deal_tier IN ({tiers_str})")
    if scope_filter == "Local":
        where.append("is_local")
    elif scope_filter == "National":
        where.append("NOT is_local")
    where_clause = " AND ".join(where)

    # -- Main deals table
    st.subheader("All Active Deals")
    df = run_query(f"""
        SELECT
            make,
            model,
            vehicle_trim,
            model_year,
            dealer_name,
            current_price,
            national_median_price,
            msrp,
            ROUND(msrp_discount_pct::numeric, 1) AS msrp_off_pct,
            deal_tier,
            ROUND(deal_score::numeric, 1) AS deal_score,
            ROUND(national_price_percentile::numeric * 100, 0) AS price_pct,
            days_on_market,
            price_drop_count AS drops,
            CASE WHEN is_local THEN 'Local' ELSE 'National' END AS scope,
            canonical_detail_url
        FROM analytics.mart_deal_scores
        WHERE {where_clause}
        ORDER BY deal_score DESC
    """)
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

    # -- Row 2: Charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Deal Tier Distribution")
        df = run_query(f"""
            SELECT
                deal_tier,
                COUNT(*) AS listings
            FROM analytics.mart_deal_scores
            WHERE {where_clause}
            GROUP BY deal_tier
            ORDER BY
                CASE deal_tier
                    WHEN 'excellent' THEN 1
                    WHEN 'good' THEN 2
                    WHEN 'fair' THEN 3
                    WHEN 'weak' THEN 4
                END
        """)
        if not df.empty:
            fig = px.bar(df, x="deal_tier", y="listings",
                         color="deal_tier",
                         color_discrete_map={
                             "excellent": "#27ae60",
                             "good": "#2ecc71",
                             "fair": "#f39c12",
                             "weak": "#e74c3c",
                         })
            fig.update_layout(showlegend=False, xaxis_title=None, yaxis_title="Listings")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Days on Market Distribution")
        df = run_query(f"""
            SELECT
                CASE
                    WHEN days_on_market <= 7  THEN '0-7 days'
                    WHEN days_on_market <= 14 THEN '8-14 days'
                    WHEN days_on_market <= 30 THEN '15-30 days'
                    WHEN days_on_market <= 60 THEN '31-60 days'
                    ELSE '60+ days'
                END AS bucket,
                COUNT(*) AS listings
            FROM analytics.mart_deal_scores
            WHERE {where_clause}
            GROUP BY 1
            ORDER BY MIN(days_on_market)
        """)
        if not df.empty:
            fig = px.bar(df, x="bucket", y="listings")
            fig.update_layout(xaxis_title=None, yaxis_title="Listings")
            st.plotly_chart(fig, use_container_width=True)

    # -- Row 3: Price drops
    st.subheader("Price Drop Events")
    df = run_query(f"""
        SELECT
            make,
            model,
            vehicle_trim,
            model_year,
            dealer_name,
            current_price,
            first_price,
            current_price - first_price AS price_change,
            ROUND(total_price_drop_pct::numeric, 1) AS total_drop_pct,
            price_drop_count AS drops,
            days_on_market,
            canonical_detail_url
        FROM analytics.mart_deal_scores
        WHERE {where_clause}
          AND price_drop_count > 0
        ORDER BY total_price_drop_pct DESC
    """)
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

    # -- Row 4: Price vs MSRP
    st.subheader("Price vs MSRP by Model")
    df = run_query(f"""
        SELECT
            model,
            ROUND(AVG(current_price)) AS avg_price,
            ROUND(AVG(msrp)) AS avg_msrp,
            ROUND(AVG(msrp_discount_pct)::numeric, 1) AS avg_msrp_off_pct,
            COUNT(*) AS listings
        FROM analytics.mart_deal_scores
        WHERE {where_clause}
          AND msrp IS NOT NULL AND msrp > 0
        GROUP BY model
        ORDER BY avg_msrp_off_pct DESC
    """)
    if not df.empty:
        fig = px.bar(df, x="model", y=["avg_price", "avg_msrp"], barmode="group",
                     hover_data=["avg_msrp_off_pct", "listings"])
        fig.update_layout(xaxis_title=None, yaxis_title="Price ($)", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Section 4: Market Trends
# ---------------------------------------------------------------------------
elif section == "Market Trends":
    st.header("Market Trends")

    # -- Row 1: Median price by model over time (from int_price_events)
    st.subheader("Median Price by Model Over Time (Weekly, SRP Source)")
    df = run_query("""
        SELECT
            date_trunc('week', pe.observed_at AT TIME ZONE 'America/Chicago') AS week,
            a.make,
            a.model,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe.price) AS median_price,
            COUNT(DISTINCT pe.vin) AS listing_count
        FROM analytics.int_price_events pe
        JOIN analytics.int_srp_vehicle_attributes a ON a.vin = pe.vin
        WHERE pe.observed_at > now() - interval '90 days'
          AND pe.price > 0
          AND pe.source = 'srp'
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """)
    if not df.empty:
        df["label"] = df["make"] + " " + df["model"]
        fig = px.line(df, x="week", y="median_price", color="label",
                      hover_data=["listing_count"])
        fig.update_layout(xaxis_title=None, yaxis_title="Median Price ($)", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)

    # -- Row 2: Inventory levels over time
    st.subheader("Inventory Levels by Model (Daily)")
    df = run_query("""
        SELECT
            date_trunc('day', fetched_at AT TIME ZONE 'America/Chicago') AS day,
            make,
            model,
            COUNT(DISTINCT vin) AS listings_seen
        FROM srp_observations
        WHERE fetched_at > now() - interval '30 days'
          AND vin IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY 1, 4 DESC
    """)
    if not df.empty:
        df["label"] = df["make"] + " " + df["model"]
        fig = px.line(df, x="day", y="listings_seen", color="label")
        fig.update_layout(xaxis_title=None, yaxis_title="Distinct VINs Seen", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)

    # -- Row 3: Days on market by model
    st.subheader("Days on Market by Model")
    df = run_query("""
        SELECT
            make,
            model,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_on_market)) AS median_days,
            ROUND(AVG(days_on_market)::numeric, 1) AS avg_days,
            MIN(days_on_market) AS min_days,
            MAX(days_on_market) AS max_days,
            COUNT(*) AS listings
        FROM analytics.mart_deal_scores
        WHERE COALESCE(listing_state, 'active') != 'unlisted'
        GROUP BY make, model
        ORDER BY median_days DESC
    """)
    if not df.empty:
        fig = px.bar(df, x="model", y="median_days", color="make",
                     hover_data=["avg_days", "min_days", "max_days", "listings"])
        fig.update_layout(xaxis_title=None, yaxis_title="Median Days on Market", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # -- Row 4: National supply vs local
    st.subheader("National Supply vs Local Availability")
    df = run_query("""
        SELECT
            make,
            model,
            COUNT(*) AS national_listings,
            COUNT(*) FILTER (WHERE is_local) AS local_listings,
            ROUND(AVG(national_listing_count)) AS avg_national_supply,
            ROUND(AVG(current_price)) AS avg_price,
            ROUND(AVG(msrp_discount_pct)::numeric, 1) AS avg_msrp_off_pct
        FROM analytics.mart_deal_scores
        WHERE COALESCE(listing_state, 'active') != 'unlisted'
        GROUP BY make, model
        ORDER BY national_listings DESC
    """)
    st.dataframe(df, use_container_width=True, hide_index=True)
