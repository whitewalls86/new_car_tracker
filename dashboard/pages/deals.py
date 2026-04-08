import plotly.express as px
import streamlit as st

from db import run_query


def render():
    st.header("Deal Finder")

    # -- Filters -------------------------------------------------------------
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

    # Build WHERE clauses with parameterized queries
    where = ["COALESCE(listing_state, 'active') != 'unlisted'"]
    query_params = []
    if selected_makes:
        placeholders = ", ".join(["%s"] * len(selected_makes))
        where.append(f"make IN ({placeholders})")
        query_params.extend(selected_makes)
    if selected_tiers:
        placeholders = ", ".join(["%s"] * len(selected_tiers))
        where.append(f"deal_tier IN ({placeholders})")
        query_params.extend(selected_tiers)
    if scope_filter == "Local":
        where.append("is_local")
    elif scope_filter == "National":
        where.append("NOT is_local")
    where_clause = " AND ".join(where)

    # -- Main deals table ----------------------------------------------------
    st.subheader("All Active Deals")
    df = run_query(f"""
        SELECT
            make, model, vehicle_trim, model_year, dealer_name,
            current_price, national_median_price, msrp,
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
    """, params=query_params if query_params else None)
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
        df = run_query(f"""
            SELECT deal_tier, COUNT(*) AS listings
            FROM analytics.mart_deal_scores
            WHERE {where_clause}
            GROUP BY deal_tier
            ORDER BY CASE deal_tier
                WHEN 'excellent' THEN 1 WHEN 'good' THEN 2
                WHEN 'fair' THEN 3 WHEN 'weak' THEN 4
            END
        """, params=query_params if query_params else None)
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
            GROUP BY 1 ORDER BY MIN(days_on_market)
        """, params=query_params if query_params else None)
        if not df.empty:
            fig = px.bar(df, x="bucket", y="listings")
            fig.update_layout(xaxis_title=None, yaxis_title="Listings")
            st.plotly_chart(fig, use_container_width=True)

    # -- Price drops ---------------------------------------------------------
    st.subheader("Price Drop Events")
    df = run_query(f"""
        SELECT
            make, model, vehicle_trim, model_year, dealer_name,
            current_price, first_price,
            current_price - first_price AS price_change,
            ROUND(total_price_drop_pct::numeric, 1) AS total_drop_pct,
            price_drop_count AS drops, days_on_market,
            canonical_detail_url
        FROM analytics.mart_deal_scores
        WHERE {where_clause} AND price_drop_count > 0
        ORDER BY total_price_drop_pct DESC
    """, params=query_params if query_params else None)
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
    df = run_query(f"""
        SELECT
            model,
            ROUND(AVG(current_price)) AS avg_price,
            ROUND(AVG(msrp)) AS avg_msrp,
            ROUND(AVG(msrp_discount_pct)::numeric, 1) AS avg_msrp_off_pct,
            COUNT(*) AS listings
        FROM analytics.mart_deal_scores
        WHERE {where_clause} AND msrp IS NOT NULL AND msrp > 0
        GROUP BY model ORDER BY avg_msrp_off_pct DESC
    """, params=query_params if query_params else None)
    if not df.empty:
        fig = px.bar(df, x="model", y=["avg_price", "avg_msrp"], barmode="group",
                     hover_data=["avg_msrp_off_pct", "listings"])
        fig.update_layout(xaxis_title=None, yaxis_title="Price ($)", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
