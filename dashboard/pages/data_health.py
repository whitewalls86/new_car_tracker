import plotly.express as px
import streamlit as st
from queries import (
    DATA_HEALTH_BATCH_OUTCOMES,
    DATA_HEALTH_COOLDOWN_COHORTS,
    DATA_HEALTH_INVENTORY_COVERAGE,
    DATA_HEALTH_PRICE_FRESHNESS,
)

from db import run_duckdb_query


def render():
    st.header("Data Health")
    st.caption("Analytics over the permanent silver record — not live operational state.")

    # -- Inventory Coverage --------------------------------------------------
    st.subheader("Inventory Coverage by Make / Model")
    st.caption("Coverage = fraction of tracked VINs with a detail observation (trim, mileage, etc.)")
    df = run_duckdb_query(DATA_HEALTH_INVENTORY_COVERAGE)
    if not df.empty:
        col1, col2, col3 = st.columns(3)
        total = df["total_vins"].sum()
        enriched = df["detail_enriched"].sum()
        with col1:
            st.metric("Total Tracked VINs", f"{total:,}")
        with col2:
            st.metric("Detail-Enriched", f"{enriched:,}")
        with col3:
            overall_pct = enriched / total * 100 if total > 0 else 0
            st.metric("Overall Coverage", f"{overall_pct:.1f}%")

        fig = px.bar(
            df,
            x="model",
            y="coverage_pct",
            color="make",
            hover_data=["total_vins", "detail_enriched", "srp_only"],
        )
        fig.update_layout(
            xaxis_title=None,
            yaxis_title="Coverage %",
            yaxis_range=[0, 100],
            legend_title=None,
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()

    # -- Price Freshness -----------------------------------------------------
    st.subheader("Price Freshness by Make / Model")
    st.caption("How current is price data for each tracked make/model?")
    df = run_duckdb_query(DATA_HEALTH_PRICE_FRESHNESS)
    if not df.empty:
        fig = px.bar(
            df,
            x="model",
            y=["fresh_lt_1d", "fresh_1_3d", "fresh_4_7d", "fresh_8_14d", "stale_gt_14d"],
            color_discrete_map={
                "fresh_lt_1d": "#27ae60",
                "fresh_1_3d": "#2ecc71",
                "fresh_4_7d": "#f39c12",
                "fresh_8_14d": "#e67e22",
                "stale_gt_14d": "#e74c3c",
            },
            barmode="stack",
            hover_data=["make", "total_vins", "fresh_lt_7d_pct"],
        )
        fig.update_layout(
            xaxis_title=None,
            yaxis_title="VINs",
            legend_title="Age of last price",
        )
        fig.for_each_trace(lambda t: t.update(
            name=t.name.replace("fresh_lt_1d", "<1d")
                       .replace("fresh_1_3d", "1-3d")
                       .replace("fresh_4_7d", "4-7d")
                       .replace("fresh_8_14d", "8-14d")
                       .replace("stale_gt_14d", ">14d")
        ))
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            df[["make", "model", "total_vins", "fresh_lt_7d_pct", "stale_gt_14d"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "fresh_lt_7d_pct": st.column_config.NumberColumn("Fresh <7d %", format="%.1f%%"),
                "stale_gt_14d": st.column_config.NumberColumn("Stale >14d"),
            },
        )

    st.divider()

    # -- Detail Batch Outcomes -----------------------------------------------
    st.subheader("Detail Scrape Extraction Yield (Last 30 Days)")
    st.caption("extraction_yield = fraction of detail observations with a valid 17-char VIN. Low yield signals parser failures.")
    df = run_duckdb_query(DATA_HEALTH_BATCH_OUTCOMES)
    if not df.empty:
        fig = px.line(
            df.sort_values("obs_date"),
            x="obs_date",
            y="extraction_yield",
            markers=True,
            hover_data=["detail_observations", "detail_artifacts", "valid_vin_count"],
        )
        fig.update_layout(
            xaxis_title=None,
            yaxis_title="Extraction Yield %",
            yaxis_range=[0, 100],
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "extraction_yield": st.column_config.NumberColumn("Yield %", format="%.1f%%"),
            },
        )

    st.divider()

    # -- Cooldown Cohorts ----------------------------------------------------
    st.subheader("403 Cooldown Backlog")
    st.caption("Listings in exponential cooldown, grouped by current attempt count.")
    df = run_duckdb_query(DATA_HEALTH_COOLDOWN_COHORTS)
    if df.empty:
        st.info("No cooldown data — either no blocked listings or events not yet flushed to MinIO.")
    else:
        total_blocked = df["listing_count"].sum()
        st.metric("Total Listings in Cooldown", f"{total_blocked:,}")
        fig = px.bar(
            df,
            x="attempt_bucket",
            y="listing_count",
            hover_data=["min_attempts", "max_attempts"],
        )
        fig.update_layout(
            xaxis_title="Attempt Count",
            yaxis_title="Listings",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)
