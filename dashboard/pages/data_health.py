import plotly.express as px
import streamlit as st
from queries import (
    DATA_HEALTH_BATCH_OUTCOMES,
    DATA_HEALTH_BLOCK_RATE,
    DATA_HEALTH_COOLDOWN_COHORTS,
    DATA_HEALTH_INVENTORY_COVERAGE,
    DATA_HEALTH_PRICE_FRESHNESS,
    DATA_HEALTH_SCRAPE_VOLUME,
)

from db import run_duckdb_query


def render():
    st.header("Data Health")
    st.caption("Analytics over the permanent silver record — not live operational state.")

    # -- Scrape Volume -------------------------------------------------------
    st.subheader("Scrape Volume")
    st.caption("Artifacts and observations processed per hour, by source type.")
    df_vol = run_duckdb_query(DATA_HEALTH_SCRAPE_VOLUME)
    if not df_vol.empty:
        granularity = st.radio(
            "Granularity", ["Hourly", "Daily", "Weekly"],
            horizontal=True, key="scrape_vol_gran",
        )
        trunc = {"Hourly": "hour", "Daily": "day", "Weekly": "week"}[granularity]
        df_agg = (
            df_vol.assign(period=df_vol["hour"].dt.floor("h" if trunc == "hour" else ("D" if trunc == "day" else "W")))
            .groupby(["period", "source"], as_index=False)
            .agg(artifact_count=("artifact_count", "sum"), observation_count=("observation_count", "sum"))
        )

        col1, col2, col3 = st.columns(3)
        today = df_vol[df_vol["hour"] >= df_vol["hour"].max().floor("D")]
        with col1:
            st.metric("Artifacts today", f"{today['artifact_count'].sum():,}")
        with col2:
            st.metric("Observations today", f"{today['observation_count'].sum():,}")
        with col3:
            hourly_avg = df_vol.groupby("hour")["observation_count"].sum().mean()
            st.metric("Avg obs / hour (14d)", f"{hourly_avg:,.0f}")

        fig = px.line(
            df_agg.sort_values("period"),
            x="period", y="observation_count", color="source",
            color_discrete_map={"srp": "#2563EB", "detail": "#7C3AED", "carousel": "#059669"},
        )
        fig.update_layout(xaxis_title=None, yaxis_title="Observations", legend_title="Source")
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.line(
            df_agg.sort_values("period"),
            x="period", y="artifact_count", color="source",
            color_discrete_map={"srp": "#2563EB", "detail": "#7C3AED", "carousel": "#059669"},
        )
        fig2.update_layout(xaxis_title=None, yaxis_title="Artifacts (batches)", legend_title="Source")
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # -- 403 Block Rate ------------------------------------------------------
    st.subheader("403 Block Rate")
    st.caption("New 403 blocks per hour and block rate as a fraction of total observations.")
    df_blk = run_duckdb_query(DATA_HEALTH_BLOCK_RATE)
    if not df_blk.empty:
        granularity_b = st.radio(
            "Granularity", ["Hourly", "Daily", "Weekly"],
            horizontal=True, key="block_rate_gran",
        )
        trunc_b = {"Hourly": "h", "Daily": "D", "Weekly": "W"}[granularity_b]
        df_blk_agg = (
            df_blk.assign(period=df_blk["hour"].dt.floor(trunc_b))
            .groupby("period", as_index=False)
            .agg(
                new_blocks=("new_blocks", "sum"),
                block_increments=("block_increments", "sum"),
                unique_listings_blocked=("unique_listings_blocked", "sum"),
                total_observations=("total_observations", "sum"),
            )
        )
        df_blk_agg["block_rate_pct"] = (
            df_blk_agg["new_blocks"] * 100.0 / df_blk_agg["total_observations"].replace(0, None)
        ).round(3)

        col1, col2, col3 = st.columns(3)
        today_blk = df_blk[df_blk["hour"] >= df_blk["hour"].max().floor("D")]
        with col1:
            st.metric("New blocks today", f"{int(today_blk['new_blocks'].sum()):,}")
        with col2:
            st.metric("Unique listings blocked today", f"{int(today_blk['unique_listings_blocked'].sum()):,}")
        with col3:
            rate_today = (
                today_blk["new_blocks"].sum() * 100.0 / today_blk["total_observations"].sum()
                if today_blk["total_observations"].sum() > 0 else 0
            )
            st.metric("Block rate today", f"{rate_today:.3f}%")

        fig = px.line(
            df_blk_agg.sort_values("period"),
            x="period", y="new_blocks",
        )
        fig.update_layout(xaxis_title=None, yaxis_title="New 403 blocks")
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.line(
            df_blk_agg.sort_values("period"),
            x="period", y="block_rate_pct",
        )
        fig2.update_layout(xaxis_title=None, yaxis_title="Block rate %")
        st.plotly_chart(fig2, use_container_width=True)
    elif df_vol.empty or df_blk.empty:
        st.info("No block data in the last 14 days.")

    st.divider()

    # -- Inventory Coverage --------------------------------------------------
    st.subheader("Inventory Coverage by Make / Model")
    st.caption(
        "Coverage = fraction of tracked VINs with a detail observation (trim, mileage, etc.)"
    )
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
    st.caption(
        "extraction_yield = fraction of detail observations with a valid 17-char VIN. "
        "Low yield signals parser failures."
    )
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
