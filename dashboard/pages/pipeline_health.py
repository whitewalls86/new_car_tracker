import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from dashboard import queries as Q
from db import run_query


def render():
    st.header("Pipeline Health")

    # -- Active run indicator ------------------------------------------------
    active_runs_df = run_query(Q.ACTIVE_RUNS)
    if not active_runs_df.empty:
        for _, row in active_runs_df.iterrows():
            progress_str = ""
            if pd.notna(row['total_count']) and int(row['total_count']) > 0:
                pct = int(row['progress_count'] / row['total_count'] * 100)
                progress_str = f" — {int(row['progress_count']):,} \
                    / {int(row['total_count']):,} ({pct}%)"
                if pd.notna(row['vins_per_min']) and row['vins_per_min'] > 0:
                    remaining = (int(row['total_count']) - int(row['progress_count'])) \
                        / row['vins_per_min']
                    progress_str += f" ~{remaining:.0f}m remaining"
            has_errors = (
                pd.notna(row['failed_jobs'])
                and int(row['failed_jobs']) > 0
            )
            err_str = (
                f" | {int(row['failed_jobs'])} errors"
                if has_errors
                else ""
            )
            started_time = row['started_at'].strftime('%H:%M')
            warning_msg = (
                f"Running: {row['trigger']} — "
                f"{int(row['elapsed_min'])}m elapsed "
                f"(started {started_time})"
                f"{progress_str}{err_str}"
            )
            st.warning(warning_msg)
    else:
        st.success("No active runs")

    # -- Airflow DAG runs ----------------------------------------------------
    _section_airflow_dag_runs()

    # -- dbt build status ----------------------------------------------------
    dbt_lock_df = run_query(Q.DBT_LOCK_STATUS)
    if not dbt_lock_df.empty and dbt_lock_df["locked"].iloc[0]:
        lock_at = dbt_lock_df["locked_at"].iloc[0]
        lock_by = dbt_lock_df["locked_by"].iloc[0] or "unknown"
        lock_str = lock_at.strftime('%H:%M') if pd.notna(lock_at) else "?"
        st.info(f"dbt building ({lock_by}) — started {lock_str}")

    # -- Detail scrape runs --------------------------------------------------
    _section_detail_runs()

    # -- Stale backlog -------------------------------------------------------
    _section_stale_backlog()

    # -- Price freshness -----------------------------------------------------
    _section_price_freshness()
    _section_blocked_cooldown()

    # -- Rotation schedule ---------------------------------------------------
    _section_rotation_schedule()

    # -- Search scrape jobs --------------------------------------------------
    _section_search_jobs()

    # -- Success rates -------------------------------------------------------
    _section_success_rate(
        "Detail Scrape Success Rate (Last 7 Days)",
        "detail_page", "7 days",
    )
    _section_success_rate(
        "Search Scrape Success Rate (Last 7 Days)",
        "results_page", "7 days",
    )

    # -- Runs over time ------------------------------------------------------
    _section_runs_over_time()

    # -- Backlog + terminated ------------------------------------------------
    col1, col2 = st.columns(2)
    with col1:
        _section_artifact_backlog()
    with col2:
        _section_terminated_runs()

    # -- Pipeline errors -----------------------------------------------------
    _section_pipeline_errors()

    # -- dbt build history ---------------------------------------------------
    _section_dbt_history()

    # -- Processor activity --------------------------------------------------
    _section_processor_activity()

    # -- Postgres health -----------------------------------------------------
    _section_postgres_health()


# ---- helpers ---------------------------------------------------------------

def _section_airflow_dag_runs():
    st.subheader("Airflow DAG Runs (Last 3 Days)")
    try:
        df = run_query(Q.AIRFLOW_DAG_RUNS)
    except Exception:
        st.warning("Airflow dag_run table not accessible — run: GRANT SELECT ON dag_run TO viewer;")
        return
    if df.empty:
        st.info("No recent scrape DAG runs found.")
        return
    display = df[["dag_id", "state", "started", "duration_min", "running"]].copy()
    display.columns = ["DAG", "State", "Started", "Duration (min)", "Running"]
    st.dataframe(display, use_container_width=True, hide_index=True)


def _section_rotation_schedule():
    st.subheader("Search Scrape Rotation Schedule")
    df = run_query(Q.ROTATION_SCHEDULE)
    if not df.empty:
        st.dataframe(df, width="stretch", hide_index=True)
    else:
        st.info("No rotation slots configured.")


def _section_detail_runs():
    st.subheader("Recent Detail Scrape Runs")
    df = run_query(Q.RECENT_DETAIL_RUNS)
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No detail scrape runs found.")


def _section_stale_backlog():
    left_col, right_col = st.columns([1, 1])

    df_stale = run_query(Q.STALE_VEHICLE_BACKLOG)
    df_cooldown = run_query(Q.COOLDOWN_BACKLOG)

    with left_col:
        st.subheader("Stale Vehicle Backlog")
        st.dataframe(df_stale, width="stretch", hide_index=True)

    with right_col:
        st.subheader("Cooldown Backlog")
        st.dataframe(df_cooldown, width="stretch", hide_index=True)


def _section_price_freshness():
    st.subheader("Price Freshness — Expiring in Next 24h")
    df = run_query(Q.PRICE_FRESHNESS)
    if not df.empty:
        df = df.sort_values("hours_until_stale")
        fig = px.bar(
            df, x="expiry_bucket", y=["enriched", "full_details_stale"], barmode="stack",
            labels={"value": "VINs", "expiry_bucket": "Expires In"},
            color_discrete_map={
                "enriched": "#3498db",
                "full_details_stale": "#e67e22"
            },
        )
        fig.update_layout(
            xaxis_title="Hours until stale",
            yaxis_title="Active VINs",
            legend_title="Price Tier",
            xaxis={
                "categoryorder": "array",
                "categoryarray": df["expiry_bucket"].tolist()
            }
        )
        st.plotly_chart(fig, use_container_width=True)


def _section_blocked_cooldown():
    st.subheader("Blocked Listings — Next Eligible Count")
    df = run_query(Q.BLOCKED_COOLDOWN_HISTOGRAM)
    if not df.empty:
        df = df.sort_values("hours_until_eligible")
        fig = px.bar(
            df, x="eligible_bucket", y=["total"],
            labels={"value": "VINs", "eligible_bucket": "Eligible In"},
            color_discrete_map={"total": "#3498db"},
        )
        fig.update_layout(
            xaxis_title="Hours until eligible",
            yaxis_title="Listing Ids",
            xaxis={
                "categoryorder": "array",
                "categoryarray": df["eligible_bucket"].tolist()
            }
        )
        st.plotly_chart(fig, use_container_width=True)


def _section_success_rate(title: str, artifact_type: str, interval: str):
    st.subheader(title)
    df = run_query(Q.SUCCESS_RATE.format(artifact_type=artifact_type, interval=interval))
    if not df.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        for result, color in [
            ("200 OK", "#2ecc71"),
            ("403 Blocked", "#e74c3c"),
            ("Error/Timeout", "#95a5a6")
        ]:
            subset = df[df["result"] == result]
            if not subset.empty:
                fig.add_trace(go.Bar(x=subset["day"], y=subset["fetches"], name=result,
                                     marker_color=color), secondary_y=False)
        daily_totals = df.groupby("day")["fetches"].sum()
        daily_ok = df[df["result"] == "200 OK"].set_index("day")["fetches"]
        success_pct = (daily_ok / daily_totals * 100).fillna(0).reset_index()
        success_pct.columns = ["day", "pct"]
        fig.add_trace(go.Scatter(x=success_pct["day"], y=success_pct["pct"],
                                  name="Success %", mode="lines+markers",
                                  line=dict(color="black", width=2),
                                  marker=dict(size=6)), secondary_y=True)
        fig.update_layout(
            barmode="stack",
            xaxis_title=None,
            legend=dict(orientation="h", y=-0.15)
        )
        fig.update_yaxes(title_text="Fetches", secondary_y=False)
        fig.update_yaxes(title_text="Success %", secondary_y=True, range=[0, 103])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"No {artifact_type.replace('_', ' ')} artifacts in the last {interval}.")


def _section_search_jobs():
    st.subheader("Search Scrape Jobs (Last 7 Days)")
    df = run_query(Q.SEARCH_SCRAPE_JOBS)
    if df.empty:
        st.info("No search scrape jobs in the last 7 days.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def _section_runs_over_time():
    st.subheader("Runs Over Time")
    df = run_query(Q.RUNS_OVER_TIME)
    if not df.empty:
        fig = px.bar(df, x="day", y="runs", color="trigger", barmode="group")
        fig.update_layout(xaxis_title=None, yaxis_title="Runs", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)


def _section_artifact_backlog():
    st.subheader("Artifact Processing Backlog")
    df = run_query(Q.ARTIFACT_BACKLOG)
    st.dataframe(df, use_container_width=True, hide_index=True)


def _section_terminated_runs():
    st.subheader("Terminated Runs (Last 7 Days)")
    df = run_query(Q.TERMINATED_RUNS)
    if df.empty:
        st.success("No terminated runs in the last 7 days.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def _section_pipeline_errors():
    st.subheader("Recent Pipeline Errors")
    df = run_query(Q.PIPELINE_ERRORS)
    if df.empty:
        st.success("No pipeline errors recorded.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def _section_dbt_history():
    st.subheader("dbt Build History")
    df = run_query(Q.DBT_BUILD_HISTORY)
    if df.empty:
        st.info("No dbt builds recorded yet.")
        return
    last = df.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        ts = pd.to_datetime(last["started_at"]).tz_convert("America/Chicago")
        st.metric("Last Build", ts.strftime("%b %d %H:%M"))
    with c2:
        duration = (
            f"{last['duration_s']:.0f}s"
            if pd.notna(last["duration_s"])
            else "—"
        )
        st.metric("Duration", duration)
    with c3:
        status = "✓ OK" if last["ok"] else "✗ Failed"
        st.metric("Status", status)
    with c4:
        models_passed = (
            int(last["models_pass"])
            if pd.notna(last["models_pass"])
            else "—"
        )
        st.metric("Models Passed", models_passed)

    display = df[
        ["started_at", "duration_s", "ok", "intent", "models_pass", "models_error"]
    ].copy()
    display["started_at"] = (
        pd.to_datetime(display["started_at"])
        .dt.tz_convert("America/Chicago")
        .dt.strftime("%b %d %H:%M")
    )
    display["status"] = display["ok"].map(
        {True: "✓ OK", False: "✗ Failed"}
    )
    display = display[
        ["started_at", "duration_s", "status", "intent", "models_pass", "models_error"]
    ]
    display.columns = ["Time", "Duration (s)", "Status", "Intent", "Pass", "Error"]
    st.dataframe(display, use_container_width=True, hide_index=True)


def _section_processor_activity():
    st.subheader("Processor Activity")
    proc_summary_df = run_query(Q.PROCESSOR_ACTIVITY)
    if not proc_summary_df.empty:
        st.dataframe(proc_summary_df, use_container_width=True, hide_index=True)

    st.caption("Processing throughput — last 24 hours")
    proc_hourly_df = run_query(Q.PROCESSING_THROUGHPUT)
    if not proc_hourly_df.empty:
        fig = px.bar(proc_hourly_df, x="hour", y="processed", color="processor", barmode="group")
        fig.update_layout(xaxis_title=None, yaxis_title="Artifacts Processed", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No processing activity in the last 24 hours.")

    st.caption("Detail parser data extraction coverage")
    proc_coverage_df = run_query(Q.DETAIL_EXTRACTION_COVERAGE)
    if not proc_coverage_df.empty:
        st.dataframe(proc_coverage_df, use_container_width=True, hide_index=True)
    else:
        st.info("No detail processing data in the last 14 days.")


def _section_postgres_health():
    st.subheader("Postgres Health")
    df_conn = run_query(Q.PG_STAT_CONNECTIONS)
    if not df_conn.empty:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Active Connections", int(df_conn["active"].iloc[0]))
        with c2:
            st.metric("Idle-in-Transaction", int(df_conn["idle_in_tx"].iloc[0]))
        with c3:
            val = df_conn["longest_query_s"].iloc[0]
            st.metric("Longest Query (s)", f"{val:.1f}" if pd.notna(val) else "0")

    df_slow = run_query(Q.PG_STAT_SLOW_QUERIES)
    if df_slow.empty:
        st.success("No long-running queries (>5s).")
    else:
        st.dataframe(df_slow, use_container_width=True, hide_index=True)
