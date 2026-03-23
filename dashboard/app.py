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


def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
    except Exception:
        # Connection may be dead — clear cache and reconnect
        try:
            conn.close()
        except Exception:
            pass
        get_connection.clear()
        conn = get_connection()
        return pd.read_sql(sql, conn, params=params)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("Cartracker")
if st.sidebar.button("Refresh Data"):
    st.rerun()

# Quicklinks
st.sidebar.markdown("---")
st.sidebar.markdown("**Quick Links**")
st.sidebar.markdown("[n8n Workflows](http://localhost:5678)")
st.sidebar.markdown("[Search Config Admin](http://localhost:8000/admin)")
st.sidebar.markdown("[pgAdmin](http://localhost:5050)")

# Data freshness
_freshness_df = run_query("""
    SELECT MAX(price_observed_at) AT TIME ZONE 'America/Chicago' AS ts
    FROM analytics.mart_vehicle_snapshot
""")
_freshness_val = _freshness_df["ts"].iloc[0]
if pd.notna(_freshness_val):
    st.sidebar.caption(f"Data as of: {_freshness_val.strftime('%b %d %H:%M')}")

tab1, tab2, tab3, tab4 = st.tabs(["Pipeline Health", "Inventory Overview", "Deal Finder", "Market Trends"])

# ---------------------------------------------------------------------------
# Section 1: Pipeline Health
# ---------------------------------------------------------------------------
with tab1:
    st.header("Pipeline Health")

    # -- Active run indicator
    active_runs_df = run_query("""
        SELECT r.trigger, r.started_at AT TIME ZONE 'America/Chicago' AS started_at,
               ROUND(EXTRACT(EPOCH FROM now() - r.started_at) / 60) AS elapsed_min,
               r.progress_count, r.total_count,
               CASE WHEN r.total_count > 0
                    THEN ROUND(r.progress_count::numeric / (EXTRACT(EPOCH FROM now() - r.started_at) / 60), 1)
               END AS vins_per_min,
               (SELECT COUNT(*) FROM scrape_jobs j
                WHERE j.run_id = r.run_id AND j.status = 'failed') AS failed_jobs
        FROM runs r WHERE r.status = 'running' ORDER BY r.started_at
    """)
    if not active_runs_df.empty:
        for _, row in active_runs_df.iterrows():
            progress_str = ""
            if pd.notna(row['total_count']) and int(row['total_count']) > 0:
                pct = int(row['progress_count'] / row['total_count'] * 100)
                progress_str = f" — {int(row['progress_count']):,} / {int(row['total_count']):,} ({pct}%)"
                if pd.notna(row['vins_per_min']) and row['vins_per_min'] > 0:
                    remaining = (int(row['total_count']) - int(row['progress_count'])) / row['vins_per_min']
                    progress_str += f" ~{remaining:.0f}m remaining"
            err_str = f" | {int(row['failed_jobs'])} errors" if pd.notna(row['failed_jobs']) and int(row['failed_jobs']) > 0 else ""
            st.warning(f"Running: {row['trigger']} — {int(row['elapsed_min'])}m elapsed (started {row['started_at'].strftime('%H:%M')}){progress_str}{err_str}")
    else:
        st.success("No active runs")

    # -- All Recent Runs
    st.subheader("Recent Runs (All Types)")
    recent_runs_df = run_query("""
        SELECT
            r.started_at AT TIME ZONE 'America/Chicago' AS started,
            r.trigger,
            CASE
                WHEN r.finished_at IS NOT NULL
                THEN ROUND(EXTRACT(EPOCH FROM (r.finished_at - r.started_at)) / 60, 1)::text || 'm'
                ELSE ROUND(EXTRACT(EPOCH FROM (now() - r.started_at)) / 60)::text || 'm'
            END AS duration,
            r.status,
            r.total_count AS batch,
            r.progress_count AS processed,
            CASE WHEN r.finished_at IS NOT NULL AND r.progress_count > 0
                 THEN ROUND(r.progress_count / (EXTRACT(EPOCH FROM (r.finished_at - r.started_at)) / 60), 1)
            END AS rate_per_min,
            COALESCE(r.error_count, 0) AS errors,
            r.last_error
        FROM runs r
        WHERE r.started_at > now() - interval '48 hours'
          AND r.status != 'skipped'
        ORDER BY r.started_at DESC
        LIMIT 30
    """)
    if not recent_runs_df.empty:
        st.dataframe(recent_runs_df, use_container_width=True, hide_index=True)
    else:
        st.info("No runs in the last 48 hours.")

    # -- Table 1: Search Scrape Rotation Schedule
    st.subheader("Search Scrape Rotation Schedule")
    rotation_df = run_query("""
        WITH slot_configs AS (
            SELECT
                rotation_slot,
                string_agg(search_key, ', ' ORDER BY search_key) AS search_keys,
                MAX(last_queued_at) AS last_queued_at
            FROM search_configs
            WHERE enabled = true AND rotation_slot IS NOT NULL
            GROUP BY rotation_slot
        ),
        slot_last_run AS (
            SELECT DISTINCT ON (sc.rotation_slot)
                sc.rotation_slot,
                r.run_id,
                r.status AS run_status,
                r.started_at
            FROM search_configs sc
            JOIN scrape_jobs j ON j.search_key = sc.search_key
            JOIN runs r ON r.run_id = j.run_id AND r.trigger = 'search scrape'
            WHERE sc.enabled = true AND sc.rotation_slot IS NOT NULL
            ORDER BY sc.rotation_slot, r.started_at DESC
        ),
        slot_results AS (
            SELECT
                slr.rotation_slot,
                COUNT(DISTINCT a.artifact_id) AS pages,
                COUNT(DISTINCT a.artifact_id) FILTER (
                    WHERE a.http_status IS NULL OR a.http_status >= 400
                ) AS errors,
                COUNT(DISTINCT so.vin) AS vins_observed
            FROM slot_last_run slr
            JOIN scrape_jobs j ON j.run_id = slr.run_id
                AND j.search_key IN (
                    SELECT search_key FROM search_configs
                    WHERE rotation_slot = slr.rotation_slot
                )
            JOIN raw_artifacts a ON a.run_id = slr.run_id
                AND a.artifact_type = 'results_page'
                AND a.search_key = j.search_key
                AND a.search_scope = j.scope
            LEFT JOIN srp_observations so ON so.artifact_id = a.artifact_id
                AND so.vin IS NOT NULL
            GROUP BY slr.rotation_slot
        )
        SELECT
            c.rotation_slot AS slot,
            c.search_keys,
            c.last_queued_at AT TIME ZONE 'America/Chicago' AS last_fired,
            ROUND(EXTRACT(EPOCH FROM (now() - c.last_queued_at)) / 3600, 1) AS hours_ago,
            COALESCE(slr.run_status, '-') AS last_status,
            COALESCE(res.pages, 0) AS pages,
            COALESCE(res.errors, 0) AS errors,
            COALESCE(res.vins_observed, 0) AS vins_observed,
            (c.last_queued_at + interval '1439 minutes') AT TIME ZONE 'America/Chicago' AS next_eligible,
            CASE
                WHEN c.last_queued_at IS NULL THEN 'Ready now'
                WHEN now() > c.last_queued_at + interval '1439 minutes' THEN 'Ready now'
                ELSE 'In ' || ROUND(EXTRACT(EPOCH FROM (c.last_queued_at + interval '1439 minutes' - now())) / 3600, 1)::text || 'h'
            END AS next_status
        FROM slot_configs c
        LEFT JOIN slot_last_run slr ON slr.rotation_slot = c.rotation_slot
        LEFT JOIN slot_results res ON res.rotation_slot = c.rotation_slot
        ORDER BY c.rotation_slot
    """)
    if not rotation_df.empty:
        st.dataframe(rotation_df, use_container_width=True, hide_index=True)
    else:
        st.info("No rotation slots configured.")

    # -- Table 2: Recent Detail Scrape Runs
    st.subheader("Recent Detail Scrape Runs")
    detail_runs_df = run_query("""
        SELECT
            r.started_at AT TIME ZONE 'America/Chicago' AS started,
            CASE
                WHEN r.finished_at IS NOT NULL
                THEN ROUND(EXTRACT(EPOCH FROM (r.finished_at - r.started_at)) / 60)::text || 'm'
                ELSE ROUND(EXTRACT(EPOCH FROM (now() - r.started_at)) / 60)::text || 'm (running)'
            END AS duration,
            r.status,
            r.total_count AS batch_size,
            COUNT(DISTINCT CASE WHEN a.artifact_type = 'detail_page' AND a.http_status = 200 THEN a.artifact_id END) AS detail_pages_ok,
            COUNT(DISTINCT CASE WHEN a.artifact_type = 'detail_page' AND (a.http_status != 200 OR a.http_status IS NULL) THEN a.artifact_id END) AS detail_errors,
            r.error_count AS job_errors,
            r.last_error
        FROM runs r
        LEFT JOIN raw_artifacts a ON a.run_id = r.run_id
        WHERE r.trigger = 'detail scrape'
          AND r.status != 'skipped'
        GROUP BY r.run_id, r.started_at, r.finished_at, r.status, r.total_count, r.error_count, r.last_error
        ORDER BY r.started_at DESC
        LIMIT 10
    """)
    if not detail_runs_df.empty:
        st.dataframe(detail_runs_df, use_container_width=True, hide_index=True)
    else:
        st.info("No detail scrape runs found.")

    # -- Last Detail Run Results
    st.subheader("Last Detail Run Results")
    last_detail_df = run_query("""
        WITH last_run AS (
            SELECT run_id, started_at, finished_at
            FROM runs
            WHERE trigger = 'detail scrape'
              AND status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
        ),
        run_obs AS (
            SELECT
                d.vin,
                d.listing_id,
                d.listing_state,
                d.price,
                d.dealer_name
            FROM detail_observations d
            JOIN last_run lr ON d.fetched_at BETWEEN lr.started_at AND lr.finished_at
        ),
        new_vins AS (
            SELECT ro.vin
            FROM run_obs ro
            WHERE ro.vin IS NOT NULL AND length(ro.vin) = 17
              AND NOT EXISTS (
                  SELECT 1 FROM detail_observations older
                  WHERE older.vin = ro.vin
                    AND older.fetched_at < (SELECT started_at FROM last_run)
              )
        )
        SELECT
            (SELECT COUNT(*) FROM run_obs) AS total_observed,
            (SELECT COUNT(*) FROM run_obs WHERE listing_state = 'unlisted') AS unlisted,
            (SELECT COUNT(*) FROM new_vins) AS new_vehicles,
            (SELECT started_at AT TIME ZONE 'America/Chicago' FROM last_run) AS run_started
    """)
    if not last_detail_df.empty and pd.notna(last_detail_df["run_started"].iloc[0]):
        r = last_detail_df.iloc[0]
        st.caption(f"Run started: {r['run_started'].strftime('%b %d %H:%M')}")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Observed", f"{int(r['total_observed']):,}")
        with col2:
            st.metric("Unlisted", f"{int(r['unlisted']):,}")
        with col3:
            st.metric("New Vehicles", f"{int(r['new_vehicles']):,}")

        # Breakdown of new vehicles by make/model
        new_by_model_df = run_query("""
            WITH last_run AS (
                SELECT run_id, started_at, finished_at
                FROM runs
                WHERE trigger = 'detail scrape'
                  AND status = 'success'
                ORDER BY started_at DESC
                LIMIT 1
            ),
            run_obs AS (
                SELECT d.vin
                FROM detail_observations d
                JOIN last_run lr ON d.fetched_at BETWEEN lr.started_at AND lr.finished_at
                WHERE d.vin IS NOT NULL AND length(d.vin) = 17
            ),
            new_vins AS (
                SELECT ro.vin
                FROM run_obs ro
                LEFT JOIN detail_observations older
                  ON older.vin = ro.vin
                  AND older.fetched_at < (SELECT started_at FROM last_run)
                WHERE older.vin IS NULL
            )
            SELECT
                a.make,
                a.model,
                COUNT(*) AS new_count
            FROM new_vins nv
            JOIN analytics.int_vehicle_attributes a ON a.vin = nv.vin
            INNER JOIN analytics.int_scrape_targets t ON t.make = a.make AND t.model = a.model
            GROUP BY a.make, a.model
            ORDER BY new_count DESC
        """)
        if not new_by_model_df.empty:
            st.dataframe(new_by_model_df, use_container_width=True, hide_index=True)
        else:
            st.info("No new vehicles in the last detail run.")
    else:
        st.info("No completed detail runs found.")

    # -- Stale backlog
    st.subheader("Stale Vehicle Backlog")
    df = run_query("""
        SELECT stale_reason, vehicle_count, avg_tier1_age_hours, avg_price_age_hours
        FROM (
            SELECT
                stale_reason,
                COUNT(*) AS vehicle_count,
                ROUND(AVG(tier1_age_hours)::numeric, 1) AS avg_tier1_age_hours,
                ROUND(AVG(price_age_hours)::numeric, 1) AS avg_price_age_hours
            FROM ops.ops_vehicle_staleness
            WHERE listing_state IS DISTINCT FROM 'unlisted'
            GROUP BY stale_reason
            UNION ALL
            SELECT
                'unmapped_carousel' AS stale_reason,
                COUNT(*) AS vehicle_count,
                NULL::numeric AS avg_tier1_age_hours,
                NULL::numeric AS avg_price_age_hours
            FROM analytics.int_carousel_price_events_unmapped
        ) combined
        ORDER BY vehicle_count DESC
    """)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # -- Price freshness distribution
    st.subheader("Price Freshness — Expiring in Next 24h")
    freshness_df = run_query("""
        SELECT
            CASE
                WHEN price_age_hours > 24   THEN 'Already stale'
                WHEN price_age_hours >= 21  THEN 'Expiring 0-3h'
                WHEN price_age_hours >= 18  THEN 'Expiring 3-6h'
                WHEN price_age_hours >= 15  THEN 'Expiring 6-9h'
                WHEN price_age_hours >= 12  THEN 'Expiring 9-12h'
                WHEN price_age_hours >= 9   THEN 'Expiring 12-15h'
                WHEN price_age_hours >= 6   THEN 'Expiring 15-18h'
                WHEN price_age_hours >= 3   THEN 'Expiring 18-21h'
                ELSE                             'Expiring 21-24h'
            END AS expiry_bucket,
            COUNT(*) FILTER (WHERE price_tier = 1 AND NOT is_full_details_stale) AS tier1,
            COUNT(*) FILTER (WHERE price_tier = 2 AND NOT is_full_details_stale) AS tier2,
            COUNT(*) FILTER (WHERE is_full_details_stale) AS full_details_stale,
            COUNT(*) AS total
        FROM ops.ops_vehicle_staleness
        GROUP BY 1
        ORDER BY MIN(price_age_hours) DESC
    """)
    if not freshness_df.empty:
        fig = px.bar(
            freshness_df, x="expiry_bucket", y=["tier1", "tier2", "full_details_stale"], barmode="stack",
            labels={"value": "VINs", "expiry_bucket": "Expires In"},
            color_discrete_map={"tier1": "#3498db", "tier2": "#95a5a6", "full_details_stale": "#e67e22"},
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

    # -- Row 3b: Search scrape success rate (daily)
    st.subheader("Search Scrape Success Rate (Last 7 Days)")
    df = run_query("""
        SELECT
            date_trunc('day', a.fetched_at AT TIME ZONE 'America/Chicago') AS day,
            CASE
                WHEN a.http_status = 200 THEN '200 OK'
                WHEN a.http_status = 403 THEN '403 Blocked'
                WHEN a.http_status IS NULL THEN 'Error/Timeout'
                ELSE a.http_status::text
            END AS result,
            COUNT(*) AS fetches
        FROM raw_artifacts a
        WHERE a.artifact_type = 'results_page'
          AND a.fetched_at > now() - interval '7 days'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    if not df.empty:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Pivot for stacked bars
        for result, color in [("200 OK", "#2ecc71"), ("403 Blocked", "#e74c3c"), ("Error/Timeout", "#95a5a6")]:
            subset = df[df["result"] == result]
            if not subset.empty:
                fig.add_trace(go.Bar(x=subset["day"], y=subset["fetches"], name=result,
                                     marker_color=color), secondary_y=False)

        # Success % line on secondary y-axis
        daily_totals = df.groupby("day")["fetches"].sum()
        daily_ok = df[df["result"] == "200 OK"].set_index("day")["fetches"]
        success_pct = (daily_ok / daily_totals * 100).fillna(0).reset_index()
        success_pct.columns = ["day", "pct"]
        fig.add_trace(go.Scatter(x=success_pct["day"], y=success_pct["pct"],
                                  name="Success %", mode="lines+markers",
                                  line=dict(color="white", width=2),
                                  marker=dict(size=6)), secondary_y=True)

        fig.update_layout(barmode="stack", xaxis_title=None, legend=dict(orientation="h", y=-0.15))
        fig.update_yaxes(title_text="Fetches", secondary_y=False)
        fig.update_yaxes(title_text="Success %", secondary_y=True, range=[0, 100])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No search scrape artifacts in the last 7 days.")

    # -- Search scrape jobs
    st.subheader("Search Scrape Jobs (Last 7 Days)")
    df = run_query("""
        SELECT
            r.run_id,
            r.started_at AT TIME ZONE 'America/Chicago' AS run_started,
            r.status AS run_status,
            j.search_key,
            j.scope,
            j.status AS job_status,
            j.artifact_count,
            j.retry_count,
            j.error
        FROM runs r
        JOIN scrape_jobs j ON j.run_id = r.run_id
        WHERE r.trigger = 'search scrape'
          AND r.started_at > now() - interval '7 days'
        ORDER BY r.started_at DESC, j.search_key, j.scope
    """)
    if df.empty:
        st.info("No search scrape jobs in the last 7 days.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

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
              AND status != 'skipped'
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

    st.subheader("dbt Build History")
    df = run_query("SELECT * FROM dbt_runs ORDER BY started_at DESC LIMIT 10")
    if df.empty:
        st.info("No dbt builds recorded yet.")
    else:
        last = df.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            ts = pd.to_datetime(last["started_at"]).tz_convert("America/Chicago")
            st.metric("Last Build", ts.strftime("%b %d %H:%M"))
        with col2:
            st.metric("Duration", f"{last['duration_s']:.0f}s" if pd.notna(last["duration_s"]) else "—")
        with col3:
            st.metric("Status", "✓ OK" if last["ok"] else "✗ Failed")
        with col4:
            st.metric("Models Passed", int(last["models_pass"]) if pd.notna(last["models_pass"]) else "—")

        display = df[["started_at", "duration_s", "ok", "intent", "models_pass", "models_error"]].copy()
        display["started_at"] = pd.to_datetime(display["started_at"]).dt.tz_convert("America/Chicago").dt.strftime("%b %d %H:%M")
        display["status"] = display["ok"].map({True: "✓ OK", False: "✗ Failed"})
        display = display[["started_at", "duration_s", "status", "intent", "models_pass", "models_error"]]
        display.columns = ["Time", "Duration (s)", "Status", "Intent", "Pass", "Error"]
        st.dataframe(display, use_container_width=True, hide_index=True)

    # -- Processor Activity
    st.subheader("Processor Activity")

    # Summary metrics
    proc_summary_df = run_query("""
        SELECT
            processor,
            COUNT(*) FILTER (WHERE status = 'ok') AS ok,
            COUNT(*) FILTER (WHERE status IN ('retry', 'processing')) AS pending,
            COUNT(*) FILTER (WHERE status = 'ok'
                AND message ILIKE '%cloudflare%') AS cloudflare_blocked,
            COUNT(*) FILTER (WHERE status = 'ok'
                AND meta->>'primary_json_present' = 'true') AS has_primary_data,
            MAX(processed_at) AT TIME ZONE 'America/Chicago' AS last_processed
        FROM artifact_processing
        GROUP BY processor
        ORDER BY processor
    """)
    if not proc_summary_df.empty:
        st.dataframe(proc_summary_df, use_container_width=True, hide_index=True)

    # Recent processing throughput (last 24h, hourly)
    st.caption("Processing throughput — last 24 hours")
    proc_hourly_df = run_query("""
        SELECT
            date_trunc('hour', processed_at AT TIME ZONE 'America/Chicago') AS hour,
            processor,
            COUNT(*) AS processed,
            COUNT(*) FILTER (WHERE status = 'ok') AS ok,
            COUNT(*) FILTER (WHERE status NOT IN ('ok')) AS errors
        FROM artifact_processing
        WHERE processed_at > now() - interval '24 hours'
        GROUP BY 1, 2
        ORDER BY 1 DESC, 2
    """)
    if not proc_hourly_df.empty:
        fig = px.bar(proc_hourly_df, x="hour", y="processed", color="processor",
                     barmode="group")
        fig.update_layout(xaxis_title=None, yaxis_title="Artifacts Processed",
                          legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No processing activity in the last 24 hours.")

    # Detail parser coverage
    st.caption("Detail parser data extraction coverage")
    proc_coverage_df = run_query("""
        SELECT
            date_trunc('day', ap.processed_at AT TIME ZONE 'America/Chicago') AS day,
            COUNT(*) AS total_processed,
            COUNT(*) FILTER (WHERE ap.meta->>'primary_json_present' = 'true') AS has_vehicle_data,
            COUNT(*) FILTER (WHERE ap.message ILIKE '%cloudflare%') AS cloudflare_blocked,
            COUNT(*) FILTER (WHERE ap.meta->>'primary_json_present' = 'false'
                AND (ap.message IS NULL OR ap.message NOT ILIKE '%cloudflare%')) AS no_data,
            ROUND(100.0 * COUNT(*) FILTER (WHERE ap.meta->>'primary_json_present' = 'true')
                / NULLIF(COUNT(*), 0), 1) AS extraction_pct
        FROM artifact_processing ap
        WHERE ap.processor LIKE 'cars_detail_page__%'
          AND ap.processed_at > now() - interval '14 days'
        GROUP BY 1
        ORDER BY 1 DESC
    """)
    if not proc_coverage_df.empty:
        st.dataframe(proc_coverage_df, use_container_width=True, hide_index=True)
    else:
        st.info("No detail processing data in the last 14 days.")

    st.subheader("Postgres Health")
    df_conn = run_query("""
        SELECT
            COUNT(*) FILTER (WHERE state = 'active') AS active,
            COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_tx,
            ROUND(MAX(CASE WHEN state = 'active' AND query_start IS NOT NULL
                          THEN EXTRACT(EPOCH FROM (now() - query_start)) END)::numeric, 1) AS longest_query_s
        FROM pg_stat_activity
        WHERE backend_type = 'client backend'
    """)
    if not df_conn.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Active Connections", int(df_conn["active"].iloc[0]))
        with col2:
            st.metric("Idle-in-Transaction", int(df_conn["idle_in_tx"].iloc[0]))
        with col3:
            val = df_conn["longest_query_s"].iloc[0]
            st.metric("Longest Query (s)", f"{val:.1f}" if pd.notna(val) else "0")

    df_slow = run_query("""
        SELECT
            pid,
            state,
            ROUND(EXTRACT(EPOCH FROM (now() - query_start))::numeric, 1) AS duration_s,
            LEFT(query, 80) AS query
        FROM pg_stat_activity
        WHERE state = 'active'
          AND query_start < now() - interval '5 seconds'
          AND backend_type = 'client backend'
        ORDER BY duration_s DESC
    """)
    if df_slow.empty:
        st.success("No long-running queries (>5s).")
    else:
        st.dataframe(df_slow, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Section 2: Inventory Overview
# ---------------------------------------------------------------------------
with tab2:
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
            date_trunc('day', first_seen_at AT TIME ZONE 'America/Chicago') AS day,
            make,
            COUNT(*) AS new_listings
        FROM analytics.mart_deal_scores
        WHERE COALESCE(listing_state, 'active') != 'unlisted'
          AND first_seen_at > now() - interval '30 days'
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
                upper(d.vin) AS vin,
                MIN(d.fetched_at) AS unlisted_at
            FROM detail_observations d
            WHERE d.listing_state = 'unlisted'
              AND d.vin IS NOT NULL
              AND length(d.vin) = 17
              AND d.fetched_at > now() - interval '30 days'
            GROUP BY upper(d.vin)
        )
        SELECT
            date_trunc('day', unlisted_at AT TIME ZONE 'America/Chicago') AS day,
            COUNT(*) AS vehicles_unlisted
        FROM first_unlisted
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
with tab3:
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

    # Build WHERE clauses with parameterized queries to prevent SQL injection
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
        """, params=query_params if query_params else None)
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
        """, params=query_params if query_params else None)
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
    """, params=query_params if query_params else None)
    if not df.empty:
        fig = px.bar(df, x="model", y=["avg_price", "avg_msrp"], barmode="group",
                     hover_data=["avg_msrp_off_pct", "listings"])
        fig.update_layout(xaxis_title=None, yaxis_title="Price ($)", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Section 4: Market Trends
# ---------------------------------------------------------------------------
with tab4:
    st.header("Market Trends")

    # -- Row 1: Median price by model over time (from int_price_events)
    st.subheader("Median Price by Model Over Time (Weekly, SRP Source)")
    df = run_query("""
        SELECT
            date_trunc('week', s.fetched_at AT TIME ZONE 'America/Chicago') AS week,
            s.make,
            s.model,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.price) AS median_price,
            COUNT(DISTINCT s.vin) AS listing_count
        FROM srp_observations s
        INNER JOIN analytics.int_scrape_targets t
            ON t.make = s.make AND t.model = s.model
        WHERE s.fetched_at > now() - interval '90 days'
          AND s.price > 0
          AND s.vin IS NOT NULL
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
            date_trunc('day', s.fetched_at AT TIME ZONE 'America/Chicago') AS day,
            s.make,
            s.model,
            COUNT(DISTINCT s.vin) AS listings_seen
        FROM srp_observations s
        INNER JOIN analytics.int_scrape_targets t
            ON t.make = s.make AND t.model = s.model
        WHERE s.fetched_at > now() - interval '30 days'
          AND s.vin IS NOT NULL
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
