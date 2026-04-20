import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from db import run_query


def render():
    st.header("Pipeline Health")

    # -- Active run indicator ------------------------------------------------
    active_runs_df = run_query("""
        SELECT r.trigger, r.started_at AT TIME ZONE 'America/Chicago' AS started_at,
               ROUND(EXTRACT(EPOCH FROM now() - r.started_at) / 60) AS elapsed_min,
               r.progress_count, r.total_count,
               CASE WHEN r.total_count > 0
                    THEN ROUND(r.progress_count::numeric / 
                               (EXTRACT(EPOCH FROM now() - r.started_at) / 60), 1)
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

    # -- dbt build status ----------------------------------------------------
    dbt_lock_df = run_query("""
        SELECT 
            locked, 
            locked_at AT TIME ZONE 'America/Chicago' AS locked_at, 
            locked_by 
        FROM dbt_lock 
        WHERE id = 1""")
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

def _section_rotation_schedule():
    st.subheader("Search Scrape Rotation Schedule")
    df = run_query("""
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
            (c.last_queued_at + interval '1439 minutes') 
                   AT TIME ZONE 'America/Chicago' AS next_eligible,
            CASE
                WHEN c.last_queued_at IS NULL THEN 'Ready now'
                WHEN now() > c.last_queued_at + interval '1439 minutes'
                    THEN 'Ready now'
                ELSE 'In ' || ROUND(
                    EXTRACT(EPOCH FROM (
                        c.last_queued_at + interval '1439 minutes' - now()
                    )) / 3600, 1
                )::text || 'h'
            END AS next_status
        FROM slot_configs c
        LEFT JOIN slot_last_run slr ON slr.rotation_slot = c.rotation_slot
        LEFT JOIN slot_results res ON res.rotation_slot = c.rotation_slot
        ORDER BY c.rotation_slot
    """)
    if not df.empty:
        st.dataframe(df, width="stretch", hide_index=True)
    else:
        st.info("No rotation slots configured.")


def _section_detail_runs():
    st.subheader("Recent Detail Scrape Runs")
    df = run_query("""
        With my_runs AS (
            SELECT 
                *
            FROM
                runs
            WHERE
                trigger = 'detail scrape'
            ORDER BY started_at DESC
            LIMIT 20
        ), price_min AS (
			SELECT vin, MIN(observed_at) as min_observed_at
			FROM analytics.int_price_events
			GROUP BY vin
		), filtered_artifacts AS (
			SELECT ra.*
			FROM raw_artifacts ra
			JOIN my_runs r USING (run_id)
		)
        SELECT
            r.started_at AT TIME ZONE 'America/Chicago' AS started,
            CASE
                WHEN r.finished_at IS NOT NULL
                THEN ROUND(EXTRACT(EPOCH FROM (r.finished_at - r.started_at)) / 60)::text || 'm'
                ELSE ROUND(EXTRACT(EPOCH FROM (now() - r.started_at)) / 60)::text || 'm (running)'
            END AS duration,
            r.status,
            r.total_count AS batch_size,
            r.error_count as num_errors,
            COUNT(DISTINCT d.vin) FILTER (WHERE d.price IS NOT NULL) AS prices_refreshed,
            COUNT(DISTINCT ra.artifact_id) 
                FILTER (WHERE d.listing_state = 'unlisted') AS newly_unlisted,
            COUNT(DISTINCT ra.artifact_id) 
                FILTER (
                   WHERE ap.message = 'unlisted' 
                   AND d.artifact_id IS NULL
                   ) AS unlisted_carousel_hit,
            COUNT(DISTINCT d.vin17) FILTER (WHERE pe.vin IS NULL) AS newly_mapped_vins
        FROM
            my_runs r
        LEFT JOIN filtered_artifacts ra on r.run_id = ra.run_id
        LEFT JOIN artifact_processing ap ON ra.artifact_id = ap.artifact_id
        LEFT JOIN analytics.stg_detail_observations d on ra.artifact_id = d.artifact_id
        LEFT JOIN price_min pe on d.vin = pe.vin AND pe.min_observed_at <= r.started_at
        GROUP BY 
            r.run_id, 
            r.started_at, 
            r.finished_at, 
            r.status, 
            r.total_count, 
            r.error_count, 
            r.last_error
        ORDER BY started DESC;
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No detail scrape runs found.")



def _section_stale_backlog():
    left_col, right_col = st.columns([1, 1])

    df_stale = run_query("""
        WITH batch_marking AS (
            SELECT 
                q.listing_id,
                q.stale_reason,
                ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY q.priority, q.listing_id) as priority_row
            FROM ops.ops_detail_scrape_queue q
            LEFT JOIN detail_scrape_claims c
                ON c.listing_id = q.listing_id::uuid AND c.status = 'running'
            WHERE c.listing_id IS NULL
        ), first_part AS (
            SELECT
                CASE 
                    WHEN priority_row < 601 THEN '00_next_batch' 
                    WHEN priority_row < 1201 THEN '01_following_batch'
                    WHEN priority_row < 1801 THEN '02_third_batch'
                    ELSE '03_backlog' END as batch_param,
                COUNT(*) FILTER (WHERE stale_reason LIKE 'price_only%')::varchar as price_only,
                COUNT(*) FILTER (
                            WHERE stale_reason LIKE 'force_stale_36h%'
                         )::varchar as force_stale,
                COUNT(*) FILTER (WHERE stale_reason LIKE 'full_details%')::varchar AS full_details,
                COUNT(*) FILTER (
                            WHERE stale_reason LIKE 'unmapped_carousel'
                         )::varchar as unmapped_carousel,
                COUNT(*) FILTER (
                            WHERE stale_reason LIKE 'dealer_unenriched'
                         )::varchar as dealer_unenriched,
                COUNT(*)::varchar AS total_count
            FROM batch_marking q
            GROUP BY 1
			ORDER BY batch_param ASC
        ), second_part AS (
            SELECT
                'Grand Total' as batch_param,
                COUNT(*) FILTER (WHERE stale_reason LIKE 'price_only%')::varchar as price_only,
                COUNT(*) FILTER (
                            WHERE stale_reason LIKE 'force_stale_36h%'
                         )::varchar as force_stale,
                COUNT(*) FILTER (WHERE stale_reason LIKE 'full_details%')::varchar AS full_details,
                COUNT(*) FILTER (
                            WHERE stale_reason LIKE 'unmapped_carousel%'
                         )::varchar as unmapped_carousel,
                COUNT(*) FILTER (
                            WHERE stale_reason LIKE 'dealer_unenriched%'
                         )::varchar as dealer_unenriched,
                COUNT(*)::varchar AS total_count
            FROM batch_marking q
            GROUP BY 1
        )
        SELECT * FROM first_part
        UNION ALL
        SELECT 
            '----------' as batch_param, 
            '----------' as price_only,
            '----------' as force_stale,
            '----------' as full_details,
            '----------' as unmapped_carousel,
            '----------' as dealer_unenriched,
            '----------' as total_count
        UNION ALL       
        SELECT * FROM second_part
    """)

    df_cooldown = run_query("""
        WITH batch_marking AS (
            SELECT 
                q.listing_id,
                q.stale_reason,
                ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY q.priority, q.listing_id) as priority_row
            FROM ops.ops_detail_scrape_queue q
            LEFT JOIN detail_scrape_claims c
                ON c.listing_id = q.listing_id::uuid AND c.status = 'running'
            WHERE c.listing_id IS NULL
        )
        SELECT
            bc.num_of_attempts
            ,MIN(bc.next_eligible_at) FILTER (
                                        WHERE bc.next_eligible_at > now() 
                                      ) AT TIME ZONE 'America/Chicago' as next_attempt_at
            ,COUNT(bc.listing_id) as num_listings
            ,COUNT(bc.listing_id) FILTER (
                                WHERE bc.next_eligible_at < now() 
                                AND ovs.stale_reason != 'not_stale'
                            ) as eligible_now
            ,COUNT(bc.listing_id) FILTER (
                                    WHERE q.priority_row < 601 AND q.priority_row IS NOT NULL
                                    ) as num_in_next_batch
        FROM
            analytics.stg_blocked_cooldown bc
        LEFT JOIN batch_marking q ON q.listing_id = bc.listing_id
        LEFT JOIN ops.ops_vehicle_staleness ovs ON bc.listing_id = ovs.listing_id
        GROUP BY
            bc.num_of_attempts
        ORDER BY
            bc.num_of_attempts
    """)

    with left_col:
        st.subheader("Stale Vehicle Backlog")
        st.dataframe(df_stale, width="stretch", hide_index=True)

    with right_col:
        st.subheader("Cooldown Backlog")
        st.dataframe(df_cooldown, width="stretch", hide_index=True)



def _section_price_freshness():
    st.subheader("Price Freshness — Expiring in Next 24h")
    df = run_query("""
        WITH buckets AS (
            SELECT
                FLOOR(LEAST(vs.price_age_hours, 24) * 2) / 2 AS age_floor,
                vs.price_tier,
                vs.is_full_details_stale
            FROM ops.ops_vehicle_staleness vs
            LEFT JOIN analytics.stg_blocked_cooldown bc
                   ON bc.listing_id = vs.listing_id
            WHERE vs.price_age_hours IS NOT NULL
                  AND bc.listing_id IS NULL
        )
        SELECT
            (24 - age_floor)::numeric AS hours_until_stale,
            TO_CHAR((24 - age_floor)::numeric, 'FM90.0') || 'h' AS expiry_bucket,
            COUNT(*) FILTER (WHERE price_tier = 1 AND NOT is_full_details_stale) AS tier1,
            COUNT(*) FILTER (WHERE price_tier = 2 AND NOT is_full_details_stale) AS tier2,
            COUNT(*) FILTER (WHERE is_full_details_stale) AS full_details_stale,
            COUNT(*) AS total
        FROM buckets
        GROUP BY age_floor
        ORDER BY age_floor DESC
    """)
    if not df.empty:
        df = df.sort_values("hours_until_stale")
        fig = px.bar(
            df, x="expiry_bucket", y=["tier1", "tier2", "full_details_stale"], barmode="stack",
            labels={"value": "VINs", "expiry_bucket": "Expires In"},
            color_discrete_map={
                "tier1": "#3498db", 
                "tier2": "#95a5a6", 
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
    df = run_query("""
        WITH buckets AS (
            SELECT
                FLOOR(
                    GREATEST(
                        (EXTRACT(EPOCH FROM (next_eligible_at - now())) / 3600),
                        0
                    ) / 2
                ) * 2 AS age_floor
            FROM analytics.stg_blocked_cooldown
            WHERE next_eligible_at IS NOT NULL
        )
        SELECT
            age_floor::numeric AS hours_until_eligible,
            TO_CHAR(age_floor::numeric, 'FM90.0') || 'h' AS eligible_bucket,
            COUNT(*) AS total
        FROM buckets
        GROUP BY age_floor
        ORDER BY age_floor DESC
    """)
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
    df = run_query(f"""
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
        WHERE artifact_type = '{artifact_type}'
          AND fetched_at > now() - interval '{interval}'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
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
    df = run_query("""
        SELECT
            r.run_id,
            r.started_at AT TIME ZONE 'America/Chicago' AS run_started,
            r.status AS run_status,
            j.search_key, j.scope, j.status AS job_status,
            j.artifact_count,
            COUNT(srp.vin) as vins_recorded,
            COUNT(srp.vin) FILTER (WHERE pe.vin IS NULL) as new_vins_recorded
        FROM runs r
        JOIN scrape_jobs j ON j.run_id = r.run_id
        LEFT JOIN raw_artifacts ra 
            on j.scope = ra.search_scope and ra.run_id = r.run_id and ra.search_key = j.search_key
        LEFT JOIN analytics.stg_srp_observations srp on ra.artifact_id = srp.artifact_id
        LEFT JOIN ( 
            SELECT vin, min(observed_at) as first_seen 
            FROM  analytics.int_price_events group by vin
        ) pe ON srp.vin17 = pe.vin AND pe.first_seen < r.started_at
        WHERE r.trigger = 'search scrape'
          AND r.started_at > now() - interval '7 days'
        GROUP BY
            1,2,3,4,5,6,7
        ORDER BY r.started_at DESC, j.search_key, j.scope;
    """)
    if df.empty:
        st.info("No search scrape jobs in the last 7 days.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def _section_runs_over_time():
    st.subheader("Runs Over Time")
    df = run_query("""
        SELECT
            date_trunc('day', started_at AT TIME ZONE 'America/Chicago') AS day,
            trigger, COUNT(*) AS runs,
            COUNT(*) FILTER (WHERE status = 'success') AS successful,
            COUNT(*) FILTER (WHERE status = 'terminated') AS terminated,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed
        FROM runs
        WHERE started_at > now() - interval '7 days' AND status NOT IN ('skipped', 'terminated')
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    if not df.empty:
        fig = px.bar(df, x="day", y="runs", color="trigger", barmode="group")
        fig.update_layout(xaxis_title=None, yaxis_title="Runs", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)


def _section_artifact_backlog():
    st.subheader("Artifact Processing Backlog")
    df = run_query("""
        SELECT processor, status, COUNT(*) AS count,
               MIN(processed_at) AT TIME ZONE 'America/Chicago' AS oldest
        FROM artifact_processing
        WHERE status IN ('retry', 'processing')
        GROUP BY processor, status ORDER BY count DESC
    """)
    st.dataframe(df, use_container_width=True, hide_index=True)


def _section_terminated_runs():
    st.subheader("Terminated Runs (Last 7 Days)")
    df = run_query("""
        SELECT trigger, COUNT(*) AS terminated_count,
               MAX(started_at) AT TIME ZONE 'America/Chicago' AS most_recent
        FROM runs
        WHERE status = 'terminated' AND started_at > now() - interval '7 days'
        GROUP BY trigger ORDER BY terminated_count DESC
    """)
    if df.empty:
        st.success("No terminated runs in the last 7 days.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def _section_pipeline_errors():
    st.subheader("Recent Pipeline Errors")
    df = run_query("""
        SELECT occurred_at AT TIME ZONE 'America/Chicago' AS occurred_at_ct,
               workflow_name, node_name, error_type, error_message
        FROM pipeline_errors ORDER BY occurred_at DESC LIMIT 50
    """)
    if df.empty:
        st.success("No pipeline errors recorded.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def _section_dbt_history():
    st.subheader("dbt Build History")
    df = run_query("SELECT * FROM dbt_runs ORDER BY started_at DESC LIMIT 10")
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
    proc_summary_df = run_query("""
        SELECT processor,
               COUNT(*) FILTER (WHERE status = 'ok') AS ok,
               COUNT(*) FILTER (
                            WHERE status IN ('retry', 'processing')
                        ) AS pending,
               COUNT(*) FILTER (
                            WHERE status = 'ok' AND message ILIKE '%cloudflare%'
                        ) AS cloudflare_blocked,
               COUNT(*) FILTER (
                            WHERE status = 'ok' AND meta->>'primary_json_present' = 'true'
                            ) AS has_primary_data,
               MAX(processed_at) AT TIME ZONE 'America/Chicago' AS last_processed
        FROM artifact_processing GROUP BY processor ORDER BY processor
    """)
    if not proc_summary_df.empty:
        st.dataframe(proc_summary_df, use_container_width=True, hide_index=True)

    st.caption("Processing throughput — last 24 hours")
    proc_hourly_df = run_query("""
        SELECT date_trunc('hour', processed_at AT TIME ZONE 'America/Chicago') AS hour,
               processor, COUNT(*) AS processed,
               COUNT(*) FILTER (WHERE status = 'ok') AS ok,
               COUNT(*) FILTER (WHERE status NOT IN ('ok')) AS errors
        FROM artifact_processing
        WHERE processed_at > now() - interval '24 hours'
        GROUP BY 1, 2 ORDER BY 1 DESC, 2
    """)
    if not proc_hourly_df.empty:
        fig = px.bar(proc_hourly_df, x="hour", y="processed", color="processor", barmode="group")
        fig.update_layout(xaxis_title=None, yaxis_title="Artifacts Processed", legend_title=None)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No processing activity in the last 24 hours.")

    st.caption("Detail parser data extraction coverage")
    proc_coverage_df = run_query("""
        SELECT date_trunc('day', ap.processed_at AT TIME ZONE 'America/Chicago') AS day,
               COUNT(*) AS total_processed,
               COUNT(*) FILTER (
                            WHERE ap.meta->>'primary_json_present' = 'true'
                        ) AS has_vehicle_data,
               COUNT(*) FILTER (WHERE ap.message LIKE '%403%') AS cloudflare_blocked,
               COUNT(*) FILTER (WHERE ap.meta->>'primary_json_present' = 'false'
                   AND (ap.message IS NULL OR ap.message NOT ILIKE '%cloudflare%')) AS no_data,
               ROUND(100.0 * COUNT(*) FILTER (WHERE ap.meta->>'primary_json_present' = 'true')
                   / NULLIF(COUNT(*), 0), 1) AS extraction_pct
        FROM artifact_processing ap
        WHERE ap.processor LIKE 'cars_detail_page__%'
          AND ap.processed_at > now() - interval '14 days'
        GROUP BY 1 ORDER BY 1 DESC
    """)
    if not proc_coverage_df.empty:
        st.dataframe(proc_coverage_df, use_container_width=True, hide_index=True)
    else:
        st.info("No detail processing data in the last 14 days.")


def _section_postgres_health():
    st.subheader("Postgres Health")
    df_conn = run_query("""
        SELECT COUNT(*) FILTER (WHERE state = 'active') AS active,
               COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_tx,
               ROUND(MAX(
                    CASE 
                        WHEN state = 'active' AND query_start IS NOT NULL
                            THEN EXTRACT(EPOCH FROM (now() - query_start)) 
                    END
                )::numeric, 1) AS longest_query_s
        FROM pg_stat_activity WHERE backend_type = 'client backend'
    """)
    if not df_conn.empty:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Active Connections", int(df_conn["active"].iloc[0]))
        with c2:
            st.metric("Idle-in-Transaction", int(df_conn["idle_in_tx"].iloc[0]))
        with c3:
            val = df_conn["longest_query_s"].iloc[0]
            st.metric("Longest Query (s)", f"{val:.1f}" if pd.notna(val) else "0")

    df_slow = run_query("""
        SELECT pid, state,
               ROUND(EXTRACT(EPOCH FROM (now() - query_start))::numeric, 1) AS duration_s,
               LEFT(query, 80) AS query
        FROM pg_stat_activity
        WHERE state = 'active' AND query_start < now() - interval '5 seconds'
          AND backend_type = 'client backend'
        ORDER BY duration_s DESC
    """)
    if df_slow.empty:
        st.success("No long-running queries (>5s).")
    else:
        st.dataframe(df_slow, use_container_width=True, hide_index=True)
