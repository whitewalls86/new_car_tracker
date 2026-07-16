{#
  Plan 125 Gate B: adapter-dispatched spellings for the DuckDB-specific SQL the
  portability audit catalogued as F4/F5/F9/F10/F12.

  Why macros rather than per-target model files: one model file has to serve
  both targets through the whole dual-run period (Gate B -> Gate E), because the
  DuckDB build stays canonical and is the parity baseline. Forking the SQL would
  mean every future model change lands twice, and a parity failure could just be
  drift between two copies rather than a real engine difference.

  DuckDB is the incumbent spec: each spark__ implementation below reproduces
  what DuckDB *actually does*, which in three cases is NOT the obvious Spark
  translation. Every claim here was measured against both engines at Gate B --
  see docs/plan_125_portability_audit.md § "Gate B dialect measurements". Do not
  "simplify" these back to the obvious spelling; the obvious spelling is wrong.
#}


{#- ---------------------------------------------------------------------------
    F5: datediff with a unit argument.

    THE TRAP: DuckDB's datediff('hour', a, b) counts *hour boundaries crossed*,
    not elapsed hours. Measured:
        01:59 -> 02:01 (2 minutes)  = 1
        00:30 -> 03:10 (2h40m)      = 3
    The intuitive Spark translation, (unix_timestamp(b) - unix_timestamp(a))/3600,
    returns 0 and 2 for those -- wrong on 3 of 6 measured cases. It computes
    elapsed time, which is a different question.

    Truncating both operands to the hour first and then diffing reproduces the
    boundary count exactly (6/6 measured cases, negatives and nulls included).
    This feeds run_duration_hours and hours_until_change, which are real model
    features, so the difference would surface as silent feature drift rather
    than an error.
---------------------------------------------------------------------------- -#}

{% macro datediff_hours(start_ts, end_ts) %}
  {{ return(adapter.dispatch('datediff_hours', 'cartracker')(start_ts, end_ts)) }}
{% endmacro %}

{% macro default__datediff_hours(start_ts, end_ts) %}
  datediff('hour', {{ start_ts }}, {{ end_ts }})
{% endmacro %}

{% macro spark__datediff_hours(start_ts, end_ts) %}
  cast(
    (unix_timestamp(date_trunc('HOUR', {{ end_ts }}))
     - unix_timestamp(date_trunc('HOUR', {{ start_ts }}))) / 3600
    as bigint
  )
{% endmacro %}


{#- Spark's bare datediff(end, start) is days-only and, unlike the hour case,
    already matches DuckDB's day-boundary count exactly (4/4 measured cases,
    including negatives). Note the reversed argument order. -#}

{% macro datediff_days(start_ts, end_ts) %}
  {{ return(adapter.dispatch('datediff_days', 'cartracker')(start_ts, end_ts)) }}
{% endmacro %}

{% macro default__datediff_days(start_ts, end_ts) %}
  datediff('day', {{ start_ts }}, {{ end_ts }})
{% endmacro %}

{% macro spark__datediff_days(start_ts, end_ts) %}
  datediff({{ end_ts }}, {{ start_ts }})
{% endmacro %}


{#- ---------------------------------------------------------------------------
    F4: arg_max / arg_min.

    Spark spells these max_by/min_by, but they are NOT drop-in equivalents:
    DuckDB's arg_max ignores rows whose VALUE is null, while Spark's max_by
    happily returns a null value if that row won the ordering. Measured on
    values ((null,2),('b',1)): DuckDB arg_max -> 'b', Spark max_by -> NULL.

    The FILTER clause restores DuckDB's semantics (measured: -> 'b'), and an
    all-null group still yields NULL on both. This matters for
    int_listing_volatility_features' arg_max(customer_id, fetched_at), where
    customer_id is genuinely nullable.

    RESIDUAL, not fixed here: on a TIE in the ordering column the two engines
    pick different rows (DuckDB the first, Spark the last -- measured). Neither
    engine documents a guarantee, so the DuckDB model is already
    non-deterministic under ties; this port does not make that worse, but it
    does mean a tie can show up as a parity difference. The Gate B parity script
    reports these rather than hiding them.
---------------------------------------------------------------------------- -#}

{% macro arg_max(value_expr, order_expr) %}
  {{ return(adapter.dispatch('arg_max', 'cartracker')(value_expr, order_expr)) }}
{% endmacro %}

{% macro default__arg_max(value_expr, order_expr) %}
  arg_max({{ value_expr }}, {{ order_expr }})
{% endmacro %}

{% macro spark__arg_max(value_expr, order_expr) %}
  max_by({{ value_expr }}, {{ order_expr }}) filter (where {{ value_expr }} is not null)
{% endmacro %}


{% macro arg_min(value_expr, order_expr) %}
  {{ return(adapter.dispatch('arg_min', 'cartracker')(value_expr, order_expr)) }}
{% endmacro %}

{% macro default__arg_min(value_expr, order_expr) %}
  arg_min({{ value_expr }}, {{ order_expr }})
{% endmacro %}

{% macro spark__arg_min(value_expr, order_expr) %}
  min_by({{ value_expr }}, {{ order_expr }}) filter (where {{ value_expr }} is not null)
{% endmacro %}


{#- median(x). DuckDB has it natively; Spark spells the same thing
    percentile(x, 0.5). Both measured at 2.5 on (1,2,3,4) -- a true
    interpolating median, not a discrete one, on both sides. -#}

{% macro median_of(value_expr) %}
  {{ return(adapter.dispatch('median_of', 'cartracker')(value_expr)) }}
{% endmacro %}

{% macro default__median_of(value_expr) %}
  median({{ value_expr }})
{% endmacro %}

{% macro spark__median_of(value_expr) %}
  percentile({{ value_expr }}, 0.5)
{% endmacro %}


{#- ---------------------------------------------------------------------------
    F10/F12: casting a fractional number to int.

    THE TRAP: DuckDB's `x::int` ROUNDS; Spark's `cast(x as int)` TRUNCATES
    toward zero. On percentile_cont(0.10) over 1..10 both engines compute 1.9,
    then DuckDB yields 2 and Spark yields 1 -- the audit's predicted
    "one-dollar difference on every benchmark row", confirmed.

    DuckDB rounds a DOUBLE half-to-even (measured: 2.5->2, 3.5->4), which is
    Spark's bround(), NOT round() (round() is half-up and would give 2.5->3).
    cast(bround(x) as int) matched DuckDB on 7/7 measured cases.

    Only for fractional->int. Plain int->string and decimal(p,s) casts are
    already identical across both engines (measured), so those stay as literal
    `cast(... as ...)` in the models.
---------------------------------------------------------------------------- -#}

{% macro cast_to_int(value_expr) %}
  {{ return(adapter.dispatch('cast_to_int', 'cartracker')(value_expr)) }}
{% endmacro %}

{% macro default__cast_to_int(value_expr) %}
  cast({{ value_expr }} as int)
{% endmacro %}

{% macro spark__cast_to_int(value_expr) %}
  cast(bround({{ value_expr }}) as int)
{% endmacro %}


{#- ---------------------------------------------------------------------------
    F9: regex matching. DuckDB uses regexp_matches()/the Postgres `!~`
    operator; Spark uses RLIKE. Both return NULL (not false) on a null input,
    measured -- so the null-guard in stg_observations' CASE and in the
    valid_vin test behaves the same either way.
---------------------------------------------------------------------------- -#}

{% macro regex_matches(value_expr, pattern) %}
  {{ return(adapter.dispatch('regex_matches', 'cartracker')(value_expr, pattern)) }}
{% endmacro %}

{% macro default__regex_matches(value_expr, pattern) %}
  regexp_matches({{ value_expr }}, '{{ pattern }}')
{% endmacro %}

{% macro spark__regex_matches(value_expr, pattern) %}
  ({{ value_expr }} rlike '{{ pattern }}')
{% endmacro %}


{% macro regex_not_matches(value_expr, pattern) %}
  {{ return(adapter.dispatch('regex_not_matches', 'cartracker')(value_expr, pattern)) }}
{% endmacro %}

{% macro default__regex_not_matches(value_expr, pattern) %}
  {{ value_expr }} !~ '{{ pattern }}'
{% endmacro %}

{% macro spark__regex_not_matches(value_expr, pattern) %}
  not ({{ value_expr }} rlike '{{ pattern }}')
{% endmacro %}


{#- ---------------------------------------------------------------------------
    F11: casting the as_of_at backtest var to a timestamp.

    DuckDB uses ::timestamptz. Spark has no TIMESTAMPTZ at all -- its TIMESTAMP
    is instant-typed and resolves the literal's offset against
    spark.sql.session.timeZone, which spark_conf_for_dbt_session() pins to UTC.
    So `cast(x as timestamp)` is the equivalent ONLY because that pin exists;
    without it every backtest as_of boundary would shift by the host's offset.
---------------------------------------------------------------------------- -#}

{% macro cast_to_timestamptz(value_expr) %}
  {{ return(adapter.dispatch('cast_to_timestamptz', 'cartracker')(value_expr)) }}
{% endmacro %}

{% macro default__cast_to_timestamptz(value_expr) %}
  {{ value_expr }}::timestamptz
{% endmacro %}

{% macro spark__cast_to_timestamptz(value_expr) %}
  cast({{ value_expr }} as timestamp)
{% endmacro %}
