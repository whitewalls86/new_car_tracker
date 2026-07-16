{#
  Plan 125 Gate A: adapter-dispatched reader for MinIO-backed Parquet sources.

  Why this exists at all: the two adapters reach the same Parquet files by
  completely different mechanisms, and neither understands the other's.

    * dbt-duckdb resolves source() through meta.external_location -- a
      `read_parquet(...)` call registered as a view by the
      register_upstream_external_models() on-run-start hook. That macro ships
      with dbt-duckdb and does not exist on dbt-spark.
    * dbt-spark has no equivalent hook, and Spark has no read_parquet().
      The portable spelling is `parquet.`<path>``, which Spark's Parquet
      datasource reads directly (discovering Hive partitions itself).

  So on the `spark` target this renders the path form; on every other target
  it renders source() exactly as before, leaving the DuckDB production path
  byte-for-byte unchanged.

  Scope note: this is deliberately the smallest thing that works for the Gate
  A chain, not a general portability layer. It handles plain-Parquet sources
  only -- the postgres_scan() sources (audit F8) are an architectural problem,
  not a dialect one, and are out of scope until Gate B.
#}

{% macro spark_source_location(source_name, table_name) %}
  {#- Resolve meta.spark_external_location off the source node in the graph.
      Guarded by `execute`: at parse time the graph is not yet populated, and
      an unguarded lookup would raise before dbt can even build the DAG. -#}
  {%- if not execute -%}
    {{- return('') -}}
  {%- endif -%}

  {%- set unique_id = 'source.' ~ project_name ~ '.' ~ source_name ~ '.' ~ table_name -%}
  {%- set node = graph.sources.get(unique_id) -%}
  {%- if node is none -%}
    {{- exceptions.raise_compiler_error(
      "parquet_source(): no source node '" ~ unique_id ~ "' in the graph."
    ) -}}
  {%- endif -%}

  {%- set location = node.meta.get('spark_external_location') -%}
  {%- if not location -%}
    {{- exceptions.raise_compiler_error(
      "parquet_source(): source '" ~ unique_id ~ "' has no meta.spark_external_location, "
      ~ "so it cannot be read on the spark target. Add one (an s3a:// directory) to "
      ~ "dbt/models/sources.yml, or keep this model off the spark target."
    ) -}}
  {%- endif -%}

  {{- location -}}
{% endmacro %}


{% macro parquet_source(source_name, table_name) %}
  {%- if target.type == 'spark' -%}
    {#- Call source() anyway and discard the relation. It is never used in the
        rendered SQL, but calling it is what registers the model->source edge
        during parsing, so lineage, docs, and `--select source:...+` keep
        working on the spark target exactly as they do on duckdb. -#}
    {%- set _ = source(source_name, table_name) -%}
    parquet.`{{ spark_source_location(source_name, table_name) }}`
  {%- else -%}
    {{ source(source_name, table_name) }}
  {%- endif -%}
{% endmacro %}
