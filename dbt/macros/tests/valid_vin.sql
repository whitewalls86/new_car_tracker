{% test valid_vin(model, column_name) %}

select *
from {{ model }}
{#- Plan 125 Gate B (audit F9): the `!~` operator is Postgres/DuckDB-only;
    Spark spells it NOT ... RLIKE. Routed through regex_not_matches() because a
    test macro fails at TEST time, not build time -- so on the spark target this
    would have looked like a clean port right up until `dbt test`. -#}
where {{ column_name }} is null
   or length({{ column_name }}) <> 17
   or {{ regex_not_matches(column_name, '^[A-Z0-9]{17}$') }}

{% endtest %}
