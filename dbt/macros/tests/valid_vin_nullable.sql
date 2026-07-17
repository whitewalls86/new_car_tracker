{% test valid_vin_nullable(model, column_name) %}

select *
from {{ model }}
{#- Plan 125 Gate B (audit F9): see valid_vin.sql -- `!~` is not Spark SQL. -#}
where {{ column_name }} is not null
  and (
    length({{ column_name }}) <> 17
    or {{ regex_not_matches('upper(' ~ column_name ~ ')', '^[A-Z0-9]{17}$') }}
  )

{% endtest %}
