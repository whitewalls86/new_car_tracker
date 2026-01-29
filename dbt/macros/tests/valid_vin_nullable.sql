{% test valid_vin_nullable(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and (
    length({{ column_name }}) <> 17
    or upper({{ column_name }}) !~ '^[A-Z0-9]{17}$'
  )

{% endtest %}
