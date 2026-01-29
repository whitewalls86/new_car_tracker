{% test valid_vin(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is null
   or length({{ column_name }}) <> 17
   or {{ column_name }} !~ '^[A-Z0-9]{17}$'

{% endtest %}
