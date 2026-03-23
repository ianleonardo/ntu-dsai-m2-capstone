{% test non_negative_or_null(model, column_name) %}
  {# Fails rows where the value parses as a number and is < 0. NULL / non-numeric / blank pass. #}
  select *
  from {{ model }}
  where safe_cast({{ column_name }} as float64) is not null
    and safe_cast({{ column_name }} as float64) < 0
{% endtest %}
