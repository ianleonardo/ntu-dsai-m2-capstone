{% macro transaction_code_type_label(column) %}
CASE UPPER(TRIM(CAST({{ column }} AS STRING)))
    WHEN 'P' THEN 'Purchase'
    WHEN 'S' THEN 'Sale'
    ELSE NULL
END
{% endmacro %}
