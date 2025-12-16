{% macro to_nz(ts_expr) %}
  convert_timezone('UTC', 'Pacific/Auckland', ({{ ts_expr }}::timestamp_ntz))::timestamp_ntz
{% endmacro %}
