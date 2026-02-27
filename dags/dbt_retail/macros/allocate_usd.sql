{% macro allocate_usd(line_amount, order_total_local, order_total_usd) -%}
  case
    when {{ order_total_local }} is null or {{ order_total_local }} = 0 then 0
    else round(({{ line_amount }} / {{ order_total_local }}) * {{ order_total_usd }}, 2)
  end
{%- endmacro %}
