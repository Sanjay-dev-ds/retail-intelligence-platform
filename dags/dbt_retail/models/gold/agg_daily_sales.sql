{{
  config(
    materialized='incremental',
    unique_key='sale_date',
    incremental_strategy='merge',
    tags=['gold', 'aggregation']
  )
}}
with fct as (
  select
    date(order_date) as sale_date,
    order_id,
    quantity,
    final_price_usd
  from {{ ref('fct_sales') }}
  {% if is_incremental() %}
  where date(order_date) >= date_sub((select max(sale_date) from {{ this }}), interval 1 day)
  {% endif %}
)
select
  sale_date,
  count(distinct order_id) as total_orders,
  sum(quantity) as total_items,
  round(sum(final_price_usd), 2) as total_amount_usd
from fct
group by sale_date
order by sale_date
