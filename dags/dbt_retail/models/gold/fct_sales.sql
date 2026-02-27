{{
  config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='merge',
    merge_update_columns=['order_id', 'user_id', 'product_id', 'category', 'order_date', 'order_status', 'currency', 'quantity', 'unit_price', 'discount_pct', 'final_price_local', 'final_price_usd', 'ingestion_ts'],
    tags=['gold', 'fact']
  )
}}
with orders as (
  select
    order_id,
    user_id,
    order_date,
    total_amount_local,
    total_amount_usd,
    order_status,
    currency,
    ingestion_ts
  from {{ ref('stg_orders') }}
  {% if is_incremental() %}
  where ingestion_ts > (select coalesce(max(ingestion_ts), timestamp('1900-01-01')) from {{ this }})
  {% endif %}
),
items as (
  select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    final_price,
    ingestion_ts
  from {{ ref('stg_order_items') }}
  {% if is_incremental() %}
  where ingestion_ts > (select coalesce(max(ingestion_ts), timestamp('1900-01-01')) from {{ this }})
  {% endif %}
),
customers as (
  select
    user_id,
    customer_key
  from {{ ref('dim_customers') }}
),
products as (
  select
    product_id,
    product_key,
    category
  from {{ ref('dim_products') }}
),
order_totals as (
  select
    order_id,
    sum(final_price) as order_line_total_local
  from {{ ref('stg_order_items') }}
  group by order_id
),
joined as (
  select
    i.order_item_id,
    i.order_id,
    o.user_id,
    c.customer_key,
    i.product_id,
    p.product_key,
    p.category,
    o.order_date,
    o.order_status,
    o.currency,
    i.quantity,
    i.unit_price,
    i.discount_pct,
    i.final_price as final_price_local,
    {{ allocate_usd('i.final_price', 't.order_line_total_local', 'o.total_amount_usd') }} as final_price_usd,
    coalesce(o.ingestion_ts, i.ingestion_ts) as ingestion_ts
  from items i
  join orders o on i.order_id = o.order_id
  left join order_totals t on i.order_id = t.order_id
  left join customers c on o.user_id = c.user_id
  left join products p on i.product_id = p.product_id
)
select * from joined
