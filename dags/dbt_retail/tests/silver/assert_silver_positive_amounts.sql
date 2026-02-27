-- Silver layer: no negative or zero amounts in stg_orders / stg_order_items
select * from (
  select
    "stg_orders" as model_name,
    order_id as key,
    total_amount_usd as amount
  from {{ ref("stg_orders") }}
  where total_amount_usd is not null and total_amount_usd <= 0
  union all
  select
    "stg_order_items" as model_name,
    order_item_id as key,
    final_price as amount
  from {{ ref("stg_order_items") }}
  where final_price is not null and final_price <= 0
)