-- Singular test: fail if any order line has non-positive quantity.
-- dbt expects this query to return zero rows.
select
  order_item_id,
  order_id,
  quantity
from {{ ref('fct_sales') }}
where quantity is null or quantity <= 0
