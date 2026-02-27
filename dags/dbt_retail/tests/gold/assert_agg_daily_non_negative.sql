-- Gold layer: aggregated revenue must be non-negative
select
  sale_date,
  total_amount_usd
from {{ ref("agg_daily_sales") }}
where total_amount_usd < 0
