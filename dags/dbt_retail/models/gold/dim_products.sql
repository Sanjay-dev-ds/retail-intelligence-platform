{{
  config(
    materialized='table',
    tags=['gold', 'dimension']
  )
}}
with products as (
  select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    title,
    category,
    brand,
    price,
    discount_percentage,
    rating,
    stock
  from {{ ref('stg_products') }}
)
select * from products
