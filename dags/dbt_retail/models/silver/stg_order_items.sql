with src as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_pct,
        final_price,
        order_date,
        source,
        ingestion_ts
    from {{ source('retail_bronze', 'order_items') }}
)

select * from src

