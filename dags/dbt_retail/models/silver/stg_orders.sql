with src as (
    select
        order_id,
        user_id,
        order_date,
        currency,
        total_amount_local,
        total_amount_usd,
        discount_amount,
        payment_method,
        order_status,
        source,
        ingestion_ts
    from {{ source('retail_bronze', 'orders') }}
)

select * from src

