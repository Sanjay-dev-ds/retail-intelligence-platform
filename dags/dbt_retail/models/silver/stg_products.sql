with src as (
    select
        id as product_id,
        title,
        category,
        brand,
        price,
        discountPercentage as discount_percentage,
        rating,
        stock,
        source,
        ingestion_ts
    from {{ source('retail_bronze', 'products') }}
)

select * from src

