with src as (
    select
        id as user_id,
        firstName as first_name,
        lastName as last_name,
        email,
        phone,
        gender,
        age,
        birthDate as birth_date,
        role,
        source,
        ingestion_ts
    from {{ source('retail_bronze', 'users') }}
)

select * from src

