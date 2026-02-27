{{
  config(
    materialized='table',
    tags=['gold', 'dimension']
  )
}}
with customers as (
  select
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as customer_key,
    user_id,
    first_name,
    last_name,
    email,
    phone,
    gender,
    age,
    birth_date,
    role
  from {{ ref('stg_users') }}
)
select * from customers
