{{
  config(
    materialized='table',
    tags=['gold', 'dimension']
  )
}}
with order_dates as (
  select distinct date(order_date) as date_day
  from {{ ref('stg_orders') }}
),
spine as (
  select
    date_day,
    extract(year from date_day) as year_num,
    extract(month from date_day) as month_num,
    extract(day from date_day) as day_of_month,
    format_date('%A', date_day) as day_of_week_name,
    extract(dayofweek from date_day) as day_of_week_num,
    format_date('%Y-%m', date_day) as year_month
  from order_dates
)
select * from spine order by date_day
