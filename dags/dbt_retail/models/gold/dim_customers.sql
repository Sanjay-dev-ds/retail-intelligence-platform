{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_scd_id',
    tags=['gold', 'dimension']
  )
}}

with snapshot_data as (
  select
    user_id,
    dbt_scd_id,
    dbt_valid_from                                    as valid_from,
    coalesce(date(dbt_valid_to), date '9999-12-31')   as valid_to,
    (dbt_valid_to is null)                            as is_current,
    first_name, last_name, email, phone,
    gender, age, birth_date, role
  from {{ ref('scd_customers') }}
),

new_rows as (
  select snapshot_data.*
  from snapshot_data

  {% if is_incremental() %}
    left join {{ this }} existing
      on snapshot_data.dbt_scd_id = existing.dbt_scd_id
    where existing.dbt_scd_id is null
  {% endif %}
),

keyed as (
  select
    {% if is_incremental() %}
      (
        select coalesce(max(customer_key), 1000000)
        from {{ this }}
      )
      + row_number() over (order by valid_from, user_id)
    {% else %}
      1000000 + row_number() over (order by valid_from, user_id)
    {% endif %}                                       as customer_key,
    new_rows.*
  from new_rows
),

-- Capture existing rows that need to be closed out
-- i.e. they exist in dim but snapshot has since updated their valid_to
updated_rows as (

  {% if is_incremental() %}
  select
    existing.customer_key,                            -- preserve original key
    existing.user_id,
    existing.dbt_scd_id,
    existing.valid_from,
    snap.valid_to,                                    -- updated valid_to from snapshot
    snap.is_current,                                  -- will be false
    existing.first_name, existing.last_name, existing.email, existing.phone,
    existing.gender, existing.age, existing.birth_date, existing.role
  from {{ this }} existing
  inner join snapshot_data snap
    on existing.dbt_scd_id = snap.dbt_scd_id
  where existing.is_current = true
    and snap.is_current = false                       -- snapshot closed it out

  {% else %}
  select * from keyed where false             -- empty on full refresh
  {% endif %}

),

final as (
  select * from keyed
  union all
  select * from updated_rows
)

select * from final


/*
## What changed and why

The `updated_rows` CTE finds rows that:
- **Exist in `dim_customers`** with `is_current = true`
- **But the snapshot now shows** `is_current = false` (meaning a change was detected)

It re-emits those rows with the corrected `valid_to` and `is_current = false`. Because the merge key is `dbt_scd_id`, dbt will **update** the existing row in place rather than insert a duplicate.

**Flow for a changed record:**

Before:  customer_key=1000001, user_id=93, is_current=true,  valid_to=9999-12-31
After:   customer_key=1000001, user_id=93, is_current=false, valid_to=2024-02-27  ← updated
         customer_key=1000045, user_id=93, is_current=true,  valid_to=9999-12-31  ← new row

*/
