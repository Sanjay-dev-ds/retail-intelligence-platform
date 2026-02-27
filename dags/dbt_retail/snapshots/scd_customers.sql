{% snapshot scd_customers %}
{{
  config(
    target_schema='snapshots',
    strategy='check',
    unique_key='user_id',
    check_cols=['first_name', 'last_name', 'email', 'phone', 'gender', 'age', 'birth_date', 'role'],
    invalidate_hard_deletes=True
  )
}}

select
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
{% endsnapshot %}