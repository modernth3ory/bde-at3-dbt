
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131


{% snapshot host_snapshot %}

{{
  config(
    target_schema='raw',
    strategy='timestamp',
    unique_key='HOST_ID',
    updated_at='SCRAPED_DATE'
  )
}}

select
  HOST_ID,
  SCRAPED_DATE,
  HOST_NAME,
  HOST_SINCE,
  HOST_IS_SUPERHOST,
  HOST_NEIGHBOURHOOD
from {{ source('raw', 'listings') }}

{% endsnapshot %}




