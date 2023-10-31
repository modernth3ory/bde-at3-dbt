{% snapshot property_snapshot %}

{{
  config(
    target_schema='raw',
    strategy='timestamp',
    unique_key='LISTING_ID',
    updated_at='SCRAPED_DATE'
  )
}}

select
  listing_id, 
  scraped_date, 
  listing_neighbourhood, 
  property_type, 
  ROOM_TYPE, 
  accommodates
from {{ source('raw', 'listings') }}

--  Previous inclusions

--  LISTING_ID,
--  SCRAPE_ID,
--  SCRAPED_DATE,
--  PROPERTY_TYPE,
--  LISTING_NEIGHBOURHOOD,
--  HOST_ID,
--  HOST_NAME,
--  HOST_SINCE,
--  HOST_IS_SUPERHOST,
--  HOST_NEIGHBOURHOOD,
--  PRICE,
--  HAS_AVAILABILITY,
--  AVAILABILITY_30,
--  NUMBER_OF_REVIEWS,
--  REVIEW_SCORES_RATING,
--  REVIEW_SCORES_ACCURACY,
--  REVIEW_SCORES_CLEANLINESS,
--  REVIEW_SCORES_CHECKIN,
--  REVIEW_SCORES_COMMUNICATION,
--  REVIEW_SCORES_VALUE


{% endsnapshot %}
