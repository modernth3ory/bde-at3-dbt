
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131



-- Transform and maintain the 'dim_lga' table.
-------------------------------------------------------------------#

{{
    config(
        materialized = 'table',
        unique_key = 'suburb_name'
    )
}}



-- Step 1: Generate 'dim_lga' records with start and end dates.
-------------------------------------------------------------------#

SELECT *

FROM {{ ref('suburb_stg') }}
