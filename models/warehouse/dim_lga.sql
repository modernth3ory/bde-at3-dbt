
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131



-- Transform and maintain the 'dim_lga' table.
-------------------------------------------------------------------#

{{
    config(
        materialized = 'table',
        unique_key = 'lga_code'
    )
}}



-- Step 1: Generate 'dim_lga' records.
-------------------------------------------------------------------#

SELECT 
    lga_code, 
    lga_name

FROM {{ ref('lga_stg') }}

