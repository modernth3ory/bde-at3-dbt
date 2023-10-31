
-- University of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131

-- Step 0: Define the 'dim_residents' table.
-------------------------------------------------------------------#

{{
    config(
        materialized = 'table',
        unique_key = 'lga_code_2016'
    )
}}



-- Step 1: Select and transform data for 'dim_residents'.
-------------------------------------------------------------------#

SELECT
    ag.*,
    lga.LGA_name
FROM {{ ref('residents_stg') }} ag
JOIN {{ ref('lga_stg') }} lga ON ag.lga_code_2016 = lga.lga_code
