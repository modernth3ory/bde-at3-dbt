
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131








-- Prior to staging, SQL queries (namely date ranges) were employed in Postgres to determine if ranges were appropriate to meet the criteria.
---------------------------------------------------------------------------------------------------------------------------#

-- Records were cross-checked with existing statistics, to account for variance between year of census. 
-- ( https://www.abs.gov.au/statistics/people/population/national-state-and-territory-population/latest-release )

-- No records appeared wildly exaggerated or improbable, and thus were deemed appropriate for staging.












-------------------------------------------------------------------#
-- Staging | This model is used to stage the LGA data from the raw schema.
-------------------------------------------------------------------#



-- Step 0: Define the staging model for LGA data.
----------------------------------------------------------------------#

{{ config(
    materialized='view'
) }}



-- Step 1: Transformations for LGA data.
----------------------------------------------------------------------#

WITH transformed_lga AS (
    SELECT
        'LGA' || lga_code AS lga_code, 
        lga_name,
        ingestion_datetime
    FROM raw.lga
)



-- Step 2: Select the final result.
----------------------------------------------------------------------#

SELECT *
FROM transformed_lga



