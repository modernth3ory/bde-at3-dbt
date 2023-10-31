
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131




-- Prior to staging, SQL queries were employed in Postgres to determine if ranges were appropriate to meet the criteria.
---------------------------------------------------------------------------------------------------------------------------#

-- Records were cross-checked with existing statistics, to account for variance between year of census. 
-- ( https://www.abs.gov.au/statistics/people/population/national-state-and-territory-population/latest-release )




-- By specifying the date ranges, filtering for improbable values that exceed the ranges were removed.
---------------------------------------------------------------------------------------------------------------------------#
-- Example:

--        CASE
--            WHEN host_since ~ '^\d{1,2}/\d{1,2}/\d{4}$' 
--            THEN TO_DATE(host_since, 'DD/MM/YYYY')
--            ELSE NULL
--        END AS host_since,

---------------------------------------------------------------------------------------------------------------------------#









-------------------------------------------------------------------#
-- Staging | This model is used to stage the host data from the raw schema.
-------------------------------------------------------------------#



-- Step 0: Define the staging model for host data.
----------------------------------------------------------------------#

{{ config(
    materialized='view'
) }}



-- Step 1: Omit duplicates.
----------------------------------------------------------------------#

WITH deduplicated_host AS (
    SELECT DISTINCT *
    FROM raw.host_snapshot
),



-- Step 2: Requesting variables from the host_snapshot. 
-- Handling invalid numeric values and flexible date formats.
----------------------------------------------------------------------#

host_filtered_and_cleaned AS (
    SELECT 
        host_id, 
        TO_DATE(scraped_date, 'YYYY-MM-DD') AS scraped_date,
        host_name, 

        CASE
            WHEN host_since ~ '^\d{1,2}/\d{1,2}/\d{4}$' 
            THEN TO_DATE(host_since, 'DD/MM/YYYY')
            ELSE NULL
        END AS host_since,

        CASE
            WHEN host_is_superhost = 'f' 
            THEN 0
            WHEN host_is_superhost = 't' 
            THEN 1
            ELSE NULL
        END AS host_is_superhost, 

        host_neighbourhood
    FROM deduplicated_host
)



-- Step 3: Select the final result.
----------------------------------------------------------------------#

SELECT 
    host_id, 
    scraped_date, 
    host_name, 
    host_since, 
    host_is_superhost, 
    host_neighbourhood
FROM host_filtered_and_cleaned
