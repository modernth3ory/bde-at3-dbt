
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131



-- Transform and maintain the 'dim_host' table.
-------------------------------------------------------------------#

{{
    config(
        materialized = 'table',
        unique_key = 'surrogate_host'
    )
}}



-- Step 1: Identify the current host data.
-------------------------------------------------------------------#

WITH current_data AS (
    SELECT 
        host_id,
        scraped_date,
        host_name, 
        host_since, 
        host_is_superhost, 
        host_neighbourhood,
        scraped_date AS starting_date,

        LEAD(scraped_date, 1, '9999-12-31') OVER (PARTITION BY host_id ORDER BY scraped_date) AS ending_date,
        ROW_NUMBER() OVER (PARTITION BY host_id ORDER BY scraped_date DESC) AS rn

    FROM {{ ref('host_stg') }}
)



-- Step 2: Select the final result.
-------------------------------------------------------------------#

SELECT 
    CONCAT(CAST(host_id AS VARCHAR), '_', CAST(scraped_date AS VARCHAR)) as surrogate_host,

    host_id,
    scraped_date,
    starting_date,
    ending_date,
    host_name, 
    host_since, 
    host_is_superhost, 
    host_neighbourhood,

    CASE
        WHEN rn = 1 THEN 1 
        ELSE 0 END AS flag

FROM current_data