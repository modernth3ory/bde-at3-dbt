
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131





-- Transform the 'dim_property' table.
-------------------------------------------------------------------#

{{
    config(
        materialized = 'table',
        unique_key = 'surrogate_property'
    )
}}



-- Extract "Current data" from the staging table.
-------------------------------------------------------------------#

WITH current_data AS (

    SELECT 
        listing_id,
        scraped_date,
        listing_neighbourhood, 
        property_type, 
        room_type, 
        accommodates,

        LEAD(scraped_date, 1, '9999-12-31') OVER (PARTITION BY listing_id ORDER BY scraped_date) AS ending_date,
        ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY scraped_date DESC) AS row_num

    FROM {{ ref('property_stg') }}
)



-- Select records with effective dates and flags.
-------------------------------------------------------------------#

SELECT 
    CONCAT(CAST(listing_id AS VARCHAR), '_', CAST(scraped_date AS VARCHAR)) as surrogate_property,
    listing_id,
    scraped_date,
    ending_date,
    listing_neighbourhood, 
    property_type, 
    room_type, 
    accommodates,

    CASE
        WHEN row_num = 1 THEN 1 
        ELSE 0 
    END AS flag

FROM current_data
