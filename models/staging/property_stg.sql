
-- Universtiy of Technology Sydney
-- Big Data Engineering, 2023

-- Nathan Collins
-- 12062131








-- Prior to staging, SQL queries were employed in Postgres to determine if ranges were appropriate to meet the criteria.
---------------------------------------------------------------------------------------------------------------------------#

-- Records were cross-checked with existing statistics, to account for variance between year of census. 
-- ( https://www.abs.gov.au/statistics/people/population/national-state-and-territory-population/latest-release )

-- No records appeared wildly exaggerated or improbable, and thus were deemed appropriate for staging.




-------------------------------------------------------------------#
-- Staging | This model is used to stage the property-associated data from the raw schema.
-------------------------------------------------------------------#



-- Step 0: Define the staging model for property-associated data.
----------------------------------------------------------------------#

{{
    config(
        materialized='view'
    )
}}



-- Step 1: Omit duplicates.
----------------------------------------------------------------------#
WITH deduplicated_listings AS (
    SELECT DISTINCT *
    FROM raw.property_snapshot
),



-- Step 2: Handle invalid numeric values and flexible date formats.
----------------------------------------------------------------------#
listings_filtered_invalid_numerics AS (
    SELECT 
        listing_id, 
        TO_DATE(scraped_date, 'YYYY-MM-DD') AS scraped_date, 
        listing_neighbourhood, 
        property_type, 
        ROOM_TYPE, 
        accommodates
    FROM deduplicated_listings
)



-- Step 3: Select the final result.
----------------------------------------------------------------------#
SELECT 
    listing_id, 
    scraped_date, 
    listing_neighbourhood, 
    property_type, 
    ROOM_TYPE, 
    accommodates
FROM listings_filtered_invalid_numerics

